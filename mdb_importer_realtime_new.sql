DO $$
BEGIN

DROP TABLE IF EXISTS realtime_positions;
CREATE TABLE realtime_positions (
  vehicle_id text,
  trip_id text,
  route_id text,
  latitude float,
  longitude float,
  startdate date,
  timestamp timestamptz
);

DROP TABLE IF EXISTS realtime_shapes;
CREATE TABLE realtime_shapes (
  vehicle_id text,
  trip_id text,
  route_id text,
  geometry geometry(LineString, 4326)
);

COPY realtime_positions (
  vehicle_id,
  trip_id,
  route_id,
  latitude,
  longitude,
  startdate,
  timestamp
) 
FROM '/var/lib/postgresql/map_matched_positions.csv' DELIMITER ',' CSV HEADER;

COPY realtime_shapes (
  vehicle_id,
  trip_id,
  route_id,
  geometry
)
FROM '/var/lib/postgresql/map_matched_shapes.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS matched_points;
CREATE TEMP TABLE matched_points AS
SELECT 
    rp.trip_id,
    rp.route_id,
    rp.vehicle_id,
    rp.startdate,
    ST_SetSRID(ST_MakePoint(rp.longitude, rp.latitude), 4326) AS point_geom,
    rp.timestamp,
    ST_LineLocatePoint(rs.geometry, ST_SetSRID(ST_MakePoint(rp.longitude, rp.latitude), 4326)) AS fraction
FROM realtime_positions rp
JOIN realtime_shapes rs USING (trip_id, route_id, vehicle_id)
ORDER BY rp.trip_id, rp.route_id, rp.vehicle_id, rp.startdate, rp.timestamp;

-- Extract all shape points with their fractional positions
DROP TABLE IF EXISTS all_shape_points;
CREATE TEMP TABLE all_shape_points AS
SELECT 
    rs.trip_id,
    rs.route_id,
    rs.vehicle_id,
    (dp).path[1] AS point_idx,
    (dp).geom AS point_geom,
    ST_LineLocatePoint(rs.geometry, (dp).geom) AS fraction
FROM realtime_shapes rs
JOIN LATERAL ST_DumpPoints(rs.geometry) AS dp ON true;

-- Create a numbered sequence of matched points for each trip
DROP TABLE IF EXISTS numbered_matched_points;
CREATE TEMP TABLE numbered_matched_points AS
SELECT 
    mp.*,
    ROW_NUMBER() OVER (PARTITION BY trip_id, route_id, vehicle_id, startdate ORDER BY timestamp) AS point_num
FROM matched_points mp;

-- Create segments between consecutive matched points
DROP TABLE IF EXISTS segments;
CREATE TEMP TABLE segments AS
SELECT 
    n1.trip_id,
    n1.route_id,
    n1.vehicle_id,
    n1.startdate,
    n1.point_geom AS start_point,
    n2.point_geom AS end_point,
    n1.fraction AS start_frac,
    n2.fraction AS end_frac,
    n1.timestamp AS start_time,
    n2.timestamp AS end_time,
    n1.point_num AS segment_num
FROM numbered_matched_points n1
JOIN numbered_matched_points n2 
    ON n1.trip_id = n2.trip_id 
    AND n1.route_id = n2.route_id 
    AND n1.vehicle_id = n2.vehicle_id 
    AND n1.startdate = n2.startdate
    AND n1.point_num = n2.point_num - 1;

-- Join shape points with segments to interpolate times
DROP TABLE IF EXISTS shape_points_with_segments;
CREATE TEMP TABLE shape_points_with_segments AS
SELECT 
    sp.trip_id,
    sp.route_id,
    sp.vehicle_id,
    sp.point_geom,
    sp.fraction AS shape_frac,
    s.segment_num,
    s.start_frac,
    s.end_frac,
    s.start_time,
    s.end_time,
    CASE 
        WHEN s.start_frac = s.end_frac THEN 0
        WHEN s.end_frac > s.start_frac THEN 
            (sp.fraction - s.start_frac) / (s.end_frac - s.start_frac)
        ELSE 
            (s.start_frac - sp.fraction) / (s.start_frac - s.end_frac)
    END AS interpolation_factor
FROM all_shape_points sp
JOIN segments s 
    ON sp.trip_id = s.trip_id 
    AND sp.route_id = s.route_id 
    AND sp.vehicle_id = s.vehicle_id
WHERE sp.fraction BETWEEN 
    LEAST(s.start_frac, s.end_frac) AND GREATEST(s.start_frac, s.end_frac);

-- Calculate interpolated times for shape points
DROP TABLE IF EXISTS interpolated_shape_points;
CREATE TABLE interpolated_shape_points AS
SELECT 
    trip_id,
    route_id,
    vehicle_id,
    point_geom,
    start_time + (end_time - start_time) * interpolation_factor AS interpolated_time
FROM shape_points_with_segments
WHERE interpolation_factor BETWEEN 0 AND 1;

-- Combine original matched points with interpolated shape points
DROP TABLE IF EXISTS all_timed_points;
CREATE TEMP TABLE all_timed_points AS
SELECT 
    trip_id,
    route_id,
    vehicle_id,
    startdate,
    point_geom,
    timestamp AS time
FROM matched_points
UNION ALL
SELECT 
    isp.trip_id,
    isp.route_id,
    isp.vehicle_id,
    mp.startdate,
    isp.point_geom,
    isp.interpolated_time AS time
FROM interpolated_shape_points isp
JOIN matched_points mp 
    ON isp.trip_id = mp.trip_id 
    AND isp.route_id = mp.route_id 
    AND isp.vehicle_id = mp.vehicle_id
GROUP BY 
    isp.trip_id,
    isp.route_id,
    isp.vehicle_id,
    mp.startdate,
    isp.point_geom,
    isp.interpolated_time;

-- Validate temporal ordering and remove duplicates
DROP TABLE IF EXISTS valid_timed_points;
CREATE TEMP TABLE valid_timed_points AS
WITH ordered_points AS (
    SELECT 
        *,
        LAG(time) OVER (PARTITION BY trip_id, route_id, vehicle_id, startdate ORDER BY time) AS prev_time,
        LAG(point_geom) OVER (PARTITION BY trip_id, route_id, vehicle_id, startdate ORDER BY time) AS prev_geom
    FROM all_timed_points
)
SELECT 
    trip_id,
    route_id,
    vehicle_id,
    startdate,
    point_geom,
    time
FROM ordered_points
WHERE 
    prev_time IS NULL 
    OR (
        time > prev_time 
        AND NOT ST_Equals(point_geom, prev_geom)
    );

-- Create the final trajectories
DROP TABLE IF EXISTS realtime_trips_mdb;
CREATE TABLE realtime_trips_mdb (
    trip_id text NOT NULL,
    route_id text NOT NULL,
    startdate date NOT NULL,
    trip tgeompoint,
    traj geometry,
    starttime timestamptz,
    PRIMARY KEY (trip_id, startdate)
);

-- Insert valid trajectories
INSERT INTO realtime_trips_mdb (trip_id, route_id, startdate, trip, traj, starttime)
SELECT 
    trip_id,
    route_id,
    startdate,
    transform(tgeompointseq(array_agg(tgeompoint(point_geom, time) ORDER BY time)), 5514) AS trip,
    ST_MakeLine(point_geom ORDER BY time) AS traj,
    MIN(time) AS starttime
FROM valid_timed_points
GROUP BY trip_id, route_id, startdate
HAVING COUNT(*) > 1 AND ST_IsValid(ST_MakeLine(point_geom ORDER BY time));

-- Remove any invalid trajectories
DELETE FROM realtime_trips_mdb
WHERE ST_GeometryType(traj) = 'ST_Point' OR NOT ST_IsSimple(traj);
					   
END; 
$$;