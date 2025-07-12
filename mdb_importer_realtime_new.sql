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

-- Load data from CSVs
RAISE NOTICE 'Loading realtime_positions...';
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

RAISE NOTICE 'Loading realtime_shapes...';
COPY realtime_shapes (
  vehicle_id,
  trip_id,
  route_id,
  geometry
)
FROM '/var/lib/postgresql/map_matched_shapes.csv' DELIMITER ',' CSV HEADER;

-- Extract shape points
RAISE NOTICE 'Extracting shape points...';
DROP TABLE IF EXISTS shape_points;
CREATE TEMP TABLE shape_points AS
SELECT
  rs.trip_id,
  rs.route_id,
  rs.vehicle_id,
  rp.startdate,
  (dp).path[1] AS point_idx,
  (dp).geom AS point_geom,
  ST_LineLocatePoint(rs.geometry, (dp).geom) AS fraction
FROM realtime_shapes rs
JOIN (
  SELECT trip_id, route_id, vehicle_id, ST_DumpPoints(geometry) AS dp
  FROM realtime_shapes
) dumped ON rs.trip_id = dumped.trip_id AND rs.route_id = dumped.route_id AND rs.vehicle_id = dumped.vehicle_id
JOIN (
  SELECT DISTINCT trip_id, route_id, vehicle_id, startdate
  FROM realtime_positions
) rp ON rp.trip_id = rs.trip_id AND rp.route_id = rs.route_id AND rp.vehicle_id = rs.vehicle_id;

-- Project real positions onto geometry
RAISE NOTICE 'Projecting real positions...';
DROP TABLE IF EXISTS projected_positions;
CREATE TEMP TABLE projected_positions AS
SELECT
  rp.trip_id,
  rp.route_id,
  rp.vehicle_id,
  rp.startdate,
  rp.timestamp,
  ST_LineLocatePoint(rs.geometry, ST_SetSRID(ST_MakePoint(rp.longitude, rp.latitude), 4326)) AS fraction,
  ST_LineInterpolatePoint(rs.geometry, ST_LineLocatePoint(rs.geometry, ST_SetSRID(ST_MakePoint(rp.longitude, rp.latitude), 4326))) AS point_geom
FROM realtime_positions rp
JOIN realtime_shapes rs USING (trip_id, route_id, vehicle_id);

-- Build segments from real positions
RAISE NOTICE 'Building segments...';
DROP TABLE IF EXISTS real_segments;
CREATE TABLE real_segments AS
SELECT
  trip_id,
  route_id,
  vehicle_id,
  startdate,
  fraction AS start_frac,
  LEAD(fraction) OVER w AS end_frac,
  timestamp AS start_ts,
  LEAD(timestamp) OVER w AS end_ts,
  point_geom
FROM projected_positions
WINDOW w AS (PARTITION BY trip_id, route_id, vehicle_id, startdate ORDER BY timestamp);

-- Interpolate times for shape points
RAISE NOTICE 'Interpolating shape point times...';
DROP TABLE IF EXISTS shape_points_timed;
CREATE TEMP TABLE shape_points_timed AS
SELECT
  sp.trip_id,
  sp.route_id,
  sp.vehicle_id,
  sp.startdate,
  sp.point_geom,
  rs.start_ts + make_interval(secs => EXTRACT(EPOCH FROM (rs.end_ts - rs.start_ts)) * 
                                      ((sp.fraction - rs.start_frac) / NULLIF(rs.end_frac - rs.start_frac, 0))) AS t
FROM shape_points sp
JOIN real_segments rs
  ON sp.trip_id = rs.trip_id
 AND sp.route_id = rs.route_id
 AND sp.vehicle_id = rs.vehicle_id
 AND sp.startdate = rs.startdate
 AND sp.fraction BETWEEN rs.start_frac AND rs.end_frac;

-- Combine projected and interpolated points
RAISE NOTICE 'Combining points...';
DROP TABLE IF EXISTS realtime_points;
CREATE TABLE realtime_points (
  trip_id text,
  route_id text,
  vehicle_id text,
  startdate date,
  point_geom geometry(Point, 4326),
  t timestamptz
);

INSERT INTO realtime_points
SELECT trip_id, route_id, vehicle_id, startdate, point_geom, t FROM shape_points_timed
UNION
SELECT trip_id, route_id, vehicle_id, startdate, point_geom, timestamp AS t FROM projected_positions;

-- Build tgeompoint trip sequences
RAISE NOTICE 'Building realtime_trips_mdb...';
DROP TABLE IF EXISTS realtime_trips_mdb;
CREATE TABLE realtime_trips_mdb (
  trip_id text NOT NULL,
  route_id text NOT NULL,
  startdate date NOT NULL,
  trip tgeompoint,
  PRIMARY KEY (trip_id, startdate)
);

WITH filtered_points AS (
  SELECT *,
         bool_and(point_geom IS NOT NULL AND t IS NOT NULL)
           OVER (PARTITION BY trip_id, startdate ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS valid_so_far
  FROM (
    SELECT DISTINCT ON (trip_id, startdate, t)
           trip_id, route_id, startdate, point_geom, t
    FROM realtime_points
    ORDER BY trip_id, startdate, t
  ) sub
)
INSERT INTO realtime_trips_mdb (trip_id, route_id, startdate, trip)
SELECT trip_id, route_id, startdate,
       transform(tgeompointseq(array_agg(tgeompoint(point_geom, t) ORDER BY t)), 5514)
FROM filtered_points
WHERE valid_so_far
GROUP BY trip_id, route_id, startdate;

-- Add trajectory and start time
RAISE NOTICE 'Adding trajectory and start time...';
ALTER TABLE realtime_trips_mdb ADD COLUMN traj geometry;
ALTER TABLE realtime_trips_mdb ADD COLUMN starttime timestamptz;

RAISE NOTICE 'Updating trajectory and start time...';
UPDATE realtime_trips_mdb SET traj = trajectory(trip);
UPDATE realtime_trips_mdb SET starttime = startTimestamp(trip);

RAISE NOTICE 'Removing point trajectory...';
DELETE FROM realtime_trips_mdb
WHERE ST_GeometryType(traj) = 'ST_Point';

END;
$$;