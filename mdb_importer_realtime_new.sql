DROP TABLE IF EXISTS realtime_positions;
CREATE TABLE realtime_positions (
  vehicle_id text,
  trip_id text,
  route_id text,
  latitude float,
  longitude float,
  startdate date,
  timestamp bigint
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
FROM '/var/lib/postgresql/normalized_map_matched_positions.csv' DELIMITER ',' CSV HEADER;

COPY realtime_shapes (
  vehicle_id,
  trip_id,
  route_id,
  geometry
)
FROM '/var/lib/postgresql/map_matched_shapes.csv' DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS realtime_points;
CREATE TABLE realtime_points (
  trip_id text,
  route_id text,
  vehicle_id text,
  startdate date,
  point_geom geometry(Point, 4326),
  t bigint
);

DO $$
BEGIN
  RAISE NOTICE '...Generating interpolated realtime points on shape';

  DROP TABLE IF EXISTS trip_times;
  CREATE TEMP TABLE trip_times AS
  SELECT trip_id, route_id, vehicle_id, startdate,
         MIN(timestamp) AS t_start,
         MAX(timestamp) AS t_end
  FROM realtime_positions
  GROUP BY trip_id, route_id, vehicle_id, startdate;

  DROP TABLE IF EXISTS densified_shapes;	
  CREATE TEMP TABLE densified_shapes AS
  SELECT 
    s.trip_id,
    s.route_id,
    s.vehicle_id,
    tt.startdate,
    ST_LineInterpolatePoint(s.geometry, gs.fraction) AS point_geom,
    gs.fraction,
    tt.t_start,
    tt.t_end
  FROM realtime_shapes s
  JOIN trip_times tt USING (trip_id)
  JOIN LATERAL (
    SELECT generate_series(0, 1, 0.005) AS fraction
  ) gs ON TRUE;

  INSERT INTO realtime_points (trip_id, route_id, vehicle_id, startdate, point_geom, t)
  SELECT 
    trip_id,
    route_id,
    vehicle_id,
    startdate,
    point_geom,
    FLOOR(t_start + (t_end - t_start) * fraction)::bigint AS t
  FROM densified_shapes;

END;
$$;

DROP TABLE IF EXISTS realtime_trips_mdb;
CREATE TABLE realtime_trips_mdb (
  trip_id text NOT NULL,
  route_id text NOT NULL,
  startdate date NOT NULL,
  trip tgeompoint,
  PRIMARY KEY (trip_id, startdate)
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting realtime_trips_mdb with deduplicated timestamps';

  INSERT INTO realtime_trips_mdb (trip_id, route_id, startdate, trip)
  SELECT trip_id, route_id, startdate,
         transform(tgeompointseq(array_agg(tgeompoint(point_geom, to_timestamp(t)) ORDER BY t)), 5514)
  FROM (
    SELECT DISTINCT ON (trip_id, startdate, t)
           trip_id, route_id, startdate, point_geom, t
    FROM realtime_points
    ORDER BY trip_id, startdate, t
  ) AS deduped
  GROUP BY trip_id, route_id, startdate;
END;
$$;

ALTER TABLE realtime_trips_mdb ADD COLUMN traj geometry;
ALTER TABLE realtime_trips_mdb ADD COLUMN starttime timestamptz;

DO $$
BEGIN
  UPDATE realtime_trips_mdb SET traj = trajectory(trip);
  UPDATE realtime_trips_mdb SET starttime = startTimestamp(trip);
END;
$$;