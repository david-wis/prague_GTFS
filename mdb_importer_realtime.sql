CREATE EXTENSION MOBILITYDB;
DROP TABLE IF EXISTS positions CASCADE;

CREATE TABLE IF NOT EXISTS positions (
  vehicle_id text NOT NULL,
  trip_id text,
  route_id text,
  latitude float NOT NULL,
  longitude float NOT NULL,
  bearing float,
  current_stop_sequence int,
  startdate text,
  starttime text,
  timestamp bigint NOT NULL
);

COPY positions(
  vehicle_id,
  trip_id,
  route_id,
  latitude,
  longitude,
  bearing,
  current_stop_sequence,
  startdate,
  starttime,
  timestamp
) FROM '/tmp/vehicle_positions.csv' DELIMITER ',' CSV HEADER;

DELETE FROM positions a
USING positions b
WHERE
  a.ctid > b.ctid -- Identificador interno de Postgres
  AND a.trip_id = b.trip_id
  AND a.vehicle_id = b.vehicle_id
  AND a.timestamp = b.timestamp;

ALTER TABLE positions
ADD CONSTRAINT positions_pk PRIMARY KEY (trip_id, vehicle_id, timestamp);


-- SELECT 
--   trip_id, 
--   vehicle_id, 
--   timestamp, 
--   COUNT(*) AS total_rows,
--   ARRAY_LENGTH(ARRAY_AGG(DISTINCT ST_AsText(ST_MakePoint(longitude, latitude))), 1) AS num_distinct_coords
-- FROM positions
-- GROUP BY trip_id, vehicle_id, timestamp;
-- HAVING COUNT(*) = 56;


DO $$ BEGIN RAISE NOTICE '...Altering positions'; END; $$;

ALTER TABLE positions ADD COLUMN point geometry;
UPDATE positions
SET point = ST_SetSRID(ST_Point(longitude, latitude, 4326),5514);

DO $$ BEGIN RAISE NOTICE '...Creating trip_mdb'; END; $$;

DROP TABLE IF EXISTS trips_mdbrt;
CREATE TABLE trips_mdbrt (
    trip_id text NOT NULL,
    vehicle_id text NOT NULL,
    startdate text,
    starttime text,
    starttimefull timestamp,
    trip tgeompoint,
    PRIMARY KEY (trip_id, vehicle_id, startdate, starttime)
);

DO $$ BEGIN RAISE NOTICE '...Inserting trip_mdb'; END; $$;

INSERT INTO trips_mdbrt(
    trip_id,
    vehicle_id,
    startdate,
    starttime,
    trip)
SELECT trip_id, vehicle_id, startdate, starttime, tgeompointseq(array_agg(tgeompoint(point, (to_timestamp(timestamp) at time zone 'Europe/Prague')) ORDER BY timestamp))
FROM positions
WHERE startdate IS NOT NULL
GROUP BY trip_id, vehicle_id, starttime, startdate;

UPDATE trips_mdbrt
SET starttimefull = TO_TIMESTAMP(CONCAT(startdate, ' ',starttime),'YYYYMMDD HH24:MI:SS') 
WHERE startdate != '' AND starttime < '24:00:00';

DO $$ BEGIN RAISE NOTICE '...Updating trip_mdb'; END; $$;

ALTER TABLE trips_mdbrt ADD COLUMN traj geometry;
UPDATE trips_mdbrt
SET traj = trajectory(trip);
