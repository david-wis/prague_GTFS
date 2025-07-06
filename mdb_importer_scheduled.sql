-- Inspired in: https://github.dev/pabloito/MDB-Importer

DROP VIEW IF EXISTS service_dates;
CREATE VIEW service_dates AS (
	SELECT service_id, date_trunc('day', d)::date AS date
	FROM calendar c, generate_series(start_date, end_date, '1 day'::interval) AS d
	WHERE (
		(monday = 'available' AND extract(isodow FROM d) = 1) OR
		(tuesday = 'available' AND extract(isodow FROM d) = 2) OR
		(wednesday = 'available' AND extract(isodow FROM d) = 3) OR
		(thursday = 'available' AND extract(isodow FROM d) = 4) OR
		(friday = 'available' AND extract(isodow FROM d) = 5) OR
		(saturday = 'available' AND extract(isodow FROM d) = 6) OR
		(sunday = 'available' AND extract(isodow FROM d) = 7)
	)
	EXCEPT
	SELECT service_id, date
	FROM calendar_dates WHERE exception_type = 'removed'
	UNION
	SELECT c.service_id, date
	FROM calendar c JOIN calendar_dates d ON c.service_id = d.service_id
	WHERE exception_type = 'added' AND start_date <= date AND date <= end_date
);

-- Crear trip_stops
DROP TABLE IF EXISTS trip_stops;
CREATE TABLE trip_stops (
  trip_id text,
  stop_sequence integer,
  num_stops integer,
  route_id text,
  service_id text,
  shape_id text,
  stop_id text,
  arrival_time interval,
  perc float
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting trip_stops';
  INSERT INTO trip_stops (trip_id, stop_sequence, num_stops, route_id, service_id, shape_id, stop_id, arrival_time)
  SELECT t.trip_id, stop_sequence,
         MAX(stop_sequence) OVER (PARTITION BY t.trip_id),
         route_id, service_id, t.shape_id, st.stop_id, arrival_time
  FROM trips t JOIN stop_times st ON t.trip_id = st.trip_id;
END;
$$;

DO $$
BEGIN
  RAISE NOTICE '...Updating trip_stops';
  UPDATE trip_stops t
  SET perc = CASE
    WHEN stop_sequence =  1 THEN 0::float
    WHEN stop_sequence =  num_stops THEN 1.0::float
    ELSE ST_LineLocatePoint(g.traj, s.stop_loc)
  END
  FROM trajectories g, stops s
  WHERE t.shape_id = g.shape_id
    AND t.stop_id = s.stop_id;
END;
$$;

-- Crear trip_segs
DROP TABLE IF EXISTS trip_segs;
CREATE TABLE trip_segs (
  trip_id text,
  route_id text,
  service_id text,
  stop1_sequence integer,
  stop2_sequence integer,
  num_stops integer,
  shape_id text,
  stop1_arrival_time interval,
  stop2_arrival_time interval,
  perc1 float,
  perc2 float,
  seg_geom geometry,
  seg_length float,
  no_points integer,
  PRIMARY KEY (trip_id, stop1_sequence)
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting trip_segs';
  INSERT INTO trip_segs (trip_id, route_id, service_id, stop1_sequence, stop2_sequence,
                         num_stops, shape_id, stop1_arrival_time, stop2_arrival_time, perc1, perc2)
  WITH temp AS (
    SELECT t.trip_id, t.route_id, t.service_id, t.stop_sequence,
           LEAD(stop_sequence) OVER w AS stop_sequence2,
           MAX(stop_sequence) OVER (PARTITION BY trip_id),
           t.shape_id, t.arrival_time, LEAD(arrival_time) OVER w,
           t.perc, LEAD(perc) OVER w
    FROM trip_stops t
    WINDOW w AS (PARTITION BY trip_id ORDER BY stop_sequence)
  )
  SELECT * FROM temp WHERE stop_sequence2 IS NOT null;
END;
$$;

DO $$
BEGIN
  RAISE NOTICE '...Updating trip_segs';
  UPDATE trip_segs t
  SET seg_geom = CASE
    WHEN perc1 > perc2 THEN seg_geom
    ELSE ST_LineSubstring(g.traj, perc1, perc2)
  END
  FROM trajectories g
  WHERE t.shape_id = g.shape_id;
END;
$$;

DELETE FROM trip_segs
WHERE trip_id IN (
  SELECT trip_id
  FROM trip_segs
  WHERE seg_geom IS NULL
);
-- 5963 trips (7.8% aprox)

DO $$
BEGIN
  RAISE NOTICE '...Updating trip_segs 2';
  UPDATE trip_segs t
  SET seg_length = ST_Length(seg_geom), no_points = ST_NumPoints(seg_geom);
END;
$$;

-- Crear trip_points
DROP TABLE IF EXISTS trip_points;
CREATE TABLE trip_points (
  trip_id text,
  route_id text,
  service_id text,
  stop1_sequence integer,
  point_sequence integer,
  point_geom geometry,
  point_arrival_time interval,
  PRIMARY KEY (trip_id, stop1_sequence, point_sequence)
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting trip_points';
  INSERT INTO trip_points (trip_id, route_id, service_id, stop1_sequence,
                           point_sequence, point_geom, point_arrival_time)
  WITH temp1 AS (
    SELECT trip_id, route_id, service_id, stop1_sequence,
           stop2_sequence, num_stops, stop1_arrival_time, stop2_arrival_time, seg_length,
           (dp).path[1] AS point_sequence, no_points, (dp).geom as point_geom
    FROM trip_segs, ST_DumpPoints(seg_geom) AS dp
  ),
  temp2 AS (
    SELECT trip_id, route_id, service_id, stop1_sequence,
           stop1_arrival_time, stop2_arrival_time, seg_length,  point_sequence,
           no_points, point_geom
    FROM temp1
    WHERE point_sequence <> no_points OR stop2_sequence = num_stops
  ),
  temp3 AS (
    SELECT trip_id, route_id, service_id, stop1_sequence,
           stop1_arrival_time, stop2_arrival_time, point_sequence, no_points, point_geom,
           ST_Length(ST_MakeLine(array_agg(point_geom) OVER w)) / seg_length AS perc
    FROM temp2
    WINDOW w AS (PARTITION BY trip_id, service_id, stop1_sequence ORDER BY point_sequence)
  )
  SELECT trip_id, route_id, service_id, stop1_sequence,
         point_sequence, point_geom,
         CASE
           WHEN point_sequence = 1 THEN stop1_arrival_time
           WHEN point_sequence = no_points THEN stop2_arrival_time
           ELSE stop1_arrival_time + ((stop2_arrival_time - stop1_arrival_time) * perc)
         END AS point_arrival_time
  FROM temp3;
END;
$$;

DROP TABLE IF EXISTS trips_input;
CREATE TABLE trips_input (
  trip_id text,
  route_id text,
  service_id text,
  date date,
  point_geom geometry,
  t timestamptz
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting trip_input';
  INSERT INTO trips_input
  SELECT trip_id, route_id, t.service_id,
         date, point_geom, date + point_arrival_time AS t
  FROM trip_points t
  JOIN service_dates s ON t.service_id = s.service_id
  WHERE date BETWEEN '2025-06-29' AND '2025-07-01';
END;
$$;


DROP TABLE IF EXISTS trips_mdb;
CREATE TABLE trips_mdb (
  trip_id text NOT NULL,
  route_id text NOT NULL,
  date date NOT NULL,
  trip tgeompoint,
  PRIMARY KEY (trip_id, date)
);

DO $$
BEGIN
  RAISE NOTICE '...Inserting trip_mdb';
  WITH only_first_point_trips_input AS (
	  SELECT DISTINCT ON (trip_id, route_id, date, t) trip_id, route_id, date, t, point_geom 
	  FROM trips_input
  )
  INSERT INTO trips_mdb(trip_id, route_id, date, trip)
  SELECT trip_id, route_id, date, tgeompointseq(array_agg(tgeompoint(point_geom, t) ORDER BY t))
  FROM only_first_point_trips_input
  GROUP BY trip_id, route_id, date;
END;
$$;

ALTER TABLE trips_mdb ADD COLUMN traj geometry;
ALTER TABLE trips_mdb ADD COLUMN starttime timestamp;

DO $$
BEGIN
  RAISE NOTICE '...Updating trip_mdb';
  UPDATE trips_mdb SET traj = trajectory(trip);
  UPDATE trips_mdb SET starttime = startTimestamp(trip);
END;
$$;