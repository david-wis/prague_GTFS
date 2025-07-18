-- Tiempos de demora por cada trip
DROP TABLE IF EXISTS trip_stops_rt;
CREATE TABLE trip_stops_rt AS
WITH Temp AS (
    SELECT
        t.trip_id AS actual_trip_id,
        ad.trip_id AS schedule_trip_id,
        shape_id,
        s.stop_id,
        s.stop_name,
        ad.stop_sequence,
        s.stop_loc::geometry,
        t_arrival AS schedule_time,
        nearestApproachInstant(t.trip, s.stop_loc) AS stop_instant,
				nearestApproachDistance(t.trip, s.stop_loc) as nearest_distance
    FROM
        realtime_trips_mdb t,
        arrivals_departures ad,
        stops s
    WHERE
        ad.date = '2025-07-11'::timestamp
				AND t.startdate::date = '2025-07-11' 
        AND ad.stop_id = s.stop_id
        AND t.trip_id=ad.trip_id
        AND nearestApproachDistance(t.trip, s.stop_loc) < 10
)
SELECT
    actual_trip_id,
    schedule_trip_id,
    shape_id,
    stop_id,
    stop_name,
    stop_sequence,
    stop_loc,
    schedule_time,
    getTimestamp(stop_instant) AS actual_time,
	age(getTimestamp(stop_instant), schedule_time),
	nearest_distance,
    ST_Transform(getValue(stop_instant), 4326) AS trip_geom
FROM Temp;

-- Verificar la cantidad de viajes con demoras
SELECT 
  COUNT(*)
FROM 
  realtime_trips_mdb r
  JOIN trip_stops_rt t ON t.actual_trip_ID = r.trip_id
  JOIN routes ro ON ro.route_short_name = r.route_id AND ro.route_type = '3'
WHERE 
  ABS(EXTRACT(EPOCH FROM t.age)) > 600;

-- Crear segmentos para real time
DROP TABLE IF EXISTS trip_segments_rt;
CREATE TABLE trip_segments_rt AS
SELECT 
    actual_trip_id,
    schedule_trip_id,
    shape_id,
    stop_id AS end_stop_id,
    schedule_time AS end_time_schedule,
    actual_time AS end_time_actual,
    LAG(stop_id) OVER (
        PARTITION BY actual_trip_id 
        ORDER BY stop_sequence
    ) AS start_stop_id,
    LAG(schedule_time) OVER (
        PARTITION BY actual_trip_id 
        ORDER BY stop_sequence
    ) AS start_time_schedule,
    LAG(actual_time) OVER (
        PARTITION BY actual_trip_id 
        ORDER BY stop_sequence
    ) AS start_time_actual
FROM 
    trip_stops_rt;


DROP MATERIALIZED VIEW IF EXISTS trip_speeds_diffs;
CREATE MATERIALIZED VIEW trip_speeds_diffs AS
SELECT 
    t.start_stop_id || t.end_stop_id AS id,
    AVG(
        s.seg_length / EXTRACT(EPOCH FROM (s.stop2_arrival_time - s.stop1_arrival_time)) * 3.6
    ) AS speed_kmh,
    AVG(
        s.seg_length / EXTRACT(EPOCH FROM (t.end_time_actual - t.start_time_actual)) * 3.6
    ) AS speed_kmh_actual,
    AVG(
        s.seg_length / EXTRACT(EPOCH FROM (t.end_time_actual - t.start_time_actual)) * 3.6
    ) - AVG(
        s.seg_length / EXTRACT(EPOCH FROM (s.stop2_arrival_time - s.stop1_arrival_time)) * 3.6
    ) AS diff,
    s.seg_geom
FROM 
    trip_segments_rt t,
    trip_segs s
WHERE 
    t.start_stop_id = s.stop1_id
    AND t.end_stop_id = s.stop2_id
    AND t.shape_id = s.shape_id
    AND t.start_time_actual IS NOT NULL
    AND EXTRACT(EPOCH FROM (t.end_time_actual - t.start_time_actual)) > 0
    AND EXTRACT(EPOCH FROM (s.stop2_arrival_time - s.stop1_arrival_time)) > 0
    AND s.seg_geom IS NOT NULL
GROUP BY 
    s.seg_geom,
    t.start_stop_id,
    t.end_stop_id;