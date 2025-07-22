-- Visualizar porcentajes de tipos de rutas
WITH route_types(route_type, name) AS (
	SELECT '0', 'streetcar' UNION
	SELECT '1', 'subway' UNION
	SELECT '2', 'rail' UNION
	SELECT '3', 'bus' UNION
	SELECT '4', 'ferry' UNION
	SELECT '5', 'cable tram' UNION
	SELECT '6', 'aerial' UNION
	SELECT '7', 'funicular' UNION
	SELECT '11', 'trolley' UNION
	SELECT '12', 'monorrail'
),
route_groups AS 
(
	SELECT 
	  route_type,
	  COUNT(*) AS qty,
	  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS perc
	FROM routes
	GROUP BY route_type
)
SELECT name, qty, perc 
FROM route_groups g JOIN route_types t ON g.route_type = t.route_type
ORDER BY perc DESC;


-- Visualizar trips que entran y salen al poligono (multiline)
DROP MATERIALIZED VIEW IF EXISTS MultilineShapeTrips;
CREATE MATERIALIZED VIEW MultilineShapeTrips AS
SELECT ROW_NUMBER() OVER () as id, trip_id, ST_GeometryType(ST_Intersection(traj, ST_Transform(geom, 5514))), ST_Difference(
	  traj,
      ST_Transform(geom, 5514)
  )
  AS geom_part
  FROM trips_mdb, province
  WHERE ST_Intersects(traj, ST_Transform(geom, 5514)) AND ST_GeometryType(ST_Intersection(traj, ST_Transform(geom, 5514))) = 'ST_MultiLineString';


-- Agregacion de rutas por segmento
DROP MATERIALIZED VIEW IF EXISTS SegmentsDisplay;
CREATE MATERIALIZED VIEW SegmentsDisplay AS
SELECT
    c.from_stop_id || c.to_stop_id as id,
    s.seg_geom,
    COUNT(DISTINCT c.route_id) AS num_routes
FROM
    trip_segs s,
    connections c,
    trips t
WHERE
    t.trip_id = s.trip_id
    AND s.route_id = c.route_id
    AND t.direction_id = c.direction_id
    AND s.stop1_sequence = c.from_stop_sequence
    AND s.stop2_sequence = c.to_stop_sequence
    AND date = '2025-07-08'
    AND route_type = '3'
    AND stop1_arrival_time BETWEEN '15:00:00' AND '17:00:00'
GROUP BY
    c.from_stop_id,
    c.from_stop_name,
    c.to_stop_id,
    c.to_stop_name,
    s.seg_geom;

 -- Clippear raster
CREATE TABLE prague_pop AS (
    WITH clipgeo AS (
        SELECT ST_Transform(geom, 4326) AS provincegeo
        FROM province LIMIT 1
    )
    SELECT ST_Clip(a.rast::raster, 1, c.provincegeo, true) AS st_clip
    FROM population a, clipgeo c
);

ALTER TABLE prague_pop RENAME COLUMN st_clip TO rast;

-- Creacion de indices para generar grilla
CREATE INDEX IF NOT EXISTS idx_province_geom ON province USING GIST (ST_Transform(geom, 5514));
CREATE INDEX IF NOT EXISTS idx_trips_mdb_traj ON trips_mdb USING GIST (traj);


-- Generacion de grilla de 1kmx1km
DROP TABLE IF EXISTS province_grid CASCADE;
CREATE TABLE province_grid AS
SELECT
  row_number() OVER () AS id,
  (ST_SquareGrid(
      1000,
      ST_Transform(geom, 5514)
   )).geom
FROM province;

DROP TABLE IF EXISTS province_grid_clipped CASCADE;
CREATE TABLE province_grid_clipped AS
SELECT
  g.id,
  ST_Intersection(g.geom, ST_Transform(p.geom, 5514)) AS geom
FROM province_grid g
JOIN province p
  ON ST_Intersects(g.geom, ST_Transform(p.geom, 5514))
WHERE NOT ST_IsEmpty(ST_Intersection(g.geom, ST_Transform(p.geom, 5514)));


-- Contar cantidad de trips por grilla
DROP TABLE IF EXISTS grid_trip_counts CASCADE;
CREATE TABLE grid_trip_counts AS
SELECT
  g.id AS grid_id,
  COUNT(DISTINCT t.trip_id) AS trips_count,
  g.geom
FROM province_grid_clipped g
LEFT JOIN trips_mdb t
  ON ST_Intersects(t.traj, g.geom)
GROUP BY g.id, g.geom
ORDER BY g.id;

-- Trajectorias desde el centro a cada shopping
DROP MATERIALIZED VIEW IF EXISTS trajectories_center_shopping;
CREATE MATERIALIZED VIEW trajectories_center_shopping AS
SELECT
    s.name,
    ST_SetSRID(ST_MakePoint(14.420917, 50.087008), 4326) AS center_geom,
    ST_Transform(s.geom, 4326) AS mall_geom,
    ST_DistanceSphere(
        ST_SetSRID(ST_MakePoint(14.420917, 50.087008), 4326),
        ST_Transform(s.geom, 4326)
    ) / 1000.0 AS distance_km,
    ST_MakeLine(
        ST_SetSRID(ST_MakePoint(14.420917, 50.087008), 4326),
        ST_Transform(s.geom, 4326)
    ) AS geom
FROM shopping_malls s
ORDER BY distance_km;

-- Identificar los trips cercanos a los shoppings
DROP TABLE IF EXISTS shopping_trip_intervals;
CREATE TABLE shopping_trip_intervals (
    shopping_name TEXT,
    interv TEXT,
    trips_nearby INTEGER
);

INSERT INTO shopping_trip_intervals (shopping_name, interv, trips_nearby)
WITH instants AS (
  SELECT 
    s.name AS shopping_name,
    t.trip_id,
    getTimestamp(unnest(instants(t.trip))) AS instant_time
  FROM shopping_malls s
  JOIN trips_mdb t ON ST_DWithin(s.geom, t.traj, 200)
), ranges AS (
  SELECT 
    shopping_name,
    trip_id,
    instant_time,
    (EXTRACT(HOUR FROM instant_time)::int / 2) * 2 AS range_start
  FROM instants
)
SELECT
  shopping_name,
  lpad(range_start::text, 2, '0') || ':00–' || lpad((range_start+1)::text, 2, '0') || ':59' AS interv,
  COUNT(DISTINCT trip_id) AS trips_nearby
FROM ranges
GROUP BY shopping_name, range_start
ORDER BY shopping_name, range_start;


-- Calcular velocidades promedio de los segmentos
DROP MATERIALIZED VIEW IF EXISTS schedule_speeds;
CREATE MATERIALIZED VIEW schedule_speeds AS
SELECT 
    route_id || stop1_sequence || stop2_sequence as id,
    AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) AS speed_kmh,
    seg_geom
FROM trip_segs s
WHERE stop2_arrival_time <> stop1_arrival_time
GROUP BY route_id, stop1_sequence, stop2_sequence, seg_geom;


SELECT COUNT(*) FROM trip_segs
WHERE stop2_arrival_time <> stop1_arrival_time;
-- 1281030

SELECT COUNT(*) FROM trip_segs
WHERE seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6 < 30
AND stop2_arrival_time <> stop1_arrival_time;
-- 821914 (el 60% de los segmentos tiene una velocidad de 30km/h o menos)

-- Calcular velocidades promedio de los segmentos mayores a 50km/h
DROP VIEW IF EXISTS trips_over_50kmh;
CREATE MATERIALIZED VIEW trips_over_50kmh AS
SELECT 
    ROW_NUMBER() OVER () AS id,
    AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) AS speed_kmh,
    seg_geom
FROM trip_segs s
JOIN routes USING (route_id)
WHERE stop2_arrival_time <> stop1_arrival_time AND route_type = '3'
GROUP BY route_id, stop1_sequence, stop2_sequence, seg_geom
HAVING AVG(seg_length / EXTRACT(EPOCH FROM (stop2_arrival_time - stop1_arrival_time)) * 3.6) > 50;

-- Ver el ejemplo de Pod Lochkovem / Do Pražského okruhu
SELECT * FROM trips_over_50kmh
WHERE ST_Intersects(ST_Transform(seg_geom, 5514), ST_Buffer(ST_Point(-749498.6,-1051660.6, 5514), 10)) 

-- Districts
DROP MATERIALIZED VIEW avg_speed_by_district_0_6;
CREATE MATERIALIZED VIEW avg_speed_by_district_0_6 AS
SELECT
  d.naz_sop AS district,
  d.geom AS district_geom,
  AVG(s.seg_length / EXTRACT(EPOCH FROM (s.stop2_arrival_time - s.stop1_arrival_time)) * 3.6) AS avg_speed_kmh
FROM trip_segs s
JOIN prague_districts d
  ON ST_Intersects(s.seg_geom, d.geom)
WHERE
  s.stop2_arrival_time <> s.stop1_arrival_time
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) >= 0
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) < 6
GROUP BY d.naz_sop, d.geom
ORDER BY d.naz_sop;

DROP MATERIALIZED VIEW IF EXISTS avg_speed_by_district_6_12;
CREATE MATERIALIZED VIEW avg_speed_by_district_6_12 AS
SELECT
  d.naz_sop AS district,
  d.geom AS district_geom,
  AVG(s.seg_length / EXTRACT(EPOCH FROM (s.stop2_arrival_time - s.stop1_arrival_time)) * 3.6) AS avg_speed_kmh,
  COUNT(*) AS segment_count
FROM trip_segs s
JOIN prague_districts d
  ON ST_Intersects(s.seg_geom, d.geom)
WHERE
  s.stop2_arrival_time <> s.stop1_arrival_time
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) >= 6
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) < 12
GROUP BY d.naz_sop, d.geom
ORDER BY d.naz_sop;

DROP MATERIALIZED VIEW IF EXISTS avg_speed_by_district_12_18;
CREATE MATERIALIZED VIEW avg_speed_by_district_12_18 AS
SELECT
  d.naz_sop AS district,
  d.geom AS district_geom,
  AVG(s.seg_length / EXTRACT(EPOCH FROM (s.stop2_arrival_time - s.stop1_arrival_time)) * 3.6) AS avg_speed_kmh,
  COUNT(*) AS segment_count
FROM trip_segs s
JOIN prague_districts d
  ON ST_Intersects(s.seg_geom, d.geom)
WHERE
  s.stop2_arrival_time <> s.stop1_arrival_time
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) >= 12
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) < 18
GROUP BY d.naz_sop, d.geom
ORDER BY d.naz_sop;

DROP MATERIALIZED VIEW IF EXISTS avg_speed_by_district_18_24;
CREATE MATERIALIZED VIEW avg_speed_by_district_18_24 AS
SELECT
  d.naz_sop AS district,
  d.geom AS district_geom,
  AVG(s.seg_length / EXTRACT(EPOCH FROM (s.stop2_arrival_time - s.stop1_arrival_time)) * 3.6) AS avg_speed_kmh,
  COUNT(*) AS segment_count
FROM trip_segs s
JOIN prague_districts d
  ON ST_Intersects(s.seg_geom, d.geom)
WHERE
  s.stop2_arrival_time <> s.stop1_arrival_time
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) >= 18
  AND EXTRACT(HOUR FROM s.stop1_arrival_time) < 24
GROUP BY d.naz_sop, d.geom
ORDER BY d.naz_sop;