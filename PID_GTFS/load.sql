-- ========================
-- GTFS Import Script - Prague Buses
-- ========================

-- ========================
-- Table definitions
-- ========================

CREATE TABLE IF NOT EXISTS agency (
    agency_id TEXT,
    agency_name TEXT,
    agency_url TEXT,
    agency_timezone TEXT,
    agency_lang TEXT,
    agency_phone TEXT
);

CREATE TABLE IF NOT EXISTS calendar_dates_raw (
    service_id TEXT,
    date TEXT,
    exception_type INTEGER
);

CREATE TABLE IF NOT EXISTS calendar_dates AS
SELECT
    service_id,
    TO_DATE(date, 'YYYYMMDD') AS date,
    exception_type
FROM calendar_dates_raw;

CREATE TABLE IF NOT EXISTS calendar_raw (
    service_id TEXT,
    monday INTEGER,
    tuesday INTEGER,
    wednesday INTEGER,
    thursday INTEGER,
    friday INTEGER,
    saturday INTEGER,
    sunday INTEGER,
    start_date TEXT,
    end_date TEXT
);

CREATE TABLE IF NOT EXISTS calendar AS
SELECT
    service_id,
    monday, tuesday, wednesday, thursday, friday, saturday, sunday,
    TO_DATE(start_date, 'YYYYMMDD') AS start_date,
    TO_DATE(end_date, 'YYYYMMDD') AS end_date
FROM calendar_raw;

CREATE TABLE IF NOT EXISTS fare_attributes (
    fare_id TEXT,
    price NUMERIC,
    currency_type TEXT,
    payment_method INTEGER,
    transfers INTEGER,
    agency_id TEXT,
    transfer_duration TEXT
);

CREATE TABLE IF NOT EXISTS fare_rules (
    fare_id TEXT,
    contains_id TEXT,
    route_id TEXT
);

CREATE TABLE IF NOT EXISTS feed_info_raw (
    feed_publisher_name TEXT,
    feed_publisher_url TEXT,
    feed_lang TEXT,
    feed_start_date TEXT,
    feed_end_date TEXT,
    feed_contact_email TEXT
);

CREATE TABLE IF NOT EXISTS feed_info AS
SELECT
    feed_publisher_name,
    feed_publisher_url,
    feed_lang,
    TO_DATE(feed_start_date, 'YYYYMMDD') AS feed_start_date,
    TO_DATE(feed_end_date, 'YYYYMMDD') AS feed_end_date,
    feed_contact_email
FROM feed_info_raw;

CREATE TABLE IF NOT EXISTS levels (
    level_id TEXT,
    level_index DOUBLE PRECISION,
    level_name TEXT
);

CREATE TABLE IF NOT EXISTS pathways (
    pathway_id TEXT,
    from_stop_id TEXT,
    to_stop_id TEXT,
    pathway_mode INTEGER,
    is_bidirectional INTEGER,
    traversal_time INTEGER,
    signposted_as TEXT,
    reversed_signposted_as TEXT,
    bikes_prohibited INTEGER
);

CREATE TABLE IF NOT EXISTS route_stops (
    route_id TEXT,
    direction_id INTEGER,
    stop_id TEXT,
    stop_sequence INTEGER
);

CREATE TABLE IF NOT EXISTS routes (
    route_id TEXT,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER,
    route_url TEXT,
    route_color TEXT,
    route_text_color TEXT,
    is_night BOOLEAN,
    is_regional BOOLEAN,
    is_substitute_transport BOOLEAN
);

CREATE TABLE IF NOT EXISTS route_sub_agencies (
    route_id TEXT,
    route_licence_number TEXT,
    sub_agency_id TEXT,
    sub_agency_name TEXT
);

CREATE TABLE IF NOT EXISTS shapes (
    shape_id TEXT,
    shape_pt_lat DOUBLE PRECISION,
    shape_pt_lon DOUBLE PRECISION,
    shape_pt_sequence INTEGER,
    shape_dist_traveled DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS stops (
    stop_id TEXT,
    stop_name TEXT,
    stop_lat DOUBLE PRECISION,
    stop_lon DOUBLE PRECISION,
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER,
    parent_station TEXT,
    wheelchair_boarding INTEGER,
    level_id TEXT,
    platform_code TEXT,
    asw_node_id TEXT,
    asw_stop_id TEXT,
    zone_region_type TEXT
);

CREATE TABLE IF NOT EXISTS stop_times_raw (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    stop_headsign TEXT,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled DOUBLE PRECISION,
    trip_operation_type INTEGER,
    bikes_allowed INTEGER
);

CREATE TABLE IF NOT EXISTS stop_times AS
SELECT
    trip_id,
    arrival_time::TIME,
    departure_time::TIME,
    stop_id,
    stop_sequence,
    stop_headsign,
    pickup_type,
    drop_off_type,
    shape_dist_traveled,
    trip_operation_type,
    bikes_allowed
FROM stop_times_raw;

CREATE TABLE IF NOT EXISTS transfers (
    from_stop_id TEXT,
    to_stop_id TEXT,
    transfer_type INTEGER,
    min_transfer_time INTEGER,
    from_trip_id TEXT,
    to_trip_id TEXT,
    max_waiting_time INTEGER
);

CREATE TABLE IF NOT EXISTS trips (
    route_id TEXT,
    service_id TEXT,
    trip_id TEXT,
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER,
    exceptional INTEGER,
    sub_agency_id TEXT
);

CREATE TABLE IF NOT EXISTS vehicle_allocations (
    route_id TEXT,
    vehicle_category_id TEXT
);

CREATE TABLE IF NOT EXISTS vehicle_boardings (
    vehicle_category_id TEXT,
    child_sequence INTEGER,
    boarding_area_id TEXT
);

CREATE TABLE IF NOT EXISTS vehicle_categories (
    vehicle_category_id TEXT
);

CREATE TABLE IF NOT EXISTS vehicle_couplings (
    parent_id TEXT,
    child_id TEXT,
    child_sequence INTEGER
);

-- ========================
-- COPY commands
-- ========================
\copy agency FROM '/var/lib/postgresql/PID_GTFS/agency.txt' WITH (FORMAT csv, HEADER true);
\copy calendar_dates_raw FROM '/var/lib/postgresql/PID_GTFS/calendar_dates.txt' WITH (FORMAT csv, HEADER true);
\copy calendar_raw FROM '/var/lib/postgresql/PID_GTFS/calendar.txt' WITH (FORMAT csv, HEADER true);
\copy fare_attributes FROM '/var/lib/postgresql/PID_GTFS/fare_attributes.txt' WITH (FORMAT csv, HEADER true);
\copy fare_rules FROM '/var/lib/postgresql/PID_GTFS/fare_rules.txt' WITH (FORMAT csv, HEADER true);
\copy feed_info_raw FROM '/var/lib/postgresql/PID_GTFS/feed_info.txt' WITH (FORMAT csv, HEADER true);
\copy levels FROM '/var/lib/postgresql/PID_GTFS/levels.txt' WITH (FORMAT csv, HEADER true);
\copy pathways FROM '/var/lib/postgresql/PID_GTFS/pathways.txt' WITH (FORMAT csv, HEADER true);
\copy route_stops FROM '/var/lib/postgresql/PID_GTFS/route_stops.txt' WITH (FORMAT csv, HEADER true);
\copy routes FROM '/var/lib/postgresql/PID_GTFS/routes.txt' WITH (FORMAT csv, HEADER true);
\copy route_sub_agencies FROM '/var/lib/postgresql/PID_GTFS/route_sub_agencies.txt' WITH (FORMAT csv, HEADER true);
\copy shapes FROM '/var/lib/postgresql/PID_GTFS/shapes.txt' WITH (FORMAT csv, HEADER true);
\copy stops FROM '/var/lib/postgresql/PID_GTFS/stops.txt' WITH (FORMAT csv, HEADER true);
\copy stop_times_raw FROM '/var/lib/postgresql/PID_GTFS/stop_times.txt' WITH (FORMAT csv, HEADER true);
\copy transfers FROM '/var/lib/postgresql/PID_GTFS/transfers.txt' WITH (FORMAT csv, HEADER true);
\copy trips FROM '/var/lib/postgresql/PID_GTFS/trips.txt' WITH (FORMAT csv, HEADER true);
\copy vehicle_allocations FROM '/var/lib/postgresql/PID_GTFS/vehicle_allocations.txt' WITH (FORMAT csv, HEADER true);
\copy vehicle_boardings FROM '/var/lib/postgresql/PID_GTFS/vehicle_boardings.txt' WITH (FORMAT csv, HEADER true);
\copy vehicle_categories FROM '/var/lib/postgresql/PID_GTFS/vehicle_categories.txt' WITH (FORMAT csv, HEADER true);
\copy vehicle_couplings FROM '/var/lib/postgresql/PID_GTFS/vehicle_couplings.txt' WITH (FORMAT csv, HEADER true);
