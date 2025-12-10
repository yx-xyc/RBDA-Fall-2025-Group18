-- ============================================
-- CREATE EXTERNAL TABLES FOR ALL DATASETS
-- ============================================
-- This script creates external tables for all processed datasets:
-- - MTA Station Metadata (dimension table)
-- - Weather (hourly)
-- - MTA Subway (hourly, by station/payment method)
-- - Crime (daily)
--
-- Prerequisites:
-- 1. MTA MapReduce pipeline must be run: cd preprocessing/mta_preprocessing && make run
--
-- Run: trino --catalog hive --schema yx2021_nyu_edu -f 1_create_external_tables.sql
-- ============================================

-- ============================================
-- MTA STATION METADATA (Dimension Table)
-- ============================================

DROP TABLE IF EXISTS mta_stations_csv;

CREATE TABLE mta_stations_csv (
    complex_id VARCHAR,
    is_complex VARCHAR,
    num_stations_in_complex VARCHAR,
    stop_name VARCHAR,
    display_name VARCHAR,
    constituent_station_names VARCHAR,
    station_ids VARCHAR,
    gtfs_stop_ids VARCHAR,
    borough VARCHAR,
    cbd VARCHAR,
    daytime_routes VARCHAR,
    structure_type VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    ada VARCHAR,
    ada_notes VARCHAR
)
WITH (
    format = 'CSV',
    skip_header_line_count = 1,
    external_location = 'hdfs:///user/yx2021_nyu_edu/project'
);

-- Convert to Parquet
DROP TABLE IF EXISTS mta_stations;

CREATE TABLE mta_stations
WITH (format = 'PARQUET')
AS
SELECT
    CAST(complex_id AS INTEGER) as complex_id,
    CASE WHEN LOWER(is_complex) = 'true' THEN 1 ELSE 0 END as is_complex,
    TRY_CAST(num_stations_in_complex AS INTEGER) as num_stations_in_complex,
    stop_name,
    display_name,
    borough,
    CASE WHEN LOWER(cbd) = 'true' THEN 1 ELSE 0 END as is_cbd,
    daytime_routes,
    structure_type,
    TRY_CAST(latitude AS DOUBLE) as latitude,
    TRY_CAST(longitude AS DOUBLE) as longitude,
    TRY_CAST(ada AS INTEGER) as ada_accessible,
    ada_notes
FROM mta_stations_csv
WHERE complex_id IS NOT NULL
    AND TRY_CAST(complex_id AS INTEGER) IS NOT NULL
    AND TRY_CAST(num_stations_in_complex AS INTEGER) IS NOT NULL
    AND TRY_CAST(latitude AS DOUBLE) IS NOT NULL
    AND TRY_CAST(longitude AS DOUBLE) IS NOT NULL;

SELECT 'MTA Stations' as dataset, COUNT(*) as records FROM mta_stations;

-- ============================================
-- WEATHER DATA (Hourly)
-- ============================================

DROP TABLE IF EXISTS weather_hourly_csv;

CREATE TABLE weather_hourly_csv (
    date_hour VARCHAR,
    avg_temp VARCHAR,
    avg_humidity VARCHAR,
    total_precip VARCHAR,
    total_snow VARCHAR,
    avg_snow_depth VARCHAR,
    avg_wind_speed VARCHAR,
    avg_visibility VARCHAR,
    dom_precip_type VARCHAR,
    dom_wind_dir VARCHAR,
    dom_conditions VARCHAR
)
WITH (
    format = 'CSV',
    skip_header_line_count = 0,
    external_location = 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/weather_hourly'
);

-- Convert to Parquet for efficient querying
DROP TABLE IF EXISTS weather_hourly;

CREATE TABLE weather_hourly
WITH (format = 'PARQUET')
AS
SELECT
    -- Parse truncated hour format: 2024-01-01T01 -> 2024-01-01 01:00:00
    -- Replace 'T' with space and append seconds: 2024-01-01 01:00:00
    CAST(REPLACE(date_hour, 'T', ' ') || ':00:00' AS TIMESTAMP) as weather_hour,
    CAST(avg_temp AS DOUBLE) as avg_temp_f,
    CAST(avg_humidity AS DOUBLE) as avg_humidity_pct,
    CAST(total_precip AS DOUBLE) as total_precip_inch,
    CAST(total_snow AS DOUBLE) as total_snow_inch,
    CAST(avg_snow_depth AS DOUBLE) as avg_snow_depth_inch,
    CAST(avg_wind_speed AS DOUBLE) as avg_wind_speed_mph,
    CAST(avg_visibility AS DOUBLE) as avg_visibility_miles,
    dom_precip_type,
    dom_wind_dir,
    dom_conditions,
    DATE(CAST(REPLACE(date_hour, 'T', ' ') || ':00:00' AS TIMESTAMP)) as weather_date
FROM weather_hourly_csv
WHERE date_hour IS NOT NULL
    AND TRY_CAST(REPLACE(date_hour, 'T', ' ') || ':00:00' AS TIMESTAMP) IS NOT NULL;

SELECT 'Weather' as dataset, COUNT(*) as records FROM weather_hourly;

-- ============================================
-- MTA SUBWAY DATA (Hourly by Station/Payment)
-- ============================================

DROP TABLE IF EXISTS mta_station_hourly_csv;

CREATE TABLE mta_station_hourly_csv (
    transit_timestamp VARCHAR,
    station_complex_id VARCHAR,
    payment_method VARCHAR,
    total_ridership VARCHAR
)
WITH (
    format = 'CSV',
    skip_header_line_count = 0,
    external_location = 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/station_hourly'
);

-- Convert to Parquet and aggregate citywide (across all stations)
DROP TABLE IF EXISTS mta_hourly_agg;

CREATE TABLE mta_hourly_agg
WITH (format = 'PARQUET')
AS
SELECT
    -- Parse space-separated timestamp: 2024-01-01 00:00:00
    DATE_TRUNC('hour', CAST(transit_timestamp AS TIMESTAMP)) as transit_hour,

    -- Total ridership across all stations
    SUM(CAST(total_ridership AS BIGINT)) as total_subway_ridership,

    -- Station count
    COUNT(DISTINCT station_complex_id) as active_stations,

    -- Payment method breakdown
    SUM(CASE WHEN LOWER(payment_method) = 'metrocard' THEN CAST(total_ridership AS BIGINT) ELSE 0 END) as metrocard_ridership,
    SUM(CASE WHEN LOWER(payment_method) = 'omny' THEN CAST(total_ridership AS BIGINT) ELSE 0 END) as omny_ridership,

    -- Payment method percentages
    ROUND(100.0 * SUM(CASE WHEN LOWER(payment_method) = 'omny' THEN CAST(total_ridership AS BIGINT) ELSE 0 END) /
          NULLIF(SUM(CAST(total_ridership AS BIGINT)), 0), 2) as omny_pct,

    DATE(CAST(transit_timestamp AS TIMESTAMP)) as transit_date
FROM mta_station_hourly_csv
WHERE transit_timestamp IS NOT NULL
    AND total_ridership IS NOT NULL
    AND TRY_CAST(transit_timestamp AS TIMESTAMP) IS NOT NULL
GROUP BY
    DATE_TRUNC('hour', CAST(transit_timestamp AS TIMESTAMP)),
    DATE(CAST(transit_timestamp AS TIMESTAMP));

SELECT 'MTA Aggregated' as dataset, COUNT(*) as records FROM mta_hourly_agg;

-- ============================================
-- MTA SUBWAY DATA WITH STATION METADATA
-- ============================================
-- This table joins ridership with station metadata for station-level analysis

DROP TABLE IF EXISTS mta_station_hourly;

CREATE TABLE mta_station_hourly
WITH (format = 'PARQUET')
AS
SELECT
    -- Time
    DATE_TRUNC('hour', CAST(r.transit_timestamp AS TIMESTAMP)) as transit_hour,
    DATE(CAST(r.transit_timestamp AS TIMESTAMP)) as transit_date,

    -- Station info
    TRY_CAST(r.station_complex_id AS INTEGER) as station_complex_id,
    s.stop_name,
    s.display_name,
    s.borough,
    s.is_cbd,
    s.daytime_routes,
    s.structure_type,
    s.latitude,
    s.longitude,
    s.ada_accessible,

    -- Ridership by payment method
    r.payment_method,
    CAST(r.total_ridership AS BIGINT) as ridership

FROM mta_station_hourly_csv r
LEFT JOIN mta_stations s
    ON TRY_CAST(r.station_complex_id AS INTEGER) = s.complex_id
WHERE r.transit_timestamp IS NOT NULL
    AND r.total_ridership IS NOT NULL
    AND TRY_CAST(r.transit_timestamp AS TIMESTAMP) IS NOT NULL
    AND TRY_CAST(r.station_complex_id AS INTEGER) IS NOT NULL;

SELECT 'MTA Station-Level' as dataset, COUNT(*) as records FROM mta_station_hourly;

-- ============================================
-- CRIME DATA (Daily)
-- ============================================
-- Note: Crime data uses PIPE delimiters: date|offense|borough,count,lat,lon

DROP TABLE IF EXISTS crime_daily_raw;

CREATE TABLE crime_daily_raw (
    raw_line VARCHAR
)
WITH (
    format = 'TEXTFILE',
    skip_header_line_count = 0,
    external_location = 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/crime_daily'
);

-- Parse and aggregate crime data
DROP TABLE IF EXISTS crime_daily_agg;

CREATE TABLE crime_daily_agg
WITH (format = 'PARQUET')
AS
SELECT
    -- Extract and parse date (before first pipe)
    DATE(CAST(SPLIT_PART(raw_line, '|', 1) || ':00:00' AS TIMESTAMP)) as crime_date,

    -- Total crime count
    SUM(CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)) as total_crimes,

    -- Crime type breakdown
    SUM(CASE
        WHEN UPPER(SPLIT_PART(raw_line, '|', 2)) LIKE '%ASSAULT%'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as assault_count,

    SUM(CASE
        WHEN UPPER(SPLIT_PART(raw_line, '|', 2)) LIKE '%LARCENY%'
             OR UPPER(SPLIT_PART(raw_line, '|', 2)) LIKE '%THEFT%'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as theft_count,

    SUM(CASE
        WHEN UPPER(SPLIT_PART(raw_line, '|', 2)) LIKE '%ROBBERY%'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as robbery_count,

    SUM(CASE
        WHEN UPPER(SPLIT_PART(raw_line, '|', 2)) LIKE '%BURGLARY%'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as burglary_count,

    SUM(CASE
        WHEN UPPER(SPLIT_PART(raw_line, '|', 2)) LIKE '%VEHICLE%'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as vehicle_crime_count,

    -- Borough breakdown (extract borough code from third segment, first part before comma)
    SUM(CASE
        WHEN SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 1) = 'M'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as manhattan_crimes,

    SUM(CASE
        WHEN SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 1) = 'K'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as brooklyn_crimes,

    SUM(CASE
        WHEN SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 1) = 'Q'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as queens_crimes,

    SUM(CASE
        WHEN SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 1) = 'X'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as bronx_crimes,

    SUM(CASE
        WHEN SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 1) = 'S'
        THEN CAST(SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 2) AS BIGINT)
        ELSE 0
    END) as staten_island_crimes,

    -- Unique crime types and boroughs
    COUNT(DISTINCT SPLIT_PART(raw_line, '|', 2)) as unique_offense_types,
    COUNT(DISTINCT SPLIT_PART(SPLIT_PART(raw_line, '|', 3), ',', 1)) as boroughs_with_crimes

FROM crime_daily_raw
WHERE raw_line IS NOT NULL
    AND LENGTH(raw_line) > 0
    AND TRY_CAST(SPLIT_PART(raw_line, '|', 1) || ':00:00' AS TIMESTAMP) IS NOT NULL
GROUP BY
    DATE(CAST(SPLIT_PART(raw_line, '|', 1) || ':00:00' AS TIMESTAMP));

SELECT 'Crime Daily' as dataset, COUNT(*) as records FROM crime_daily_agg;

-- ============================================
-- SUMMARY
-- ============================================

SELECT
    'EXTERNAL TABLES CREATED' as status,
    (SELECT COUNT(*) FROM mta_stations) as station_metadata_records,
    (SELECT COUNT(*) FROM weather_hourly) as weather_records,
    (SELECT COUNT(*) FROM mta_hourly_agg) as mta_citywide_records,
    (SELECT COUNT(*) FROM mta_station_hourly) as mta_station_records,
    (SELECT COUNT(*) FROM crime_daily_agg) as crime_records;

-- ============================================
-- COMPLETE
-- ============================================
-- Next: Run 2_analytics_integration.sql
