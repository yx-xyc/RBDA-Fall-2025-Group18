-- ============================================
-- CREATE EXTERNAL TABLES FOR ALL DATASETS
-- ============================================
-- This script creates external tables for all processed datasets:
-- - Weather (hourly)
-- - MTA Subway (hourly, by station/payment method)
-- - Rideshare (hourly, by location - aggregated via MapReduce)
-- - Crime (daily)
--
-- Prerequisites:
-- 1. MTA MapReduce pipeline must be run: make run-mta
-- 2. Rideshare MapReduce aggregation must be run: make run-rideshare
--
-- Run: trino --catalog hive --schema default -f 1_create_external_tables.sql
-- ============================================

-- ============================================
-- WEATHER DATA (Hourly)
-- ============================================

DROP TABLE IF EXISTS hive.default.weather_hourly_csv;

CREATE EXTERNAL TABLE hive.default.weather_hourly_csv (
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
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/weather_hourly'
TBLPROPERTIES (
    'skip.header.line.count'='0'
);

-- Convert to Parquet for efficient querying
DROP TABLE IF EXISTS hive.default.weather_hourly;

CREATE TABLE hive.default.weather_hourly
WITH (format = 'PARQUET')
AS
SELECT
    CAST(FROM_ISO8601_TIMESTAMP(date_hour) AS TIMESTAMP) as weather_hour,
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
    DATE(CAST(FROM_ISO8601_TIMESTAMP(date_hour) AS TIMESTAMP)) as weather_date
FROM hive.default.weather_hourly_csv
WHERE date_hour IS NOT NULL
    AND TRY_CAST(FROM_ISO8601_TIMESTAMP(date_hour) AS TIMESTAMP) IS NOT NULL;

SELECT 'Weather' as dataset, COUNT(*) as records FROM hive.default.weather_hourly;

-- ============================================
-- MTA SUBWAY DATA (Hourly by Station/Payment)
-- ============================================

DROP TABLE IF EXISTS hive.default.mta_station_hourly_csv;

CREATE EXTERNAL TABLE hive.default.mta_station_hourly_csv (
    transit_timestamp VARCHAR,
    station_complex_id VARCHAR,
    payment_method VARCHAR,
    total_ridership VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/station_hourly'
TBLPROPERTIES (
    'skip.header.line.count'='0'
);

-- Convert to Parquet and aggregate citywide (across all stations)
DROP TABLE IF EXISTS hive.default.mta_hourly_agg;

CREATE TABLE hive.default.mta_hourly_agg
WITH (format = 'PARQUET')
AS
SELECT
    DATE_TRUNC('hour',
        CAST(FROM_ISO8601_TIMESTAMP(transit_timestamp) AS TIMESTAMP)
    ) as transit_hour,

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

    DATE(CAST(FROM_ISO8601_TIMESTAMP(transit_timestamp) AS TIMESTAMP)) as transit_date
FROM hive.default.mta_station_hourly_csv
WHERE transit_timestamp IS NOT NULL
    AND total_ridership IS NOT NULL
    AND TRY_CAST(FROM_ISO8601_TIMESTAMP(transit_timestamp) AS TIMESTAMP) IS NOT NULL
GROUP BY
    DATE_TRUNC('hour', CAST(FROM_ISO8601_TIMESTAMP(transit_timestamp) AS TIMESTAMP)),
    DATE(CAST(FROM_ISO8601_TIMESTAMP(transit_timestamp) AS TIMESTAMP));

SELECT 'MTA Aggregated' as dataset, COUNT(*) as records FROM hive.default.mta_hourly_agg;

-- ============================================
-- RIDESHARE DATA (Hourly by Location - MapReduce Output)
-- ============================================

DROP TABLE IF EXISTS hive.default.rideshare_hourly_agg_csv;

CREATE EXTERNAL TABLE hive.default.rideshare_hourly_agg_csv (
    pickup_hour VARCHAR,
    pickup_location_id VARCHAR,
    trip_count VARCHAR,
    avg_base_fare VARCHAR,
    avg_tolls VARCHAR,
    avg_tips VARCHAR,
    avg_driver_pay VARCHAR,
    avg_total_cost VARCHAR,
    avg_trip_duration_sec VARCHAR,
    avg_trip_miles VARCHAR,
    shared_request_count VARCHAR,
    shared_match_count VARCHAR,
    shared_match_pct VARCHAR,
    wheelchair_request_count VARCHAR,
    wheelchair_match_count VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/rideshare_processed/hourly_agg'
TBLPROPERTIES (
    'skip.header.line.count'='0'
);

-- Convert to Parquet
DROP TABLE IF EXISTS hive.default.rideshare_hourly_agg;

CREATE TABLE hive.default.rideshare_hourly_agg
WITH (format = 'PARQUET')
AS
SELECT
    CAST(FROM_ISO8601_TIMESTAMP(pickup_hour) AS TIMESTAMP) as pickup_hour,
    CAST(pickup_location_id AS INTEGER) as pickup_location_id,
    CAST(trip_count AS INTEGER) as trip_count,
    CAST(avg_base_fare AS DOUBLE) as avg_base_fare,
    CAST(avg_tolls AS DOUBLE) as avg_tolls,
    CAST(avg_tips AS DOUBLE) as avg_tips,
    CAST(avg_driver_pay AS DOUBLE) as avg_driver_pay,
    CAST(avg_total_cost AS DOUBLE) as avg_total_cost,
    CAST(avg_trip_duration_sec AS DOUBLE) as avg_trip_duration_sec,
    ROUND(CAST(avg_trip_duration_sec AS DOUBLE) / 60, 2) as avg_trip_duration_min,
    CAST(avg_trip_miles AS DOUBLE) as avg_trip_miles,
    CAST(shared_request_count AS INTEGER) as shared_request_count,
    CAST(shared_match_count AS INTEGER) as shared_match_count,
    CAST(shared_match_pct AS DOUBLE) as shared_match_pct,
    CAST(wheelchair_request_count AS INTEGER) as wheelchair_request_count,
    CAST(wheelchair_match_count AS INTEGER) as wheelchair_match_count,
    DATE(CAST(FROM_ISO8601_TIMESTAMP(pickup_hour) AS TIMESTAMP)) as pickup_date
FROM hive.default.rideshare_hourly_agg_csv
WHERE pickup_hour IS NOT NULL
    AND TRY_CAST(FROM_ISO8601_TIMESTAMP(pickup_hour) AS TIMESTAMP) IS NOT NULL;

SELECT 'Rideshare Aggregated' as dataset, COUNT(*) as records FROM hive.default.rideshare_hourly_agg;

-- ============================================
-- CRIME DATA (Daily)
-- ============================================

DROP TABLE IF EXISTS hive.default.crime_daily_csv;

CREATE EXTERNAL TABLE hive.default.crime_daily_csv (
    crime_date_str VARCHAR,
    offense_desc VARCHAR,
    borough_code VARCHAR,
    avg_lat VARCHAR,
    avg_lon VARCHAR,
    crime_count VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/crime_daily'
TBLPROPERTIES (
    'skip.header.line.count'='0'
);

-- Convert to Parquet and aggregate by day
DROP TABLE IF EXISTS hive.default.crime_daily_agg;

CREATE TABLE hive.default.crime_daily_agg
WITH (format = 'PARQUET')
AS
SELECT
    CAST(FROM_ISO8601_DATE(crime_date_str) AS DATE) as crime_date,

    -- Total crime count
    SUM(CAST(crime_count AS BIGINT)) as total_crimes,

    -- Crime type breakdown (top categories)
    SUM(CASE WHEN UPPER(offense_desc) LIKE '%ASSAULT%' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as assault_count,
    SUM(CASE WHEN UPPER(offense_desc) LIKE '%LARCENY%' OR UPPER(offense_desc) LIKE '%THEFT%' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as theft_count,
    SUM(CASE WHEN UPPER(offense_desc) LIKE '%ROBBERY%' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as robbery_count,
    SUM(CASE WHEN UPPER(offense_desc) LIKE '%BURGLARY%' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as burglary_count,
    SUM(CASE WHEN UPPER(offense_desc) LIKE '%VEHICLE%' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as vehicle_crime_count,

    -- Borough breakdown
    SUM(CASE WHEN borough_code = 'M' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as manhattan_crimes,
    SUM(CASE WHEN borough_code = 'K' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as brooklyn_crimes,
    SUM(CASE WHEN borough_code = 'Q' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as queens_crimes,
    SUM(CASE WHEN borough_code = 'X' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as bronx_crimes,
    SUM(CASE WHEN borough_code = 'S' THEN CAST(crime_count AS BIGINT) ELSE 0 END) as staten_island_crimes,

    -- Unique crime types and locations
    COUNT(DISTINCT offense_desc) as unique_offense_types,
    COUNT(DISTINCT borough_code) as boroughs_with_crimes

FROM hive.default.crime_daily_csv
WHERE crime_date_str IS NOT NULL
    AND crime_count IS NOT NULL
    AND TRY_CAST(FROM_ISO8601_DATE(crime_date_str) AS DATE) IS NOT NULL
GROUP BY
    CAST(FROM_ISO8601_DATE(crime_date_str) AS DATE);

SELECT 'Crime Daily' as dataset, COUNT(*) as records FROM hive.default.crime_daily_agg;

-- ============================================
-- SUMMARY
-- ============================================

SELECT
    'EXTERNAL TABLES CREATED' as status,
    (SELECT COUNT(*) FROM hive.default.weather_hourly) as weather_records,
    (SELECT COUNT(*) FROM hive.default.mta_hourly_agg) as mta_records,
    (SELECT COUNT(*) FROM hive.default.rideshare_hourly_agg) as rideshare_records,
    (SELECT COUNT(*) FROM hive.default.crime_daily_agg) as crime_records;

-- ============================================
-- COMPLETE
-- ============================================
-- Next: Run 2_analytics_integration.sql
