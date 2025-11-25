-- ============================================
-- Trino Table Definitions for MTA Processed Data
-- ============================================
-- This file creates two layers of tables:
-- 1. CSV base tables (VARCHAR only) - reads MapReduce output
-- 2. Parquet tables (proper types) - optimized for analysis and joins
--
-- Usage:
--   Step 1: Run this file to create all tables
--   Step 2: Run the conversion queries to populate Parquet tables
--   Step 3: Use Parquet tables for all analysis and joins
-- ============================================

-- ============================================
-- PART 1: CSV BASE TABLES (Read MapReduce Output)
-- ============================================
-- These tables read the raw CSV output from MapReduce jobs
-- All columns are VARCHAR because Hive CSV format only supports VARCHAR

CREATE TABLE IF NOT EXISTS hive.default.mta_cleaned_2024_csv (
    transit_timestamp VARCHAR,
    station_complex_id VARCHAR,
    station_complex VARCHAR,
    borough VARCHAR,
    payment_method VARCHAR,
    fare_class_category VARCHAR,
    ridership VARCHAR,
    transfers VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/cleaned/',
    skip_header_line_count = 0
);

CREATE TABLE IF NOT EXISTS hive.default.mta_station_hourly_csv (
    transit_timestamp VARCHAR,
    station_complex_id VARCHAR,
    station_complex VARCHAR,
    borough VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    total_ridership VARCHAR,
    total_transfers VARCHAR,
    record_count VARCHAR,
    metrocard_txns VARCHAR,
    omny_txns VARCHAR,
    full_fare_txns VARCHAR,
    senior_disability_txns VARCHAR,
    unlimited_txns VARCHAR,
    student_txns VARCHAR,
    fair_fare_txns VARCHAR,
    other_txns VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/station_hourly/',
    skip_header_line_count = 0
);

CREATE TABLE IF NOT EXISTS hive.default.mta_borough_hourly_csv (
    transit_timestamp VARCHAR,
    borough VARCHAR,
    total_ridership VARCHAR,
    total_transfers VARCHAR,
    station_count VARCHAR,
    metrocard_ridership VARCHAR,
    omny_ridership VARCHAR,
    full_fare_ridership VARCHAR,
    senior_disability_ridership VARCHAR,
    unlimited_ridership VARCHAR,
    student_ridership VARCHAR,
    fair_fare_ridership VARCHAR,
    other_ridership VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/borough_hourly/',
    skip_header_line_count = 0
);

CREATE TABLE IF NOT EXISTS hive.default.mta_citywide_hourly_csv (
    transit_timestamp VARCHAR,
    total_ridership VARCHAR,
    total_transfers VARCHAR,
    total_stations VARCHAR,
    manhattan_ridership VARCHAR,
    brooklyn_ridership VARCHAR,
    queens_ridership VARCHAR,
    bronx_ridership VARCHAR,
    staten_island_ridership VARCHAR,
    metrocard_ridership VARCHAR,
    omny_ridership VARCHAR,
    full_fare_ridership VARCHAR,
    senior_disability_ridership VARCHAR,
    unlimited_ridership VARCHAR,
    student_ridership VARCHAR,
    fair_fare_ridership VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 'hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/citywide_hourly/',
    skip_header_line_count = 0
);

-- ============================================
-- PART 2: PARQUET TABLES (Optimized for Analysis)
-- ============================================
-- These tables have proper data types and use Parquet format
-- Use these for all analysis, joins, and queries

CREATE TABLE IF NOT EXISTS hive.default.mta_cleaned_2024 (
    transit_timestamp TIMESTAMP,
    station_complex_id VARCHAR,
    station_complex VARCHAR,
    borough VARCHAR,
    payment_method VARCHAR,
    fare_class_category VARCHAR,
    ridership INTEGER,
    transfers INTEGER,
    latitude DOUBLE,
    longitude DOUBLE
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['borough']
);

CREATE TABLE IF NOT EXISTS hive.default.mta_station_hourly (
    transit_timestamp TIMESTAMP,
    station_complex_id VARCHAR,
    station_complex VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    total_ridership BIGINT,
    total_transfers BIGINT,
    record_count INTEGER,
    metrocard_txns INTEGER,
    omny_txns INTEGER,
    full_fare_txns INTEGER,
    senior_disability_txns INTEGER,
    unlimited_txns INTEGER,
    student_txns INTEGER,
    fair_fare_txns INTEGER,
    other_txns INTEGER,
    borough VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['borough']
);

CREATE TABLE IF NOT EXISTS hive.default.mta_borough_hourly (
    transit_timestamp TIMESTAMP,
    total_ridership BIGINT,
    total_transfers BIGINT,
    station_count INTEGER,
    metrocard_ridership BIGINT,
    omny_ridership BIGINT,
    full_fare_ridership BIGINT,
    senior_disability_ridership BIGINT,
    unlimited_ridership BIGINT,
    student_ridership BIGINT,
    fair_fare_ridership BIGINT,
    other_ridership BIGINT,
    borough VARCHAR
)
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['borough']
);

CREATE TABLE IF NOT EXISTS hive.default.mta_citywide_hourly (
    transit_timestamp TIMESTAMP,
    total_ridership BIGINT,
    total_transfers BIGINT,
    total_stations INTEGER,
    manhattan_ridership BIGINT,
    brooklyn_ridership BIGINT,
    queens_ridership BIGINT,
    bronx_ridership BIGINT,
    staten_island_ridership BIGINT,
    metrocard_ridership BIGINT,
    omny_ridership BIGINT,
    full_fare_ridership BIGINT,
    senior_disability_ridership BIGINT,
    unlimited_ridership BIGINT,
    student_ridership BIGINT,
    fair_fare_ridership BIGINT
)
WITH (
    format = 'PARQUET'
);

-- ============================================
-- PART 3: DATA CONVERSION (CSV → Parquet)
-- ============================================
-- Run these queries ONCE after your MapReduce jobs complete
-- They convert CSV data to typed Parquet format

-- Convert cleaned data
INSERT INTO hive.default.mta_cleaned_2024
SELECT
    CAST(transit_timestamp AS TIMESTAMP),
    station_complex_id,
    station_complex,
    borough,
    payment_method,
    fare_class_category,
    CAST(ridership AS INTEGER),
    CAST(transfers AS INTEGER),
    CASE WHEN latitude != '' THEN CAST(latitude AS DOUBLE) ELSE NULL END,
    CASE WHEN longitude != '' THEN CAST(longitude AS DOUBLE) ELSE NULL END
FROM hive.default.mta_cleaned_2024_csv;

-- Convert station hourly data
INSERT INTO hive.default.mta_station_hourly
SELECT
    CAST(transit_timestamp AS TIMESTAMP),
    station_complex_id,
    station_complex,
    CASE WHEN latitude != '' THEN CAST(latitude AS DOUBLE) ELSE NULL END,
    CASE WHEN longitude != '' THEN CAST(longitude AS DOUBLE) ELSE NULL END,
    CAST(total_ridership AS BIGINT),
    CAST(total_transfers AS BIGINT),
    CAST(record_count AS INTEGER),
    CAST(metrocard_txns AS INTEGER),
    CAST(omny_txns AS INTEGER),
    CAST(full_fare_txns AS INTEGER),
    CAST(senior_disability_txns AS INTEGER),
    CAST(unlimited_txns AS INTEGER),
    CAST(student_txns AS INTEGER),
    CAST(fair_fare_txns AS INTEGER),
    CAST(other_txns AS INTEGER),
    borough
FROM hive.default.mta_station_hourly_csv;

-- Convert borough hourly data
INSERT INTO hive.default.mta_borough_hourly
SELECT
    CAST(transit_timestamp AS TIMESTAMP),
    CAST(total_ridership AS BIGINT),
    CAST(total_transfers AS BIGINT),
    CAST(station_count AS INTEGER),
    CAST(metrocard_ridership AS BIGINT),
    CAST(omny_ridership AS BIGINT),
    CAST(full_fare_ridership AS BIGINT),
    CAST(senior_disability_ridership AS BIGINT),
    CAST(unlimited_ridership AS BIGINT),
    CAST(student_ridership AS BIGINT),
    CAST(fair_fare_ridership AS BIGINT),
    CAST(other_ridership AS BIGINT),
    borough
FROM hive.default.mta_borough_hourly_csv;

-- Convert citywide hourly data
INSERT INTO hive.default.mta_citywide_hourly
SELECT
    CAST(transit_timestamp AS TIMESTAMP),
    CAST(total_ridership AS BIGINT),
    CAST(total_transfers AS BIGINT),
    CAST(total_stations AS INTEGER),
    CAST(manhattan_ridership AS BIGINT),
    CAST(brooklyn_ridership AS BIGINT),
    CAST(queens_ridership AS BIGINT),
    CAST(bronx_ridership AS BIGINT),
    CAST(staten_island_ridership AS BIGINT),
    CAST(metrocard_ridership AS BIGINT),
    CAST(omny_ridership AS BIGINT),
    CAST(full_fare_ridership AS BIGINT),
    CAST(senior_disability_ridership AS BIGINT),
    CAST(unlimited_ridership AS BIGINT),
    CAST(student_ridership AS BIGINT),
    CAST(fair_fare_ridership AS BIGINT)
FROM hive.default.mta_citywide_hourly_csv;

-- ============================================
-- PART 4: DATA VALIDATION QUERIES
-- ============================================
-- Run these to verify the conversion worked correctly

-- Check row counts match
SELECT 'CSV Cleaned' as source, COUNT(*) as row_count FROM hive.default.mta_cleaned_2024_csv
UNION ALL
SELECT 'Parquet Cleaned', COUNT(*) FROM hive.default.mta_cleaned_2024
UNION ALL
SELECT 'CSV Station', COUNT(*) FROM hive.default.mta_station_hourly_csv
UNION ALL
SELECT 'Parquet Station', COUNT(*) FROM hive.default.mta_station_hourly
UNION ALL
SELECT 'CSV Borough', COUNT(*) FROM hive.default.mta_borough_hourly_csv
UNION ALL
SELECT 'Parquet Borough', COUNT(*) FROM hive.default.mta_borough_hourly
UNION ALL
SELECT 'CSV Citywide', COUNT(*) FROM hive.default.mta_citywide_hourly_csv
UNION ALL
SELECT 'Parquet Citywide', COUNT(*) FROM hive.default.mta_citywide_hourly;

-- Check date ranges
SELECT
    MIN(transit_timestamp) as earliest_date,
    MAX(transit_timestamp) as latest_date,
    COUNT(DISTINCT DATE(transit_timestamp)) as unique_days
FROM hive.default.mta_citywide_hourly;

-- Check borough distribution
SELECT borough, COUNT(*) as record_count
FROM hive.default.mta_borough_hourly
GROUP BY borough
ORDER BY borough;

-- ============================================
-- PART 5: SAMPLE ANALYSIS QUERIES
-- ============================================
-- Use these Parquet tables for all your analysis

-- Top 10 busiest stations by total ridership
SELECT
    station_complex,
    borough,
    SUM(total_ridership) as total_ridership,
    AVG(total_ridership) as avg_hourly_ridership
FROM hive.default.mta_station_hourly
GROUP BY station_complex, borough
ORDER BY total_ridership DESC
LIMIT 10;

-- Daily ridership trend
SELECT
    DATE(transit_timestamp) as day,
    SUM(total_ridership) as daily_ridership,
    AVG(total_ridership) as avg_hourly_ridership
FROM hive.default.mta_citywide_hourly
GROUP BY DATE(transit_timestamp)
ORDER BY day;

-- Borough comparison by day
SELECT
    DATE(transit_timestamp) as day,
    SUM(manhattan_ridership) as manhattan,
    SUM(brooklyn_ridership) as brooklyn,
    SUM(queens_ridership) as queens,
    SUM(bronx_ridership) as bronx,
    SUM(staten_island_ridership) as staten_island
FROM hive.default.mta_citywide_hourly
GROUP BY DATE(transit_timestamp)
ORDER BY day;

-- Payment method adoption over time
SELECT
    DATE_TRUNC('week', transit_timestamp) as week,
    SUM(metrocard_ridership) as metrocard,
    SUM(omny_ridership) as omny,
    ROUND(100.0 * SUM(omny_ridership) /
          (SUM(metrocard_ridership) + SUM(omny_ridership)), 2) as omny_pct
FROM hive.default.mta_citywide_hourly
GROUP BY DATE_TRUNC('week', transit_timestamp)
ORDER BY week;

-- Hourly ridership pattern (average across all days)
SELECT
    HOUR(transit_timestamp) as hour_of_day,
    AVG(total_ridership) as avg_ridership,
    STDDEV(total_ridership) as stddev_ridership,
    MIN(total_ridership) as min_ridership,
    MAX(total_ridership) as max_ridership
FROM hive.default.mta_citywide_hourly
GROUP BY HOUR(transit_timestamp)
ORDER BY hour_of_day;

-- Weekday vs Weekend patterns
SELECT
    CASE
        WHEN DAY_OF_WEEK(transit_timestamp) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,
    HOUR(transit_timestamp) as hour_of_day,
    AVG(total_ridership) as avg_ridership
FROM hive.default.mta_citywide_hourly
GROUP BY
    CASE WHEN DAY_OF_WEEK(transit_timestamp) IN (6, 7) THEN 'Weekend' ELSE 'Weekday' END,
    HOUR(transit_timestamp)
ORDER BY day_type, hour_of_day;

-- Station ridership with coordinates (for geographic visualization)
SELECT
    station_complex,
    borough,
    latitude,
    longitude,
    SUM(total_ridership) as total_ridership,
    AVG(total_ridership) as avg_hourly_ridership
FROM hive.default.mta_station_hourly
WHERE latitude IS NOT NULL AND longitude IS NOT NULL
GROUP BY station_complex, borough, latitude, longitude
HAVING SUM(total_ridership) > 1000
ORDER BY total_ridership DESC;

-- ============================================
-- PART 6: SAMPLE JOIN QUERIES FOR FUTURE USE
-- ============================================
-- Templates for joining with weather, crime, and Uber/Lyft data

-- Example: Join with weather data (once you have it)
-- Assumes you have a weather table with columns: timestamp, temperature, precipitation, condition
/*
SELECT
    DATE(m.transit_timestamp) as day,
    AVG(w.temperature) as avg_temp,
    SUM(w.precipitation) as total_precip,
    SUM(m.total_ridership) as total_ridership,
    CORR(w.temperature, m.total_ridership) as temp_ridership_correlation
FROM hive.default.mta_citywide_hourly m
JOIN hive.default.weather w
    ON DATE_TRUNC('hour', m.transit_timestamp) = DATE_TRUNC('hour', w.timestamp)
GROUP BY DATE(m.transit_timestamp)
ORDER BY day;
*/

-- Example: Join with crime data at borough level
-- Assumes you have a crime table with columns: timestamp, borough, crime_count
/*
SELECT
    DATE(m.transit_timestamp) as day,
    m.borough,
    SUM(m.total_ridership) as subway_ridership,
    SUM(c.crime_count) as crime_count,
    CORR(m.total_ridership, c.crime_count) as correlation
FROM hive.default.mta_borough_hourly m
LEFT JOIN hive.default.crime c
    ON DATE_TRUNC('hour', m.transit_timestamp) = DATE_TRUNC('hour', c.timestamp)
    AND m.borough = c.borough
GROUP BY DATE(m.transit_timestamp), m.borough
ORDER BY day, m.borough;
*/

-- Example: Join with Uber/Lyft data at borough level
-- Assumes you have hvfhv table with columns: pickup_datetime, pickup_borough, trip_count
/*
SELECT
    DATE(m.transit_timestamp) as day,
    m.borough,
    SUM(m.total_ridership) as subway_ridership,
    SUM(u.trip_count) as rideshare_trips,
    ROUND(100.0 * SUM(m.total_ridership) /
          (SUM(m.total_ridership) + SUM(u.trip_count)), 2) as subway_mode_share_pct
FROM hive.default.mta_borough_hourly m
LEFT JOIN hive.default.hvfhv u
    ON DATE_TRUNC('hour', m.transit_timestamp) = DATE_TRUNC('hour', u.pickup_datetime)
    AND m.borough = u.pickup_borough
GROUP BY DATE(m.transit_timestamp), m.borough
ORDER BY day, m.borough;
*/
