-- Trino Table Definitions for MTA Processed Data
-- These tables use the Hive connector to read data from GCS
-- Run these commands in Trino CLI or through Trino UI

-- NOTE: Replace YOUR_BUCKET with your actual GCS bucket name

-- 1. Cleaned MTA Data (2024 only)
CREATE TABLE IF NOT EXISTS hive.default.mta_cleaned_2024 (
    transit_timestamp VARCHAR,
    station_complex_id VARCHAR,
    station_complex VARCHAR,
    borough VARCHAR,
    payment_method VARCHAR,
    fare_class_category VARCHAR,
    ridership INTEGER,
    transfers INTEGER,
    latitude VARCHAR,
    longitude VARCHAR
)
WITH (
    format = 'CSV',
    external_location = 'gs://YOUR_BUCKET/mta_processed/cleaned/',
    skip_header_line_count = 0
);

-- 2. Station-Level Hourly Aggregation
CREATE TABLE IF NOT EXISTS hive.default.mta_station_hourly (
    transit_timestamp VARCHAR,
    station_complex_id VARCHAR,
    station_complex VARCHAR,
    borough VARCHAR,
    latitude VARCHAR,
    longitude VARCHAR,
    total_ridership INTEGER,
    total_transfers INTEGER,
    record_count INTEGER,
    metrocard_txns INTEGER,
    omny_txns INTEGER,
    full_fare_txns INTEGER,
    senior_disability_txns INTEGER,
    unlimited_txns INTEGER,
    student_txns INTEGER,
    fair_fare_txns INTEGER,
    other_txns INTEGER
)
WITH (
    format = 'CSV',
    external_location = 'gs://YOUR_BUCKET/mta_processed/hourly_aggregated/',
    skip_header_line_count = 0
);

-- 3. System-Wide Hourly Aggregation
CREATE TABLE IF NOT EXISTS hive.default.mta_systemwide_hourly (
    transit_timestamp VARCHAR,
    total_ridership INTEGER,
    total_transfers INTEGER,
    total_stations INTEGER,
    manhattan_ridership INTEGER,
    brooklyn_ridership INTEGER,
    queens_ridership INTEGER,
    bronx_ridership INTEGER,
    staten_island_ridership INTEGER,
    metrocard_ridership INTEGER,
    omny_ridership INTEGER,
    full_fare_ridership INTEGER,
    senior_disability_ridership INTEGER,
    unlimited_ridership INTEGER,
    student_ridership INTEGER,
    fair_fare_ridership INTEGER
)
WITH (
    format = 'CSV',
    external_location = 'gs://YOUR_BUCKET/mta_processed/systemwide_aggregated/',
    skip_header_line_count = 0
);

-- ============================================
-- Sample Queries for Analysis
-- ============================================

-- Query 1: Top 10 busiest stations by total ridership
SELECT
    station_complex,
    borough,
    SUM(total_ridership) as total_ridership
FROM hive.default.mta_station_hourly
GROUP BY station_complex, borough
ORDER BY total_ridership DESC
LIMIT 10;

-- Query 2: System-wide ridership trend by day
SELECT
    DATE_TRUNC('day', CAST(transit_timestamp AS TIMESTAMP)) as day,
    SUM(total_ridership) as daily_ridership,
    AVG(total_ridership) as avg_hourly_ridership
FROM hive.default.mta_systemwide_hourly
GROUP BY 1
ORDER BY day;

-- Query 3: Borough comparison
SELECT
    DATE_TRUNC('day', CAST(transit_timestamp AS TIMESTAMP)) as day,
    SUM(manhattan_ridership) as manhattan,
    SUM(brooklyn_ridership) as brooklyn,
    SUM(queens_ridership) as queens,
    SUM(bronx_ridership) as bronx
FROM hive.default.mta_systemwide_hourly
GROUP BY 1
ORDER BY day;

-- Query 4: Payment method adoption over time
SELECT
    DATE_TRUNC('week', CAST(transit_timestamp AS TIMESTAMP)) as week,
    SUM(metrocard_ridership) as metrocard,
    SUM(omny_ridership) as omny,
    ROUND(CAST(100.0 * SUM(omny_ridership) AS DOUBLE) /
          (SUM(metrocard_ridership) + SUM(omny_ridership)), 2) as omny_pct
FROM hive.default.mta_systemwide_hourly
GROUP BY 1
ORDER BY week;

-- Query 5: Hourly ridership pattern (average across all days)
SELECT
    HOUR(CAST(transit_timestamp AS TIMESTAMP)) as hour_of_day,
    AVG(CAST(total_ridership AS DOUBLE)) as avg_ridership,
    STDDEV(CAST(total_ridership AS DOUBLE)) as stddev_ridership
FROM hive.default.mta_systemwide_hourly
GROUP BY 1
ORDER BY hour_of_day;

-- Query 6: Station ridership with coordinates (for geographic visualization)
SELECT
    station_complex,
    borough,
    CAST(latitude AS DOUBLE) as latitude,
    CAST(longitude AS DOUBLE) as longitude,
    SUM(total_ridership) as total_ridership,
    AVG(CAST(total_ridership AS DOUBLE)) as avg_hourly_ridership
FROM hive.default.mta_station_hourly
WHERE latitude != '' AND longitude != ''
GROUP BY station_complex, borough, latitude, longitude
HAVING SUM(total_ridership) > 1000
ORDER BY total_ridership DESC;

-- ============================================
-- Queries for Joining with Weather Data
-- ============================================

-- Example: Join MTA ridership with weather data (assuming you have weather table)
-- CREATE TABLE hive.default.weather (
--     timestamp VARCHAR,
--     temperature DOUBLE,
--     precipitation DOUBLE,
--     condition VARCHAR
-- );

-- Query 7: Ridership vs Weather correlation
-- SELECT
--     DATE_TRUNC('day', CAST(m.transit_timestamp AS TIMESTAMP)) as day,
--     AVG(CAST(w.temperature AS DOUBLE)) as avg_temp,
--     SUM(CAST(w.precipitation AS DOUBLE)) as total_precip,
--     SUM(m.total_ridership) as total_ridership
-- FROM hive.default.mta_systemwide_hourly m
-- JOIN hive.default.weather w
--     ON m.transit_timestamp = w.timestamp
-- GROUP BY 1
-- ORDER BY day;
