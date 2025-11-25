-- ============================================
-- Step 1: Create Tables
-- ============================================
-- Creates CSV base tables and Parquet tables
-- Run this first: trino --catalog hive --schema default -f 1_create_tables.sql

-- ============================================
-- CSV BASE TABLES (Read MapReduce Output)
-- ============================================
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
-- PARQUET TABLES (Optimized for Analysis)
-- ============================================
-- Proper data types and Parquet format

CREATE TABLE IF NOT EXISTS hive.default.mta_cleaned_2024 (
    transit_timestamp TIMESTAMP,
    station_complex_id VARCHAR,
    station_complex VARCHAR,
    payment_method VARCHAR,
    fare_class_category VARCHAR,
    ridership INTEGER,
    transfers INTEGER,
    latitude DOUBLE,
    longitude DOUBLE,
    borough VARCHAR
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
