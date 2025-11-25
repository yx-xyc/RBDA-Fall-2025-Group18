-- ============================================
-- Step 2: Convert CSV to Parquet
-- ============================================
-- Populates Parquet tables from CSV tables with proper type casting
-- Run this after creating tables: trino --catalog hive --schema default -f 2_convert_to_parquet.sql

-- Convert cleaned data
INSERT INTO hive.default.mta_cleaned_2024
SELECT
    CAST(transit_timestamp AS TIMESTAMP),
    station_complex_id,
    station_complex,
    payment_method,
    fare_class_category,
    CAST(ridership AS INTEGER),
    CAST(transfers AS INTEGER),
    CASE WHEN latitude != '' THEN CAST(latitude AS DOUBLE) ELSE NULL END,
    CASE WHEN longitude != '' THEN CAST(longitude AS DOUBLE) ELSE NULL END,
    borough
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
