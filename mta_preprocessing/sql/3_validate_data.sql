-- ============================================
-- Step 3: Validate Data
-- ============================================
-- Run validation queries to verify the conversion worked correctly
-- Run this: trino --catalog hive --schema default -f 3_validate_data.sql

-- Check row counts match between CSV and Parquet
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
SELECT 'Parquet Citywide', COUNT(*) FROM hive.default.mta_citywide_hourly
ORDER BY source;

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

-- Check for any NULL ridership values
SELECT
    'Cleaned' as table_name,
    COUNT(*) as null_count
FROM hive.default.mta_cleaned_2024
WHERE ridership IS NULL
UNION ALL
SELECT
    'Station Hourly',
    COUNT(*)
FROM hive.default.mta_station_hourly
WHERE total_ridership IS NULL
UNION ALL
SELECT
    'Borough Hourly',
    COUNT(*)
FROM hive.default.mta_borough_hourly
WHERE total_ridership IS NULL
UNION ALL
SELECT
    'Citywide Hourly',
    COUNT(*)
FROM hive.default.mta_citywide_hourly
WHERE total_ridership IS NULL;

-- Sample data from each table
SELECT 'Sample from mta_cleaned_2024:' as info;
SELECT * FROM hive.default.mta_cleaned_2024 LIMIT 5;

SELECT 'Sample from mta_station_hourly:' as info;
SELECT * FROM hive.default.mta_station_hourly LIMIT 5;

SELECT 'Sample from mta_borough_hourly:' as info;
SELECT * FROM hive.default.mta_borough_hourly LIMIT 5;

SELECT 'Sample from mta_citywide_hourly:' as info;
SELECT * FROM hive.default.mta_citywide_hourly LIMIT 5;
