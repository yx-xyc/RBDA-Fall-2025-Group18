-- ============================================
-- Step 4: Analysis Queries
-- ============================================
-- Sample analysis queries using the Parquet tables
-- Run individual queries or all at once: trino --catalog hive --schema default -f 4_analysis_queries.sql

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
ORDER BY total_ridership DESC
LIMIT 50;
