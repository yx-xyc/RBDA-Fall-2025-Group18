-- ============================================
-- ANALYTICS INTEGRATION - FINAL TABLE
-- ============================================
-- This script creates the final analytical table by joining:
-- - Weather (time spine, hourly)
-- - MTA Subway (hourly, aggregated citywide)
-- - Crime (daily, same values for all hours in a day)
--
-- Prerequisites: Run 1_create_external_tables.sql first
--
-- Run: trino --catalog hive --schema yx2021_nyu_edu -f 2_analytics_integration.sql
-- ============================================

DROP TABLE IF EXISTS analytics_hourly_subway;

CREATE TABLE analytics_hourly_subway
WITH (
    format = 'PARQUET',
    partitioned_by = ARRAY['analysis_date']
)
AS
SELECT
    -- ===== TIME DIMENSIONS =====
    w.weather_hour as hour_timestamp,
    HOUR(w.weather_hour) as hour_of_day,
    DAY_OF_WEEK(w.weather_hour) as day_of_week,
    CASE
        WHEN DAY_OF_WEEK(w.weather_hour) IN (6, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,

    -- ===== WEATHER METRICS =====
    w.avg_temp_f,
    w.avg_humidity_pct,
    w.total_precip_inch,
    w.total_snow_inch,
    w.avg_snow_depth_inch,
    w.avg_wind_speed_mph,
    w.avg_visibility_miles,
    w.dom_precip_type,
    w.dom_wind_dir,
    w.dom_conditions,

    -- Weather condition flags
    CASE WHEN w.total_precip_inch > 0 THEN 1 ELSE 0 END as has_precipitation,
    CASE WHEN w.total_snow_inch > 0 THEN 1 ELSE 0 END as has_snow,
    CASE WHEN w.avg_temp_f < 32 THEN 1 ELSE 0 END as is_freezing,
    CASE WHEN w.avg_visibility_miles < 5 THEN 1 ELSE 0 END as low_visibility,

    -- ===== MTA SUBWAY METRICS =====
    COALESCE(m.total_subway_ridership, 0) as total_subway_ridership,
    COALESCE(m.active_stations, 0) as active_subway_stations,
    COALESCE(m.metrocard_ridership, 0) as metrocard_ridership,
    COALESCE(m.omny_ridership, 0) as omny_ridership,
    COALESCE(m.omny_pct, 0) as omny_pct,

    -- ===== CRIME METRICS (Daily - Same for all hours in a day) =====
    COALESCE(c.total_crimes, 0) as daily_total_crimes,
    COALESCE(c.assault_count, 0) as daily_assault_count,
    COALESCE(c.theft_count, 0) as daily_theft_count,
    COALESCE(c.robbery_count, 0) as daily_robbery_count,
    COALESCE(c.burglary_count, 0) as daily_burglary_count,
    COALESCE(c.vehicle_crime_count, 0) as daily_vehicle_crime_count,
    COALESCE(c.manhattan_crimes, 0) as daily_manhattan_crimes,
    COALESCE(c.brooklyn_crimes, 0) as daily_brooklyn_crimes,
    COALESCE(c.queens_crimes, 0) as daily_queens_crimes,
    COALESCE(c.bronx_crimes, 0) as daily_bronx_crimes,
    COALESCE(c.staten_island_crimes, 0) as daily_staten_island_crimes,

    -- ===== PARTITION COLUMN =====
    w.weather_date as analysis_date

FROM weather_hourly w

-- Left join MTA data (hourly)
LEFT JOIN mta_hourly_agg m
    ON w.weather_hour = m.transit_hour

-- Left join Crime data (daily - same values for all hours in a day)
LEFT JOIN crime_daily_agg c
    ON w.weather_date = c.crime_date;

-- ============================================
-- VERIFICATION QUERIES
-- ============================================

SELECT
    'Analytics Table Created' as status,
    COUNT(*) as total_hours,
    COUNT(DISTINCT analysis_date) as total_days,
    MIN(hour_timestamp) as earliest_hour,
    MAX(hour_timestamp) as latest_hour
FROM analytics_hourly_subway;

-- Sample the data
SELECT *
FROM analytics_hourly_subway
ORDER BY hour_timestamp DESC
LIMIT 10;

-- Check data coverage
SELECT
    analysis_date,
    COUNT(*) as hours_in_day,
    SUM(total_subway_ridership) as daily_subway_rides,
    AVG(avg_temp_f) as avg_daily_temp,
    MAX(daily_total_crimes) as daily_crimes
FROM analytics_hourly_subway
GROUP BY analysis_date
ORDER BY analysis_date DESC
LIMIT 10;

-- ============================================
-- COMPLETE
-- ============================================
-- Next: Run 3_sample_queries.sql for analytical insights
