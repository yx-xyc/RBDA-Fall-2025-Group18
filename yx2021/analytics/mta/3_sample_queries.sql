-- ============================================
-- SAMPLE ANALYTICAL QUERIES
-- ============================================
-- Weather, MTA Subway, and Crime Analysis
-- Run: trino --catalog hive --schema default -f 3_sample_queries.sql
--
-- Prerequisites: Run 1_create_external_tables.sql and 2_analytics_integration.sql first
-- ============================================

-- ============================================
-- Query 1: Weather Impact on Subway Ridership
-- ============================================
-- Question: How does precipitation affect subway usage?

SELECT
    CASE
        WHEN total_precip_inch = 0 THEN 'No Rain'
        WHEN total_precip_inch <= 0.1 THEN 'Light Rain'
        WHEN total_precip_inch <= 0.3 THEN 'Moderate Rain'
        ELSE 'Heavy Rain'
    END as precipitation_level,

    COUNT(*) as hour_count,
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(STDDEV(total_subway_ridership), 0) as stddev_subway_riders,

    -- Percent difference from no-rain baseline
    ROUND(100.0 * (AVG(total_subway_ridership) -
          AVG(CASE WHEN total_precip_inch = 0 THEN total_subway_ridership END)) /
          NULLIF(AVG(CASE WHEN total_precip_inch = 0 THEN total_subway_ridership END), 0), 2) as pct_change_from_baseline

FROM analytics_hourly_subway_v3
WHERE total_subway_ridership > 0
GROUP BY
    CASE
        WHEN total_precip_inch = 0 THEN 'No Rain'
        WHEN total_precip_inch <= 0.1 THEN 'Light Rain'
        WHEN total_precip_inch <= 0.3 THEN 'Moderate Rain'
        ELSE 'Heavy Rain'
    END
ORDER BY precipitation_level;

-- ============================================
-- Query 2: Hourly Subway Patterns by Day Type
-- ============================================
-- Question: How do ridership patterns differ between weekdays and weekends?

SELECT
    day_type,
    hour_of_day,

    -- Ridership metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(MIN(total_subway_ridership), 0) as min_riders,
    ROUND(MAX(total_subway_ridership), 0) as max_riders,

    -- Peak indicators
    CASE
        WHEN hour_of_day BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN hour_of_day BETWEEN 17 AND 19 THEN 'Evening Rush'
        WHEN hour_of_day BETWEEN 22 AND 5 THEN 'Late Night'
        ELSE 'Off-Peak'
    END as time_period,

    -- Average weather during this hour
    ROUND(AVG(avg_temp_f), 1) as avg_temp

FROM analytics_hourly_subway_v3
GROUP BY day_type, hour_of_day
ORDER BY day_type, hour_of_day;

-- ============================================
-- Query 3: Temperature Impact on Ridership
-- ============================================
-- Question: How does temperature affect subway usage?

SELECT
    CASE
        WHEN avg_temp_f < 20 THEN 'Extreme Cold (<20°F)'
        WHEN avg_temp_f < 32 THEN 'Freezing (20-32°F)'
        WHEN avg_temp_f < 50 THEN 'Cold (32-50°F)'
        WHEN avg_temp_f < 70 THEN 'Moderate (50-70°F)'
        WHEN avg_temp_f < 85 THEN 'Warm (70-85°F)'
        ELSE 'Hot (>85°F)'
    END as temp_range,

    COUNT(*) as hour_count,
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(STDDEV(total_subway_ridership), 0) as stddev_riders

FROM analytics_hourly_subway_v3
WHERE total_subway_ridership > 0
GROUP BY
    CASE
        WHEN avg_temp_f < 20 THEN 'Extreme Cold (<20°F)'
        WHEN avg_temp_f < 32 THEN 'Freezing (20-32°F)'
        WHEN avg_temp_f < 50 THEN 'Cold (32-50°F)'
        WHEN avg_temp_f < 70 THEN 'Moderate (50-70°F)'
        WHEN avg_temp_f < 85 THEN 'Warm (70-85°F)'
        ELSE 'Hot (>85°F)'
    END
ORDER BY temp_range;

-- ============================================
-- Query 4: Crime and Late-Night Subway Usage
-- ============================================
-- Question: Is there a relationship between crime levels and late-night ridership?

SELECT
    CASE
        WHEN hour_of_day BETWEEN 22 AND 23 OR hour_of_day BETWEEN 0 AND 5 THEN 'Late Night (10pm-6am)'
        WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning (6am-12pm)'
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12pm-6pm)'
        ELSE 'Evening (6pm-10pm)'
    END as time_period,

    -- Crime metrics
    ROUND(AVG(daily_total_crimes), 0) as avg_daily_crimes,
    ROUND(AVG(daily_assault_count), 0) as avg_assaults,
    ROUND(AVG(daily_theft_count), 0) as avg_thefts,

    -- Subway metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,

    -- Crime per 1000 riders
    ROUND(AVG(daily_total_crimes) * 1000.0 / NULLIF(AVG(total_subway_ridership), 0), 2) as crimes_per_1000_riders

FROM analytics_hourly_subway_v3
WHERE daily_total_crimes > 0 AND total_subway_ridership > 0
GROUP BY
    CASE
        WHEN hour_of_day BETWEEN 22 AND 23 OR hour_of_day BETWEEN 0 AND 5 THEN 'Late Night (10pm-6am)'
        WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning (6am-12pm)'
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12pm-6pm)'
        ELSE 'Evening (6pm-10pm)'
    END
ORDER BY time_period;

-- ============================================
-- Query 5: Snow/Ice Impact on Subway Usage
-- ============================================
-- Question: How do snow conditions affect ridership?

SELECT
    CASE
        WHEN total_snow_inch = 0 AND avg_snow_depth_inch = 0 THEN 'No Snow'
        WHEN total_snow_inch > 0 THEN 'Actively Snowing'
        WHEN avg_snow_depth_inch > 0 THEN 'Snow on Ground'
        ELSE 'Other'
    END as snow_condition,

    is_freezing as is_below_freezing,

    COUNT(*) as hour_count,
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,

    -- Weather severity
    ROUND(AVG(avg_temp_f), 1) as avg_temp,
    ROUND(AVG(avg_visibility_miles), 1) as avg_visibility

FROM analytics_hourly_subway_v3
WHERE total_subway_ridership > 0
GROUP BY
    CASE
        WHEN total_snow_inch = 0 AND avg_snow_depth_inch = 0 THEN 'No Snow'
        WHEN total_snow_inch > 0 THEN 'Actively Snowing'
        WHEN avg_snow_depth_inch > 0 THEN 'Snow on Ground'
        ELSE 'Other'
    END,
    is_freezing
ORDER BY
    is_freezing DESC,
    snow_condition;

-- ============================================
-- Query 6: Peak Subway Demand Analysis
-- ============================================
-- Question: What are the busiest subway times and contributing factors?

SELECT
    DATE(hour_timestamp) as date,
    hour_of_day,
    day_type,

    -- Ridership
    total_subway_ridership,

    -- Weather conditions
    avg_temp_f,
    total_precip_inch,
    dom_conditions,

    -- Crime context
    daily_total_crimes,

    -- Rank by ridership
    ROW_NUMBER() OVER (ORDER BY total_subway_ridership DESC) as ridership_rank

FROM analytics_hourly_subway_v3
WHERE total_subway_ridership > 0
ORDER BY total_subway_ridership DESC
LIMIT 20;

-- ============================================
-- Query 7: Payment Method Adoption Trends (OMNY vs MetroCard)
-- ============================================
-- Question: How is OMNY adoption progressing?

SELECT
    DATE_TRUNC('week', hour_timestamp) as week,

    -- OMNY adoption metrics
    ROUND(AVG(omny_pct), 2) as avg_omny_pct,
    SUM(omny_ridership) as total_omny_rides,
    SUM(metrocard_ridership) as total_metrocard_rides,
    SUM(total_subway_ridership) as total_subway_rides,

    -- Weather context
    ROUND(AVG(avg_temp_f), 1) as avg_temp,
    ROUND(SUM(total_precip_inch), 2) as total_weekly_precip

FROM analytics_hourly_subway_v3
WHERE total_subway_ridership > 0
GROUP BY DATE_TRUNC('week', hour_timestamp)
ORDER BY week;

-- ============================================
-- Query 8: Severe Weather Event Impact
-- ============================================
-- Question: How do extreme weather events impact ridership?

SELECT
    DATE(hour_timestamp) as date,
    day_type,

    -- Weather severity indicators
    MAX(total_precip_inch) as max_precip,
    MAX(total_snow_inch) as max_snow,
    MIN(avg_temp_f) as min_temp,
    MIN(avg_visibility_miles) as min_visibility,

    -- Daily subway totals
    SUM(total_subway_ridership) as daily_subway_riders,
    ROUND(AVG(total_subway_ridership), 0) as avg_hourly_subway,

    -- Identify severe weather days
    CASE
        WHEN MAX(total_precip_inch) > 0.5 THEN 'Heavy Rain'
        WHEN MAX(total_snow_inch) > 3 THEN 'Heavy Snow'
        WHEN MIN(avg_temp_f) < 15 THEN 'Extreme Cold'
        WHEN MIN(avg_visibility_miles) < 3 THEN 'Low Visibility'
        ELSE 'Normal'
    END as weather_event_type

FROM analytics_hourly_subway_v3
GROUP BY DATE(hour_timestamp), day_type
HAVING
    MAX(total_precip_inch) > 0.5
    OR MAX(total_snow_inch) > 3
    OR MIN(avg_temp_f) < 15
    OR MIN(avg_visibility_miles) < 3
ORDER BY date DESC;

-- ============================================
-- Query 9: Borough-Level Crime and Subway Usage
-- ============================================
-- Question: How does crime distribution across boroughs relate to ridership?

SELECT
    analysis_date,
    day_type,

    -- Crime by borough
    daily_manhattan_crimes,
    daily_brooklyn_crimes,
    daily_queens_crimes,
    daily_bronx_crimes,
    daily_staten_island_crimes,
    daily_total_crimes,

    -- Subway metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_hourly_subway,

    -- Crime-to-ridership ratio
    ROUND(daily_total_crimes * 1.0 / NULLIF(SUM(total_subway_ridership), 0) * 10000, 2) as crimes_per_10k_riders

FROM analytics_hourly_subway_v3
WHERE daily_total_crimes > 0 AND total_subway_ridership > 0
GROUP BY
    analysis_date,
    day_type,
    daily_manhattan_crimes,
    daily_brooklyn_crimes,
    daily_queens_crimes,
    daily_bronx_crimes,
    daily_staten_island_crimes,
    daily_total_crimes
ORDER BY analysis_date DESC
LIMIT 30;

-- ============================================
-- Query 10: Summary Statistics - Overall Dataset
-- ============================================
-- Question: What are the overall statistics for the integrated dataset?

SELECT
    -- Date range
    MIN(hour_timestamp) as earliest_timestamp,
    MAX(hour_timestamp) as latest_timestamp,
    COUNT(DISTINCT analysis_date) as total_days,
    COUNT(*) as total_hours,

    -- Subway totals
    SUM(total_subway_ridership) as total_subway_rides,
    ROUND(AVG(total_subway_ridership), 0) as avg_hourly_subway,

    -- OMNY adoption
    ROUND(AVG(omny_pct), 2) as avg_omny_adoption_pct,

    -- Weather summary
    ROUND(AVG(avg_temp_f), 1) as avg_temperature,
    SUM(CASE WHEN has_precipitation = 1 THEN 1 ELSE 0 END) as hours_with_rain,
    SUM(CASE WHEN has_snow = 1 THEN 1 ELSE 0 END) as hours_with_snow,

    -- Crime summary
    ROUND(SUM(daily_total_crimes) / COUNT(DISTINCT analysis_date), 0) as avg_daily_crimes

FROM analytics_hourly_subway_v3;

-- ============================================
-- ALL QUERIES COMPLETE
-- ============================================
