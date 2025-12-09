-- ============================================
-- SAMPLE ANALYTICAL QUERIES
-- ============================================
-- This script contains sample queries to analyze the integrated dataset
-- Run individual queries or all at once: trino --catalog hive --schema default -f 3_sample_queries.sql
--
-- Prerequisites: Run 1_create_external_tables.sql and 2_analytics_integration.sql first
-- ============================================

-- ============================================
-- Query 1: Weather Impact on Transportation Mode Choice
-- ============================================
-- Question: How does precipitation affect the choice between subway and rideshare?

SELECT
    CASE
        WHEN total_precip_inch = 0 THEN 'No Rain'
        WHEN total_precip_inch <= 0.1 THEN 'Light Rain'
        WHEN total_precip_inch <= 0.3 THEN 'Moderate Rain'
        ELSE 'Heavy Rain'
    END as precipitation_level,

    COUNT(*) as hour_count,

    -- Average ridership/usage
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(AVG(total_rideshare_trips), 0) as avg_rideshare_trips,

    -- Mode preference ratio
    ROUND(AVG(subway_to_rideshare_ratio), 2) as avg_subway_to_rideshare_ratio,

    -- Percent change in usage
    ROUND(100.0 * (AVG(total_rideshare_trips) - AVG(CASE WHEN total_precip_inch = 0 THEN total_rideshare_trips END)) /
          NULLIF(AVG(CASE WHEN total_precip_inch = 0 THEN total_rideshare_trips END), 0), 2) as rideshare_pct_change

FROM hive.default.analytics_hourly_mobility
WHERE total_subway_ridership > 0 OR total_rideshare_trips > 0
GROUP BY
    CASE
        WHEN total_precip_inch = 0 THEN 'No Rain'
        WHEN total_precip_inch <= 0.1 THEN 'Light Rain'
        WHEN total_precip_inch <= 0.3 THEN 'Moderate Rain'
        ELSE 'Heavy Rain'
    END
ORDER BY precipitation_level;

-- ============================================
-- Query 2: Hourly Mobility Patterns by Day Type
-- ============================================
-- Question: How do mobility patterns differ between weekdays and weekends?

SELECT
    day_type,
    hour_of_day,

    -- Transportation metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(AVG(total_rideshare_trips), 0) as avg_rideshare_trips,
    ROUND(AVG(total_mobility_trips), 0) as avg_total_mobility,

    -- Peak indicators
    CASE
        WHEN hour_of_day BETWEEN 7 AND 9 THEN 'Morning Rush'
        WHEN hour_of_day BETWEEN 17 AND 19 THEN 'Evening Rush'
        WHEN hour_of_day BETWEEN 22 AND 5 THEN 'Late Night'
        ELSE 'Off-Peak'
    END as time_period

FROM hive.default.analytics_hourly_mobility
GROUP BY day_type, hour_of_day
ORDER BY day_type, hour_of_day;

-- ============================================
-- Query 3: Temperature Impact on Mobility
-- ============================================
-- Question: How does temperature affect overall mobility?

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

    -- Mobility metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(AVG(total_rideshare_trips), 0) as avg_rideshare_trips,
    ROUND(AVG(total_mobility_trips), 0) as avg_total_mobility,

    -- Cost metrics
    ROUND(AVG(avg_rideshare_cost), 2) as avg_ride_cost,

    -- Shared ride adoption
    ROUND(AVG(avg_shared_ride_pct), 2) as avg_shared_pct

FROM hive.default.analytics_hourly_mobility
GROUP BY
    CASE
        WHEN avg_temp_f < 20 THEN 'Extreme Cold (<20°F)'
        WHEN avg_temp_f < 32 THEN 'Freezing (20-32°F)'
        WHEN avg_temp_f < 50 THEN 'Cold (32-50°F)'
        WHEN avg_temp_f < 70 THEN 'Moderate (50-70°F)'
        WHEN avg_temp_f < 85 THEN 'Warm (70-85°F)'
        ELSE 'Hot (>85°F)'
    END
ORDER BY
    CASE
        WHEN avg_temp_f < 20 THEN 1
        WHEN avg_temp_f < 32 THEN 2
        WHEN avg_temp_f < 50 THEN 3
        WHEN avg_temp_f < 70 THEN 4
        WHEN avg_temp_f < 85 THEN 5
        ELSE 6
    END;

-- ============================================
-- Query 4: Crime and Mobility Correlation
-- ============================================
-- Question: Is there a relationship between crime levels and late-night mobility?

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

    -- Mobility metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(AVG(total_rideshare_trips), 0) as avg_rideshare_trips,

    -- Safety-related behavior
    ROUND(AVG(CASE WHEN total_rideshare_trips > 0
        THEN 100.0 * total_rideshare_trips / NULLIF(total_mobility_trips, 0)
        ELSE 0 END), 2) as rideshare_pct_of_mobility

FROM hive.default.analytics_hourly_mobility
WHERE daily_total_crimes > 0
GROUP BY
    CASE
        WHEN hour_of_day BETWEEN 22 AND 23 OR hour_of_day BETWEEN 0 AND 5 THEN 'Late Night (10pm-6am)'
        WHEN hour_of_day BETWEEN 6 AND 11 THEN 'Morning (6am-12pm)'
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon (12pm-6pm)'
        ELSE 'Evening (6pm-10pm)'
    END
ORDER BY
    CASE
        WHEN hour_of_day BETWEEN 6 AND 11 THEN 1
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 2
        WHEN hour_of_day BETWEEN 18 AND 21 THEN 3
        ELSE 4
    END;

-- ============================================
-- Query 5: Snow/Ice Impact on Transportation Safety and Usage
-- ============================================
-- Question: How do snow conditions affect transportation choices and rideshare costs?

SELECT
    CASE
        WHEN total_snow_inch = 0 AND avg_snow_depth_inch = 0 THEN 'No Snow'
        WHEN total_snow_inch > 0 THEN 'Actively Snowing'
        WHEN avg_snow_depth_inch > 0 THEN 'Snow on Ground'
        ELSE 'Other'
    END as snow_condition,

    is_freezing as is_below_freezing,

    COUNT(*) as hour_count,

    -- Mobility metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_subway_riders,
    ROUND(AVG(total_rideshare_trips), 0) as avg_rideshare_trips,

    -- Cost and duration (may increase in snow)
    ROUND(AVG(avg_rideshare_cost), 2) as avg_ride_cost,
    ROUND(AVG(avg_rideshare_duration_min), 2) as avg_ride_duration_min,

    -- Safety metrics
    ROUND(AVG(daily_vehicle_crime_count), 1) as avg_vehicle_crimes

FROM hive.default.analytics_hourly_mobility
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
-- Query 6: Peak Demand Analysis (Rush Hours)
-- ============================================
-- Question: What are the busiest times and what factors contribute to peak demand?

SELECT
    DATE(hour_timestamp) as date,
    hour_of_day,
    day_type,

    -- Mobility metrics
    total_mobility_trips,
    total_subway_ridership,
    total_rideshare_trips,
    subway_to_rideshare_ratio,

    -- Weather conditions
    avg_temp_f,
    total_precip_inch,
    dom_conditions,

    -- Rank by total mobility
    ROW_NUMBER() OVER (ORDER BY total_mobility_trips DESC) as mobility_rank

FROM hive.default.analytics_hourly_mobility
WHERE total_mobility_trips > 0
ORDER BY total_mobility_trips DESC
LIMIT 20;

-- ============================================
-- Query 7: Payment Method Trends (OMNY vs MetroCard)
-- ============================================
-- Question: How is OMNY adoption progressing and does it correlate with other factors?

SELECT
    DATE_TRUNC('week', hour_timestamp) as week,

    -- OMNY adoption metrics
    ROUND(AVG(omny_pct), 2) as avg_omny_pct,
    SUM(omny_ridership) as total_omny_rides,
    SUM(metrocard_ridership) as total_metrocard_rides,

    -- Total subway usage
    SUM(total_subway_ridership) as total_subway_rides,

    -- Weather correlation
    ROUND(AVG(avg_temp_f), 1) as avg_temp,
    ROUND(AVG(total_precip_inch), 2) as avg_precip

FROM hive.default.analytics_hourly_mobility
WHERE total_subway_ridership > 0
GROUP BY DATE_TRUNC('week', hour_timestamp)
ORDER BY week;

-- ============================================
-- Query 8: Adverse Weather Event Impact
-- ============================================
-- Question: How do severe weather events impact mobility?

SELECT
    DATE(hour_timestamp) as date,
    day_type,

    -- Weather severity indicators
    MAX(total_precip_inch) as max_precip,
    MAX(total_snow_inch) as max_snow,
    MIN(avg_temp_f) as min_temp,
    MIN(avg_visibility_miles) as min_visibility,

    -- Daily mobility totals
    SUM(total_subway_ridership) as daily_subway_riders,
    SUM(total_rideshare_trips) as daily_rideshare_trips,
    SUM(total_mobility_trips) as daily_total_mobility,

    -- Average hourly metrics
    ROUND(AVG(total_subway_ridership), 0) as avg_hourly_subway,
    ROUND(AVG(total_rideshare_trips), 0) as avg_hourly_rideshare,

    -- Identify severe weather days
    CASE
        WHEN MAX(total_precip_inch) > 0.5 THEN 'Heavy Rain'
        WHEN MAX(total_snow_inch) > 3 THEN 'Heavy Snow'
        WHEN MIN(avg_temp_f) < 15 THEN 'Extreme Cold'
        WHEN MIN(avg_visibility_miles) < 3 THEN 'Low Visibility'
        ELSE 'Normal'
    END as weather_event_type

FROM hive.default.analytics_hourly_mobility
GROUP BY DATE(hour_timestamp), day_type
HAVING
    MAX(total_precip_inch) > 0.5
    OR MAX(total_snow_inch) > 3
    OR MIN(avg_temp_f) < 15
    OR MIN(avg_visibility_miles) < 3
ORDER BY date DESC;

-- ============================================
-- Query 9: Borough-Level Crime Analysis
-- ============================================
-- Question: How does crime distribution across boroughs relate to overall mobility?

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

    -- Mobility metrics
    AVG(total_subway_ridership) as avg_hourly_subway,
    AVG(total_rideshare_trips) as avg_hourly_rideshare,

    -- Crime-to-mobility ratio
    ROUND(daily_total_crimes * 1.0 / NULLIF(SUM(total_mobility_trips), 0) * 1000, 2) as crimes_per_1000_trips

FROM hive.default.analytics_hourly_mobility
WHERE daily_total_crimes > 0
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

    -- Mobility totals
    SUM(total_subway_ridership) as total_subway_rides,
    SUM(total_rideshare_trips) as total_rideshare_trips,
    SUM(total_mobility_trips) as total_mobility_all_modes,

    -- Averages
    ROUND(AVG(total_subway_ridership), 0) as avg_hourly_subway,
    ROUND(AVG(total_rideshare_trips), 0) as avg_hourly_rideshare,
    ROUND(AVG(avg_rideshare_cost), 2) as overall_avg_ride_cost,
    ROUND(AVG(avg_rideshare_duration_min), 2) as overall_avg_ride_duration,

    -- Weather summary
    ROUND(AVG(avg_temp_f), 1) as avg_temperature,
    SUM(CASE WHEN has_precipitation = 1 THEN 1 ELSE 0 END) as hours_with_rain,
    SUM(CASE WHEN has_snow = 1 THEN 1 ELSE 0 END) as hours_with_snow,

    -- Crime summary
    SUM(daily_total_crimes) / COUNT(DISTINCT analysis_date) as avg_daily_crimes

FROM hive.default.analytics_hourly_mobility;

-- ============================================
-- ALL QUERIES COMPLETE
-- ============================================
