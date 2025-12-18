
--  percentile thresholds
CREATE TABLE crime_percentiles AS
SELECT 
    PERCENTILE(total_crimes, 0.33) as p33_threshold,
    PERCENTILE(total_crimes, 0.67) as p67_threshold,
    MIN(total_crimes) as min_crimes,
    MAX(total_crimes) as max_crimes,
    AVG(total_crimes) as avg_crimes,
    PERCENTILE(total_crimes, 0.50) as median_crimes
FROM analysis_station_crime_ridership_scatter;

-- View 
SELECT * FROM crime_percentiles;

-- Select representative stations 
CREATE TABLE selected_stations_for_analysis AS
WITH percentiles AS (
    SELECT 
        p33_threshold,
        p67_threshold
    FROM crime_percentiles
),
classified_stations AS (
    SELECT 
        s.complex_id,
        s.stop_name,
        s.borough,
        s.total_crimes,
        s.avg_hourly_riders,
        CASE 
            WHEN s.total_crimes <= p.p33_threshold THEN 'Low Crime (Bottom 33%)'
            WHEN s.total_crimes <= p.p67_threshold THEN 'Medium Crime (Middle 34%)'
            ELSE 'High Crime (Top 33%)'
        END as crime_category,
        CASE 
            WHEN s.total_crimes <= p.p33_threshold THEN 1
            WHEN s.total_crimes <= p.p67_threshold THEN 2
            ELSE 3
        END as category_order
    FROM analysis_station_crime_ridership_scatter s
    CROSS JOIN percentiles p
    WHERE s.total_crimes > 0
),
ranked_within_category AS (
    SELECT 
        complex_id,
        stop_name,
        borough,
        total_crimes,
        avg_hourly_riders,
        crime_category,
        category_order,
        ROW_NUMBER() OVER (
            PARTITION BY crime_category 
            ORDER BY avg_hourly_riders DESC
        ) as popularity_rank,
        ROW_NUMBER() OVER (
            PARTITION BY crime_category 
            ORDER BY total_crimes DESC
        ) as crime_rank
    FROM classified_stations
)
SELECT 
    complex_id,
    stop_name,
    borough,
    total_crimes,
    avg_hourly_riders,
    crime_category,
    category_order
FROM ranked_within_category
WHERE 
    -- Select most popular (busy) and highest crime station from each category
    (popularity_rank = 1 OR crime_rank = 1)
ORDER BY category_order, total_crimes DESC;


SELECT 
    s.*,
    CONCAT('Crimes: ', CAST(s.total_crimes AS STRING), 
           ' (Range: ', CAST(p.min_crimes AS STRING), '-', CAST(p.max_crimes AS STRING), ')') as crime_context
FROM selected_stations_for_analysis s
CROSS JOIN crime_percentiles p
ORDER BY s.category_order, s.total_crimes DESC;

CREATE TABLE analysis_daily_selected_stations AS
SELECT 
    DATE(r.ride_time) as ride_date,
    r.complex_id,
    s.stop_name,
    s.borough,
    ss.crime_category,
    ss.total_crimes as total_crimes_2024,
    SUM(r.rider_count) as daily_riders,
    COUNT(c.offense_type) as crimes_that_day,
    COLLECT_LIST(c.offense_type)[0] as sample_crime_type
FROM mta_ridership r
JOIN mta_subway_stations s ON r.complex_id = s.complex_id
JOIN selected_stations_for_analysis ss ON r.complex_id = ss.complex_id
LEFT JOIN crime_data c ON 
    DATE(r.ride_time) = c.arrest_date
    AND 111.045 * DEGREES(ACOS(
        COS(RADIANS(c.latitude)) * COS(RADIANS(s.latitude)) *
        COS(RADIANS(c.longitude) - RADIANS(s.longitude)) +
        SIN(RADIANS(c.latitude)) * SIN(RADIANS(s.latitude))
    )) < 0.5
GROUP BY 
    DATE(r.ride_time), 
    r.complex_id, 
    s.stop_name, 
    s.borough, 
    ss.crime_category,
    ss.total_crimes
ORDER BY ss.crime_category, r.complex_id, ride_date; 

-- Verify
SELECT * FROM selected_stations_for_analysis
ORDER BY category_order, total_crimes DESC;

SELECT * FROM analysis_daily_selected_stations
WHERE ride_date = '2024-01-01'
ORDER BY complex_id;

SELECT
    crime_category,
    COUNT(DISTINCT complex_id) as num_stations,
    COUNT(*) as total_days
FROM analysis_daily_selected_stations
GROUP BY crime_category;

-- Export selected stations list
INSERT OVERWRITE DIRECTORY '/user/qz2283_nyu_edu/analysis_results/selected_stations'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    complex_id,
    stop_name,
    borough,
    total_crimes,
    avg_hourly_riders,
    crime_category,
    category_order
FROM selected_stations_for_analysis
ORDER BY category_order, total_crimes DESC;

-- Export daily data
INSERT OVERWRITE DIRECTORY '/user/qz2283_nyu_edu/analysis_results/daily_selected_stations'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    ride_date,
    complex_id,
    stop_name,
    borough,
    crime_category,
    total_crimes_2024,
    daily_riders,
    crimes_that_day,
    sample_crime_type
FROM analysis_daily_selected_stations
ORDER BY crime_category, complex_id, ride_date;



