CREATE TABLE analysis_station_crime_ridership_scatter AS
WITH station_crime_counts AS (
    SELECT
        s.complex_id,
        s.stop_name,
        s.borough,
        s.latitude,
        s.longitude,
        COUNT(*) as total_crimes,
        COUNT(DISTINCT c.arrest_date) as days_with_crimes
    FROM mta_subway_stations s
    LEFT JOIN crime_data c ON
        111.045 * DEGREES(ACOS(
            COS(RADIANS(c.latitude)) * COS(RADIANS(s.latitude)) *
            COS(RADIANS(c.longitude) - RADIANS(s.longitude)) +
            SIN(RADIANS(c.latitude)) * SIN(RADIANS(s.latitude))
        )) < 0.5
    GROUP BY s.complex_id, s.stop_name, s.borough, s.latitude, s.longitude
),
station_ridership AS (
    SELECT
        complex_id,
        AVG(rider_count) as avg_hourly_riders,
        SUM(rider_count) as total_riders,
        COUNT(DISTINCT DATE(ride_time)) as days_observed
    FROM mta_ridership
    GROUP BY complex_id
)
SELECT
    c.complex_id,
    c.stop_name,
    c.borough,
    c.latitude,
    c.longitude,
    c.total_crimes,
    ROUND(c.total_crimes * 1.0 / GREATEST(c.days_with_crimes, 1), 3) as crimes_per_day,
    ROUND(r.avg_hourly_riders, 2) as avg_hourly_riders,
    ROUND(r.total_riders, 0) as total_riders,
    r.days_observed
FROM station_crime_counts c
JOIN station_ridership r ON c.complex_id = r.complex_id
ORDER BY c.total_crimes;

-- Export to HDFS
INSERT OVERWRITE DIRECTORY '/user/qz2283_nyu_edu/analysis_results/crime_ridership_scatter'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT
    complex_id,
    stop_name,
    borough,
    latitude,
    longitude,
    total_crimes,
    crimes_per_day,
    avg_hourly_riders,
    total_riders,
    days_observed
FROM analysis_station_crime_ridership_scatter;