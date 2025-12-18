-- Query 1: Impact of Rain (Simple)
SELECT 
    rain_simple,
    COUNT(*) as num_hours,
    ROUND(AVG(total_trips), 0) as avg_trips_per_hour,
    ROUND(AVG(avg_fare), 2) as avg_fare,
    ROUND(AVG(avg_tips), 2) as avg_tips,
    ROUND(AVG(avg_distance), 2) as avg_distance
FROM weather_rideshare_analysis
GROUP BY rain_simple
ORDER BY rain_simple;

-- Query 2: Impact of Rain Intensity
SELECT
    rain_category,
    COUNT(*) as num_hours,
    ROUND(AVG(total_trips), 0) as avg_trips_per_hour,
    ROUND(AVG(avg_fare), 2) as avg_fare,
    ROUND(AVG(precipitation), 3) as avg_precipitation
FROM weather_rideshare_analysis
GROUP BY rain_category
ORDER BY 
    CASE rain_category
        WHEN 'No Rain' THEN 1
        WHEN 'Light Rain' THEN 2
        WHEN 'Moderate Rain' THEN 3
        WHEN 'Heavy Rain' THEN 4
    END;

-- Query 3: Impact of Temperature
SELECT
    temp_category,
    COUNT(*) as num_hours,
    ROUND(AVG(temperature), 1) as avg_temp,
    ROUND(AVG(total_trips), 0) as avg_trips_per_hour,
    ROUND(AVG(avg_fare), 2) as avg_fare
FROM weather_rideshare_analysis
GROUP BY temp_category
ORDER BY 
    CASE temp_category
        WHEN 'Freezing' THEN 1
        WHEN 'Cold' THEN 2
        WHEN 'Cool' THEN 3
        WHEN 'Comfortable' THEN 4
        WHEN 'Warm' THEN 5
        WHEN 'Hot' THEN 6
    END;

-- Query 4: Weekday vs Weekend Analysis
SELECT
    day_type,
    rain_simple,
    COUNT(*) as num_hours,
    ROUND(AVG(total_trips), 0) as avg_trips,
    ROUND(AVG(avg_fare), 2) as avg_fare
FROM weather_rideshare_analysis
GROUP BY day_type, rain_simple
ORDER BY day_type, rain_simple;