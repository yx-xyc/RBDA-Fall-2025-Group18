-- Step 1: Define the raw data schema for the external CSV files
DROP TABLE IF EXISTS rideshare_rawdata;
CREATE TABLE rideshare_rawdata (
    hvfhs_license_num VARCHAR,
    dispatching_base_num VARCHAR,
    originating_base_num VARCHAR,
    request_datetime VARCHAR,
    on_scene_datetime VARCHAR,
    pickup_datetime VARCHAR,
    dropoff_datetime VARCHAR,
    pu_location INT,
    do_location INT,
    trip_miles DOUBLE,
    trip_time_sec BIGINT,
    base_fare DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    shared_request_flag VARCHAR,
    shared_match_flag VARCHAR,
    access_a_ride_flag VARCHAR,
    wav_request_flag VARCHAR,
    wav_match_flag VARCHAR
)
WITH (
    external_location = '/shared/group18/rideshare_cleaned/',
    format = 'CSV',
    csv_separator = ',',
    skip_header_line_count = 0
);

-- Step 2: Aggregate data into hourly segments for faster analysis
CREATE TABLE rideshare_hourly AS
SELECT
    date_hour,
    CAST(SUBSTRING(date_hour, 1, 10) AS DATE) as date,
    CAST(SUBSTRING(date_hour, 12, 2) AS INT) as hour,
    COUNT(*) as total_trips,
    SUM(CASE WHEN company = 'Uber' THEN 1 ELSE 0 END) as uber_trips,
    SUM(CASE WHEN company = 'Lyft' THEN 1 ELSE 0 END) as lyft_trips,
    SUM(CASE WHEN pickup_borough = 'Manhattan' THEN 1 ELSE 0 END) as manhattan_pickups,
    SUM(CASE WHEN pickup_borough = 'Brooklyn' THEN 1 ELSE 0 END) as brooklyn_pickups,
    SUM(CASE WHEN pickup_borough = 'Queens' THEN 1 ELSE 0 END) as queens_pickups,
    SUM(CASE WHEN pickup_borough = 'Bronx' THEN 1 ELSE 0 END) as bronx_pickups,
    SUM(CASE WHEN pickup_borough = 'Staten Island' THEN 1 ELSE 0 END) as staten_island_pickups,
    SUM(CASE WHEN pickup_borough = 'EWR' THEN 1 ELSE 0 END) as newark_pickups,
    ROUND(AVG(trip_miles), 2) as avg_distance,
    ROUND(AVG(trip_time_min), 2) as avg_duration_min,
    ROUND(AVG(base_fare), 2) as avg_fare,
    ROUND(AVG(tips), 2) as avg_tips
FROM rideshare_complete
GROUP BY date_hour;

-- Step 3: Create the Master Analysis Table (Joining Weather and Rideshare)
CREATE TABLE weather_rideshare_analysis AS
SELECT
    r.date,
    r.hour,
    r.date_hour,
    r.total_trips,
    r.uber_trips,
    r.lyft_trips,
    r.avg_fare,
    r.avg_distance,
    
    -- Weather Columns
    w.temperature,
    w.precipitation,
    w.wind_speed,
    
    -- Day of Week
    CASE DAYOFWEEK(r.date)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END as day_of_week,
    
    CASE 
        WHEN DAYOFWEEK(r.date) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END as day_type,

    -- Rain Categories
    CASE
        WHEN w.precipitation = 0 THEN 'No Rain'
        WHEN w.precipitation < 0.1 THEN 'Light Rain'
        WHEN w.precipitation < 0.3 THEN 'Moderate Rain'
        ELSE 'Heavy Rain'
    END as rain_category,

    CASE 
        WHEN w.precipitation > 0 THEN 'Rainy'
        ELSE 'Clear'
    END as rain_simple,

    -- Temperature Categories
    CASE
        WHEN w.temperature < 32 THEN 'Freezing'
        WHEN w.temperature < 45 THEN 'Cold'
        WHEN w.temperature < 60 THEN 'Cool'
        WHEN w.temperature < 75 THEN 'Comfortable'
        WHEN w.temperature < 85 THEN 'Warm'
        ELSE 'Hot'
    END as temp_category

FROM rideshare_hourly r
JOIN weather_data w ON r.date_hour = w.date_hour;