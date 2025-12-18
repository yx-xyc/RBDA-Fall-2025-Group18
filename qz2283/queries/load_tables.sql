CREATE EXTERNAL TABLE IF NOT EXISTS crime_data (
    arrest_date STRING,
    offense_type STRING,
    latitude DOUBLE,
    longitude DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/qz2283_nyu_edu/processed/crime_daily';



DROP TABLE IF EXISTS mta_subway_stations;

CREATE EXTERNAL TABLE mta_subway_stations (
    complex_id INT,
    is_complex BOOLEAN,
    num_stations_in_complex INT,
    stop_name STRING,
    display_name STRING,
    constituent_station_names STRING,
    station_ids STRING,
    gtfs_stop_ids STRING,
    borough STRING,
    cbd BOOLEAN,
    daytime_routes STRING,
    structure_type STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    ada INT,
    ada_notes STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/user/qz2283_nyu_edu/mat_complex'
TBLPROPERTIES ("skip.header.line.count"="1");



CREATE EXTERNAL TABLE IF NOT EXISTS mta_ridership (
    ride_time TIMESTAMP,
    complex_id INT,
    payment_type STRING,
    rider_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/shared/group18/mta_processed/station_hourly';