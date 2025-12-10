# Weather, Subway, and Crime Analytics

This directory contains Trino/Hive SQL scripts for Phase 2 of the project: Data Integration and Analytics.

## Overview

The analytics pipeline integrates three datasets to analyze the relationships between weather, urban mobility (subway usage), and crime in NYC for 2024:

- **Weather** (hourly) - Temperature, precipitation, wind, visibility
- **MTA Subway** (hourly) - Ridership by station and payment method
- **Crime** (daily) - Crime counts by type and borough

## Prerequisites

Before running these SQL scripts, you must complete the MTA MapReduce preprocessing:

```bash
cd ../preprocessing/mta_preprocessing
make run
```

Verify the other preprocessed datasets exist in HDFS:
- Weather: `project/preprocessing/weather_hourly`
- Crime: `project/preprocessing/crime_daily`

## Execution Order

Run the SQL scripts in this exact order:

### Step 1: Create External Tables
```bash
trino --catalog hive --schema default -f 1_create_external_tables.sql
```

Creates external tables pointing to:
- `weather_hourly` - Weather metrics (Parquet)
- `mta_hourly_agg` - Citywide subway ridership (Parquet)
- `crime_daily_agg` - Daily crime statistics (Parquet)

### Step 2: Analytics Integration
```bash
trino --catalog hive --schema default -f 2_analytics_integration.sql
```

Creates the final analytical table:
- `analytics_hourly_subway` - Joined dataset with all metrics (Parquet, partitioned by date)

### Step 3: Sample Queries
```bash
trino --catalog hive --schema default -f 3_sample_queries.sql
```

Runs 10 analytical queries including:
1. Weather impact on subway ridership
2. Hourly subway patterns (weekday vs weekend)
3. Temperature effects
4. Crime and late-night subway usage
5. Snow/ice impact
6. Peak demand analysis
7. Payment method trends (OMNY vs MetroCard)
8. Severe weather events
9. Borough-level crime analysis
10. Summary statistics

## HDFS Paths

### Input Data (Preprocessed)
- Weather: `hdfs:///user/yx2021_nyu_edu/project/preprocessing/weather_hourly`
- MTA: `hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/station_hourly`
- Crime: `hdfs:///user/yx2021_nyu_edu/project/preprocessing/crime_daily`

### Output Tables
All tables stored in Hive default database as Parquet format.

## Schema Overview

### Analytics Table Schema
```sql
analytics_hourly_subway (
    -- Time
    hour_timestamp TIMESTAMP,
    hour_of_day INTEGER,
    day_of_week INTEGER,
    day_type VARCHAR (Weekday/Weekend),

    -- Weather
    avg_temp_f DOUBLE,
    total_precip_inch DOUBLE,
    total_snow_inch DOUBLE,
    avg_wind_speed_mph DOUBLE,
    avg_visibility_miles DOUBLE,
    dom_conditions VARCHAR,
    has_precipitation INTEGER (0/1),
    has_snow INTEGER (0/1),
    is_freezing INTEGER (0/1),

    -- MTA Subway
    total_subway_ridership BIGINT,
    active_subway_stations INTEGER,
    metrocard_ridership BIGINT,
    omny_ridership BIGINT,
    omny_pct DOUBLE,

    -- Crime (daily)
    daily_total_crimes BIGINT,
    daily_assault_count BIGINT,
    daily_theft_count BIGINT,
    daily_robbery_count BIGINT,
    daily_burglary_count BIGINT,
    daily_vehicle_crime_count BIGINT,
    daily_manhattan_crimes BIGINT,
    daily_brooklyn_crimes BIGINT,
    daily_queens_crimes BIGINT,
    daily_bronx_crimes BIGINT,
    daily_staten_island_crimes BIGINT,

    -- Partition
    analysis_date DATE
)
PARTITIONED BY (analysis_date)
```

## Verification

After running each script, verify the results:

```sql
-- Check table counts
SELECT 'weather_hourly' as table_name, COUNT(*) as records FROM hive.default.weather_hourly
UNION ALL
SELECT 'mta_hourly_agg', COUNT(*) FROM hive.default.mta_hourly_agg
UNION ALL
SELECT 'crime_daily_agg', COUNT(*) FROM hive.default.crime_daily_agg
UNION ALL
SELECT 'analytics_hourly_subway', COUNT(*) FROM hive.default.analytics_hourly_subway;
```

## Key Research Questions

The integrated dataset enables analysis of:

1. **Weather Impact**: How do temperature, precipitation, and snow affect subway ridership?
2. **Temporal Patterns**: What are the hourly and daily ridership patterns? How do weekdays differ from weekends?
3. **Extreme Weather**: How do severe weather events (heavy rain, snow, extreme cold) impact subway usage?
4. **Crime Correlations**: Is there a relationship between crime levels and subway ridership? Does it vary by time of day?
5. **Technology Adoption**: How is OMNY (contactless payment) adoption progressing compared to MetroCard?
6. **Geographic Patterns**: How does crime distribution across boroughs relate to subway usage?

## Next Steps

After creating the analytical table:
1. Run the sample queries to validate data integration
2. Export results for visualization
3. Develop additional analytical queries based on research questions
4. Create dashboards using the integrated dataset
