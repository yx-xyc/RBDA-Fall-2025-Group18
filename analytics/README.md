# Urban Mobility and Crime Analytics

This directory contains Trino/Hive SQL scripts for Phase 2 of the Urban Mobility and Crime Big Data project: Data Integration and Analytics.

## Overview

The analytics pipeline integrates four datasets:
- **Weather** (hourly) - Temperature, precipitation, wind, visibility
- **MTA Subway** (hourly) - Ridership by station and payment method
- **Rideshare/HVFHV** (hourly by location) - Trip counts, costs, durations
- **Crime** (daily) - Crime counts by type and borough

## Prerequisites

Before running these SQL scripts, you must complete the MapReduce preprocessing:

1. **MTA Preprocessing**
   ```bash
   cd ../preprocessing/mta_preprocessing
   make run-mta
   ```

2. **Rideshare Aggregation** (Important: Use MapReduce, not Trino CTAS)
   ```bash
   cd ../preprocessing/mta_preprocessing
   make run-rideshare
   ```

## Execution Order

Run the SQL scripts in this exact order:

### Step 1: Create External Tables
```bash
trino --catalog hive --schema default -f 1_create_external_tables.sql
```

Creates external tables pointing to:
- `weather_hourly` - Weather metrics (Parquet)
- `mta_hourly_agg` - Citywide subway ridership (Parquet)
- `rideshare_hourly_agg` - Rideshare trips by hour/location (Parquet, from MapReduce output)
- `crime_daily_agg` - Daily crime statistics (Parquet)

### Step 2: Analytics Integration
```bash
trino --catalog hive --schema default -f 2_analytics_integration.sql
```

Creates the final analytical table:
- `analytics_hourly_mobility` - Joined dataset with all metrics (Parquet, partitioned by date)

### Step 3: Sample Queries
```bash
trino --catalog hive --schema default -f 3_sample_queries.sql
```

Runs 10 analytical queries including:
1. Weather impact on mode choice
2. Hourly mobility patterns
3. Temperature effects
4. Crime and mobility correlation
5. Snow/ice impact
6. Peak demand analysis
7. Payment method trends
8. Adverse weather events
9. Borough-level crime
10. Summary statistics

## Key Differences from Original Plan

**Original Plan**: Use Trino CTAS to aggregate 68GB of raw rideshare data
**Updated Approach**: Use MapReduce for rideshare aggregation

**Why**: MapReduce is better suited for aggregating large raw datasets (68GB). The Trino scripts now simply create external tables pointing to the MapReduce output.

## HDFS Paths

### Input Data (MapReduce Outputs)
- Weather: `hdfs:///user/yx2021_nyu_edu/project/preprocessing/weather_hourly`
- MTA: `hdfs:///user/yx2021_nyu_edu/project/preprocessing/mta_processed/station_hourly`
- Rideshare: `hdfs:///user/yx2021_nyu_edu/project/preprocessing/rideshare_processed/hourly_agg`
- Crime: `hdfs:///user/yx2021_nyu_edu/project/preprocessing/crime_daily`

### Output Tables
All tables stored in Hive default database as Parquet format.

## Schema Overview

### Analytics Table Schema
```sql
analytics_hourly_mobility (
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

    -- Rideshare
    total_rideshare_trips INTEGER,
    active_rideshare_locations INTEGER,
    avg_rideshare_cost DOUBLE,
    avg_rideshare_duration_min DOUBLE,
    avg_rideshare_miles DOUBLE,
    avg_shared_ride_pct DOUBLE,

    -- Mobility Comparison
    subway_to_rideshare_ratio DOUBLE,
    total_mobility_trips BIGINT,

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
SELECT 'rideshare_hourly_agg', COUNT(*) FROM hive.default.rideshare_hourly_agg
UNION ALL
SELECT 'crime_daily_agg', COUNT(*) FROM hive.default.crime_daily_agg
UNION ALL
SELECT 'analytics_hourly_mobility', COUNT(*) FROM hive.default.analytics_hourly_mobility;
```

## Next Steps

After creating the analytical table:
1. Run the sample queries to validate data integration
2. Export results for visualization
3. Develop additional analytical queries based on research questions
4. Create dashboards using the integrated dataset
