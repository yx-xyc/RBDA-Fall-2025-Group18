# Weather, Subway, and Crime - Big Data Analytics Project

NYU CSCI-GA-2436 (Real-time & Big Data Analytics) - Fall 2025 - Group 18

## Project Overview

This project analyzes the relationship between weather, urban mobility, and crime in NYC for 2024 by integrating three datasets:

- **Weather**: Hourly weather conditions (temperature, precipitation, wind, visibility)
- **MTA Subway**: Hourly ridership by station and payment method
- **Crime**: Daily crime statistics by type and borough

## Architecture

### Phase 1: MapReduce Preprocessing (HDFS)
- MTA data: 2-stage pipeline (cleaning → aggregation)
- Weather/Crime: Pre-processed and stored in HDFS

### Phase 2: Data Integration (Trino/Hive)
- Create external tables for all datasets
- Join into single analytical table (`analytics_hourly_subway`)
- Run analytical queries exploring weather/subway/crime relationships

## Quick Start

### 1. Run MTA MapReduce Preprocessing

```bash
# MTA data processing
cd preprocessing/mta_preprocessing
make run
```

### 2. Verify Other Datasets in HDFS

```bash
# Check weather data
hadoop fs -ls project/preprocessing/weather_hourly | head -5

# Check crime data
hadoop fs -ls project/preprocessing/crime_daily | head -5
```

### 3. Run Trino Integration

```bash
cd analytics

# Create external tables
trino --catalog hive --schema default -f 1_create_external_tables.sql

# Create integrated analytical table
trino --catalog hive --schema default -f 2_analytics_integration.sql

# Run sample queries
trino --catalog hive --schema default -f 3_sample_queries.sql
```

## Directory Structure

```
.
├── preprocessing/
│   ├── mta_preprocessing/          # MTA MapReduce jobs
│   ├── rideshare/                  # Rideshare (future work)
│   ├── crime/                      # Crime preprocessing (external)
│   └── weather/                    # Weather preprocessing (external)
├── analytics/                      # Trino/Hive SQL integration scripts
├── CLAUDE.md                       # Detailed technical documentation
└── README.md                       # This file
```

## Key Technologies

- **Hadoop MapReduce**: Distributed data processing (MTA filtering/aggregation)
- **HDFS**: Distributed storage for processed datasets
- **Trino/Hive**: SQL query engine for data integration and analysis
- **Parquet**: Columnar storage format for efficient querying

## Documentation

- **CLAUDE.md**: Comprehensive technical documentation including:
  - Build and run commands
  - MapReduce job details and schemas
  - HDFS paths
  - Development notes

- **analytics/README.md**: Analytics pipeline documentation including:
  - SQL execution order
  - Table schemas
  - Sample queries overview
  - Key research questions

## Final Analytical Table

The `analytics_hourly_subway` table integrates all datasets with these dimensions:

- **Time**: Hour, day of week, weekday/weekend
- **Weather**: Temperature, precipitation, snow, wind, visibility
- **MTA Subway**: Ridership, payment methods (MetroCard vs OMNY)
- **Crime**: Daily crime counts by type and borough

## Key Research Questions

This integrated dataset enables analysis of:

1. **Weather Impact**: How do temperature, precipitation, and snow affect subway ridership?
2. **Temporal Patterns**: What are the hourly and daily ridership patterns?
3. **Extreme Weather**: How do severe weather events impact subway usage?
4. **Crime Correlations**: Is there a relationship between crime levels and subway ridership?
5. **Technology Adoption**: How is OMNY (contactless payment) adoption progressing?
6. **Geographic Patterns**: How does crime distribution across boroughs relate to subway usage?

## Sample Analytical Queries

The project includes 10 sample queries:
1. Weather impact on subway ridership
2. Hourly subway patterns (weekday vs weekend)
3. Temperature effects on ridership
4. Crime and late-night subway usage
5. Snow/ice impact on ridership
6. Peak demand analysis
7. Payment method trends (OMNY vs MetroCard)
8. Severe weather event impact
9. Borough-level crime analysis
10. Summary statistics

## Contributors

Group 18 - NYU CSCI-GA-2436 Fall 2025
