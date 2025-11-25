# MTA Subway Data Preprocessing - MapReduce Pipeline

This directory contains MapReduce programs to preprocess MTA subway ridership data for the Urban Mobility and Crime analysis project.

## Overview

The preprocessing pipeline consists of 4 MapReduce jobs:

1. **MTAFilterClean** - Filters and cleans raw MTA data
2. **MTAStationHourly** - Aggregates by station + hour
3. **MTABoroughHourly** - Aggregates by borough + hour
4. **MTACitywideHourly** - Aggregates citywide by hour

## Why 3 Aggregation Levels?

Different analyses require different granularity:
- **Station-level:** Location-based analysis (e.g., "Which stations near high-crime areas see ridership changes?")
- **Borough-level:** Compare with Uber/Lyft data and NYPD arrests (both have borough-level data)
- **Citywide:** Overall trends and correlation with citywide weather patterns

## Data Processing Steps

### Job 1: Filter and Clean (`MTAFilterClean.java`)

**Purpose:** Clean raw MTA data and filter for 2024 records only

**Input:** Raw CSV from `gs://nyu-dataproc-hdfs-ingest/MTA_Subway_Hourly_Ridership__2020-2024.csv`

**Processing:**
- Filters records to include only 2024 data
- Validates and normalizes timestamps to hourly format (YYYY-MM-DD HH:00:00)
- Handles null/invalid values in columns:
  - Ridership: defaults to 0 if invalid
  - Transfers: defaults to 0 if invalid
  - Station ID: defaults to "UNKNOWN" if missing
  - Borough: defaults to "UNKNOWN" if missing
  - Payment method: defaults to "UNKNOWN" if missing
  - Coordinates: validates NYC lat/lon ranges (40.4-41.0, -74.3 to -73.7)
- Tracks data quality metrics via Hadoop counters

**Output Format:**
```
timestamp,station_complex_id,station_complex,borough,payment_method,fare_class_category,ridership,transfers,latitude,longitude
```

**Data Quality Report:** Prints counters for:
- Total records processed
- Valid records output
- Records filtered (wrong year)
- Invalid timestamps, stations, ridership, transfers, coordinates
- Null boroughs, payment methods
- Records with zero ridership

### Job 2: Station-Level Hourly Aggregation (`MTAStationHourly.java`)

**Purpose:** Aggregate ridership data by station and hour

**Input:** Output from Job 1 (cleaned data)

**Processing:**
- Groups by: timestamp + station_id + station_name + borough + coordinates
- Aggregates:
  - Total ridership (sum)
  - Total transfers (sum)
  - Record count
  - Payment method distribution (Metrocard vs OMNY)
  - Fare category distribution (Full Fare, Senior/Disability, Unlimited, Student, Fair Fare, Other)

**Output Format:**
```
timestamp,station_id,station_name,borough,latitude,longitude,total_ridership,total_transfers,record_count,metrocard_txns,omny_txns,full_fare_txns,senior_disability_txns,unlimited_txns,student_txns,fair_fare_txns,other_txns
```

**Use Case:** Station-level analysis, joining with weather/crime data by location and time

### Job 3: Borough-Level Hourly Aggregation (`MTABoroughHourly.java`)

**Purpose:** Aggregate ridership data by borough and hour

**Input:** Output from Job 1 (cleaned data)

**Processing:**
- Groups by: timestamp + borough
- Aggregates:
  - Total ridership (sum)
  - Total transfers (sum)
  - Station count
  - Payment method breakdown (Metrocard vs OMNY ridership)
  - Fare category breakdown

**Output Format:**
```
timestamp,borough,total_ridership,total_transfers,station_count,metrocard_ridership,omny_ridership,full_fare_ridership,senior_disability_ridership,unlimited_ridership,student_ridership,fair_fare_ridership,other_ridership
```

**Use Case:**
- Compare subway vs Uber/Lyft usage by borough (HVFHV data has borough/zone)
- Join with NYPD arrest data (has borough field)
- Mode choice analysis during different weather conditions

### Job 4: Citywide Hourly Aggregation (`MTACitywideHourly.java`)

**Purpose:** Aggregate total citywide ridership by hour

**Input:** Output from Job 1 (cleaned data)

**Processing:**
- Groups by: timestamp only
- Aggregates:
  - Total system ridership and transfers
  - Borough breakdown (Manhattan, Brooklyn, Queens, Bronx, Staten Island)
  - Payment method breakdown (Metrocard, OMNY)
  - Fare category breakdown

**Output Format:**
```
timestamp,total_ridership,total_transfers,total_stations,manhattan_ridership,brooklyn_ridership,queens_ridership,bronx_ridership,staten_island_ridership,metrocard_ridership,omny_ridership,full_fare_ridership,senior_disability_ridership,unlimited_ridership,student_ridership,fair_fare_ridership
```

**Use Case:** Citywide trend analysis, correlation with citywide weather/crime patterns

## Usage

### Quick Start with Makefile (Recommended)

The project includes a Makefile for easy compilation and pipeline execution.

**Available commands:**
```bash
make help          # Show all available commands
make compile       # Compile all Java files and create JAR files
make run           # Compile and run the complete pipeline
make clean         # Remove compiled classes and JAR files
make clean-output  # Remove HDFS output directories (with confirmation)
make clean-all     # Clean both local and HDFS files
make verify        # Check what JAR and class files exist
make status        # Show pipeline status (local JARs + HDFS output)
```

**Typical workflow:**
```bash
# First time - compile the code
make compile

# Run the complete pipeline
make run

# Start fresh (clean everything and rerun)
make clean-all
make run
```

### Alternative: Using Shell Scripts

If you prefer to use the shell scripts directly:

#### 1. Compile the Programs

```bash
chmod +x compile.sh
./compile.sh
```

This creates four JAR files:
- `mta-filter-clean.jar` (Job 1: Filter & Clean)
- `mta-station-hourly.jar` (Job 2: Station-level aggregation)
- `mta-borough-hourly.jar` (Job 3: Borough-level aggregation)
- `mta-citywide-hourly.jar` (Job 4: Citywide aggregation)

#### 2. Run the Complete Pipeline

The output path is pre-configured for HDFS:
```bash
OUTPUT_BASE="project/preprocessing/mta_processed"
```

Run the pipeline:
```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

### 3. Run Individual Jobs (for testing)

**Job 1 - Filter and Clean:**
```bash
hadoop jar mta-filter-clean.jar MTAFilterClean \
    gs://nyu-dataproc-hdfs-ingest/MTA_Subway_Hourly_Ridership__2020-2024.csv \
    project/preprocessing/mta_processed/cleaned
```

**Job 2 - Station Hourly:**
```bash
hadoop jar mta-station-hourly.jar MTAStationHourly \
    project/preprocessing/mta_processed/cleaned/part-* \
    project/preprocessing/mta_processed/station_hourly
```

**Job 3 - Borough Hourly:**
```bash
hadoop jar mta-borough-hourly.jar MTABoroughHourly \
    project/preprocessing/mta_processed/cleaned/part-* \
    project/preprocessing/mta_processed/borough_hourly
```

**Job 4 - Citywide Hourly:**
```bash
hadoop jar mta-citywide-hourly.jar MTACitywideHourly \
    project/preprocessing/mta_processed/cleaned/part-* \
    project/preprocessing/mta_processed/citywide_hourly
```

## Output Structure

After running the pipeline, you'll have:

```
project/preprocessing/mta_processed/
├── cleaned/           # Job 1: Cleaned 2024 data (~30M rows)
│   └── part-r-00000
├── station_hourly/    # Job 2: Station-level hourly (~500 MB)
│   └── part-r-00000
├── borough_hourly/    # Job 3: Borough-level hourly (~44K rows)
│   └── part-r-00000
└── citywide_hourly/   # Job 4: Citywide hourly (~8.7K rows)
    └── part-r-00000
```

View outputs:
```bash
hadoop fs -ls project/preprocessing/mta_processed/
hadoop fs -cat project/preprocessing/mta_processed/borough_hourly/part-r-00000 | head -10
```

## Next Steps

1. **Load into Trino:** Create external tables in Trino pointing to the output directories
2. **Join with Weather Data:** Use timestamp to join with hourly weather data
3. **Join with Crime Data:** Use timestamp and location to correlate with NYPD arrest data
4. **Join with Uber/Lyft Data:** Compare subway vs ride-hailing trends

## Column Selection Rationale

**Kept Columns:**
- `transit_timestamp` - Essential for time-based joins
- `station_complex_id`, `station_complex` - Location identification
- `borough` - Geographic analysis
- `payment_method`, `fare_class_category` - Ridership demographics
- `ridership`, `transfers` - Core metrics
- `latitude`, `longitude` - Spatial joins

**Dropped Columns:**
- `transit_mode` - All records are "subway" (constant)
- `Georeference` - Redundant with lat/lon

## Data Quality Considerations

1. **Zero Ridership:** Records with 0 ridership are kept but flagged (tracked via counter)
2. **Invalid Coordinates:** Set to empty string if out of NYC range
3. **Date Filtering:** Only 2024 data is retained (filters ~18GB down to ~4.5GB)
4. **Null Handling:** Uses "UNKNOWN" placeholder instead of dropping records

## Performance Notes

- **Input Size:** ~18 GB (121M rows covering 2020-2024)
- **Expected Outputs (2024 only):**
  - Cleaned: ~4.5 GB (30M rows)
  - Station hourly: ~500 MB
  - Borough hourly: ~2 MB (5 boroughs × 8,760 hours)
  - Citywide hourly: ~400 KB (8,760 hours)
- **Recommended Cluster:** 1 master + 3-5 worker nodes
- **Execution Time:** ~15-20 minutes for complete pipeline (4 jobs)
