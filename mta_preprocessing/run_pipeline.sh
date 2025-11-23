#!/bin/bash

# Complete MTA Data Preprocessing Pipeline
# This script runs all three MapReduce jobs in sequence

# Configuration
INPUT_DATA="gs://nyu-dataproc-hdfs-ingest/MTA_Subway_Hourly_Ridership__2020-2024.csv"

# Output location - storing in HDFS under your project directory
OUTPUT_BASE="project/preprocessing/mta_processed"

# Output paths for each job
CLEANED_OUTPUT="${OUTPUT_BASE}/cleaned"
STATION_OUTPUT="${OUTPUT_BASE}/station_hourly"
BOROUGH_OUTPUT="${OUTPUT_BASE}/borough_hourly"
CITYWIDE_OUTPUT="${OUTPUT_BASE}/citywide_hourly"

echo "====================================="
echo "MTA Data Preprocessing Pipeline"
echo "====================================="
echo "Input: ${INPUT_DATA}"
echo "Output Base: ${OUTPUT_BASE}"
echo ""

# Step 1: Filter and Clean (2024 data only)
echo "Step 1/4: Filtering and cleaning data (2024 records only)..."
echo "-------------------------------------"
hadoop jar mta-filter-clean.jar MTAFilterClean \
    ${INPUT_DATA} \
    ${CLEANED_OUTPUT}

if [ $? -ne 0 ]; then
    echo "ERROR: Filter and clean job failed!"
    exit 1
fi
echo "✓ Step 1 complete: Cleaned data saved to ${CLEANED_OUTPUT}"
echo ""

# Step 2: Station-Level Hourly Aggregation
echo "Step 2/4: Aggregating by station and hour..."
echo "-------------------------------------"
hadoop jar mta-station-hourly.jar MTAStationHourly \
    ${CLEANED_OUTPUT}/part-* \
    ${STATION_OUTPUT}

if [ $? -ne 0 ]; then
    echo "ERROR: Station hourly aggregation job failed!"
    exit 1
fi
echo "✓ Step 2 complete: Station-level hourly data saved to ${STATION_OUTPUT}"
echo ""

# Step 3: Borough-Level Hourly Aggregation
echo "Step 3/4: Aggregating by borough and hour..."
echo "-------------------------------------"
hadoop jar mta-borough-hourly.jar MTABoroughHourly \
    ${CLEANED_OUTPUT}/part-* \
    ${BOROUGH_OUTPUT}

if [ $? -ne 0 ]; then
    echo "ERROR: Borough hourly aggregation job failed!"
    exit 1
fi
echo "✓ Step 3 complete: Borough-level hourly data saved to ${BOROUGH_OUTPUT}"
echo ""

# Step 4: Citywide Hourly Aggregation
echo "Step 4/4: Aggregating citywide metrics..."
echo "-------------------------------------"
hadoop jar mta-citywide-hourly.jar MTACitywideHourly \
    ${CLEANED_OUTPUT}/part-* \
    ${CITYWIDE_OUTPUT}

if [ $? -ne 0 ]; then
    echo "ERROR: Citywide aggregation job failed!"
    exit 1
fi
echo "✓ Step 4 complete: Citywide hourly data saved to ${CITYWIDE_OUTPUT}"
echo ""

echo "====================================="
echo "Pipeline completed successfully!"
echo "====================================="
echo ""
echo "Output locations:"
echo "  1. Cleaned data (2024):     ${CLEANED_OUTPUT}"
echo "  2. Station-level hourly:    ${STATION_OUTPUT}"
echo "  3. Borough-level hourly:    ${BOROUGH_OUTPUT}"
echo "  4. Citywide hourly:         ${CITYWIDE_OUTPUT}"
echo ""
echo "Next steps:"
echo "  - Load these outputs into Trino for querying"
echo "  - Join with weather and crime data"
