#!/bin/bash

# Complete MTA Data Preprocessing Pipeline
# This script runs both MapReduce jobs in sequence

# Configuration
INPUT_DATA="gs://nyu-dataproc-hdfs-ingest/group_18/MTA_Subway_Hourly_Ridership__2020-2024.csv"

# Output location - storing in HDFS under your project directory
OUTPUT_BASE="project/preprocessing/mta_processed"

# Output paths for each job
CLEANED_OUTPUT="${OUTPUT_BASE}/cleaned"
STATION_OUTPUT="${OUTPUT_BASE}/station_hourly"

echo "====================================="
echo "MTA Data Preprocessing Pipeline"
echo "====================================="
echo "Input: ${INPUT_DATA}"
echo "Output Base: ${OUTPUT_BASE}"
echo ""

# Cleaning Job: Filter and Clean (2024 data only)
echo "Step 1/2: Cleaning - Filtering and cleaning data (2024 records only)..."
echo "-------------------------------------"
hadoop jar mta-filter-clean.jar MTAFilterClean \
    ${INPUT_DATA} \
    ${CLEANED_OUTPUT}

if [ $? -ne 0 ]; then
    echo "ERROR: Cleaning job failed!"
    exit 1
fi
echo "✓ Step 1 complete: Cleaned data saved to ${CLEANED_OUTPUT}"
echo ""

# Aggregation Job: Station-Level Hourly Aggregation
echo "Step 2/2: Aggregation - Aggregating by station, hour, and payment method..."
echo "-------------------------------------"
hadoop jar mta-station-hourly.jar MTAStationHourly \
    ${CLEANED_OUTPUT}/part-* \
    ${STATION_OUTPUT}

if [ $? -ne 0 ]; then
    echo "ERROR: Aggregation job failed!"
    exit 1
fi
echo "✓ Step 2 complete: Aggregated data saved to ${STATION_OUTPUT}"
echo ""

echo "====================================="
echo "Pipeline completed successfully!"
echo "====================================="
echo ""
echo "Output locations:"
echo "  1. Cleaned data (2024):     ${CLEANED_OUTPUT}"
echo "  2. Aggregated hourly data:  ${STATION_OUTPUT}"
echo ""
echo "Output schema:"
echo "  - Cleaned: timestamp, station_complex_id, payment_method, fare_class_category, ridership"
echo "  - Aggregated: timestamp, station_complex_id, payment_method, total_ridership"
echo ""
echo "Next steps:"
echo "  - Load aggregated output into Trino for querying"
echo "  - Join with MTA_Subway_Stations_and_Complexes.csv to get borough/lat/lon"
echo "  - Join with weather and crime data for analysis"
