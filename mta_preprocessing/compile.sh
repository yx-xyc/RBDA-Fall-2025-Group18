#!/bin/bash

# Compilation script for MTA MapReduce jobs
# Run this on your Dataproc cluster or local machine with Hadoop installed

# Exit on any error
set -e

echo "Compiling MTA MapReduce jobs..."

# Set Hadoop classpath
export HADOOP_CLASSPATH=$(hadoop classpath)

# Create output directory for compiled classes
mkdir -p classes

# Compile all Java files
echo "Compiling MTAFilterClean.java (Cleaning Job)..."
javac -classpath $HADOOP_CLASSPATH -d classes MTAFilterClean.java
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to compile MTAFilterClean.java"
    exit 1
fi

echo "Compiling MTAStationHourly.java (Aggregation Job)..."
javac -classpath $HADOOP_CLASSPATH -d classes MTAStationHourly.java
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to compile MTAStationHourly.java"
    exit 1
fi

# Verify class files were created
echo "Verifying compiled class files..."
ls -la classes/

# Create JAR files
echo "Creating JAR files..."
cd classes
jar -cvf ../mta-filter-clean.jar MTAFilterClean*.class
jar -cvf ../mta-station-hourly.jar MTAStationHourly*.class
cd ..

echo "Compilation complete!"
echo "Generated JAR files:"
echo "  - mta-filter-clean.jar          (Cleaning Job)"
echo "  - mta-station-hourly.jar        (Aggregation Job)"
