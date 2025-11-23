#!/bin/bash

# Compilation script for MTA MapReduce jobs
# Run this on your Dataproc cluster or local machine with Hadoop installed

echo "Compiling MTA MapReduce jobs..."

# Set Hadoop classpath
export HADOOP_CLASSPATH=$(hadoop classpath)

# Create output directory for compiled classes
mkdir -p classes

# Compile all Java files
echo "Compiling MTAFilterClean.java..."
javac -classpath $HADOOP_CLASSPATH -d classes MTAFilterClean.java

echo "Compiling MTAStationHourly.java..."
javac -classpath $HADOOP_CLASSPATH -d classes MTAStationHourly.java

echo "Compiling MTABoroughHourly.java..."
javac -classpath $HADOOP_CLASSPATH -d classes MTABoroughHourly.java

echo "Compiling MTACitywideHourly.java..."
javac -classpath $HADOOP_CLASSPATH -d classes MTACitywideHourly.java

# Create JAR files
echo "Creating JAR files..."
jar -cvf mta-filter-clean.jar -C classes MTAFilterClean*.class
jar -cvf mta-station-hourly.jar -C classes MTAStationHourly*.class
jar -cvf mta-borough-hourly.jar -C classes MTABoroughHourly*.class
jar -cvf mta-citywide-hourly.jar -C classes MTACitywideHourly*.class

echo "Compilation complete!"
echo "Generated JAR files:"
echo "  - mta-filter-clean.jar          (Job 1: Filter & Clean)"
echo "  - mta-station-hourly.jar        (Job 2: Station-level aggregation)"
echo "  - mta-borough-hourly.jar        (Job 3: Borough-level aggregation)"
echo "  - mta-citywide-hourly.jar       (Job 4: Citywide aggregation)"
