#!/bin/bash


JOB_NAME="WeatherAggregator"
LOCAL_DATA_FILE="weather_data.csv"

HDFS_INPUT_DIR="project/raw/weather_data_in"
HDFS_OUTPUT_DIR="project/processed/weather_hourly"

HADOOP_CLASSPATH=$(hadoop classpath)

echo "Setting up HDFS environment"

hadoop fs -mkdir -p $HDFS_INPUT_DIR

if [ ! -f "$LOCAL_DATA_FILE" ]; then
    echo "ERROR: Local data file '$LOCAL_DATA_FILE' not found in the current directory. Exiting."
    exit 1
fi

echo "Uploading $LOCAL_DATA_FILE to HDFS..."
hadoop fs -put -f $LOCAL_DATA_FILE $HDFS_INPUT_DIR/

echo "Cleaning up previous HDFS output directory"
hadoop fs -rm -r -f $HDFS_OUTPUT_DIR 2>/dev/null

echo "Compiling Java code"
mkdir -p classes
javac -cp $HADOOP_CLASSPATH -d classes *.java

if [ $? -ne 0 ]; then
    echo "Compilation failed. Exiting."
    exit 1
fi

echo "Creating JAR file "
JAR_FILE="${JOB_NAME}.jar"
jar -cvf $JAR_FILE -C classes/ .

echo "Running MapReduce Job: ${JOB_NAME} "
hadoop jar $JAR_FILE $JOB_NAME $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR

if [ $? -eq 0 ]; then
    echo "--- 6. Job completed successfully."
    echo "Displaying first few aggregated hourly records from $HDFS_OUTPUT_DIR:"
    hadoop fs -cat $HDFS_OUTPUT_DIR/part-r-00000 | head -n 50
else
    echo "Job failed. Check logs for details."
fi

echo "Cleaning up"
rm -rf classes/
rm -f $JAR_FILE