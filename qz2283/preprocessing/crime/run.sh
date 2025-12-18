#!/bin/bash

JOB_NAME="CrimeAggregator"
LOCAL_DATA_FILE="crime.csv"

HDFS_USER_HOME="/user/qz2283_nyu_edu"
HDFS_INPUT_DIR="${HDFS_USER_HOME}/raw/crime_data_in"
HDFS_OUTPUT_DIR="${HDFS_USER_HOME}/processed/crime_daily"

HADOOP_CLASSPATH=$(hadoop classpath)

hadoop fs -mkdir -p $HDFS_INPUT_DIR

if [ ! -f "$LOCAL_DATA_FILE" ]; then
    echo "ERROR: Local data file '$LOCAL_DATA_FILE' not found in the current directory. Exiting."
    exit 1
fi

hadoop fs -put -f $LOCAL_DATA_FILE $HDFS_INPUT_DIR/

hadoop fs -rm -r -f $HDFS_OUTPUT_DIR 2>/dev/null

rm -rf classes/ $JOB_NAME.jar

mkdir -p classes
javac -cp $HADOOP_CLASSPATH -d classes *.java

if [ $? -ne 0 ]; then
    echo "Compilation failed. Exiting."
    exit 1
fi

JAR_FILE="${JOB_NAME}.jar"
jar -cvf $JAR_FILE -C classes/ .

hadoop jar $JAR_FILE $JOB_NAME $HDFS_INPUT_DIR $HDFS_OUTPUT_DIR

if [ $? -eq 0 ]; then
    echo "Job completed successfully."
    hadoop fs -cat $HDFS_OUTPUT_DIR/part-r-00000 | head -n 20
else
    echo "Job failed."
fi

rm -rf classes/
rm -f $JAR_FILE