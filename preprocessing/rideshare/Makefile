# Makefile for Rideshare MapReduce Aggregation

# Configuration
HADOOP_CLASSPATH = $(shell hadoop classpath)
CLASSES_DIR = classes
JAVA_FILES = RideshareHourlyAgg.java
JAR_FILE = rideshare-hourly-agg.jar

# HDFS paths
INPUT_DATA = project/preprocessing/rideshare_cleaned
OUTPUT_BASE = project/preprocessing/rideshare_processed
OUTPUT_PATH = $(OUTPUT_BASE)/hourly_agg

# Default target
.PHONY: all
all: compile

# Compile Java file and create JAR
.PHONY: compile
compile:
	@echo "====================================="
	@echo "Compiling Rideshare MapReduce job..."
	@echo "====================================="
	@mkdir -p $(CLASSES_DIR)
	@echo "Compiling RideshareHourlyAgg.java..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) RideshareHourlyAgg.java
	@echo ""
	@echo "Creating JAR file..."
	@cd $(CLASSES_DIR) && jar -cvf ../$(JAR_FILE) RideshareHourlyAgg*.class
	@echo ""
	@echo "✓ Compilation complete!"
	@echo "Generated JAR file:"
	@ls -lh $(JAR_FILE) 2>/dev/null || echo "  No JAR file found"

# Run the aggregation job
.PHONY: run
run: compile
	@echo ""
	@echo "====================================="
	@echo "Running Rideshare Aggregation"
	@echo "====================================="
	@echo "Input: $(INPUT_DATA)"
	@echo "Output: $(OUTPUT_PATH)"
	@echo ""
	@echo "Processing 68GB of raw rideshare data..."
	@echo "This will aggregate by hour and location."
	@echo ""
	hadoop jar $(JAR_FILE) RideshareHourlyAgg \
		$(INPUT_DATA) \
		$(OUTPUT_PATH)
	@echo ""
	@echo "✓ Rideshare aggregation complete!"
	@echo "Output saved to: $(OUTPUT_PATH)"
	@echo ""
	@echo "Output schema:"
	@echo "  pickup_hour, pickup_location_id, trip_count, avg_base_fare,"
	@echo "  avg_tolls, avg_tips, avg_driver_pay, avg_total_cost,"
	@echo "  avg_trip_duration_sec, avg_trip_miles, shared_request_count,"
	@echo "  shared_match_count, shared_match_pct, wheelchair_request_count,"
	@echo "  wheelchair_match_count"

# Clean local compiled files
.PHONY: clean
clean:
	@echo "Cleaning compiled files..."
	@rm -rf $(CLASSES_DIR)
	@rm -f $(JAR_FILE)
	@echo "✓ Local files cleaned"

# Clean HDFS output directory
.PHONY: clean-output
clean-output:
	@echo "Cleaning HDFS output directory..."
	@echo "This will remove: $(OUTPUT_BASE)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		hadoop fs -rm -r -skipTrash $(OUTPUT_BASE) 2>/dev/null || true; \
		echo "✓ HDFS output directory cleaned"; \
	else \
		echo "Cancelled"; \
	fi

# Clean everything (local + HDFS)
.PHONY: clean-all
clean-all: clean
	@echo ""
	@hadoop fs -rm -r -skipTrash $(OUTPUT_BASE) 2>/dev/null || true
	@echo "✓ All local and HDFS files cleaned"

# Help target
.PHONY: help
help:
	@echo "Rideshare MapReduce Aggregation Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  make compile        - Compile Java and create JAR"
	@echo "  make run            - Compile and run aggregation job"
	@echo "  make clean          - Clean local compiled files"
	@echo "  make clean-output   - Clean HDFS output (prompts)"
	@echo "  make clean-all      - Clean everything"
	@echo ""
	@echo "Input:  $(INPUT_DATA)"
	@echo "Output: $(OUTPUT_PATH)"
