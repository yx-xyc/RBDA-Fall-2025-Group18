# Makefile for Urban Mobility MapReduce Pipeline

# Configuration
HADOOP_CLASSPATH = $(shell hadoop classpath)
CLASSES_DIR = classes
JAVA_FILES = MTAFilterClean.java MTAStationHourly.java RideshareHourlyAgg.java
JAR_FILES = mta-filter-clean.jar mta-station-hourly.jar rideshare-hourly-agg.jar

# HDFS paths - MTA
MTA_OUTPUT_BASE = project/preprocessing/mta_processed
MTA_CLEANED_OUTPUT = $(MTA_OUTPUT_BASE)/cleaned
MTA_STATION_OUTPUT = $(MTA_OUTPUT_BASE)/station_hourly

# HDFS paths - Rideshare
RIDESHARE_OUTPUT_BASE = project/preprocessing/rideshare_processed
RIDESHARE_AGG_OUTPUT = $(RIDESHARE_OUTPUT_BASE)/hourly_agg

# Default target
.PHONY: all
all: compile

# Compile all Java files and create JARs
.PHONY: compile
compile:
	@echo "====================================="
	@echo "Compiling MapReduce jobs..."
	@echo "====================================="
	@mkdir -p $(CLASSES_DIR)
	@echo "Compiling MTAFilterClean.java (MTA Cleaning Job)..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAFilterClean.java
	@echo "Compiling MTAStationHourly.java (MTA Aggregation Job)..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAStationHourly.java
	@echo "Compiling RideshareHourlyAgg.java (Rideshare Aggregation Job)..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) RideshareHourlyAgg.java
	@echo ""
	@echo "Creating JAR files..."
	@cd $(CLASSES_DIR) && jar -cvf ../mta-filter-clean.jar MTAFilterClean*.class
	@cd $(CLASSES_DIR) && jar -cvf ../mta-station-hourly.jar MTAStationHourly*.class
	@cd $(CLASSES_DIR) && jar -cvf ../rideshare-hourly-agg.jar RideshareHourlyAgg*.class
	@echo ""
	@echo "✓ Compilation complete!"
	@echo "Generated JAR files:"
	@ls -lh *.jar 2>/dev/null || echo "  No JAR files found"

# Compile only MTA jobs
.PHONY: compile-mta
compile-mta:
	@echo "Compiling MTA MapReduce jobs..."
	@mkdir -p $(CLASSES_DIR)
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAFilterClean.java
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAStationHourly.java
	@cd $(CLASSES_DIR) && jar -cvf ../mta-filter-clean.jar MTAFilterClean*.class
	@cd $(CLASSES_DIR) && jar -cvf ../mta-station-hourly.jar MTAStationHourly*.class
	@echo "✓ MTA jobs compiled"

# Compile only Rideshare job
.PHONY: compile-rideshare
compile-rideshare:
	@echo "Compiling Rideshare MapReduce job..."
	@mkdir -p $(CLASSES_DIR)
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) RideshareHourlyAgg.java
	@cd $(CLASSES_DIR) && jar -cvf ../rideshare-hourly-agg.jar RideshareHourlyAgg*.class
	@echo "✓ Rideshare job compiled"

# Run the MTA pipeline
.PHONY: run-mta
run-mta: compile-mta
	@echo ""
	@echo "====================================="
	@echo "Running MTA Preprocessing Pipeline"
	@echo "====================================="
	@./run_pipeline.sh

# Run the Rideshare aggregation
.PHONY: run-rideshare
run-rideshare: compile-rideshare
	@echo ""
	@echo "====================================="
	@echo "Running Rideshare Aggregation"
	@echo "====================================="
	@echo "Input: project/preprocessing/rideshare_cleaned"
	@echo "Output: $(RIDESHARE_AGG_OUTPUT)"
	@echo ""
	hadoop jar rideshare-hourly-agg.jar RideshareHourlyAgg \
		project/preprocessing/rideshare_cleaned \
		$(RIDESHARE_AGG_OUTPUT)
	@echo ""
	@echo "✓ Rideshare aggregation complete!"
	@echo "Output saved to: $(RIDESHARE_AGG_OUTPUT)"

# Run all pipelines
.PHONY: run-all
run-all: compile
	@echo ""
	@echo "====================================="
	@echo "Running All Preprocessing Pipelines"
	@echo "====================================="
	@$(MAKE) run-mta
	@$(MAKE) run-rideshare

# Clean local compiled files
.PHONY: clean
clean:
	@echo "Cleaning compiled files..."
	@rm -rf $(CLASSES_DIR)
	@rm -f $(JAR_FILES)
	@echo "✓ Local files cleaned"

# Clean HDFS output directories
.PHONY: clean-output
clean-output:
	@echo "Cleaning HDFS output directories..."
	@echo "This will remove:"
	@echo "  - $(MTA_OUTPUT_BASE)"
	@echo "  - $(RIDESHARE_OUTPUT_BASE)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		hadoop fs -rm -r -skipTrash $(MTA_OUTPUT_BASE) 2>/dev/null || true; \
		hadoop fs -rm -r -skipTrash $(RIDESHARE_OUTPUT_BASE) 2>/dev/null || true; \
		echo "✓ HDFS output directories cleaned"; \
	else \
		echo "Cancelled"; \
	fi

# Clean only MTA HDFS output
.PHONY: clean-mta-output
clean-mta-output:
	@echo "Cleaning MTA HDFS output..."
	@hadoop fs -rm -r -skipTrash $(MTA_OUTPUT_BASE) 2>/dev/null || true
	@echo "✓ MTA output cleaned"

# Clean only Rideshare HDFS output
.PHONY: clean-rideshare-output
clean-rideshare-output:
	@echo "Cleaning Rideshare HDFS output..."
	@hadoop fs -rm -r -skipTrash $(RIDESHARE_OUTPUT_BASE) 2>/dev/null || true
	@echo "✓ Rideshare output cleaned"

# Clean everything (local + HDFS)
.PHONY: clean-all
clean-all: clean
	@echo ""
	@hadoop fs -rm -r -skipTrash $(MTA_OUTPUT_BASE) 2>/dev/null || true
	@hadoop fs -rm -r -skipTrash $(RIDESHARE_OUTPUT_BASE) 2>/dev/null || true
	@echo "✓ All local and HDFS files cleaned"
