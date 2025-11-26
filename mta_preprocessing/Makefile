# Makefile for MTA MapReduce Pipeline
# Author: RBDA Fall 2025 Group 18

# Configuration
HADOOP_CLASSPATH = $(shell hadoop classpath)
CLASSES_DIR = classes
JAVA_FILES = MTAFilterClean.java MTAStationHourly.java
JAR_FILES = mta-filter-clean.jar mta-station-hourly.jar

# HDFS paths
OUTPUT_BASE = project/preprocessing/mta_processed
CLEANED_OUTPUT = $(OUTPUT_BASE)/cleaned
STATION_OUTPUT = $(OUTPUT_BASE)/station_hourly

# Default target
.PHONY: all
all: compile

# Compile all Java files and create JARs
.PHONY: compile
compile:
	@echo "====================================="
	@echo "Compiling MTA MapReduce jobs..."
	@echo "====================================="
	@mkdir -p $(CLASSES_DIR)
	@echo "Compiling MTAFilterClean.java (Cleaning Job)..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAFilterClean.java
	@echo "Compiling MTAStationHourly.java (Aggregation Job)..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAStationHourly.java
	@echo ""
	@echo "Creating JAR files..."
	@cd $(CLASSES_DIR) && jar -cvf ../mta-filter-clean.jar MTAFilterClean*.class
	@cd $(CLASSES_DIR) && jar -cvf ../mta-station-hourly.jar MTAStationHourly*.class
	@echo ""
	@echo "✓ Compilation complete!"
	@echo "Generated JAR files:"
	@ls -lh *.jar 2>/dev/null || echo "  No JAR files found"

# Run the complete pipeline
.PHONY: run
run: compile
	@echo ""
	@echo "====================================="
	@echo "Running MTA Preprocessing Pipeline"
	@echo "====================================="
	@./run_pipeline.sh

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
	@echo "  - $(CLEANED_OUTPUT)"
	@echo "  - $(STATION_OUTPUT)"
	@read -p "Are you sure? [y/N] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		hadoop fs -rm -r -skipTrash $(OUTPUT_BASE) 2>/dev/null || true; \
		echo "✓ HDFS output directories cleaned"; \
	else \
		echo "Cancelled"; \
	fi

# Clean everything (local + HDFS)
.PHONY: clean-all
clean-all: clean
	@echo ""
	@hadoop fs -rm -r -skipTrash $(OUTPUT_BASE) 2>/dev/null || true
	@echo "✓ All local and HDFS files cleaned"
