# Makefile for MTA MapReduce Pipeline
# Author: RBDA Fall 2025 Group 18

# Configuration
HADOOP_CLASSPATH = $(shell hadoop classpath)
CLASSES_DIR = classes
JAVA_FILES = MTAFilterClean.java MTAStationHourly.java MTABoroughHourly.java MTACitywideHourly.java
JAR_FILES = mta-filter-clean.jar mta-station-hourly.jar mta-borough-hourly.jar mta-citywide-hourly.jar

# HDFS paths
OUTPUT_BASE = project/preprocessing/mta_processed
CLEANED_OUTPUT = $(OUTPUT_BASE)/cleaned
STATION_OUTPUT = $(OUTPUT_BASE)/station_hourly
BOROUGH_OUTPUT = $(OUTPUT_BASE)/borough_hourly
CITYWIDE_OUTPUT = $(OUTPUT_BASE)/citywide_hourly

# Default target
.PHONY: all
all: compile

# Help target
.PHONY: help
help:
	@echo "MTA MapReduce Pipeline - Available targets:"
	@echo ""
	@echo "  make compile       - Compile all Java MapReduce jobs and create JAR files"
	@echo "  make run           - Run the complete preprocessing pipeline"
	@echo "  make clean         - Remove all compiled classes and JAR files"
	@echo "  make clean-output  - Remove all HDFS output directories"
	@echo "  make clean-all     - Clean both local files and HDFS output"
	@echo "  make verify        - Verify JAR files and class files exist"
	@echo "  make help          - Show this help message"
	@echo ""

# Compile all Java files and create JARs
.PHONY: compile
compile:
	@echo "====================================="
	@echo "Compiling MTA MapReduce jobs..."
	@echo "====================================="
	@mkdir -p $(CLASSES_DIR)
	@echo "Compiling MTAFilterClean.java..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAFilterClean.java
	@echo "Compiling MTAStationHourly.java..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTAStationHourly.java
	@echo "Compiling MTABoroughHourly.java..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTABoroughHourly.java
	@echo "Compiling MTACitywideHourly.java..."
	@javac -classpath $(HADOOP_CLASSPATH) -d $(CLASSES_DIR) MTACitywideHourly.java
	@echo ""
	@echo "Creating JAR files..."
	@cd $(CLASSES_DIR) && jar -cvf ../mta-filter-clean.jar MTAFilterClean*.class
	@cd $(CLASSES_DIR) && jar -cvf ../mta-station-hourly.jar MTAStationHourly*.class
	@cd $(CLASSES_DIR) && jar -cvf ../mta-borough-hourly.jar MTABoroughHourly*.class
	@cd $(CLASSES_DIR) && jar -cvf ../mta-citywide-hourly.jar MTACitywideHourly*.class
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

# Verify compiled files
.PHONY: verify
verify:
	@echo "Verifying compiled files..."
	@echo ""
	@echo "JAR files:"
	@ls -lh *.jar 2>/dev/null || echo "  No JAR files found"
	@echo ""
	@echo "Class files:"
	@ls -lh $(CLASSES_DIR)/*.class 2>/dev/null || echo "  No class files found"

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
	@echo "  - $(BOROUGH_OUTPUT)"
	@echo "  - $(CITYWIDE_OUTPUT)"
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

# Show pipeline status
.PHONY: status
status:
	@echo "Pipeline Status:"
	@echo ""
	@echo "Local JAR files:"
	@ls -lh *.jar 2>/dev/null || echo "  No JAR files found"
	@echo ""
	@echo "HDFS output directories:"
	@hadoop fs -ls $(OUTPUT_BASE) 2>/dev/null || echo "  No output directories found"
