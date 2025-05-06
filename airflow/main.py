#!/usr/bin/env python3
"""
COVID-19 ETL Pipeline - Main Orchestration Script

This script orchestrates the complete ETL pipeline for COVID-19 data,
handling extraction from Johns Hopkins repository, transformation into
analytics-ready format, and loading into a SQLite database.
"""

import logging
import sys
import time
from datetime import datetime

# Import our pipeline components
from scripts import (
    extract_data,
    transform_data,
    load_data,
    run_data_quality_checks,
    CONFIG
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("covid_etl")

def run_pipeline():
    """Execute the complete ETL pipeline."""
    
    start_time = time.time()
    logger.info("Starting COVID-19 ETL pipeline")
    
    try:
        # Step 1: Extract data from source
        logger.info("EXTRACT: Downloading COVID-19 data from Johns Hopkins repository")
        extract_success = extract_data()
        if not extract_success:
            logger.error("Extraction failed - stopping pipeline")
            return False
        
        # Step 2: Transform the data
        logger.info("TRANSFORM: Processing and reshaping the COVID-19 data")
        transform_success = transform_data()
        if not transform_success:
            logger.error("Transformation failed - stopping pipeline")
            return False
        
        # Step 3: Load data into the database
        logger.info("LOAD: Loading processed data into SQLite database")
        load_success = load_data()
        if not load_success:
            logger.error("Loading failed - stopping pipeline")
            return False
        
        # Step 4: Run data quality checks
        logger.info("QUALITY: Running data quality validation checks")
        quality_success = run_data_quality_checks()
        if not quality_success:
            logger.warning("Data quality issues detected - review logs")

        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"Pipeline completed successfully in {execution_time:.2f} seconds")
        
        # Save execution metadata
        with open("pipeline_metadata.txt", "a") as f:
            f.write(f"{datetime.now()}, Status: Success, Duration: {execution_time:.2f}s\n")
            
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        
        # Save execution metadata
        with open("pipeline_metadata.txt", "a") as f:
            f.write(f"{datetime.now()}, Status: Failed, Error: {str(e)}\n")
            
        return False

if __name__ == "__main__":
    run_pipeline()