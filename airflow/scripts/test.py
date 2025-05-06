"""
Troubleshooting script for COVID-19 ETL Pipeline

This script helps diagnose issues with the CSV loading process
"""

import os
import sys
import pandas as pd
import logging
import psycopg2
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.configuration import conf

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("covid_etl_troubleshooter")

def test_csv_reading(file_path):
    """Test reading a CSV file and print diagnostics"""
    logger.info(f"Testing CSV reading for file: {file_path}")
    
    try:
        # Check if file exists
        if not os.path.exists(file_path):
            logger.error(f"File does not exist: {file_path}")
            return False
            
        # Get file size
        file_size = os.path.getsize(file_path)
        logger.info(f"File size: {file_size/1024/1024:.2f} MB")
        
        # Try to read the first few rows
        logger.info("Trying to read first 5 rows:")
        df_head = pd.read_csv(file_path, nrows=5)
        logger.info(f"Successfully read first 5 rows. Column count: {len(df_head.columns)}")
        logger.info(f"Columns: {df_head.columns.tolist()}")
        
        # Try to get row count without loading entire file
        with open(file_path, 'r') as f:
            row_count = sum(1 for _ in f) - 1  # Subtract header row
        logger.info(f"Approximate row count: {row_count}")
        
        # Check for data types to identify potential parsing issues
        logger.info("Attempting to read full file to check data types...")
        df = pd.read_csv(file_path, low_memory=False)
        logger.info(f"Successfully read entire file. Shape: {df.shape}")
        logger.info(f"Data types:\n{df.dtypes}")
        
        # Check for missing values
        missing_count = df.isna().sum().sum()
        logger.info(f"Missing values count: {missing_count}")
        
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        logger.info(f"Duplicate rows: {duplicate_count}")
        
        return True
    except Exception as e:
        logger.error(f"Error during CSV testing: {str(e)}")
        return False

def test_db_connection(conn_id):
    """Test database connection"""
    logger.info(f"Testing database connection with conn_id: {conn_id}")
    
    try:
        # Create PostgreSQL hook
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        
        # Test simple query
        result = pg_hook.get_first("SELECT version()")
        logger.info(f"Database connection successful. PostgreSQL version: {result[0]}")
        
        # Test SQLAlchemy engine
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            result = conn.execute("SELECT 1").fetchone()
        logger.info("SQLAlchemy engine connection successful")
        
        # Get database size information
        size_query = """
        SELECT pg_size_pretty(pg_database_size(current_database())) as db_size
        """
        result = pg_hook.get_first(size_query)
        logger.info(f"Current database size: {result[0]}")
        
        # Get table list
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        """
        tables = pg_hook.get_records(tables_query)
        logger.info(f"Tables in database: {[t[0] for t in tables]}")
        
        # Check for locks or active connections
        locks_query = """
        SELECT pid, 
               pg_blocking_pids(pid) as blocked_by,
               query,
               now() - query_start as query_duration
        FROM pg_stat_activity
        WHERE state != 'idle'
        """
        locks = pg_hook.get_records(locks_query)
        if locks:
            logger.info(f"Active queries: {len(locks)}")
            for lock in locks:
                logger.info(f"PID: {lock[0]}, Blocked by: {lock[1]}, Duration: {lock[3]}, Query: {lock[2][:100]}...")
        else:
            logger.info("No active queries found")
            
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False

def test_small_table_load(conn_id, file_path=None):
    """Test creating a small table and loading data"""
    logger.info("Testing small table load")
    
    try:
        # Create test data if no file provided
        if file_path is None:
            logger.info("Creating test dataframe with sample data")
            test_data = {
                'id': range(1, 6),
                'name': ['Test1', 'Test2', 'Test3', 'Test4', 'Test5'],
                'value': [10.5, 20.3, 30.1, 40.2, 50.0]
            }
            df = pd.DataFrame(test_data)
        else:
            logger.info(f"Using data from {file_path}")
            df = pd.read_csv(file_path, nrows=10)  # Just read 10 rows for testing
        
        # Get PostgreSQL hook
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Create test table
        test_table = "covid_etl_test_table"
        
        # Drop table if exists
        pg_hook.run(f"DROP TABLE IF EXISTS {test_table}")
        
        # Create and load test data
        logger.info(f"Creating test table and loading {len(df)} rows")
        df.to_sql(test_table, engine, index=False)
        
        # Verify data was loaded
        result = pg_hook.get_first(f"SELECT COUNT(*) FROM {test_table}")
        logger.info(f"Rows loaded: {result[0]}")
        
        # Clean up
        pg_hook.run(f"DROP TABLE IF EXISTS {test_table}")
        logger.info("Test table dropped")
        
        return True
    except Exception as e:
        logger.error(f"Small table load test failed: {str(e)}")
        return False

if __name__ == "__main__":
    print("\n==== COVID-19 ETL Pipeline Troubleshooter ====\n")
    print("This script will help diagnose issues with the ETL pipeline.")
    
    # Allow user to specify connection ID and file path
    conn_id = input("Enter Postgres connection ID [postgres_default]: ") or "postgres_default"
    processed_dir = input("Enter path to processed data directory [./data/processed]: ") or "./data/processed"
    
    # File paths
    main_file = os.path.join(processed_dir, 'covid_data_transformed.csv')
    
    # Run tests
    print("\n1. Testing Database Connection...")
    test_db_connection(conn_id)
    
    print("\n2. Testing CSV Reading...")
    if os.path.exists(main_file):
        test_csv_reading(main_file)
    else:
        print(f"File not found: {main_file}")
        alt_file = input("Enter alternative CSV file path to test: ")
        if alt_file:
            test_csv_reading(alt_file)
    
    print("\n3. Testing Small Table Load...")
    test_small_table_load(conn_id)
    
    print("\nTroubleshooting complete. Check the log output above for issues.")
    print("\nSuggested fixes:")
    print("1. If CSV reading fails: Check file formatting, encoding, and make sure it's not corrupted")
    print("2. If database connection fails: Verify connection parameters and ensure DB is accessible")
    print("3. If small table load works but full table fails: Your file may be too large - use chunking")
    print("4. Check disk space on both client and database server")
    print("5. Check database locks and ongoing transactions")