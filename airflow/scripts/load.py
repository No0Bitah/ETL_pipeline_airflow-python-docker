"""
COVID-19 ETL Pipeline - Data Loading Module

This module loads the transformed COVID-19 data into a PostgreSQL database
and creates the necessary schema, indexes, and views for analytics.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from sqlalchemy import create_engine

# Set up logging
logger = logging.getLogger("covid_etl.load")

def load_data(config=None):
    """
    Load transformed COVID-19 data into PostgreSQL database.
    
    Returns:
        bool: True if loading was successful, False otherwise
    """
    try:
        if config is None:
            try:
                from scripts import CONFIG
                config = CONFIG
            except ImportError:
                logger.error("No config provided and couldn't import CONFIG")
                return False
            
        processed_dir = config['processed_data_path']
        conn_id = config['postgres_conn_id']
        
        logger.info(f"Starting data loading process to PostgreSQL database using connection: {conn_id}")
        
        # Get PostgreSQL connection using Airflow's PostgresHook
        pg_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = pg_hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        create_database_schema(cursor)
        
        # Create SQLAlchemy engine for pandas to_sql
        engine = pg_hook.get_sqlalchemy_engine()

        
        # Dictionary of files to load
        files_to_load = {
            'covid_data_transformed.csv': 'full_data',
            'covid_data_by_country.csv': 'country_summary',
            'covid_daily_changes.csv': 'daily_changes'
        }
        
        rows_loaded = 0
        
        # Process each file
        for file_name, table_name in files_to_load.items():
            file_path = os.path.join(processed_dir, file_name)
            logger.info(f"file path is {file_path}")


            
            if not os.path.exists(file_path):
                logger.warning(f"Processed data file not found: {file_path}")
                continue
                
            logger.info(f"Loading {file_name} into {table_name} table")
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            logger.info(f"CSV read complete. Shape: {df.shape}")
            
            # Clean column names for PostgreSQL compatibility
            df.columns = [col.replace('/', '_').replace(' ', '_').lower() for col in df.columns]
            
            # Convert date formats
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            # # Check if the table exists before deleting
            # exists = check_if_table_exists(pg_hook, table_name)
            # if exists:
            #     logger.info(f"Deleting existing data from table: {table_name}")
            #     pg_hook.run(f"DELETE FROM {table_name}")
            # else:
            #     logger.warning(f"Table {table_name} does not exist; skipping DELETE")
            pg_hook.run(f"DROP TABLE IF EXISTS {table_name} CASCADE")

            logger.info(f"Creating {table_name} table and loading {len(df)} rows")
            df.to_sql(table_name, engine, if_exists='replace', index=False)

            
            # Get row count
            result = pg_hook.get_records(f"SELECT COUNT(*) FROM {table_name}")
            count = result[0][0]
            logger.info(f"Loaded {count} rows into {table_name}")
            rows_loaded += count


        
        # Create analytical views
        create_analytical_views(pg_hook)
        
        # Create indexes for performance
        create_indexes(pg_hook)
        
        # Check if etl_metadata table exists before inserting
        if check_if_table_exists(pg_hook, 'etl_metadata'):
            # Save load metadata in the database
            pg_hook.run("""
            INSERT INTO etl_metadata (operation, timestamp, details)
            VALUES (%s, %s, %s)
            """, parameters=('load', datetime.now().isoformat(), f'Total rows loaded: {rows_loaded}'))
        else:
            logger.warning("etl_metadata table does not exist. Skipping metadata insertion.")
        
        # Commit changes and close connection
        conn.commit()
        conn.close()
        
        logger.info(f"Data loading completed successfully: {rows_loaded} total rows loaded")
        return True
        
    except Exception as e:
        logger.error(f"Error during data loading: {str(e)}")
        if 'conn' in locals() and conn:
            conn.rollback()
            conn.close()
        raise AirflowException(f"Data loading failed: {str(e)}")

def check_if_table_exists(pg_hook, table_name):
    """
    Check if a table exists in the database.
    
    Args:
        pg_hook: PostgresHook instance
        table_name: Name of the table to check
        
    Returns:
        bool: True if the table exists, False otherwise
    """
    try:
        # Query to check if table exists in PostgreSQL
        query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = %s
        )
        """
        result = pg_hook.get_first(query, parameters=(table_name,))
        return result[0]
    except Exception as e:
        logger.error(f"Error checking if table {table_name} exists: {str(e)}")
        return False

def create_database_schema(cursor):
    """Create the database schema if it doesn't exist."""
    try:
        # Create ETL metadata table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS etl_metadata (
            id SERIAL PRIMARY KEY,
            operation TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            details TEXT
        )
        """)
        
        # Create main COVID data table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS full_data (
            province_state TEXT,
            country_region TEXT NOT NULL,
            lat REAL,
            long REAL,
            date DATE NOT NULL,
            confirmed INTEGER NOT NULL,
            deaths INTEGER NOT NULL,
            recovered INTEGER NOT NULL,
            active INTEGER NOT NULL,
            mortality_rate REAL,
            PRIMARY KEY (country_region, province_state, date)
        )
        """)
        
        # Create country summary table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS country_summary (
            country_region TEXT NOT NULL,
            date DATE NOT NULL,
            confirmed INTEGER NOT NULL,
            deaths INTEGER NOT NULL,
            recovered INTEGER NOT NULL,
            active INTEGER NOT NULL,
            mortality_rate REAL,
            PRIMARY KEY (country_region, date)
        )
        """)
        
        # Create daily changes table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_changes (
            province_state TEXT,
            country_region TEXT NOT NULL,
            lat REAL,
            long REAL,
            date DATE NOT NULL,
            confirmed INTEGER NOT NULL,
            deaths INTEGER NOT NULL,
            recovered INTEGER NOT NULL,
            active INTEGER NOT NULL,
            mortality_rate REAL,
            new_confirmed INTEGER,
            new_deaths INTEGER,
            new_recovered INTEGER,
            PRIMARY KEY (country_region, province_state, date)
        )
        """)
        
        logger.info("Database schema created successfully")
        
    except Exception as e:
        logger.error(f"Error creating database schema: {str(e)}")
        raise

def create_analytical_views(pg_hook):
    """Create useful analytical views using PostgresHook."""
    try:
        # View for latest global summary
        pg_hook.run("""
        CREATE OR REPLACE VIEW vw_latest_global_summary AS
        SELECT 
            SUM(confirmed) as total_confirmed,
            SUM(deaths) as total_deaths,
            SUM(recovered) as total_recovered,
            SUM(active) as total_active,
            (CAST(SUM(deaths) AS FLOAT) / NULLIF(CAST(SUM(confirmed) AS FLOAT), 0)) * 100 as global_mortality_rate
        FROM country_summary
        WHERE date = (SELECT MAX(date) FROM country_summary)
        """)
        
        # View for top 10 countries by confirmed cases
        pg_hook.run("""
        CREATE OR REPLACE VIEW vw_top_countries AS
        SELECT 
            country_region as country,
            confirmed,
            deaths,
            recovered,
            active,
            mortality_rate
        FROM country_summary
        WHERE date = (SELECT MAX(date) FROM country_summary)
        ORDER BY confirmed DESC
        LIMIT 10
        """)
        
        # View for daily global timeline
        pg_hook.run("""
        CREATE OR REPLACE VIEW vw_global_daily AS
        SELECT 
            date,
            SUM(confirmed) as total_confirmed,
            SUM(deaths) as total_deaths,
            SUM(recovered) as total_recovered,
            SUM(active) as total_active,
            SUM(new_confirmed) as new_confirmed,
            SUM(new_deaths) as new_deaths,
            SUM(new_recovered) as new_recovered
        FROM daily_changes
        GROUP BY date
        ORDER BY date
        """)
        
        logger.info("Analytical views created successfully")
        
    except Exception as e:
        logger.error(f"Error creating analytical views: {str(e)}")
        raise

def create_indexes(pg_hook):
    """Create indexes for query performance."""
    try:
        # Indexes for covid_full_data
        pg_hook.run("CREATE INDEX IF NOT EXISTS idx_full_country ON full_data(country_region)")
        pg_hook.run("CREATE INDEX IF NOT EXISTS idx_full_date ON full_data(date)")
        
        # Indexes for covid_country_summary
        pg_hook.run("CREATE INDEX IF NOT EXISTS idx_summary_date ON country_summary(date)")
        
        # Indexes for covid_daily_changes
        pg_hook.run("CREATE INDEX IF NOT EXISTS idx_changes_country ON daily_changes(country_region)")
        pg_hook.run("CREATE INDEX IF NOT EXISTS idx_changes_date ON daily_changes(date)")
        
        logger.info("Database indexes created successfully")
        
    except Exception as e:
        logger.error(f"Error creating database indexes: {str(e)}")
        raise



if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the loading process
    success = load_data()
    print(f"Data loading {'successful' if success else 'failed'}")