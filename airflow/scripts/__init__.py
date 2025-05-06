# scripts/__init__.py

"""
COVID-19 ETL Pipeline Package

This package contains all the components needed to extract, transform and load
COVID-19 data from Johns Hopkins University's repository into a structured
database for analytics.
"""

from scripts.extract_data import extract_data
from scripts.transform_data import transform_data
from scripts.load_data import load_data
# from scripts.run_data_quality_checks import run_data_quality_checks

# Package version
__version__ = '0.1.0'

# Define what's available when importing from this package
__all__ = [
    'extract_data',
    'transform_data', 
    'load_data',
    'run_data_quality_checks'
]

# Configuration constants
# CONFIG = {
#     'raw_data_path': 'data/raw',
#     'processed_data_path': 'data/processed',
#     'database_path': 'covid_analytics.db',
#     'log_level': 'INFO'
# }


CONFIG = {
    'raw_data_path': '/opt/airflow/data/raw',
    'processed_data_path': '/opt/airflow/data/processed',
    'database_path': 'covid_analytics.db',
    'log_level': 'INFO',
    'database_config': {
            'dbname': 'covid_data',
            'user': 'airflow',
            'password': 'airflow',
            'host': '172.18.0.4',
            'port': '5432'
        }
}
