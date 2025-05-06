"""
COVID-19 ETL Pipeline - Data Extraction Module

This module handles the extraction of COVID-19 data from the Johns Hopkins 
University GitHub repository and stores it locally as raw CSV files.
"""

import os
import logging
import requests
import pandas as pd
from datetime import datetime

# Set up logging
logger = logging.getLogger("covid_etl.extract")

# URLs for the JHU COVID-19 data
URLS = {
    'confirmed': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv',
    'deaths': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv',
    'recovered': 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv'
}

def extract_data(config=None):
    """
    Extract COVID-19 data from Johns Hopkins repository.
    
    Returns:
        bool: True if extraction was successful, False otherwise
    """
    if config is None:
        try:
            from scripts import CONFIG
            config = CONFIG
        except ImportError:
            logger.error("No config provided and couldn't import CONFIG")
            return False
        
    # Create raw data directory if it doesn't exist
    raw_dir = config['raw_data_path']
    os.makedirs(raw_dir, exist_ok=True)
    
    successful_downloads = 0
    
    # Download each dataset
    for category, url in URLS.items():
        output_file = os.path.join(raw_dir, f"{category}.csv")
        
        try:
            logger.info(f"Downloading {category} data from {url}")
            
            # Send HTTP request with timeout and headers
            # headers = {'User-Agent': 'COVID-19-ETL-Pipeline/0.1.0'}
            response = requests.get(url, timeout=30)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            # Save the raw data
            with open(output_file, 'wb') as f:
                f.write(response.content)
            
            # Verify the downloaded file
            if os.path.getsize(output_file) > 0:
                df = pd.read_csv(output_file)
                row_count = len(df)
                col_count = len(df.columns)
                logger.info(f"Successfully downloaded {category} data: {row_count} rows x {col_count} columns")
                successful_downloads += 1
            else:
                logger.error(f"Downloaded file {output_file} is empty")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error downloading {category} data: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing {category} data: {str(e)}")
    
    # Save metadata about the extraction
    metadata_file = os.path.join(raw_dir, "extraction_metadata.txt")
    with open(metadata_file, 'w') as f:
        f.write(f"Extraction timestamp: {datetime.now().isoformat()}\n")
        f.write(f"Successful downloads: {successful_downloads}/{len(URLS)}\n")


