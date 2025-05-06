"""
COVID-19 ETL Pipeline - Data Transformation Module

This module transforms raw COVID-19 data into analytics-ready formats,
creating processed datasets that are suitable for loading into a database.
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# Set up logging
logger = logging.getLogger("covid_etl.transform")

def transform_data(config=None):
    """
    Transform raw COVID-19 data into analytics-ready formats.
    
    Returns:
        bool: True if transformation was successful, False otherwise
    """
    try:
        if config is None:
            try:
                from scripts import CONFIG
                config = CONFIG
            except ImportError:
                logger.error("No config provided and couldn't import CONFIG")
                return False
            
        raw_dir = config['raw_data_path']
        processed_dir = config['processed_data_path']
        os.makedirs(processed_dir, exist_ok=True)
        
        logger.info("Starting data transformation process")
        
        # Step 1: Read and reshape each dataset
        datasets = {}
        for category in ['confirmed', 'deaths', 'recovered']:
            file_path = os.path.join(raw_dir, f"{category}.csv")
            
            if not os.path.exists(file_path):
                logger.warning(f"Raw data file not found: {file_path}")
                continue
                
            logger.info(f"Processing {category} dataset")
            
            # Read the raw data
            df = pd.read_csv(file_path)
            
            # Reshape from wide to long format
            id_vars = ['Province/State', 'Country/Region', 'Lat', 'Long']
            df_long = pd.melt(
                df, 
                id_vars=id_vars,
                var_name='date', 
                value_name=category
            )
            
            # Convert date string to datetime
            df_long['date'] = pd.to_datetime(df_long['date'], format='%m/%d/%y')
            
            # Store the processed dataset
            datasets[category] = df_long
            logger.info(f"Reshaped {category} data: {len(df_long)} rows")
        
        # Step 2: Merge the datasets
        logger.info("Merging datasets")
        if 'confirmed' not in datasets:
            logger.error("Required 'confirmed' dataset missing")
            return False
            
        transformed_data = datasets['confirmed']
        
        # Merge deaths data if available
        if 'deaths' in datasets:
            transformed_data = pd.merge(
                transformed_data, 
                datasets['deaths'][['Province/State', 'Country/Region', 'date', 'deaths']],
                on=['Province/State', 'Country/Region', 'date'],
                how='left'
            )
        else:
            transformed_data['deaths'] = 0
            
        # Merge recovered data if available
        if 'recovered' in datasets:
            transformed_data = pd.merge(
                transformed_data, 
                datasets['recovered'][['Province/State', 'Country/Region', 'date', 'recovered']],
                on=['Province/State', 'Country/Region', 'date'],
                how='left'
            )
        else:
            transformed_data['recovered'] = 0
        
        # Fill missing values
        transformed_data = transformed_data.fillna({
            'deaths': 0,
            'recovered': 0,
            'Province/State': 'Unknown'
        })
        
        # Step 3: Create calculated fields
        logger.info("Creating calculated metrics")
        transformed_data['active'] = transformed_data['confirmed'] - transformed_data['deaths'] - transformed_data['recovered']
        transformed_data['mortality_rate'] = np.where(
            transformed_data['confirmed'] > 0,
            (transformed_data['deaths'] / transformed_data['confirmed']) * 100,
            0
        )
        
        # Step 4: Save the main transformed dataset
        output_file = os.path.join(processed_dir, 'covid_data_transformed.csv')
        transformed_data.to_csv(output_file, index=False)
        logger.info(f"Saved main transformed dataset: {len(transformed_data)} rows")
        
        # Step 5: Create country-level aggregation
        logger.info("Creating country-level aggregation")
        country_data = transformed_data.groupby(['Country/Region', 'date']).agg({
            'confirmed': 'sum',
            'deaths': 'sum',
            'recovered': 'sum',
            'active': 'sum'
        }).reset_index()
        
        # Calculate country-level mortality rate
        country_data['mortality_rate'] = np.where(
            country_data['confirmed'] > 0,
            (country_data['deaths'] / country_data['confirmed']) * 100,
            0
        )
        
        output_file = os.path.join(processed_dir, 'covid_data_by_country.csv')
        country_data.to_csv(output_file, index=False)
        logger.info(f"Saved country-level dataset: {len(country_data)} rows")
        
        # Step 6: Calculate daily changes
        logger.info("Calculating daily changes")
        # Sort data to prepare for diff calculation
        transformed_data = transformed_data.sort_values(['Country/Region', 'Province/State', 'date'])
        
        # Group by region and calculate daily differences
        daily_data = transformed_data.copy()
        for col in ['confirmed', 'deaths', 'recovered']:
            daily_data[f'new_{col}'] = daily_data.groupby(['Country/Region', 'Province/State'])[col].diff().fillna(0)
            # Ensure no negative values for new cases (data corrections)
            daily_data[f'new_{col}'] = daily_data[f'new_{col}'].clip(lower=0)
        
        output_file = os.path.join(processed_dir, 'covid_daily_changes.csv')
        daily_data.to_csv(output_file, index=False)
        logger.info(f"Saved daily changes dataset: {len(daily_data)} rows")
        
        # Save transformation metadata
        metadata_file = os.path.join(processed_dir, "transformation_metadata.txt")
        with open(metadata_file, 'w') as f:
            f.write(f"Transformation timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Total records processed: {len(transformed_data)}\n")
            f.write(f"Countries included: {transformed_data['Country/Region'].nunique()}\n")
            f.write(f"Date range: {transformed_data['date'].min()} to {transformed_data['date'].max()}\n")
        
        logger.info("Data transformation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during data transformation: {str(e)}")
        return False


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run the transformation process
    success = transform_data()
    print(f"Transformation {'successful' if success else 'failed'}")