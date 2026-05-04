"""
Bronze Loader
Saves raw data from ENTSO-E into our Bronze layer.

The Bronze layer stores data EXACTLY as received — no transformations.
This is critical because if we make a mistake in processing,
we can always reprocess from the original raw data.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from ingestion.entsoe_client import ENTSOEClient

load_dotenv()
logger = logging.getLogger('bronze_loader')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read paths from environment variables
BRONZE_PATH = Path(os.getenv('BRONZE_PATH', './data/bronze'))


def save_series_to_parquet(series: pd.Series, filepath: Path, data_type: str):
    """
    Convert a pandas Series to a DataFrame and save as Parquet.
    Parquet is a compressed columnar format — much smaller than CSV.
    """
    if series is None:
        logger.warning(f"No data to save for {data_type}")
        return
    
    # Convert Series to DataFrame, reset index so timestamps become a column
    df = series.reset_index()
    df.columns = ['timestamp', data_type]
    
    # Make sure the timestamp column is a string (for Parquet compatibility)
    df['timestamp'] = df['timestamp'].astype(str)
    
    # Create directory if it doesn't exist
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Save to Parquet format
    df.to_parquet(filepath, index=False)
    logger.info(f"Saved {len(df)} records to {filepath}")


def save_dataframe_to_parquet(df: pd.DataFrame, filepath: Path):
    """Save a DataFrame to Parquet, handling multi-level column names."""
    if df is None:
        return
    
    # Flatten multi-level column names (common with generation data)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(filter(None, col)).strip() for col in df.columns.values]
    
    # Reset index to convert timestamps to a column
    df = df.reset_index()
    df.columns = [str(c) for c in df.columns]  # ensure all column names are strings
    df[df.columns[0]] = df[df.columns[0]].astype(str)  # timestamp to string
    
    filepath.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(filepath, index=False)
    logger.info(f"Saved {len(df)} records to {filepath}")


def save_metadata(country_code: str, start, end, fetch_time: str, filepath: Path):
    """
    Save metadata JSON file alongside the data.
    This records WHEN the data was fetched, WHAT time range it covers,
    and any other context. Critical for debugging and data lineage.
    """
    metadata = {
        'country_code': country_code,
        'data_start': str(start),
        'data_end': str(end),
        'fetch_timestamp_utc': fetch_time,
        'pipeline_version': '1.0.0',
        'data_source': 'ENTSO-E Transparency Platform'
    }
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w') as f:
        json.dump(metadata, f, indent=2)
    logger.info(f"Saved metadata to {filepath}")


def load_to_bronze(days_back: int = 1):
    """
    Main function: fetch data from ENTSO-E and save to Bronze layer.
    
    Files are organised by: country / year / month / day
    Example: data/bronze/BE/2026/05/04/prices.parquet
    
    This partitioning lets us efficiently read just the data we need.
    """
    logger.info("Starting Bronze ingestion...")
    
    # Initialise the ENTSO-E client
    client = ENTSOEClient()
    
    # Fetch all data
    all_data, start, end = client.fetch_all_countries(days_back=days_back)
    
    # Today's date for folder organisation
    today = datetime.utcnow()
    date_path = f"{today.year}/{today.month:02d}/{today.day:02d}"
    
    # Save each country's data
    for country_code, data in all_data.items():
        logger.info(f"Saving Bronze data for {country_code}...")
        
        base_path = BRONZE_PATH / country_code / date_path
        
        # Save prices (EUR/MWh per hour)
        save_series_to_parquet(
            series=data['prices'],
            filepath=base_path / 'prices.parquet',
            data_type='price_eur_per_mwh'
        )
        
        # Save electricity load (MW consumed per hour)
        if data['load'] is not None:
            # Load can be a DataFrame or Series depending on the response
            load_data = data['load']
            if isinstance(load_data, pd.DataFrame):
                save_dataframe_to_parquet(load_data, base_path / 'load.parquet')
            else:
                save_series_to_parquet(load_data, base_path / 'load.parquet', 'load_mw')
        
        # Save generation mix (MW per energy type per hour)
        save_dataframe_to_parquet(
            df=data['generation'],
            filepath=base_path / 'generation.parquet'
        )
        
        # Save metadata
        save_metadata(
            country_code=country_code,
            start=start,
            end=end,
            fetch_time=data['fetch_timestamp'],
            filepath=base_path / 'metadata.json'
        )
    
    logger.info("Bronze ingestion complete!")
    return True


# Run directly to test
if __name__ == '__main__':
    load_to_bronze(days_back=3)  # fetch last 3 days as a test