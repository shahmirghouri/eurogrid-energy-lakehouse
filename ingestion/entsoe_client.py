"""
ENTSO-E API Client
Fetches electricity market data for European countries.
ENTSO-E = European Network of Transmission System Operators for Electricity
They publish official EU electricity data - prices, load, generation.
"""

import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from entsoe import EntsoePandasClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Set up logging so we can see what's happening
# This is better than using print() for production code
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('entsoe_client')

# Country codes used by ENTSO-E
# These are the 4 countries we will monitor
COUNTRIES = {
    'BE': 'Belgium',
    'DE_LU': 'Germany-Luxembourg',
    'FR': 'France',
    'NL': 'Netherlands'
}


class ENTSOEClient:
    """
    Client to interact with the ENTSO-E Transparency Platform API.
    Wraps the entsoe-py library to make our code cleaner.
    """

    def __init__(self):
        # Read the API key from environment variable (.env file)
        # Never hardcode your API key directly in the code!
        api_key = os.getenv('ENTSOE_API_KEY')
        
        if not api_key or api_key == 'your_token_here_replace_when_received':
            raise ValueError(
                "ENTSO-E API key not found! "
                "Set ENTSOE_API_KEY in your .env file."
            )
        
        # Create the ENTSO-E client using our API key
        self.client = EntsoePandasClient(api_key=api_key)
        logger.info("ENTSO-E client initialised successfully")

    def fetch_day_ahead_prices(self, country_code: str, 
                                start: pd.Timestamp, 
                                end: pd.Timestamp) -> pd.Series:
        """
        Fetch day-ahead electricity prices for a country.
        
        Day-ahead prices = electricity prices set the day before delivery.
        Unit: EUR per MWh (euros per megawatt-hour)
        
        Args:
            country_code: e.g. 'BE' for Belgium
            start: start datetime (timezone-aware)
            end: end datetime (timezone-aware)
            
        Returns:
            pandas Series with hourly prices indexed by timestamp
        """
        try:
            logger.info(
                f"Fetching prices for {country_code} from {start.date()} to {end.date()}"
            )
            prices = self.client.query_day_ahead_prices(
                country_code=country_code,
                start=start,
                end=end
            )
            logger.info(f"Got {len(prices)} price records for {country_code}")
            return prices
        
        except Exception as e:
            # Log the error but don't crash — we want other countries to still run
            logger.error(f"Failed to fetch prices for {country_code}: {e}")
            return None

    def fetch_load(self, country_code: str, 
                    start: pd.Timestamp, 
                    end: pd.Timestamp) -> pd.Series:
        """
        Fetch actual electricity load (consumption) for a country.
        Unit: MW (megawatts)
        """
        try:
            logger.info(f"Fetching load for {country_code}")
            load = self.client.query_load(
                country_code=country_code,
                start=start,
                end=end
            )
            return load
        except Exception as e:
            logger.error(f"Failed to fetch load for {country_code}: {e}")
            return None

    def fetch_generation(self, country_code: str, 
                          start: pd.Timestamp, 
                          end: pd.Timestamp) -> pd.DataFrame:
        """
        Fetch actual generation per production type (solar, wind, gas, nuclear, etc).
        Returns a DataFrame with one column per energy type.
        """
        try:
            logger.info(f"Fetching generation mix for {country_code}")
            generation = self.client.query_generation(
                country_code=country_code,
                start=start,
                end=end,
                psr_type=None  # None means all energy types
            )
            return generation
        except Exception as e:
            logger.error(f"Failed to fetch generation for {country_code}: {e}")
            return None

    def fetch_all_countries(self, days_back: int = 1) -> dict:
        """
        Fetch all data types for all 4 countries.
        
        Args:
            days_back: How many days of history to fetch (default: 1 = yesterday)
            
        Returns:
            Dictionary with country codes as keys, data as values
        """
        # Set the time window
        # ENTSO-E uses Europe/Brussels timezone
        timezone = 'Europe/Brussels'
        end = pd.Timestamp.now(tz=timezone).floor('H')  # current hour
        start = end - timedelta(days=days_back)
        
        logger.info(f"Fetching data from {start} to {end}")
        
        results = {}
        
        for country_code, country_name in COUNTRIES.items():
            logger.info(f"Processing {country_name} ({country_code})...")
            
            results[country_code] = {
                'prices': self.fetch_day_ahead_prices(country_code, start, end),
                'load': self.fetch_load(country_code, start, end),
                'generation': self.fetch_generation(country_code, start, end),
                'fetch_timestamp': datetime.utcnow().isoformat(),
                'country_name': country_name
            }
        
        return results, start, end


# This block only runs when you execute this file directly
# (not when it's imported by another file)
if __name__ == '__main__':
    # Quick test to make sure everything works
    client = ENTSOEClient()
    data, start, end = client.fetch_all_countries(days_back=1)
    
    for country, content in data.items():
        prices = content['prices']
        if prices is not None:
            print(f"\n{country}: {len(prices)} price records")
            print(prices.head(3))
        else:
            print(f"\n{country}: No price data retrieved")