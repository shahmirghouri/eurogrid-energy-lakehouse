"""
Silver Layer Transformation Job (PySpark)
Reads Bronze Parquet files, cleans and validates data,
writes clean Silver Parquet files with added metadata.
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType, IntegerType
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger('silver_job')

BRONZE_PATH = os.getenv('BRONZE_PATH', './data/bronze')
SILVER_PATH = os.getenv('SILVER_PATH', './data/silver')

COUNTRIES = ['BE', 'DE_LU', 'FR', 'NL']

# Country name mapping (for enrichment)
COUNTRY_NAMES = {
    'BE': 'Belgium',
    'DE_LU': 'Germany-Luxembourg',
    'FR': 'France',
    'NL': 'Netherlands'
}


def create_spark_session() -> SparkSession:
    """
    Create a local PySpark session with Delta Lake support.
    """
    # 1. Define the Delta Lake version that matches your Spark version.
    # For Spark 3.5.x, use 3.1.0. For Spark 3.4.x, use 2.4.0.
    DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"

    spark = (
        SparkSession.builder
        .appName("EuroGrid Silver Transformation")
        .master("local[*]")
        # 2. These configs tell Spark HOW to use Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # 3. This line tells Spark WHERE to download the Delta libraries from
        .config("spark.jars.packages", DELTA_PACKAGE)
        # Performance settings for local mode
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created with Delta Lake support")
    return spark

def transform_prices(spark: SparkSession, country: str, date_path: str):
    """
    Transform raw price data from Bronze to Silver.
    
    Transformations applied:
    1. Parse timestamp string to proper timestamp type
    2. Remove null prices
    3. Flag suspicious prices (outside valid range)
    4. Add country metadata columns
    5. Add processing timestamp
    """
    bronze_file = Path(BRONZE_PATH) / country / date_path / 'prices.parquet'
    
    if not bronze_file.exists():
        logger.warning(f"Bronze prices file not found: {bronze_file}")
        return None
    
    logger.info(f"Transforming prices for {country}...")
    
    # Read the raw Bronze file
    df = spark.read.parquet(str(bronze_file))
    
    # Step 1: Parse timestamp strings to proper Spark TimestampType
    df = df.withColumn(
        'timestamp',
        F.to_timestamp(F.col('timestamp'))
    )
    
    # Step 2: Remove rows where price is null
    rows_before = df.count()
    df = df.dropna(subset=['price_eur_per_mwh'])
    rows_after = df.count()
    
    if rows_before != rows_after:
        logger.warning(f"Dropped {rows_before - rows_after} null price rows for {country}")
    
    # Step 3: Add data quality flags
    # Valid ENTSO-E price range: -500 to 3000 EUR/MWh
    df = df.withColumn(
        'is_negative_price',
        F.col('price_eur_per_mwh') < 0
    ).withColumn(
        'is_suspicious_price',
        (F.col('price_eur_per_mwh') < -500) | (F.col('price_eur_per_mwh') > 3000)
    )
    
    # Step 4: Add country metadata
    df = df.withColumn('country_code', F.lit(country))
    df = df.withColumn('country_name', F.lit(COUNTRY_NAMES.get(country, country)))
    
    # Step 5: Add time dimension columns (useful for SQL queries later)
    df = df.withColumn('year', F.year(F.col('timestamp')))
    df = df.withColumn('month', F.month(F.col('timestamp')))
    df = df.withColumn('day', F.dayofmonth(F.col('timestamp')))
    df = df.withColumn('hour', F.hour(F.col('timestamp')))
    df = df.withColumn('day_of_week', F.dayofweek(F.col('timestamp')))
    
    # Step 6: Add processing metadata
    df = df.withColumn(
        'silver_processed_at',
        F.lit(datetime.utcnow().isoformat())
    )
    
    # Remove exact duplicate rows
    df = df.dropDuplicates(['timestamp', 'country_code'])
    
    logger.info(f"{country} prices: {df.count()} records after Silver transformation")
    return df


def save_to_silver(df, country: str, data_type: str, date_path: str):
    """
    Save transformed DataFrame to Silver layer as Parquet.
    Partitioned by year/month for efficient querying.
    """
    if df is None:
        return
    
    output_path = str(Path(SILVER_PATH) / data_type / country)
    
    # Write as Parquet, partitioned by year and month
    # This makes queries like "give me all data for March 2026" very fast
    (df.write
       .mode("append")          # append new data, don't overwrite existing
       .partitionBy("year", "month")  # organise files by year/month
       .parquet(output_path))
    
    logger.info(f"Saved Silver {data_type} for {country} to {output_path}")


def print_statistics(spark: SparkSession, country: str, date_path: str):
    """Print summary statistics for today's Silver data."""
    prices_path = str(Path(SILVER_PATH) / 'prices' / country)
    
    try:
        df = spark.read.parquet(prices_path)
        
        stats = df.agg(
            F.count('price_eur_per_mwh').alias('record_count'),
            F.min('price_eur_per_mwh').alias('min_price'),
            F.max('price_eur_per_mwh').alias('max_price'),
            F.avg('price_eur_per_mwh').alias('avg_price'),
            F.sum(F.col('is_negative_price').cast('int')).alias('negative_hours')
        ).collect()[0]
        
        logger.info(
            f"\n{'='*50}\n"
            f"{country} Price Statistics:\n"
            f"  Records:        {stats['record_count']}\n"
            f"  Min price:      {stats['min_price']:.2f} EUR/MWh\n"
            f"  Max price:      {stats['max_price']:.2f} EUR/MWh\n"
            f"  Avg price:      {stats['avg_price']:.2f} EUR/MWh\n"
            f"  Negative hours: {stats['negative_hours']}\n"
            f"{'='*50}"
        )
    except Exception as e:
        logger.warning(f"Could not compute statistics: {e}")


def run_silver_job():
    """Main entry point for the Silver transformation job."""
    logger.info("Starting Silver transformation job...")
    
    spark = create_spark_session()
    
    today = datetime.utcnow()
    date_path = f"{today.year}/{today.month:02d}/{today.day:02d}"
    
    for country in COUNTRIES:
        logger.info(f"\nProcessing Silver for {country}...")
        
        # Transform prices
        prices_df = transform_prices(spark, country, date_path)
        save_to_silver(prices_df, country, 'prices', date_path)
        
        # Print stats to see what we have
        print_statistics(spark, country, date_path)
    
    spark.stop()
    logger.info("Silver transformation job complete!")


if __name__ == '__main__':
    run_silver_job()