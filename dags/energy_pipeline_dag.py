"""
EuroGrid Energy Pipeline DAG
Runs daily at 07:00 Brussels time to fetch previous day's electricity data.

DAG = Directed Acyclic Graph — Airflow's name for a pipeline.
Each 'task' in the DAG is one step. We define the order they run.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

# Default arguments applied to every task in this DAG
default_args = {
    'owner': 'muhammad_ghouri',
    'depends_on_past': False,     # don't require yesterday's run to succeed
    'email_on_failure': False,    # set to True + add email to get alerts
    'email_on_retry': False,
    'retries': 3,                  # retry failed tasks 3 times
    'retry_delay': timedelta(minutes=5),  # wait 5 mins between retries
}

# Define the DAG
with DAG(
    dag_id='eurogrid_energy_pipeline',
    default_args=default_args,
    description='Fetch EU electricity market data from ENTSO-E daily',
    
    # Run every day at 07:00 UTC
    # Cron format: minute hour day month weekday
    schedule_interval='0 7 * * *',
    
    # Start date (in the past - required by Airflow)
    start_date=datetime(2024, 1, 1),
    
    # Don't run for every day since start_date (we don't want 500 backfill runs)
    catchup=False,
    
    # Tags help you filter DAGs in the Airflow UI
    tags=['energy', 'entsoe', 'bronze', 'portfolio'],
) as dag:

    # -------------------------------------------------------
    # Task 1: Fetch and save Bronze data from ENTSO-E
    # -------------------------------------------------------
    def run_bronze_ingestion(**context):
        """
        This function runs inside Airflow.
        context contains Airflow metadata (run date, task instance, etc.)
        """
        # Import here to avoid issues with Airflow's module loading
        import sys
        sys.path.insert(0, '/opt/airflow')
        
        from ingestion.bronze_loader import load_to_bronze
        
        # Fetch 1 day of data (yesterday)
        result = load_to_bronze(days_back=1)
        
        if result:
            print("Bronze ingestion completed successfully!")
        else:
            raise Exception("Bronze ingestion failed!")

    # -------------------------------------------------------
    # Task 2: Validate that data was actually saved
    # -------------------------------------------------------
    def validate_bronze_data(**context):
        """
        Simple data quality check after ingestion.
        Verifies files were created and are not empty.
        """
        import os
        from pathlib import Path
        from datetime import datetime
        import pandas as pd
        
        today = datetime.utcnow()
        base_path = Path('/opt/airflow/data/bronze')
        date_path = f"{today.year}/{today.month:02d}/{today.day:02d}"
        
        countries_validated = 0
        issues = []
        
        for country in ['BE', 'DE_LU', 'FR', 'NL']:
            prices_file = base_path / country / date_path / 'prices.parquet'
            
            if not prices_file.exists():
                issues.append(f"{country}: prices.parquet NOT FOUND")
                continue
            
            # Check file is not empty
            df = pd.read_parquet(prices_file)
            if len(df) == 0:
                issues.append(f"{country}: prices.parquet is EMPTY")
            else:
                print(f"{country}: OK - {len(df)} price records")
                countries_validated += 1
        
        if issues:
            # Log warnings but don't fail — partial data is better than none
            for issue in issues:
                print(f"WARNING: {issue}")
        
        print(f"Validated {countries_validated}/4 countries successfully")

    # -------------------------------------------------------
    # Task 3: Check for negative electricity prices (the fun part!)
    # -------------------------------------------------------
    def check_negative_prices(**context):
        """
        Detects hours where electricity prices went negative.
        This happens when renewables overproduce — fascinating market signal!
        Logs to Airflow so you can see it in the dashboard.
        """
        import pandas as pd
        from pathlib import Path
        from datetime import datetime
        
        today = datetime.utcnow()
        base_path = Path('/opt/airflow/data/bronze')
        date_path = f"{today.year}/{today.month:02d}/{today.day:02d}"
        
        for country in ['BE', 'DE_LU', 'FR', 'NL']:
            prices_file = base_path / country / date_path / 'prices.parquet'
            
            if not prices_file.exists():
                continue
            
            df = pd.read_parquet(prices_file)
            negative_hours = df[df['price_eur_per_mwh'] < 0]
            
            if len(negative_hours) > 0:
                print(f"ALERT: {country} had {len(negative_hours)} hours of NEGATIVE prices!")
                print(negative_hours.to_string())
                # In production you would send a Slack message here
            else:
                print(f"{country}: All prices positive")

    # Create task objects
    task_bronze_ingestion = PythonOperator(
        task_id='bronze_ingestion',
        python_callable=run_bronze_ingestion,
    )

    task_validate = PythonOperator(
        task_id='validate_bronze_data',
        python_callable=validate_bronze_data,
    )

    task_check_prices = PythonOperator(
        task_id='check_negative_prices',
        python_callable=check_negative_prices,
    )

    # Define the order: ingest → validate → check prices
    # >> means "runs before"
    task_bronze_ingestion >> task_validate >> task_check_prices