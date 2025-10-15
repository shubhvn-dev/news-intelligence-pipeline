"""
GDELT Data Ingestion DAG

This DAG downloads and processes GDELT (Global Database of Events, Language, and Tone)
events data into the Bronze layer of our data lake.

GDELT provides structured data about global events extracted from news articles,
updating every 15 minutes with worldwide coverage.

Author: Data Engineering Team
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import gzip
import pandas as pd
import os
from pathlib import Path

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Bronze layer path
BRONZE_PATH = '/opt/airflow/data/bronze/gdelt'

def get_latest_gdelt_url():
    """
    Fetch the URL of the most recent GDELT export file.
    
    Returns:
        str: URL of the latest GDELT export CSV file
        
    Raises:
        ValueError: If export file URL cannot be found
        requests.RequestException: If HTTP request fails
    """
    try:
        # GDELT updates every 15 minutes - this file contains latest URLs
        update_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
        response = requests.get(update_url, timeout=30)
        response.raise_for_status()
        
        # Parse the response (space-separated: size hash url)
        lines = response.text.strip().split('\n')
        
        # Find the export file (events data)
        for line in lines:
            parts = line.split()
            if len(parts) >= 3 and 'export.CSV.zip' in parts[2]:
                url = parts[2]
                print(f"Found latest GDELT export: {url}")
                return url
        
        raise ValueError("Could not find export.CSV.zip in lastupdate.txt")
        
    except Exception as e:
        print(f"Error fetching GDELT URL: {e}")
        raise

def download_and_extract_gdelt(**context):
    """
    Download GDELT file and extract the CSV content.
    
    This task downloads the latest GDELT export file (ZIP containing CSV) and extracts it
    to a temporary location for processing.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        str: Path to the extracted CSV file
        
    Raises:
        requests.RequestException: If download fails
        IOError: If file extraction fails
    """
    import zipfile
    
    try:
        # Get the latest URL
        gdelt_url = get_latest_gdelt_url()
        
        # Create temp directory
        os.makedirs('/tmp/gdelt', exist_ok=True)
        
        # Download the file
        print("Downloading GDELT file...")
        response = requests.get(gdelt_url, timeout=120)
        response.raise_for_status()
        
        zip_path = '/tmp/gdelt/latest.zip'
        with open(zip_path, 'wb') as f:
            f.write(response.content)
        
        file_size_mb = len(response.content) / 1024 / 1024
        print(f"Downloaded {file_size_mb:.2f} MB")
        
        # Extract the ZIP file (it's a real ZIP, not gzip)
        csv_path = '/tmp/gdelt/latest.csv'
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get the first (and only) file in the ZIP
            csv_filename = zip_ref.namelist()[0]
            print(f"Extracting {csv_filename}...")
            
            # Extract to our target path
            with zip_ref.open(csv_filename) as source, open(csv_path, 'wb') as target:
                target.write(source.read())
        
        print(f"Extracted to {csv_path}")
        
        # Push the file path to XCom for next task
        context['task_instance'].xcom_push(key='csv_path', value=csv_path)
        context['task_instance'].xcom_push(key='gdelt_url', value=gdelt_url)
        
        return csv_path
        
    except Exception as e:
        print(f"Error downloading GDELT: {e}")
        raise

def parse_and_save_to_bronze(**context):
    """
    Parse GDELT CSV and save to Bronze layer as Parquet.
    
    This task reads the downloaded GDELT CSV file, applies schema, adds metadata,
    and saves the data to the Bronze layer in Parquet format with date partitioning.
    
    Args:
        context: Airflow context dictionary
        
    Returns:
        dict: Statistics about the ingestion (num_events, countries, etc.)
        
    Raises:
        pandas.errors.ParserError: If CSV parsing fails
        IOError: If file operations fail
    """
    try:
        # Get the CSV path from previous task
        csv_path = context['task_instance'].xcom_pull(
            task_ids='download_gdelt', 
            key='csv_path'
        )
        
        # If XCom didn't work, try the default location
        if csv_path is None:
            csv_path = '/tmp/gdelt/latest.csv'
            print(f"Warning: XCom returned None, using default path: {csv_path}")
        
        # Verify the file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found at {csv_path}")
        
        print(f"Parsing GDELT CSV: {csv_path}")
        
        # GDELT v2.0 Event column names (all 61 columns)
        # Source: http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
        column_names = [
            'GLOBALEVENTID', 'SQLDATE', 'MonthYear', 'Year', 'FractionDate',
            'Actor1Code', 'Actor1Name', 'Actor1CountryCode', 'Actor1KnownGroupCode',
            'Actor1EthnicCode', 'Actor1Religion1Code', 'Actor1Religion2Code',
            'Actor1Type1Code', 'Actor1Type2Code', 'Actor1Type3Code',
            'Actor2Code', 'Actor2Name', 'Actor2CountryCode', 'Actor2KnownGroupCode',
            'Actor2EthnicCode', 'Actor2Religion1Code', 'Actor2Religion2Code',
            'Actor2Type1Code', 'Actor2Type2Code', 'Actor2Type3Code',
            'IsRootEvent', 'EventCode', 'EventBaseCode', 'EventRootCode',
            'QuadClass', 'GoldsteinScale', 'NumMentions', 'NumSources', 'NumArticles',
            'AvgTone',
            'Actor1Geo_Type', 'Actor1Geo_FullName', 'Actor1Geo_CountryCode',
            'Actor1Geo_ADM1Code', 'Actor1Geo_ADM2Code', 'Actor1Geo_Lat', 'Actor1Geo_Long',
            'Actor1Geo_FeatureID',
            'Actor2Geo_Type', 'Actor2Geo_FullName', 'Actor2Geo_CountryCode',
            'Actor2Geo_ADM1Code', 'Actor2Geo_ADM2Code', 'Actor2Geo_Lat', 'Actor2Geo_Long',
            'Actor2Geo_FeatureID',
            'ActionGeo_Type', 'ActionGeo_FullName', 'ActionGeo_CountryCode',
            'ActionGeo_ADM1Code', 'ActionGeo_ADM2Code', 'ActionGeo_Lat', 'ActionGeo_Long',
            'ActionGeo_FeatureID',
            'DATEADDED', 'SOURCEURL'
        ]
        
        # Read CSV (tab-separated, no header)
        df = pd.read_csv(
            csv_path,
            sep='\t',
            header=None,
            names=column_names,
            usecols=range(len(column_names)),
            low_memory=False,
            encoding='utf-8',
            on_bad_lines='skip'
        )
        
        print(f"Loaded {len(df):,} events")
        print(f"Date range: {df['SQLDATE'].min()} to {df['SQLDATE'].max()}")
        print(f"Countries: {df['ActionGeo_CountryCode'].nunique()}")
        
        # Convert NumArticles to numeric, coercing errors to NaN
        df['NumArticles'] = pd.to_numeric(df['NumArticles'], errors='coerce').fillna(0).astype(int)
        
        print(f"Articles: {df['NumArticles'].sum():,}")
        
        # Add ingestion metadata
        df['ingestion_timestamp'] = datetime.now()
        df['source'] = 'gdelt'
        
        # Create Bronze directory with date partitioning
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_dir = f"{BRONZE_PATH}/date={date_str}"
        os.makedirs(output_dir, exist_ok=True)
        
        # Save as Parquet with Snappy compression
        output_path = f"{output_dir}/events.parquet"
        df.to_parquet(output_path, compression='snappy', index=False)
        
        file_size_mb = os.path.getsize(output_path) / 1024 / 1024
        print(f"Saved to Bronze: {output_path} ({file_size_mb:.2f} MB)")
        
        # Push statistics to XCom for reporting
        stats = {
            'num_events': len(df),
            'num_countries': int(df['ActionGeo_CountryCode'].nunique()),
            'num_articles': int(df['NumArticles'].sum()),
            'output_path': output_path,
            'file_size_mb': round(file_size_mb, 2)
        }
        
        context['task_instance'].xcom_push(key='stats', value=stats)
        
        return stats
        
    except Exception as e:
        print(f"Error parsing GDELT: {e}")
        raise

def print_summary(**context):
    """
    Print a summary of the ingestion process.
    
    Retrieves statistics from the previous task and prints a formatted summary
    to the task logs for monitoring and verification purposes.
    
    Args:
        context: Airflow context dictionary
    """
    stats = context['task_instance'].xcom_pull(
        task_ids='parse_and_save', 
        key='stats'
    )
    
    print("\n" + "="*60)
    print("GDELT INGESTION SUMMARY")
    print("="*60)
    print(f"Events ingested:     {stats['num_events']:,}")
    print(f"Countries covered:   {stats['num_countries']}")
    print(f"Articles referenced: {stats['num_articles']:,}")
    print(f"Output file:         {stats['output_path']}")
    print(f"File size:           {stats['file_size_mb']} MB")
    print("="*60 + "\n")

# Define the DAG
dag = DAG(
    'gdelt_ingestion',
    default_args=default_args,
    description='Ingest GDELT global events data into Bronze layer',
    schedule_interval=None,  # Manual trigger for initial testing
    catchup=False,
    tags=['ingestion', 'gdelt', 'bronze'],
)

# Define tasks
task_download = PythonOperator(
    task_id='download_gdelt',
    python_callable=download_and_extract_gdelt,
    dag=dag,
)

task_parse = PythonOperator(
    task_id='parse_and_save',
    python_callable=parse_and_save_to_bronze,
    dag=dag,
)

task_summary = PythonOperator(
    task_id='print_summary',
    python_callable=print_summary,
    dag=dag,
)

# Set task dependencies
task_download >> task_parse >> task_summary