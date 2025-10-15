"""
Gemini AI Processing DAG

Processes Silver layer events with Gemini to generate summaries.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sys
import os
from dotenv import load_dotenv

# Add gemini module to path
sys.path.append('/opt/airflow/gemini')

from summarizer import GeminiSummarizer

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 14),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SILVER_PATH = '/opt/airflow/data/silver/gdelt'
GOLD_PATH = '/opt/airflow/data/gold/gdelt'

def process_events_with_gemini(**context):
    """
    Process Silver layer events with Gemini AI.
    
    Generates summaries for all events and saves to Gold layer.
    """
    # Load environment variables from mounted .env file
    load_dotenv('/opt/airflow/.env')
    
    date_partition = context['ds']  # Execution date
    
    print(f"\n{'='*60}")
    print("GEMINI PROCESSING - Silver to Gold")
    print(f"{'='*60}")
    print(f"Date partition: {date_partition}")
    print(f"{'='*60}\n")
    
    # Read Silver layer data
    silver_input = f"{SILVER_PATH}/date={date_partition}"
    print(f"Reading from: {silver_input}")
    
    df = pd.read_parquet(silver_input)
    print(f"Loaded {len(df):,} events")
    
    # Limit to first 50 events for testing (remove this in production)
    df_sample = df
    print(f"Processing first {len(df_sample)} events (testing mode)")
    
    # Convert to list of dicts
    events = df_sample.to_dict('records')
    
    # Initialize Gemini
    print("\nInitializing Gemini summarizer...")
    
    # Get API key from environment
    api_key = os.getenv('GEMINI_API_KEY')
    
    # Debug output
    print(f".env file exists: {os.path.exists('/opt/airflow/.env')}")
    print(f"API key loaded: {'Yes' if api_key else 'No'}")
    if api_key:
        print(f"API key preview: {api_key[:10]}...")
    
    if not api_key:
        raise ValueError("GEMINI_API_KEY not found in environment. Check if .env file is mounted correctly.")
    
    summarizer = GeminiSummarizer(api_key=api_key)
    
    # Process events
    print(f"\nGenerating summaries with article fetching...")
    print("This may take a few minutes...\n")
    
    results = summarizer.summarize_batch(
        events, 
        delay=5.0,  # 1 second between requests to avoid rate limits
        fetch_articles=True
    )
    
    # Create results dataframe
    summaries_df = pd.DataFrame(results)
    
    # Merge with original data
    df_with_summaries = df_sample.merge(
        summaries_df[['event_id', 'summary', 'article_fetched', 'success']],
        left_on='GLOBALEVENTID',
        right_on='event_id',
        how='left'
    )
    
    # Add processing metadata
    df_with_summaries['gemini_processing_timestamp'] = datetime.now()
    df_with_summaries['gemini_model'] = 'gemini-2.0-flash'
    
    # Save to Gold layer
    os.makedirs(f"{GOLD_PATH}/date={date_partition}", exist_ok=True)
    output_path = f"{GOLD_PATH}/date={date_partition}/events_with_summaries.parquet"
    
    df_with_summaries.to_parquet(output_path, compression='snappy', index=False)
    
    print(f"\nSaved to: {output_path}")
    
    # Print statistics
    successful = summaries_df['success'].sum()
    articles_fetched = summaries_df['article_fetched'].sum()
    
    print(f"\n{'='*60}")
    print("GEMINI PROCESSING SUMMARY")
    print(f"{'='*60}")
    print(f"Events processed:      {len(results):,}")
    print(f"Successful summaries:  {successful:,}")
    print(f"Articles fetched:      {articles_fetched:,}")
    print(f"Success rate:          {100*successful/len(results):.1f}%")
    print(f"Article fetch rate:    {100*articles_fetched/len(results):.1f}%")
    
    # Cost summary
    summarizer.print_cost_summary()
    
    # Push stats to XCom
    stats = {
        'events_processed': len(results),
        'successful': int(successful),
        'articles_fetched': int(articles_fetched),
        'output_path': output_path
    }
    
    context['task_instance'].xcom_push(key='stats', value=stats)
    
    return stats

def print_summary(**context):
    """Print final summary."""
    stats = context['task_instance'].xcom_pull(
        task_ids='process_with_gemini',
        key='stats'
    )
    
    print("\n" + "="*60)
    print("GEMINI PROCESSING COMPLETE")
    print("="*60)
    print(f"Events processed:     {stats['events_processed']:,}")
    print(f"Successful summaries: {stats['successful']:,}")
    print(f"Articles fetched:     {stats['articles_fetched']:,}")
    print(f"Output file:          {stats['output_path']}")
    print("="*60 + "\n")

# Define DAG
dag = DAG(
    'gemini_processing',
    default_args=default_args,
    description='Process events with Gemini AI for summarization',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['gemini', 'ai', 'gold'],
)

# Define tasks
task_process = PythonOperator(
    task_id='process_with_gemini',
    python_callable=process_events_with_gemini,
    dag=dag,
)

task_summary = PythonOperator(
    task_id='print_summary',
    python_callable=print_summary,
    dag=dag,
)

# Task dependencies
task_process >> task_summary