"""
GDELT Data Processor

Reads raw GDELT data from Bronze layer, cleans it, and prepares it for analysis.
Outputs to Silver layer with quality improvements.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, length, regexp_replace, 
    to_timestamp, year, month, dayofmonth, lit
)
from pyspark.sql.types import DoubleType
from datetime import datetime
import sys

def create_spark_session(app_name="GDELT Processor"):
    """
    Create and configure Spark session.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def clean_gdelt_data(df):
    """
    Clean and standardize GDELT data.
    
    Args:
        df: Input DataFrame from Bronze layer
        
    Returns:
        DataFrame: Cleaned data
    """
    print("Cleaning GDELT data...")
    
    # Convert numeric columns to proper types
    numeric_cols = ['GoldsteinScale', 'NumMentions', 'NumSources', 
                    'NumArticles', 'AvgTone', 'ActionGeo_Lat', 'ActionGeo_Long']
    
    for col_name in numeric_cols:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
    
    # Clean text columns - remove extra whitespace
    text_cols = ['Actor1Name', 'Actor2Name', 'ActionGeo_FullName']
    for col_name in text_cols:
        df = df.withColumn(
            col_name,
            when(col(col_name).isNotNull(), 
                 trim(regexp_replace(col(col_name), r'\s+', ' ')))
        )
    
    # Filter out invalid records
    df_clean = df.filter(
        # Must have valid event ID
        (col('GLOBALEVENTID').isNotNull()) &
        # Must have at least one actor
        (col('Actor1Name').isNotNull() | col('Actor2Name').isNotNull()) &
        # Must have valid date
        (col('SQLDATE').isNotNull()) &
        # Must have source URL
        (col('SOURCEURL').isNotNull())
    )
    
    print(f"Records after cleaning: {df_clean.count():,}")
    
    return df_clean

def filter_quality_events(df):
    """
    Filter for high-quality events - keeping it simple for now.
    
    Args:
        df: Cleaned DataFrame
        
    Returns:
        DataFrame: Filtered events
    """
    print("Applying minimal quality filters...")
    
    # Just keep events that have at least a country code
    df_quality = df.filter(
        col('ActionGeo_CountryCode').isNotNull()
    )
    
    print(f"Events with country code: {df_quality.count():,}")
    
    return df_quality

def deduplicate_events(df):
    """
    Remove duplicate events based on multiple criteria.
    
    Args:
        df: DataFrame with potential duplicates
        
    Returns:
        DataFrame: Deduplicated events
    """
    print("Deduplicating events...")
    
    initial_count = df.count()
    
    # Remove exact duplicates by GLOBALEVENTID
    df_dedup = df.dropDuplicates(['GLOBALEVENTID'])
    
    print(f"Removed {initial_count - df_dedup.count():,} duplicate events")
    print(f"Unique events: {df_dedup.count():,}")
    
    return df_dedup

def add_derived_columns(df):
    """
    Add useful derived columns for analysis.
    
    Args:
        df: Input DataFrame
        
    Returns:
        DataFrame: Enhanced with derived columns
    """
    print("Adding derived columns...")
    
    df_enhanced = df \
        .withColumn('processing_timestamp', lit(datetime.now())) \
        .withColumn('has_actor1', col('Actor1Name').isNotNull().cast('int')) \
        .withColumn('has_actor2', col('Actor2Name').isNotNull().cast('int')) \
        .withColumn('tone_category', 
                   when(col('AvgTone') > 5, 'positive')
                   .when(col('AvgTone') < -5, 'negative')
                   .otherwise('neutral')) \
        .withColumn('event_scale',
                   when(col('GoldsteinScale') > 5, 'cooperative')
                   .when(col('GoldsteinScale') < -5, 'conflictual')
                   .otherwise('moderate'))
    
    return df_enhanced

def process_gdelt_to_silver(bronze_path, silver_path, date_partition):
    """
    Main processing function: Bronze to Silver layer.
    
    Args:
        bronze_path: Path to Bronze layer data
        silver_path: Path to Silver layer output
        date_partition: Date partition to process (YYYY-MM-DD)
    """
    print(f"\n{'='*60}")
    print(f"GDELT SPARK PROCESSOR - Bronze to Silver")
    print(f"{'='*60}")
    print(f"Date partition: {date_partition}")
    print(f"Bronze path: {bronze_path}")
    print(f"Silver path: {silver_path}")
    print(f"{'='*60}\n")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read Bronze data
        input_path = f"{bronze_path}/date={date_partition}"
        print(f"Reading from: {input_path}")
        
        df_bronze = spark.read.parquet(input_path)
        initial_count = df_bronze.count()
        print(f"Initial records: {initial_count:,}")
        print(f"Initial columns: {len(df_bronze.columns)}")
        
        # Processing pipeline
        df_clean = clean_gdelt_data(df_bronze)
        df_quality = filter_quality_events(df_clean)
        df_dedup = deduplicate_events(df_quality)
        df_silver = add_derived_columns(df_dedup)
        
        # Save to Silver layer
        output_path = f"{silver_path}/date={date_partition}"
        print(f"\nWriting to: {output_path}")
        
        df_silver.write \
            .mode('overwrite') \
            .parquet(output_path)
        
        final_count = df_silver.count()
        
        # Print summary
        print(f"\n{'='*60}")
        print("PROCESSING SUMMARY")
        print(f"{'='*60}")
        print(f"Input records:        {initial_count:,}")
        print(f"Output records:       {final_count:,}")
        print(f"Records removed:      {initial_count - final_count:,} ({100*(initial_count-final_count)/initial_count:.1f}%)")
        print(f"Quality retention:    {100*final_count/initial_count:.1f}%")
        print(f"Output columns:       {len(df_silver.columns)}")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"Error processing GDELT data: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    # Parse command line arguments
    if len(sys.argv) < 4:
        print("Usage: spark-submit gdelt_processor.py <bronze_path> <silver_path> <date_partition>")
        print("Example: spark-submit gdelt_processor.py /opt/spark-data/bronze/gdelt /opt/spark-data/silver/gdelt 2025-10-15")
        sys.exit(1)
    
    bronze_path = sys.argv[1]
    silver_path = sys.argv[2]
    date_partition = sys.argv[3]
    
    process_gdelt_to_silver(bronze_path, silver_path, date_partition)