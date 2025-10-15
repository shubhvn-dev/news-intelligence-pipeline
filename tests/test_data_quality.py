"""
Data quality tests for GDELT pipeline
"""
import pandas as pd
import pytest
import os

DATA_DIR = "data"

class TestBronzeLayer:
    """Tests for Bronze layer data quality"""
    
    def test_bronze_exists(self):
        """Test that Bronze layer directory exists"""
        bronze_path = f"{DATA_DIR}/bronze/gdelt/date=2025-10-15"
        assert os.path.exists(bronze_path), "Bronze layer should exist"
    
    def test_bronze_has_data(self):
        """Test that Bronze layer contains data"""
        bronze_path = f"{DATA_DIR}/bronze/gdelt/date=2025-10-15"
        if os.path.exists(bronze_path):
            df = pd.read_parquet(bronze_path)
            assert len(df) > 0, "Bronze layer should contain data"
            assert len(df) > 1000, "Should have ingested 1000+ events"

class TestSilverLayer:
    """Tests for Silver layer data quality"""
    
    def test_silver_required_columns(self):
        """Test that required columns exist in Silver layer"""
        silver_path = f"{DATA_DIR}/silver/gdelt/date=2025-10-15"
        if os.path.exists(silver_path):
            df = pd.read_parquet(silver_path)
            required_cols = [
                'GLOBALEVENTID', 'Actor1Name', 'AvgTone', 
                'ActionGeo_CountryCode', 'NumArticles'
            ]
            for col in required_cols:
                assert col in df.columns, f"Missing required column: {col}"
    
    def test_silver_data_quality_retention(self):
        """Test that data quality meets 90% threshold"""
        bronze_path = f"{DATA_DIR}/bronze/gdelt/date=2025-10-15"
        silver_path = f"{DATA_DIR}/silver/gdelt/date=2025-10-15"
        
        if os.path.exists(bronze_path) and os.path.exists(silver_path):
            bronze = pd.read_parquet(bronze_path)
            silver = pd.read_parquet(silver_path)
            retention = len(silver) / len(bronze)
            assert retention >= 0.90, f"Data quality retention {retention:.1%} below 90%"
    
    def test_silver_sentiment_valid_range(self):
        """Test that sentiment scores are in valid range"""
        silver_path = f"{DATA_DIR}/silver/gdelt/date=2025-10-15"
        if os.path.exists(silver_path):
            df = pd.read_parquet(silver_path)
            valid_sentiment = df['AvgTone'].between(-100, 100).all()
            assert valid_sentiment, "Sentiment scores should be between -100 and 100"
    
    def test_silver_coordinates_valid(self):
        """Test that geographic coordinates are valid"""
        silver_path = f"{DATA_DIR}/silver/gdelt/date=2025-10-15"
        if os.path.exists(silver_path):
            df = pd.read_parquet(silver_path)
            coords = df[df['ActionGeo_Lat'].notna()]
            if len(coords) > 0:
                assert coords['ActionGeo_Lat'].between(-90, 90).all(), "Invalid latitude"
                assert coords['ActionGeo_Long'].between(-180, 180).all(), "Invalid longitude"

class TestGoldLayer:
    """Tests for Gold layer AI enrichment"""
    
    def test_gold_has_summaries(self):
        """Test that Gold layer contains AI summaries"""
        gold_path = f"{DATA_DIR}/gold/gdelt/date=2025-10-15"
        if os.path.exists(gold_path):
            df = pd.read_parquet(gold_path)
            assert 'summary' in df.columns, "Gold layer should have summary column"
            assert 'success' in df.columns, "Gold layer should have success column"
    
    def test_gold_success_rate(self):
        """Test that AI processing has reasonable success rate"""
        gold_path = f"{DATA_DIR}/gold/gdelt/date=2025-10-15"
        if os.path.exists(gold_path):
            df = pd.read_parquet(gold_path)
            if 'success' in df.columns:
                success_rate = df['success'].sum() / len(df)
                assert success_rate >= 0.50, f"AI success rate {success_rate:.1%} too low"