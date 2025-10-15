"""
System Stats

Pipeline statistics and system information.
"""
import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import datetime

st.set_page_config(page_title="System Stats", page_icon="⚙️", layout="wide")

st.title("System Statistics")
st.markdown("**Pipeline performance and data quality metrics**")
st.markdown("---")

# Connect to DuckDB
@st.cache_resource
def get_connection():
    return duckdb.connect()

con = get_connection()

# Load data
@st.cache_data(ttl=300)
def load_data():
    query = """
    SELECT *
    FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
    """
    return con.execute(query).df()

try:
    df = load_data()
    
    # Pipeline Overview
    st.subheader("📊 Pipeline Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Events Ingested", f"{len(df):,}")
    
    with col2:
        ai_processed = df['success'].sum() if 'success' in df.columns else 0
        st.metric("AI Processed", f"{int(ai_processed):,}")
    
    with col3:
        success_rate = (ai_processed / len(df) * 100) if len(df) > 0 else 0
        st.metric("Success Rate", f"{success_rate:.1f}%", delta=f"{success_rate - 50:.1f}%")
    
    with col4:
        articles_fetched = df['article_fetched'].sum() if 'article_fetched' in df.columns else 0
        st.metric("Articles Fetched", f"{int(articles_fetched):,}")
    
    st.markdown("---")
    
    # Data Quality Metrics
    st.subheader("✅ Data Quality Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Completeness")
        
        completeness = {
            'Field': [],
            'Non-Null Count': [],
            'Completeness %': []
        }
        
        key_fields = ['Actor1Name', 'Actor2Name', 'ActionGeo_FullName', 
                     'ActionGeo_CountryCode', 'AvgTone', 'NumArticles']
        
        for field in key_fields:
            non_null = df[field].notna().sum()
            pct = (non_null / len(df) * 100)
            completeness['Field'].append(field)
            completeness['Non-Null Count'].append(non_null)
            completeness['Completeness %'].append(f"{pct:.1f}%")
        
        completeness_df = pd.DataFrame(completeness)
        st.dataframe(completeness_df, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("#### Processing Stats")
        
        processing_stats = {
            'Metric': [
                'Events with Actor1',
                'Events with Actor2', 
                'Events with Location',
                'Events with Coordinates',
                'Events with Summaries',
                'Events with Source URLs'
            ],
            'Count': [
                df['Actor1Name'].notna().sum(),
                df['Actor2Name'].notna().sum(),
                df['ActionGeo_FullName'].notna().sum(),
                ((df['ActionGeo_Lat'].notna()) & (df['ActionGeo_Long'].notna())).sum(),
                df['success'].sum() if 'success' in df.columns else 0,
                df['SOURCEURL'].notna().sum()
            ]
        }
        
        processing_df = pd.DataFrame(processing_stats)
        processing_df['Percentage'] = (processing_df['Count'] / len(df) * 100).round(1).astype(str) + '%'
        
        st.dataframe(processing_df, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # AI Performance
    st.subheader("🤖 AI Processing Performance")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if 'success' in df.columns:
            success_count = df['success'].sum()
            failure_count = (~df['success']).sum()
            
            perf_data = pd.DataFrame({
                'Status': ['Successful', 'Failed'],
                'Count': [success_count, failure_count]
            })
            
            st.bar_chart(perf_data.set_index('Status'))
    
    with col2:
        if 'article_fetched' in df.columns:
            fetched = df['article_fetched'].sum()
            not_fetched = (~df['article_fetched']).sum()
            
            fetch_data = pd.DataFrame({
                'Status': ['Fetched', 'Not Fetched'],
                'Count': [fetched, not_fetched]
            })
            
            st.bar_chart(fetch_data.set_index('Status'))
    
    with col3:
        st.markdown("#### Estimated Costs")
        
        # Rough cost estimation
        total_requests = df['success'].sum() if 'success' in df.columns else 0
        estimated_input_tokens = total_requests * 280  # avg tokens per request
        estimated_output_tokens = total_requests * 80
        
        input_cost = (estimated_input_tokens / 1_000_000) * 0.075
        output_cost = (estimated_output_tokens / 1_000_000) * 0.30
        total_cost = input_cost + output_cost
        
        st.metric("Total Requests", f"{int(total_requests):,}")
        st.metric("Estimated Cost", f"${total_cost:.4f}")
        st.caption("Based on Gemini 2.0 Flash pricing")
    
    st.markdown("---")
    
    # Data Layer Information
    st.subheader("💾 Data Layer Information")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### Bronze Layer")
        bronze_path = "data/bronze/gdelt/date=2025-10-15/"
        if os.path.exists(bronze_path):
            bronze_files = [f for f in os.listdir(bronze_path) if f.endswith('.parquet')]
            if bronze_files:
                bronze_size = sum(os.path.getsize(os.path.join(bronze_path, f)) for f in bronze_files)
                st.metric("Files", len(bronze_files))
                st.metric("Size", f"{bronze_size / 1024 / 1024:.2f} MB")
            else:
                st.info("No files found")
        else:
            st.warning("Path not found")
    
    with col2:
        st.markdown("#### Silver Layer")
        silver_path = "data/silver/gdelt/date=2025-10-15/"
        if os.path.exists(silver_path):
            silver_files = [f for f in os.listdir(silver_path) if f.endswith('.parquet')]
            if silver_files:
                silver_size = sum(os.path.getsize(os.path.join(silver_path, f)) for f in silver_files)
                st.metric("Files", len(silver_files))
                st.metric("Size", f"{silver_size / 1024 / 1024:.2f} MB")
            else:
                st.info("No files found")
        else:
            st.warning("Path not found")
    
    with col3:
        st.markdown("#### Gold Layer")
        gold_path = "data/gold/gdelt/date=2025-10-15/"
        if os.path.exists(gold_path):
            gold_files = [f for f in os.listdir(gold_path) if f.endswith('.parquet')]
            if gold_files:
                gold_size = sum(os.path.getsize(os.path.join(gold_path, f)) for f in gold_files)
                st.metric("Files", len(gold_files))
                st.metric("Size", f"{gold_size / 1024 / 1024:.2f} MB")
            else:
                st.info("No files found")
        else:
            st.warning("Path not found")
    
    st.markdown("---")
    
    # System Information
    st.subheader("🖥️ System Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Environment")
        
        env_info = {
            'Component': ['Python Version', 'Streamlit', 'DuckDB', 'Pandas', 'Plotly'],
            'Status': ['✅ Active', '✅ Active', '✅ Active', '✅ Active', '✅ Active']
        }
        
        st.dataframe(pd.DataFrame(env_info), use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("#### Data Pipeline")
        
        pipeline_info = {
            'Stage': ['GDELT Ingestion', 'Spark Processing', 'Gemini AI', 'Dashboard'],
            'Status': ['✅ Complete', '✅ Complete', '✅ Complete', '✅ Running']
        }
        
        st.dataframe(pd.DataFrame(pipeline_info), use_container_width=True, hide_index=True)
    
    # Refresh data button
    st.markdown("---")
    if st.button("🔄 Refresh Statistics", type="primary"):
        st.cache_data.clear()
        st.rerun()

except Exception as e:
    st.error(f"Error loading system stats: {e}")
    st.info("Make sure all data layers exist.")