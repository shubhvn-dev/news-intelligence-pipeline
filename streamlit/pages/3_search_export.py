"""
Search & Export

Advanced search functionality and data export capabilities.
"""
import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="Search & Export", page_icon="🔍", layout="wide")

st.title("Search & Export")
st.markdown("**Search events and export filtered data**")
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
    
    # Search Section
    st.subheader("🔍 Advanced Search")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        search_term = st.text_input(
            "Search in Actor Names, Locations, or Summaries",
            placeholder="e.g., Chicago, MAYOR, budget..."
        )
    
    with col2:
        search_field = st.selectbox(
            "Search in",
            ["All Fields", "Actor Names", "Locations", "AI Summaries"]
        )
    
    # Apply search
    if search_term:
        search_lower = search_term.lower()
        
        if search_field == "Actor Names":
            mask = (
                df['Actor1Name'].fillna('').str.lower().str.contains(search_lower) |
                df['Actor2Name'].fillna('').str.lower().str.contains(search_lower)
            )
        elif search_field == "Locations":
            mask = df['ActionGeo_FullName'].fillna('').str.lower().str.contains(search_lower)
        elif search_field == "AI Summaries":
            mask = df['summary'].fillna('').str.lower().str.contains(search_lower)
        else:  # All Fields
            mask = (
                df['Actor1Name'].fillna('').str.lower().str.contains(search_lower) |
                df['Actor2Name'].fillna('').str.lower().str.contains(search_lower) |
                df['ActionGeo_FullName'].fillna('').str.lower().str.contains(search_lower) |
                df['summary'].fillna('').str.lower().str.contains(search_lower)
            )
        
        df_filtered = df[mask]
        st.success(f"Found {len(df_filtered)} events matching '{search_term}'")
    else:
        df_filtered = df
    
    st.markdown("---")
    
    # Advanced Filters
    st.subheader("🎚️ Advanced Filters")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        countries = ['All'] + sorted(df_filtered['ActionGeo_CountryCode'].dropna().unique().tolist())
        selected_country = st.selectbox("Country", countries)
        if selected_country != 'All':
            df_filtered = df_filtered[df_filtered['ActionGeo_CountryCode'] == selected_country]
    
    with col2:
        sentiment_filter = st.selectbox(
            "Sentiment",
            ["All", "Very Positive (>5)", "Positive (2-5)", "Neutral (-2 to 2)", 
             "Negative (-5 to -2)", "Very Negative (<-5)"]
        )
        
        if sentiment_filter == "Very Positive (>5)":
            df_filtered = df_filtered[df_filtered['AvgTone'] > 5]
        elif sentiment_filter == "Positive (2-5)":
            df_filtered = df_filtered[(df_filtered['AvgTone'] > 2) & (df_filtered['AvgTone'] <= 5)]
        elif sentiment_filter == "Neutral (-2 to 2)":
            df_filtered = df_filtered[(df_filtered['AvgTone'] >= -2) & (df_filtered['AvgTone'] <= 2)]
        elif sentiment_filter == "Negative (-5 to -2)":
            df_filtered = df_filtered[(df_filtered['AvgTone'] >= -5) & (df_filtered['AvgTone'] < -2)]
        elif sentiment_filter == "Very Negative (<-5)":
            df_filtered = df_filtered[df_filtered['AvgTone'] < -5]
    
    with col3:
        min_articles = st.number_input("Min Articles", min_value=0, max_value=100, value=0)
        if min_articles > 0:
            df_filtered = df_filtered[df_filtered['NumArticles'] >= min_articles]
    
    with col4:
        has_summary = st.selectbox("Has AI Summary", ["All", "Yes", "No"])
        if has_summary == "Yes":
            df_filtered = df_filtered[df_filtered['success'] == True]
        elif has_summary == "No":
            df_filtered = df_filtered[df_filtered['success'] == False]
    
    # Results Summary
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Filtered Events", f"{len(df_filtered):,}")
    
    with col2:
        if len(df_filtered) > 0:
            avg_sentiment = df_filtered['AvgTone'].mean()
            st.metric("Avg Sentiment", f"{avg_sentiment:.2f}")
    
    with col3:
        if len(df_filtered) > 0:
            total_articles = df_filtered['NumArticles'].sum()
            st.metric("Total Articles", f"{int(total_articles):,}")
    
    with col4:
        if len(df_filtered) > 0:
            countries_count = df_filtered['ActionGeo_CountryCode'].nunique()
            st.metric("Countries", f"{countries_count}")
    
    # Display Results
    st.markdown("---")
    st.subheader("📋 Search Results")
    
    if len(df_filtered) > 0:
        # Display format selection
        display_format = st.radio(
            "Display Format",
            ["Table", "Cards", "Summaries Only"],
            horizontal=True
        )
        
        if display_format == "Table":
            # Table view
            display_cols = [
                'GLOBALEVENTID', 'Actor1Name', 'Actor2Name', 'ActionGeo_FullName',
                'ActionGeo_CountryCode', 'AvgTone', 'NumArticles', 'success'
            ]
            
            st.dataframe(
                df_filtered[display_cols].head(100),
                use_container_width=True,
                height=500
            )
            
        elif display_format == "Cards":
            # Card view
            num_to_show = st.slider("Number of events to display", 5, 50, 10)
            
            for idx, row in df_filtered.head(num_to_show).iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.markdown(f"### {row['Actor1Name']}")
                        st.markdown(f"**Location:** {row['ActionGeo_FullName']} ({row['ActionGeo_CountryCode']})")
                        
                        if row['Actor2Name']:
                            st.markdown(f"**Secondary Actor:** {row['Actor2Name']}")
                        
                        if row['success'] and pd.notna(row['summary']):
                            st.markdown(f"**Summary:** {row['summary']}")
                    
                    with col2:
                        st.metric("Event ID", row['GLOBALEVENTID'])
                        st.metric("Sentiment", f"{row['AvgTone']:.2f}")
                        st.metric("Articles", int(row['NumArticles']))
                        
                        if row['SOURCEURL']:
                            st.markdown(f"[📰 Source]({row['SOURCEURL']})")
                    
                    st.markdown("---")
        
        else:  # Summaries Only
            summaries_df = df_filtered[df_filtered['success'] == True]
            
            if len(summaries_df) > 0:
                num_summaries = st.slider("Number of summaries", 5, 30, 10)
                
                for idx, row in summaries_df.head(num_summaries).iterrows():
                    with st.expander(
                        f"**{row['Actor1Name']}** - {row['ActionGeo_FullName']} "
                        f"(Sentiment: {row['AvgTone']:.2f})"
                    ):
                        st.write(row['summary'])
                        
                        if row['SOURCEURL']:
                            st.markdown(f"[📰 Read Full Article]({row['SOURCEURL']})")
            else:
                st.info("No events with AI summaries in filtered results.")
    
    else:
        st.warning("No events found matching your criteria.")
    
    # Export Section
    st.markdown("---")
    st.subheader("💾 Export Data")
    
    if len(df_filtered) > 0:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Export as CSV
            csv_buffer = io.StringIO()
            export_cols = [
                'GLOBALEVENTID', 'Actor1Name', 'Actor2Name', 'ActionGeo_FullName',
                'ActionGeo_CountryCode', 'AvgTone', 'NumArticles', 'summary', 
                'success', 'SOURCEURL'
            ]
            df_filtered[export_cols].to_csv(csv_buffer, index=False)
            
            st.download_button(
                label="📥 Download CSV",
                data=csv_buffer.getvalue(),
                file_name=f"gdelt_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Export summaries only
            if len(df_filtered[df_filtered['success'] == True]) > 0:
                summaries_text = "\n\n".join([
                    f"Event ID: {row['GLOBALEVENTID']}\n"
                    f"Actor: {row['Actor1Name']}\n"
                    f"Location: {row['ActionGeo_FullName']}\n"
                    f"Sentiment: {row['AvgTone']:.2f}\n"
                    f"Summary: {row['summary']}\n"
                    f"{'='*60}"
                    for _, row in df_filtered[df_filtered['success'] == True].iterrows()
                ])
                
                st.download_button(
                    label="📥 Download Summaries",
                    data=summaries_text,
                    file_name=f"summaries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                    mime="text/plain"
                )
            else:
                st.button("📥 Download Summaries", disabled=True)
                st.caption("No summaries available")
        
        with col3:
            # Export as JSON
            json_data = df_filtered[export_cols].to_json(orient='records', indent=2)
            
            st.download_button(
                label="📥 Download JSON",
                data=json_data,
                file_name=f"gdelt_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                mime="application/json"
            )
        
        st.caption(f"Exporting {len(df_filtered)} events")
    
    else:
        st.info("Filter events to enable export functionality.")

except Exception as e:
    st.error(f"Error: {e}")
    st.info("Make sure the Gold layer data exists.")