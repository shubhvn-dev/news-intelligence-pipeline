"""
Geographic Map View

Interactive map showing event locations with sentiment indicators.
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Map View", page_icon="🗺️", layout="wide")

st.title("Geographic Event Map")
st.markdown("**Interactive visualization of global events by location**")
st.markdown("---")

# Connect to DuckDB
@st.cache_resource
def get_connection():
    return duckdb.connect()

con = get_connection()

# Load data with coordinates
@st.cache_data(ttl=300)
def load_map_data():
    query = """
    SELECT 
        GLOBALEVENTID,
        Actor1Name,
        Actor2Name,
        ActionGeo_FullName,
        ActionGeo_CountryCode,
        ActionGeo_Lat,
        ActionGeo_Long,
        AvgTone,
        NumArticles,
        summary,
        success
    FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
    WHERE ActionGeo_Lat IS NOT NULL 
      AND ActionGeo_Long IS NOT NULL
      AND ActionGeo_Lat != 0
      AND ActionGeo_Long != 0
    """
    return con.execute(query).df()

try:
    df = load_map_data()
    
    # Sidebar filters
    st.sidebar.header("Map Filters")
    
    # Sentiment filter
    sentiment_options = st.sidebar.radio(
        "Sentiment Filter",
        ["All", "Positive", "Neutral", "Negative"]
    )
    
    if sentiment_options == "Positive":
        df = df[df['AvgTone'] > 2]
    elif sentiment_options == "Negative":
        df = df[df['AvgTone'] < -2]
    elif sentiment_options == "Neutral":
        df = df[(df['AvgTone'] >= -2) & (df['AvgTone'] <= 2)]
    
    # Show only events with summaries
    show_summaries_only = st.sidebar.checkbox("Show only events with AI summaries", value=False)
    if show_summaries_only:
        df = df[df['success'] == True]
    
    # Stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Events on Map", f"{len(df):,}")
    with col2:
        st.metric("Countries", f"{df['ActionGeo_CountryCode'].nunique()}")
    with col3:
        avg_sentiment = df['AvgTone'].mean()
        st.metric("Avg Sentiment", f"{avg_sentiment:.2f}")
    
    st.markdown("---")
    
    # Create sentiment category for color coding
    df['sentiment_category'] = pd.cut(
        df['AvgTone'],
        bins=[-100, -5, -2, 2, 5, 100],
        labels=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
    )
    
    # Create hover text
    df['hover_text'] = (
        '<b>' + df['Actor1Name'] + '</b><br>' +
        'Location: ' + df['ActionGeo_FullName'] + '<br>' +
        'Sentiment: ' + df['AvgTone'].round(2).astype(str) + '<br>' +
        'Articles: ' + df['NumArticles'].astype(str)
    )
    
    # Create the map
    fig = px.scatter_mapbox(
        df,
        lat='ActionGeo_Lat',
        lon='ActionGeo_Long',
        color='sentiment_category',
        size='NumArticles',
        hover_name='Actor1Name',
        hover_data={
            'ActionGeo_FullName': True,
            'AvgTone': ':.2f',
            'NumArticles': True,
            'ActionGeo_Lat': False,
            'ActionGeo_Long': False,
            'sentiment_category': False
        },
        color_discrete_map={
            'Very Negative': '#d62728',
            'Negative': '#ff7f0e',
            'Neutral': '#7f7f7f',
            'Positive': '#2ca02c',
            'Very Positive': '#1f77b4'
        },
        zoom=1,
        height=600,
        title="Global Event Distribution"
    )
    
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"r":0,"t":40,"l":0,"b":0}
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show event details
    st.markdown("---")
    st.subheader("Event Details")
    
    # Select an event
    if len(df) > 0:
        event_list = [f"{row['Actor1Name']} - {row['ActionGeo_FullName']}" 
                     for _, row in df.head(20).iterrows()]
        
        selected_event = st.selectbox("Select an event to view details:", event_list)
        
        if selected_event:
            idx = event_list.index(selected_event)
            event = df.iloc[idx]
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.markdown(f"### {event['Actor1Name']}")
                st.markdown(f"**Location:** {event['ActionGeo_FullName']}")
                
                if event['success'] and pd.notna(event['summary']):
                    st.markdown("**AI Summary:**")
                    st.info(event['summary'])
                else:
                    st.warning("No AI summary available for this event")
            
            with col2:
                st.metric("Event ID", event['GLOBALEVENTID'])
                st.metric("Sentiment", f"{event['AvgTone']:.2f}")
                st.metric("Articles", int(event['NumArticles']))
                st.metric("Country", event['ActionGeo_CountryCode'])
    
except Exception as e:
    st.error(f"Error loading map data: {e}")
    st.info("Make sure the Gold layer data exists with valid coordinates.")