"""
News Intelligence Dashboard

Interactive dashboard for exploring GDELT news events with AI summaries.
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page config
st.set_page_config(
    page_title="News Intelligence Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 5px;
        border: 1px solid #e0e0e0;
    }
    .stMetric label {
        color: #31333f !important;
        font-weight: 600 !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #0068c9 !important;
        font-size: 2rem !important;
    }
    </style>
    """, unsafe_allow_html=True)

# Title
st.title("📰 News Intelligence Dashboard")
st.markdown("**Real-time Global Events Analysis with AI-Powered Summaries**")
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
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Country filter
    countries = sorted(df['ActionGeo_CountryCode'].dropna().unique())
    selected_countries = st.sidebar.multiselect(
        "Select Countries",
        options=countries,
        default=countries[:5] if len(countries) >= 5 else countries
    )
    
    # Sentiment filter
    sentiment_range = st.sidebar.slider(
        "Sentiment Range",
        min_value=float(df['AvgTone'].min()),
        max_value=float(df['AvgTone'].max()),
        value=(float(df['AvgTone'].min()), float(df['AvgTone'].max()))
    )
    
    # Filter data
    if selected_countries:
        df_filtered = df[df['ActionGeo_CountryCode'].isin(selected_countries)]
    else:
        df_filtered = df
    
    df_filtered = df_filtered[
        (df_filtered['AvgTone'] >= sentiment_range[0]) & 
        (df_filtered['AvgTone'] <= sentiment_range[1])
    ]
    
    # Top metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 Total Events", f"{len(df_filtered):,}")
    
    with col2:
        summarized = df_filtered['success'].sum() if 'success' in df_filtered.columns else 0
        st.metric("🤖 AI Summaries", f"{int(summarized):,}")
    
    with col3:
        avg_sentiment = df_filtered['AvgTone'].mean()
        st.metric("😊 Avg Sentiment", f"{avg_sentiment:.2f}")
    
    with col4:
        countries_count = df_filtered['ActionGeo_CountryCode'].nunique()
        st.metric("🌍 Countries", f"{countries_count}")
    
    st.markdown("---")
    
    # Two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📍 Events by Country")
        country_counts = df_filtered.groupby('ActionGeo_CountryCode').size().reset_index(name='count')
        country_counts = country_counts.sort_values('count', ascending=False).head(10)
        
        fig = px.bar(
            country_counts,
            x='ActionGeo_CountryCode',
            y='count',
            labels={'ActionGeo_CountryCode': 'Country', 'count': 'Events'},
            color='count',
            color_continuous_scale='Blues'
        )
        fig.update_layout(showlegend=False, height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("😊 Sentiment Distribution")
        
        # Create sentiment categories
        df_filtered['sentiment_cat'] = pd.cut(
            df_filtered['AvgTone'],
            bins=[-100, -5, -2, 2, 5, 100],
            labels=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
        )
        
        sentiment_counts = df_filtered['sentiment_cat'].value_counts().reset_index()
        sentiment_counts.columns = ['Sentiment', 'Count']
        
        colors = {
            'Very Negative': '#d62728',
            'Negative': '#ff7f0e',
            'Neutral': '#7f7f7f',
            'Positive': '#2ca02c',
            'Very Positive': '#1f77b4'
        }
        
        fig = px.pie(
            sentiment_counts,
            values='Count',
            names='Sentiment',
            color='Sentiment',
            color_discrete_map=colors
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Top actors
    st.markdown("---")
    st.subheader("👥 Top Actors")
    
    actor_stats = df_filtered.groupby('Actor1Name').agg({
        'GLOBALEVENTID': 'count',
        'AvgTone': 'mean'
    }).reset_index()
    actor_stats.columns = ['Actor', 'Events', 'Avg Sentiment']
    actor_stats = actor_stats.sort_values('Events', ascending=False).head(15)
    actor_stats['Avg Sentiment'] = actor_stats['Avg Sentiment'].round(2)
    
    fig = px.bar(
        actor_stats,
        x='Events',
        y='Actor',
        orientation='h',
        color='Avg Sentiment',
        color_continuous_scale='RdYlGn',
        color_continuous_midpoint=0,
        labels={'Events': 'Number of Events'}
    )
    fig.update_layout(height=500, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)
    
    # AI Summaries Section
    st.markdown("---")
    st.subheader("🤖 AI-Generated Summaries")
    
    # Filter for successful summaries
    df_summaries = df_filtered[df_filtered['success'] == True].copy()
    
    if len(df_summaries) > 0:
        # Select number of summaries to show
        num_summaries = st.slider("Number of summaries to display", 5, 20, 10)
        
        # Display summaries
        for idx, row in df_summaries.head(num_summaries).iterrows():
            with st.expander(f"📰 {row['Actor1Name']} - {row['ActionGeo_FullName']} (Sentiment: {row['AvgTone']:.2f})"):
                st.markdown(f"**Summary:**")
                st.write(row['summary'])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Event ID", row['GLOBALEVENTID'])
                with col2:
                    st.metric("Articles", int(row['NumArticles']))
                with col3:
                    article_status = "✅ Fetched" if row['article_fetched'] else "❌ Failed"
                    st.metric("Article", article_status)
                
                if row['SOURCEURL']:
                    st.markdown(f"🔗 [Source Article]({row['SOURCEURL']})")
    else:
        st.info("No AI summaries available for the selected filters.")
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style='text-align: center; color: gray;'>
            <p>Data Source: GDELT Project | AI: Google Gemini 2.0 Flash | Built with Streamlit</p>
        </div>
    """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Make sure the Gold layer data exists at: `data/gold/gdelt/date=2025-10-15/`")