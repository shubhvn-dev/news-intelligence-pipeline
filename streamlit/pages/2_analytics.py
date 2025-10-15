"""
Deep Analytics

Detailed analytics and insights on news events.
"""
import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="Analytics", page_icon="📊", layout="wide")

st.title("Deep Analytics")
st.markdown("**Comprehensive analysis of global news events**")
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
    
    # Key Metrics Section
    st.subheader("Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Events", f"{len(df):,}")
    
    with col2:
        summarized = df['success'].sum() if 'success' in df.columns else 0
        success_rate = (summarized / len(df) * 100) if len(df) > 0 else 0
        st.metric("AI Success Rate", f"{success_rate:.1f}%")
    
    with col3:
        total_articles = df['NumArticles'].sum()
        st.metric("Total Articles", f"{int(total_articles):,}")
    
    with col4:
        avg_articles = df['NumArticles'].mean()
        st.metric("Avg Articles/Event", f"{avg_articles:.1f}")
    
    with col5:
        unique_actors = df['Actor1Name'].nunique()
        st.metric("Unique Actors", f"{unique_actors:,}")
    
    st.markdown("---")
    
    # Sentiment Analysis Section
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Sentiment by Country")
        
        country_sentiment = df.groupby('ActionGeo_CountryCode').agg({
            'AvgTone': 'mean',
            'GLOBALEVENTID': 'count'
        }).reset_index()
        country_sentiment.columns = ['Country', 'Avg Sentiment', 'Event Count']
        country_sentiment = country_sentiment.sort_values('Event Count', ascending=False).head(10)
        
        fig = px.scatter(
            country_sentiment,
            x='Event Count',
            y='Avg Sentiment',
            size='Event Count',
            color='Avg Sentiment',
            text='Country',
            color_continuous_scale='RdYlGn',
            color_continuous_midpoint=0,
            labels={'Event Count': 'Number of Events', 'Avg Sentiment': 'Average Sentiment'}
        )
        fig.update_traces(textposition='top center')
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Event Distribution")
        
        # Create event categories based on GoldsteinScale
        df['event_intensity'] = pd.cut(
            df['GoldsteinScale'],
            bins=[-10, -5, -2, 2, 5, 10],
            labels=['Very Negative', 'Negative', 'Neutral', 'Positive', 'Very Positive']
        )
        
        intensity_counts = df['event_intensity'].value_counts().reset_index()
        intensity_counts.columns = ['Intensity', 'Count']
        
        fig = px.funnel(
            intensity_counts,
            x='Count',
            y='Intensity',
            color='Intensity',
            color_discrete_map={
                'Very Negative': '#d62728',
                'Negative': '#ff7f0e',
                'Neutral': '#7f7f7f',
                'Positive': '#2ca02c',
                'Very Positive': '#1f77b4'
            }
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Actor Analysis
    st.subheader("Top Actors Comparison")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Top actors with sentiment
        actor_stats = df.groupby('Actor1Name').agg({
            'GLOBALEVENTID': 'count',
            'AvgTone': 'mean',
            'NumArticles': 'sum'
        }).reset_index()
        actor_stats.columns = ['Actor', 'Events', 'Avg Sentiment', 'Total Articles']
        actor_stats = actor_stats.sort_values('Events', ascending=False).head(15)
        actor_stats['Avg Sentiment'] = actor_stats['Avg Sentiment'].round(2)
        
        fig = make_subplots(
            rows=1, cols=2,
            subplot_titles=('Event Count', 'Average Sentiment'),
            specs=[[{"type": "bar"}, {"type": "bar"}]]
        )
        
        fig.add_trace(
            go.Bar(
                y=actor_stats['Actor'],
                x=actor_stats['Events'],
                orientation='h',
                name='Events',
                marker_color='lightblue'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Bar(
                y=actor_stats['Actor'],
                x=actor_stats['Avg Sentiment'],
                orientation='h',
                name='Sentiment',
                marker_color=actor_stats['Avg Sentiment'].apply(
                    lambda x: 'green' if x > 0 else 'red' if x < 0 else 'gray'
                )
            ),
            row=1, col=2
        )
        
        fig.update_layout(height=500, showlegend=False)
        fig.update_yaxes(categoryorder='total ascending', row=1, col=1)
        fig.update_yaxes(categoryorder='total ascending', row=1, col=2)
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### Top 5 Actors")
        for idx, row in actor_stats.head(5).iterrows():
            with st.container():
                st.markdown(f"**{row['Actor']}**")
                st.metric("Events", f"{int(row['Events'])}")
                st.metric("Sentiment", f"{row['Avg Sentiment']:.2f}")
                st.metric("Articles", f"{int(row['Total Articles'])}")
                st.markdown("---")
    
    # Article Coverage Analysis
    st.markdown("---")
    st.subheader("Media Coverage Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Events by Article Count")
        
        # Categorize by article count
        df['coverage_level'] = pd.cut(
            df['NumArticles'],
            bins=[0, 1, 5, 10, 50, 1000],
            labels=['Single Article', '2-5 Articles', '6-10 Articles', '11-50 Articles', '50+ Articles']
        )
        
        coverage_counts = df['coverage_level'].value_counts().reset_index()
        coverage_counts.columns = ['Coverage Level', 'Count']
        
        fig = px.bar(
            coverage_counts,
            x='Coverage Level',
            y='Count',
            color='Count',
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### Sentiment vs Coverage")
        
        fig = px.scatter(
            df,
            x='NumArticles',
            y='AvgTone',
            color='ActionGeo_CountryCode',
            size='NumArticles',
            hover_data=['Actor1Name', 'ActionGeo_FullName'],
            labels={'NumArticles': 'Number of Articles', 'AvgTone': 'Sentiment'},
            opacity=0.6
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    # Data Table
    st.markdown("---")
    st.subheader("Detailed Event Data")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        min_articles = st.number_input("Min Articles", min_value=0, value=0)
    
    with col2:
        selected_country = st.selectbox(
            "Country",
            options=['All'] + sorted(df['ActionGeo_CountryCode'].dropna().unique().tolist())
        )
    
    with col3:
        sentiment_filter = st.selectbox(
            "Sentiment",
            options=['All', 'Positive (>2)', 'Neutral (-2 to 2)', 'Negative (<-2)']
        )
    
    # Apply filters
    filtered_df = df.copy()
    
    if min_articles > 0:
        filtered_df = filtered_df[filtered_df['NumArticles'] >= min_articles]
    
    if selected_country != 'All':
        filtered_df = filtered_df[filtered_df['ActionGeo_CountryCode'] == selected_country]
    
    if sentiment_filter == 'Positive (>2)':
        filtered_df = filtered_df[filtered_df['AvgTone'] > 2]
    elif sentiment_filter == 'Neutral (-2 to 2)':
        filtered_df = filtered_df[(filtered_df['AvgTone'] >= -2) & (filtered_df['AvgTone'] <= 2)]
    elif sentiment_filter == 'Negative (<-2)':
        filtered_df = filtered_df[filtered_df['AvgTone'] < -2]
    
    # Display table
    display_cols = ['GLOBALEVENTID', 'Actor1Name', 'Actor2Name', 'ActionGeo_FullName', 
                   'AvgTone', 'NumArticles', 'success']
    
    st.dataframe(
        filtered_df[display_cols].head(50),
        use_container_width=True,
        height=400
    )
    
    st.caption(f"Showing {min(50, len(filtered_df))} of {len(filtered_df)} filtered events")

except Exception as e:
    st.error(f"Error loading analytics: {e}")
    st.info("Make sure the Gold layer data exists.")