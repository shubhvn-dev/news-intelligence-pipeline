"""
Test DuckDB analytics queries on Gold layer data
"""
import duckdb
import pandas as pd

# Connect to DuckDB (in-memory)
con = duckdb.connect()

print("="*60)
print("GDELT ANALYTICS - Gold Layer")
print("="*60)

# Query 1: Top Countries
print("\n1. TOP 10 COUNTRIES BY EVENT COUNT")
print("-"*60)
result = con.execute("""
    SELECT 
        ActionGeo_CountryCode as country,
        COUNT(*) as event_count,
        ROUND(AVG(AvgTone), 2) as avg_sentiment,
        SUM(NumArticles) as total_articles
    FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
    WHERE ActionGeo_CountryCode IS NOT NULL
    GROUP BY ActionGeo_CountryCode
    ORDER BY event_count DESC
    LIMIT 10
""").df()
print(result.to_string(index=False))

# Query 2: Sentiment Distribution
print("\n\n2. SENTIMENT DISTRIBUTION")
print("-"*60)
result = con.execute("""
    SELECT 
        CASE 
            WHEN AvgTone < -5 THEN 'Very Negative'
            WHEN AvgTone < -2 THEN 'Negative'
            WHEN AvgTone < 2 THEN 'Neutral'
            WHEN AvgTone < 5 THEN 'Positive'
            ELSE 'Very Positive'
        END as sentiment_category,
        COUNT(*) as count,
        ROUND(AVG(AvgTone), 2) as avg_tone
    FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
    GROUP BY sentiment_category
    ORDER BY avg_tone
""").df()
print(result.to_string(index=False))

# Query 3: Top Actors
print("\n\n3. TOP 15 ACTORS BY EVENT COUNT")
print("-"*60)
result = con.execute("""
    SELECT 
        Actor1Name as actor,
        COUNT(*) as event_count,
        ROUND(AVG(AvgTone), 2) as avg_sentiment
    FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
    WHERE Actor1Name IS NOT NULL
    GROUP BY Actor1Name
    ORDER BY event_count DESC
    LIMIT 15
""").df()
print(result.to_string(index=False))

# Query 4: Summary Statistics
print("\n\n4. AI SUMMARIZATION STATISTICS")
print("-"*60)
result = con.execute("""
    SELECT 
        COUNT(*) as total_events,
        SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as summarized_events,
        SUM(CASE WHEN article_fetched = true THEN 1 ELSE 0 END) as articles_fetched,
        ROUND(100.0 * SUM(CASE WHEN success = true THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
    FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
""").df()
print(result.to_string(index=False))

# Query 5: Sample Summaries
print("\n\n5. SAMPLE AI-GENERATED SUMMARIES")
print("-"*60)
result = con.execute("""
    SELECT 
        Actor1Name,
        ActionGeo_FullName as location,
        SUBSTRING(summary, 1, 150) || '...' as summary_preview
    FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
    WHERE success = true
    LIMIT 5
""").df()
for idx, row in result.iterrows():
    print(f"\n{idx+1}. {row['Actor1Name']} in {row['location']}")
    print(f"   {row['summary_preview']}")

print("\n" + "="*60)
print("Analytics Complete!")
print("="*60 + "\n")

con.close()