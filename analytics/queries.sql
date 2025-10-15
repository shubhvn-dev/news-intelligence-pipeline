-- Top 10 Countries by Event Count
SELECT 
    ActionGeo_CountryCode as country,
    COUNT(*) as event_count,
    AVG(AvgTone) as avg_sentiment,
    SUM(NumArticles) as total_articles
FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
WHERE ActionGeo_CountryCode IS NOT NULL
GROUP BY ActionGeo_CountryCode
ORDER BY event_count DESC
LIMIT 10;

-- Sentiment Distribution
SELECT 
    CASE 
        WHEN AvgTone < -5 THEN 'Very Negative'
        WHEN AvgTone < -2 THEN 'Negative'
        WHEN AvgTone < 2 THEN 'Neutral'
        WHEN AvgTone < 5 THEN 'Positive'
        ELSE 'Very Positive'
    END as sentiment_category,
    COUNT(*) as count,
    AVG(AvgTone) as avg_tone
FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
GROUP BY sentiment_category
ORDER BY avg_tone;

-- Top Actors
SELECT 
    Actor1Name as actor,
    COUNT(*) as event_count,
    AVG(AvgTone) as avg_sentiment
FROM 'data/gold/gdelt/date=2025-10-15/*.parquet'
WHERE Actor1Name IS NOT NULL
GROUP BY Actor1Name
ORDER BY event_count DESC
LIMIT 15;

-- Events with Summaries
SELECT 
    COUNT(*) as total_events,
    SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as summarized_events,
    SUM(CASE WHEN article_fetched = true THEN 1 ELSE 0 END) as articles_fetched,
    ROUND(100.0 * SUM(CASE WHEN success = true THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM 'data/gold/gdelt/date=2025-10-15/*.parquet';