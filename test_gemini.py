"""
Test Gemini summarization on sample events
"""
import sys
sys.path.append('gemini')

from summarizer import GeminiSummarizer
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Verify API key is loaded
api_key = os.getenv('GEMINI_API_KEY')
if not api_key:
    print("ERROR: GEMINI_API_KEY not found in .env file")
    print("Please add your API key to the .env file")
    sys.exit(1)

print(f"API Key loaded: {api_key[:10]}... (first 10 chars)")

# Read sample events from Silver layer
print("\nLoading events from Silver layer...")
df = pd.read_parquet('data/silver/gdelt/date=2025-10-15/')

# Get first 3 events with both actors for better summaries
sample_events = df[df['has_actor2'] == 1].head(3)

print(f"Found {len(sample_events)} events to summarize\n")

# Convert to list of dicts
events = sample_events.to_dict('records')

# Initialize summarizer
print("Initializing Gemini summarizer...")
summarizer = GeminiSummarizer(api_key=api_key)

# Summarize events
print("Generating summaries...\n")
results = summarizer.summarize_batch(events, delay=1.0)

# Display results
print("\n" + "="*60)
print("SUMMARIES")
print("="*60)
for i, result in enumerate(results):
    event = events[i]
    print(f"\nEvent {i+1}:")
    print(f"  ID: {result['event_id']}")
    print(f"  Actor1: {event['Actor1Name']}")
    print(f"  Actor2: {event['Actor2Name']}")
    print(f"  Location: {event['ActionGeo_FullName']}")
    print(f"  Summary: {result['summary']}")
    print(f"  Success: {result['success']}")

# Show cost estimate
summarizer.print_cost_summary()