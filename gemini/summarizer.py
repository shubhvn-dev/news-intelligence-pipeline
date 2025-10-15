"""
Gemini-powered article summarization

Uses Gemini 2.0 Flash to generate concise summaries of news events.
"""
import google.generativeai as genai
import os
import time
from typing import Dict, List, Optional
import json
import requests
from bs4 import BeautifulSoup

class GeminiSummarizer:
    """
    Handles article summarization using Gemini API.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Gemini client.
        
        Args:
            api_key: Gemini API key (if None, reads from environment)
        """
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('models/gemini-2.0-flash')
        
        self.request_count = 0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
    
    def fetch_article_text(self, url: str, timeout: int = 10) -> Optional[str]:
        """
        Fetch article text from URL.
        
        Args:
            url: Article URL
            timeout: Request timeout in seconds
            
        Returns:
            Article text or None if fetch fails
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text
            text = soup.get_text()
            
            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = ' '.join(chunk for chunk in chunks if chunk)
            
            # Limit to first 2000 characters to save tokens
            return text[:2000] if text else None
            
        except Exception as e:
            print(f"\n  Failed to fetch article: {str(e)[:50]}")
            return None
    
    def summarize_event(self, event_data: Dict, fetch_articles: bool = True) -> Dict:
        """
        Summarize a single GDELT event.
        
        Args:
            event_data: Dictionary containing event details
            fetch_articles: Whether to fetch and use actual article text
            
        Returns:
            Dictionary with summary and metadata
        """
        actor1 = event_data.get('Actor1Name', 'Unknown')
        actor2 = event_data.get('Actor2Name', 'Unknown') if event_data.get('Actor2Name') else None
        location = event_data.get('ActionGeo_FullName', 'Unknown location')
        date = event_data.get('SQLDATE', 'Unknown')
        source_url = event_data.get('SOURCEURL', '')
        
        # Try to fetch actual article text
        article_text = None
        if fetch_articles and source_url:
            article_text = self.fetch_article_text(source_url)
        
        if article_text:
            # Summarize actual article content
            prompt = f"""Summarize this news article in 2-3 clear sentences.
Focus on the key facts: who, what, when, where, why.
Be specific and informative.

Location: {location}
Date: {date}

Article excerpt:
{article_text[:1500]}

Summary:"""
        else:
            # Fallback to metadata-only summary
            actors = f"{actor1}" + (f" and {actor2}" if actor2 and actor2 != 'Unknown' else "")
            prompt = f"""Based on this event metadata, provide a brief 2-sentence summary of what likely happened.
Be specific about the actions taken.

Event Details:
- Date: {date}
- Actors: {actors}
- Location: {location}

Summary:"""
        
        try:
            response = self.model.generate_content(prompt)
            summary = response.text.strip()
            
            # Track token usage (approximate)
            self.request_count += 1
            self.total_input_tokens += len(prompt.split()) * 1.3
            self.total_output_tokens += len(summary.split()) * 1.3
            
            return {
                'event_id': event_data.get('GLOBALEVENTID'),
                'summary': summary,
                'article_fetched': article_text is not None,
                'success': True,
                'error': None
            }
            
        except Exception as e:
            print(f"\nError summarizing event {event_data.get('GLOBALEVENTID')}: {e}")
            return {
                'event_id': event_data.get('GLOBALEVENTID'),
                'summary': None,
                'article_fetched': False,
                'success': False,
                'error': str(e)
            }
    
    def summarize_batch(self, events: List[Dict], delay: float = 0.5, fetch_articles: bool = True) -> List[Dict]:
        """
        Summarize multiple events with rate limiting.
        
        Args:
            events: List of event dictionaries
            delay: Delay between requests in seconds
            fetch_articles: Whether to fetch actual article content
            
        Returns:
            List of summary results
        """
        results = []
        
        for i, event in enumerate(events):
            print(f"Processing event {i+1}/{len(events)}...", end='\r')
            
            result = self.summarize_event(event, fetch_articles=fetch_articles)
            results.append(result)
            
            # Rate limiting
            if i < len(events) - 1:
                time.sleep(delay)
        
        print(f"\nCompleted {len(results)} summaries")
        articles_fetched = sum(1 for r in results if r.get('article_fetched'))
        print(f"Articles fetched: {articles_fetched}/{len(results)}")
        return results
    
    def get_cost_estimate(self) -> Dict:
        """
        Estimate API usage costs.
        
        Returns:
            Dictionary with cost breakdown
        """
        # Gemini 2.0 Flash pricing
        input_cost_per_1m = 0.075  # $0.075 per 1M tokens
        output_cost_per_1m = 0.30  # $0.30 per 1M tokens
        
        input_cost = (self.total_input_tokens / 1_000_000) * input_cost_per_1m
        output_cost = (self.total_output_tokens / 1_000_000) * output_cost_per_1m
        total_cost = input_cost + output_cost
        
        return {
            'requests': self.request_count,
            'input_tokens': int(self.total_input_tokens),
            'output_tokens': int(self.total_output_tokens),
            'estimated_input_cost': round(input_cost, 4),
            'estimated_output_cost': round(output_cost, 4),
            'estimated_total_cost': round(total_cost, 4)
        }
    
    def print_cost_summary(self):
        """Print a formatted cost summary."""
        costs = self.get_cost_estimate()
        
        print("\n" + "="*60)
        print("GEMINI API USAGE SUMMARY")
        print("="*60)
        print(f"Total requests:        {costs['requests']:,}")
        print(f"Input tokens:          {costs['input_tokens']:,}")
        print(f"Output tokens:         {costs['output_tokens']:,}")
        print(f"Estimated input cost:  ${costs['estimated_input_cost']:.4f}")
        print(f"Estimated output cost: ${costs['estimated_output_cost']:.4f}")
        print(f"Estimated total cost:  ${costs['estimated_total_cost']:.4f}")
        print("="*60 + "\n")