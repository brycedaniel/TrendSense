import os
import pandas as pd
import nltk
from datetime import datetime, timedelta
from google.cloud import bigquery
from textblob import TextBlob
import yfinance as yf
import time
from typing import List, Dict
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_market_news(tickers: List[str], days_back: int = 30) -> pd.DataFrame:
    """
    Fetches news articles for the given tickers with robust error handling.
    """
    all_news = []
    cutoff_date = datetime.now().date() - timedelta(days=days_back)
    
    for ticker in tickers:
        logging.info(f"Fetching news for {ticker}")
        try:
            # Add delay between requests
            time.sleep(2)
            
            # Create ticker object with timeout
            stock = yf.Ticker(ticker)
            
            # Verify we can access the news attribute
            if not hasattr(stock, 'news'):
                logging.error(f"No news attribute found for {ticker}")
                continue
                
            # Get news with timeout and validation
            try:
                news = stock.news
                if not news:  # Check if news is empty
                    logging.info(f"No news found for {ticker}")
                    continue
                    
                if not isinstance(news, list):  # Validate news format
                    logging.error(f"Invalid news format for {ticker}")
                    continue
                
            except Exception as e:
                logging.error(f"Error accessing news for {ticker}: {e}")
                continue
            
            # Process each news item
            for item in news:
                try:
                    # Validate news item
                    if not isinstance(item, dict):
                        continue
                        
                    publish_timestamp = item.get('providerPublishTime')
                    if not publish_timestamp:
                        continue
                        
                    publish_date = datetime.fromtimestamp(publish_timestamp).date()
                    
                    # Check if news is within date range
                    if publish_date >= cutoff_date:
                        title = item.get('title', '')
                        if not title:  # Skip items without title
                            continue
                            
                        # Calculate sentiment
                        try:
                            analysis = TextBlob(title)
                            sentiment_score = analysis.sentiment.polarity
                        except Exception as e:
                            logging.warning(f"Sentiment analysis failed for {ticker}: {e}")
                            sentiment_score = 0
                            
                        # Determine sentiment label
                        if sentiment_score > 0.35:
                            sentiment_label = "Bullish"
                        elif 0.15 < sentiment_score <= 0.35:
                            sentiment_label = "Somewhat-Bullish"
                        elif -0.15 <= sentiment_score <= 0.15:
                            sentiment_label = "Neutral"
                        elif -0.35 <= sentiment_score < -0.15:
                            sentiment_label = "Somewhat-Bearish"
                        else:
                            sentiment_label = "Bearish"
                        
                        # Create news item with validation
                        news_item = {
                            'ticker': ticker,
                            'title': title,
                            'summary': title,
                            'publisher': item.get('publisher', ''),
                            'link': item.get('link', ''),
                            'publish_date': datetime.fromtimestamp(publish_timestamp),
                            'type': item.get('type', ''),
                            'related_tickers': ', '.join(item.get('relatedTickers', [])),
                            'source': 'yahoo',
                            'overall_sentiment_score': sentiment_score,
                            'overall_sentiment_label': sentiment_label
                        }
                        
                        all_news.append(news_item)
                        logging.info(f"Processed news item for {ticker}")
                        
                except Exception as e:
                    logging.error(f"Error processing news item for {ticker}: {e}")
                    continue
                    
        except Exception as e:
            logging.error(f"Failed to process ticker {ticker}: {e}")
            continue
            
        # Log progress
        logging.info(f"Completed processing {ticker}")
    
    # Create DataFrame with validation
    if not all_news:
        logging.warning("No news items were collected")
        return pd.DataFrame()
        
    try:
        df = pd.DataFrame(all_news)
        logging.info(f"Successfully created DataFrame with {len(df)} rows")
        return df
    except Exception as e:
        logging.error(f"Failed to create DataFrame: {e}")
        return pd.DataFrame()

def upload_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str) -> bool:
    """
    Uploads DataFrame to BigQuery with enhanced error handling.
    Returns True if successful, False otherwise.
    """
    if df.empty:
        logging.warning("No data to upload to BigQuery")
        return False
        
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        
        # Upload with timeout
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result(timeout=300)  # 5-minute timeout
        
        logging.info(f"Successfully uploaded {len(df)} rows to {table_ref}")
        return True
        
    except Exception as e:
        logging.error(f"BigQuery upload failed: {e}")
        return False

def main():
    """
    Main function with better error handling and logging.
    """
    # Define indices and stocks
    indices = ['^IXIC', '^GSPC']
    tech_stocks = [
        'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT'  # Start with a smaller set for testing
    ]
    
    try:
        # Fetch news for indices
        logging.info("Fetching news for market indices...")
        market_news = get_market_news(tickers=indices)
        
        # Fetch news for tech stocks
        logging.info("Fetching news for tech stocks...")
        tech_news = get_market_news(tickers=tech_stocks)
        
        # Combine results
        combined_news = pd.concat([market_news, tech_news], ignore_index=True)
        
        if not combined_news.empty:
            success = upload_to_bigquery(
                combined_news,
                project_id="trendsense",
                dataset_id="market_data",
                table_id="News_Yahoo_Extract"
            )
            if success:
                logging.info("Process completed successfully")
            else:
                logging.error("Failed to upload to BigQuery")
        else:
            logging.warning("No news data collected")
            
    except Exception as e:
        logging.error(f"Process failed: {e}")

if __name__ == "__main__":
    main()


