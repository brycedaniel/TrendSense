import functions_framework
import requests
import pandas as pd
import time
import os
import logging
from datetime import datetime
from textblob import TextBlob
from google.cloud import bigquery
from datetime import datetime, timedelta


# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Retrieve NewsAPI key from environment variable
api_key = 'afc3fe9ac08745439bf521cb5b974fbc'
if not api_key:
    logger.error("NewsAPI key is missing. Please set NEWS_API_KEY environment variable.")
    raise ValueError("NewsAPI key is required")

# BigQuery configuration
project_id = os.getenv('BIGQUERY_PROJECT_ID', 'trendsense')
dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'market_data')
table_id = os.getenv('BIGQUERY_TABLE_ID', 'News_News_Extract')

# List of tickers to search news for
tickers = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
            'BWXT', 'ARBK', 'AMD', 'NVDA', 'BTC', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
            'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZ', 'CRM', 'NOW', 'CHTR', 'TDS', 'META', 'RGTI','QUBT',
            'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
]

# Get today's date in ISO format
today = datetime.now().strftime('%Y-%m-%d')
two_days_ago = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')


# Function for TextBlob sentiment analysis
def textblob_sentiment(text):
    if text:
        return TextBlob(text).sentiment.polarity  # Sentiment polarity from -1 to 1
    return 0

# Function to fetch market news for the current day
def get_market_news(ticker):
    url = (
        f'https://newsapi.org/v2/everything?q={ticker}&from={two_days_ago}&to={today}&sortBy=publishedAt&language=en&apiKey={api_key}'
    )
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json().get('articles', [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for {ticker}: {e}")
        return []

# Create table if it doesn't exist
def create_table_if_not_exists(client, project_id, dataset_id, table_id):
    try:
        table_ref = client.dataset(dataset_id).table(table_id)
        
        try:
            client.get_table(table_ref)
            logger.info(f"Table {table_id} already exists.")
        except Exception:
            schema = [
                bigquery.SchemaField("ticker", "STRING"),
                bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("summary_textblob_sentiment", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("publisher", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("publish_date", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("related_tickers", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logger.info(f"Table {table_id} created successfully.")
    except Exception as e:
        logger.error(f"Error creating or checking table: {e}")
        raise

# Save data to BigQuery
def save_to_bigquery(data, project_id, dataset_id, table_id):
    try:
        client = bigquery.Client()
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        # Ensure DataFrame columns match BigQuery schema
        data['publish_date'] = pd.to_datetime(data['publish_date'], errors='coerce')
        
        # Handle potential None/NaN values
        data = data.where(pd.notnull(data), None)
        
        # Load data into BigQuery
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            # Explicitly define schema to handle type conversions
            schema=[
                bigquery.SchemaField("ticker", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("summary", "STRING"),
                bigquery.SchemaField("summary_textblob_sentiment", "FLOAT"),
                bigquery.SchemaField("publisher", "STRING"),
                bigquery.SchemaField("link", "STRING"),
                bigquery.SchemaField("publish_date", "TIMESTAMP"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("related_tickers", "STRING"),
                bigquery.SchemaField("source", "STRING"),
            ]
        )
        
        job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
        job.result()  # Wait for the load job to complete
        logger.info(f"Data successfully saved to BigQuery table: {table_ref}")
    except Exception as e:
        logger.error(f"Error saving to BigQuery: {e}")
        raise

# Cloud Function Entry Point
@functions_framework.http
def main(request):
    try:
        all_news = []
        for ticker in tickers:
            logger.info(f"Fetching news for {ticker}...")
            articles = get_market_news(ticker)
            
            for article in articles:
                title = article.get('title', '')
                summary = article.get('description', '')
                
                # Sentiment analysis using TextBlob
                summary_textblob_sentiment = textblob_sentiment(summary)
                
                # Article schema
                news_entry = {
                    'ticker': ticker,
                    'title': title,
                    'summary': summary,
                    'summary_textblob_sentiment': summary_textblob_sentiment,
                    'publisher': article.get('source', {}).get('name', ''),
                    'link': article.get('url', ''),
                    'publish_date': article.get('publishedAt', ''),
                    'type': 'general',  # Default value
                    'related_tickers': '',  # Default empty
                    'source': 'NewsAPI',  # Identify source
                }
                all_news.append(news_entry)
            
            # Avoid rate limiting
            time.sleep(1)

        # Convert data to a DataFrame
        df = pd.DataFrame(all_news)

        if not df.empty:
            # Ensure the table exists before saving data
            client = bigquery.Client()
            create_table_if_not_exists(client, project_id, dataset_id, table_id)
            
            # Save to BigQuery
            save_to_bigquery(df, project_id, dataset_id, table_id)
            return {
                "status": "success",
                "message": f"Data saved to BigQuery table: {project_id}.{dataset_id}.{table_id}",
                "total_articles": len(all_news)
            }
        else:
            logger.warning("No news articles found for today.")
            return {
                "status": "success",
                "message": "No news articles found for today.",
                "total_articles": 0
            }
    except Exception as e:
        logger.error(f"Unexpected error in main function: {e}", exc_info=True)
        return {
            "status": "error",
            "message": f"An error occurred: {str(e)}",
            "total_articles": 0
        }