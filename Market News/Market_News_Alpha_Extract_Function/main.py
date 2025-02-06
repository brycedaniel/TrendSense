
import requests
import pandas as pd
from datetime import datetime, timedelta
import pytz  # Import for timezone conversion
import logging
import os
from google.cloud import bigquery

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_market_news(api_key):
    """
    Retrieve market news from Alpha Vantage API.
    """
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'NEWS_SENTIMENT',
        'apikey': api_key
    }
    
    try:
        logger.info("Fetching market news")
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        news_data = response.json()

        if 'Note' in news_data:
            logger.warning(f"API limit message: {news_data['Note']}")
            return []

        if 'feed' in news_data:
            # Filter articles to include only today's articles
            today = datetime.now().strftime('%Y-%m-%d')
            filtered_news = [
                item for item in news_data['feed'] 
                if datetime.strptime(item['time_published'], "%Y%m%dT%H%M%S").strftime('%Y-%m-%d') == today
            ]
            logger.info(f"Retrieved {len(filtered_news)} news items for today.")
            return filtered_news
        else:
            logger.warning("No news found in response")
            return []
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        return []

# Define the specific tickers to include
TICKERS_TO_INCLUDE = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZ', 'CRM', 'NOW', 'CHTR', 'TDS', 'META',
    'RGTI', 'QUBT', 'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
]

def process_news_items(news_items):
    """
    Process news items into a structured DataFrame, including sentiment,
    filtering for specific tickers.
    """
    try:
        mst = pytz.timezone("MST")  # Define MST timezone
        processed_items = []
        for item in news_items:
            # Get only the tickers that are in our TICKERS_TO_INCLUDE list
            article_tickers = [ts['ticker'] for ts in item.get('ticker_sentiment', [])]
            matching_tickers = list(set(article_tickers).intersection(TICKERS_TO_INCLUDE))
            
            # Skip if no matching tickers
            if not matching_tickers:
                continue
            
            # Convert publish_date format and timezone
            raw_date = item.get('time_published', '')
            try:
                utc_date = datetime.strptime(raw_date, "%Y%m%dT%H%M%S").replace(tzinfo=pytz.UTC)
                mst_date = utc_date.astimezone(mst).strftime("%m/%d/%Y %H:%M")
            except ValueError:
                mst_date = "Invalid Date"

            # Create separate entries for each matching ticker
            for ticker in matching_tickers:
                processed_item = {
                    'ticker': ticker,  # Single ticker instead of all tickers
                    'title': item.get('title', ''),
                    'summary': item.get('summary', ''),
                    'publisher': item.get('source', ''),
                    'link': item.get('url', ''),
                    'publish_date': mst_date,
                    'related_tickers': ', '.join(matching_tickers),  # Only include matching tickers
                    'source': 'Alpha',
                    'overall_sentiment_score': item.get('overall_sentiment_score', ''),
                    'overall_sentiment_label': item.get('overall_sentiment_label', '')
                }
                processed_items.append(processed_item)
        
        # Reorder columns
        column_order = ['ticker', 'title', 'summary', 'publisher', 'link', 'publish_date', 
                       'related_tickers', 'source', 'overall_sentiment_score', 'overall_sentiment_label']
        return pd.DataFrame(processed_items)[column_order]
    except Exception as e:
        logger.error(f"Error processing news items: {str(e)}")
        return pd.DataFrame()


def save_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Save processed news data to Google BigQuery using auto-detect schema.

    Parameters:
    df (pd.DataFrame): DataFrame containing news data
    project_id (str): Google Cloud Project ID
    dataset_id (str): BigQuery dataset ID
    table_id (str): BigQuery table ID
    """
    try:
        if df.empty:
            logger.warning("No news data to save.")
            return None

        # Initialize BigQuery client
        client = bigquery.Client()

        # Define BigQuery table ID
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # Load data into BigQuery with overwrite configuration
        job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        logger.info(f"Data successfully saved to BigQuery: {table_ref}")
        return table_ref
    except Exception as e:
        logger.error(f"Error saving data to BigQuery: {str(e)}")
        return None

def main(request):
    """
    Google Cloud Function handler for fetching, processing, and saving market news data.
    """
    # Configuration
    api_key = "FLGDYAANWX6EFL9P"  # Alpha Vantage API key from environment variable
    project_id = "trendsense"                     # Your Google Cloud project ID
    dataset_id = "market_data"                    # BigQuery dataset ID
    table_id = "News_Alpha_Extract"               # BigQuery table ID

    try:
        # Fetch market news
        news_items = get_market_news(api_key)
        if not news_items:
            logger.warning("No news data retrieved.")
            return "No news data retrieved.", 200

        # Process news items
        news_df = process_news_items(news_items)

        # Save processed news data to BigQuery
        save_result = save_to_bigquery(news_df, project_id, dataset_id, table_id)

        if save_result:
            return f"News data saved successfully to BigQuery table: {save_result}", 200
        else:
            return "Failed to save news data to BigQuery.", 500
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        return f"Internal Server Error: {str(e)}", 500
