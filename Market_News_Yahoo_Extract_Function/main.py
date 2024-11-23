# Current works as of 11/22 12:37 in clound but has issues.
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from google.cloud import bigquery
from textblob import TextBlob


# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def label_sentiment(score):
    """
    Label the sentiment based on the score.
    """
    if score is None:
        return "Unknown"  # Handle cases where score is None
    if score > 0.35:
        return "Bullish"
    elif 0.15 < score <= 0.35:
        return "Somewhat-Bullish"
    elif -0.15 <= score <= 0.15:
        return "Neutral"
    elif -0.35 <= score < -0.15:
        return "Somewhat-Bearish"
    else:
        return "Bearish"

def calculate_sentiment(text):
    try:
        if not text:
            print("[WARNING] Empty text provided for sentiment analysis.")
            return None
        analysis = TextBlob(text)
        return analysis.sentiment.polarity
    except Exception as e:
        print(f"[ERROR] Sentiment analysis failed: {e}")
        return None  # Return None for failed analysis

def get_market_news(tickers):
    all_news = []
    today = datetime.now().date()
    one_day_ago = today - timedelta(days=1)

    for ticker in tickers:
        stock = yf.Ticker(ticker)
        try:
            news = stock.news
            for item in news:
                try:
                    title = item.get('title', '')
                    print(f"[DEBUG] Processing title: {title}")

                    sentiment_score = calculate_sentiment(title)
                    sentiment_label = label_sentiment(sentiment_score)

                    news_item = {
                        'ticker': ticker,
                        'title': title,
                        'publisher': item.get('publisher', ''),
                        'link': item.get('link', ''),
                        'publish_date': datetime.fromtimestamp(item.get('providerPublishTime', 0)).isoformat(),
                        'type': item.get('type', ''),
                        'related_tickers': ', '.join(item.get('relatedTickers', [])),
                        'source': 'yahoo',
                        'overall_sentiment_score': sentiment_score,
                        'overall_sentiment_label': sentiment_label,
                    }
                    all_news.append(news_item)
                except Exception as e:
                    print(f"[ERROR] Error processing news item: {e}")
        except Exception as e:
            print(f"[ERROR] Error retrieving news for {ticker}: {e}")

    return pd.DataFrame(all_news)


def save_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Save processed news data to Google BigQuery.
    """
    try:
        if df.empty:
            logger.warning("No data to save.")
            return None
        
        client = bigquery.Client()
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_TRUNCATE")

        logger.info(f"Saving data to BigQuery table: {table_ref}")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for job completion
        logger.info("Data successfully saved to BigQuery.")
        return table_ref
    except Exception as e:
        logger.error(f"Error saving data to BigQuery: {str(e)}")
        return None

def fetch_market_news(request):
    """
    Cloud Function entry point.
    """
    # Configuration
    project_id = "trendsense"
    dataset_id = "market_data"
    table_id = "News_Yahoo_Extract"
    tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']

    try:
        # Fetch and process market news
        news_df = get_market_news(tickers)
        if news_df.empty:
            logger.warning("No news data retrieved.")
            return "No news data retrieved.", 200

        # Save to BigQuery
        save_result = save_to_bigquery(news_df, project_id, dataset_id, table_id)
        if save_result:
            return f"Data successfully saved to BigQuery: {save_result}", 200
        else:
            return "Failed to save data to BigQuery.", 500
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        return f"Internal Server Error: {str(e)}", 500
