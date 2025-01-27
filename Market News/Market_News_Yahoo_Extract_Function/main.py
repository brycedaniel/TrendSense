# Yahoo Extract with BigQuery Upload This is not running in Google Function.  Need to automate locally

"""
Summary:
This script is designed to scrape market news data from Yahoo Finance for a specific set of market indices and tech stocks. 
The purpose is to analyze the sentiment of news headlines, categorize them as bullish, bearish, or neutral, 
and store the processed data in a BigQuery table. This data will support trend analysis, market sentiment monitoring, 
and data-driven decision-making for traders or analysts. The script is automated for local execution with the ability 
to overwrite the BigQuery table to maintain the latest dataset.

Why this approach:
- **Yahoo Finance News API**: Provides timely and relevant market news data for indices and tech stocks.
- **Sentiment Analysis**: Enables us to gauge market sentiment from news headlines using the `TextBlob` library.
- **BigQuery Integration**: Ensures that data is stored in a scalable, queryable format for further analysis and visualization.
- **Automation**: This script automates the process of fetching, processing, and uploading data for seamless updates.
"""

import os
import pandas as pd
import nltk
from datetime import datetime, timedelta
from google.cloud import bigquery
from textblob import TextBlob
import yfinance as yf



# Function to calculate sentiment polarity score for a given text
def calculate_sentiment(text):
    """
    Uses TextBlob to calculate the sentiment polarity score of a given text.
    Sentiment polarity ranges from -1 (negative) to +1 (positive).
    """
    try:
        analysis = TextBlob(text)
        return analysis.sentiment.polarity
    except Exception as e:
        print(f"[ERROR] Sentiment analysis failed: {e}")
        return 0

# Function to label sentiment based on the polarity score
def label_sentiment(score):
    """
    Converts sentiment polarity scores into qualitative labels:
    - Bullish (positive sentiment)
    - Neutral (no strong sentiment)
    - Bearish (negative sentiment)
    """
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

# Function to retrieve market news for specific tickers
def get_market_news(tickers, days_back=30):
    """
    Fetches news articles for the given tickers from Yahoo Finance.
    Filters news items based on the publication date (within the last `days_back` days).
    Adds sentiment analysis and other metadata for each news item.
    """
    all_news = []
    today = datetime.now().date()
    cutoff_date = today - timedelta(days=days_back)

    for ticker in tickers:
        stock = yf.Ticker(ticker)
        try:
            news = stock.news  # Fetch news for the ticker
            for item in news:
                try:
                    publish_timestamp = item.get('providerPublishTime', 0)
                    publish_date = datetime.fromtimestamp(publish_timestamp).date()

                    # Only process news items published within the specified date range
                    if publish_date >= cutoff_date:
                        title = item.get('title', '')  # News headline
                        sentiment_score = calculate_sentiment(title)
                        sentiment_label = label_sentiment(sentiment_score)

                        # Compile metadata for the news item
                        news_item = {
                            'ticker': ticker,
                            'title': title,
                            'summary': title,  # Duplicate title for simplicity
                            'publisher': item.get('publisher', ''),
                            'link': item.get('link', ''),
                            'publish_date': datetime.fromtimestamp(publish_timestamp),
                            'type': item.get('type', ''),
                            'related_tickers': ', '.join(item.get('relatedTickers', [])),
                            'source': 'yahoo',
                            'overall_sentiment_score': sentiment_score,
                            'overall_sentiment_label': sentiment_label,
                        }
                        all_news.append(news_item)
                except Exception as e:
                    print(f"[ERROR] Error processing news item for {ticker}: {e}")
        except Exception as e:
            print(f"[ERROR] Error retrieving news for {ticker}: {e}")
    return pd.DataFrame(all_news)

# Function to upload processed data to BigQuery
def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Uploads a DataFrame to a specified BigQuery table.
    Uses WRITE_TRUNCATE mode to overwrite existing data with the new upload.
    """
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # Configure the load job to overwrite the table
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Load data to BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete

        print(f"[INFO] Data successfully uploaded to BigQuery table: {table_ref}")
    except Exception as e:
        print(f"[ERROR] Failed to upload data to BigQuery: {e}")

# Main function to orchestrate fetching, processing, and uploading market news
def fetch_and_save_market_news():
    """
    Main entry point for the script. Fetches news for market indices and tech stocks,
    processes it with sentiment analysis, and uploads it to BigQuery.
    """
    # Define indices and tickers to fetch news for
    indices = ['^IXIC', '^DJI', '^RUT', '^GSPC']  # Major market indices
    market_news = get_market_news(tickers=indices)

    tech_stocks = [
        'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
        'BWXT', 'ARBK', 'AMD', 'NVDA', 'BTC', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
        'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS',
        'META', 'RGTI', 'QUBT', 'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
    ]
    tech_news = get_market_news(tickers=tech_stocks)

    # Combine market and tech news into a single DataFrame
    combined_news = pd.concat([market_news, tech_news], ignore_index=True)

    if not combined_news.empty:
        # Upload combined data to BigQuery
        upload_to_bigquery(
            combined_news,
            project_id="trendsense",
            dataset_id="market_data",
            table_id="News_Yahoo_Extract"
        )
    else:
        print("[INFO] No news data to upload.")

# Execute the script
if __name__ == "__main__":
    fetch_and_save_market_news()

