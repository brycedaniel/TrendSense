# Yahoo Extract with BigQuery Upload This is not running in Google Function.  Need to automate locally

import nltk
import os
from google.cloud import bigquery

# Explicitly set the nltk_data path
nltk_data_path = r"C:\Users\BryceDaniel\OneDrive - Lincoln Telephone Company\MSBA\GitHub\TrendSense\Market News\Market_News_Yahoo_Extract_Function\nltk_data"
nltk.data.path.append(nltk_data_path)

# Ensure 'punkt' is downloaded into the correct folder
nltk.download('punkt', download_dir=nltk_data_path)

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from textblob import TextBlob

def calculate_sentiment(text):
    try:
        analysis = TextBlob(text)
        return analysis.sentiment.polarity
    except Exception as e:
        print(f"[ERROR] Sentiment analysis failed: {e}")
        return 0

def label_sentiment(score):
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

def get_market_news(tickers, days_back=30):
    all_news = []
    today = datetime.now().date()
    cutoff_date = today - timedelta(days=days_back)

    for ticker in tickers:
        stock = yf.Ticker(ticker)
        try:
            news = stock.news
            for item in news:
                try:
                    # Extract publish timestamp and date
                    publish_timestamp = item.get('providerPublishTime', 0)
                    publish_date = datetime.fromtimestamp(publish_timestamp).date()

                    # Only process news within the desired date range
                    if publish_date >= cutoff_date:
                        title = item.get('title', '')
                        sentiment_score = calculate_sentiment(title)
                        sentiment_label = label_sentiment(sentiment_score)

                        news_item = {
                            'ticker': ticker,
                            'title': title,
                            'summary': title,  # Replicate title in the summary column
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
                except Exception as news_item_error:
                    print(f"[ERROR] Error processing news item: {news_item_error}")
        except Exception as e:
            print(f"[ERROR] Error retrieving news for {ticker}: {str(e)}")
    return pd.DataFrame(all_news)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        job = client.load_table_from_dataframe(df, table_ref)
        job.result()  # Wait for the job to complete

        print(f"[INFO] Data successfully uploaded to BigQuery table: {table_ref}")
    except Exception as e:
        print(f"[ERROR] Failed to upload data to BigQuery: {e}")

def fetch_and_save_market_news():
    indices = ['^IXIC', '^DJI', '^RUT', '^GSPC']
    market_news = get_market_news(tickers=indices)
    #if not market_news.empty:
    #    market_news['category'] = 'General'

    tech_stocks = [
        'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
        'BWXT', 'ARBK', 'AMD', 'NVDA', 'BTC', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
        'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META','RGTI','QUBT',
        'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
    ]
    tech_news = get_market_news(tickers=tech_stocks)
    #if not tech_news.empty:
    #    tech_news['category'] = 'Tech'

    combined_news = pd.concat([market_news, tech_news], ignore_index=True)

    if not combined_news.empty:
        upload_to_bigquery(
            combined_news,
            project_id="trendsense",
            dataset_id="market_data",
            table_id="News_Yahoo_Extract"
        )
    else:
        print("[INFO] No news data to upload.")

if __name__ == "__main__":
    fetch_and_save_market_news()

