from flask import Flask, request, jsonify
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from textblob import TextBlob
from newspaper import Article
from google.cloud import bigquery
import nltk
import os

# Append the nltk_data path
nltk.data.path.append(os.path.join(os.getcwd(), "nltk_data"))
print("[DEBUG] NLTK data paths:")
for path in nltk.data.path:
    print(f" - {path}")

# Check if punkt is available
try:
    nltk.data.find("tokenizers/punkt")
    print("[DEBUG] 'punkt' resource found.")
except LookupError:
    print("[DEBUG] 'punkt' resource not found. Downloading...")
    nltk.download("punkt")

def fetch_article_summary(link):
    print(f"[DEBUG] Attempting to fetch summary for: {link}")
    try:
        article = Article(link)
        article.download()
        article.parse()
        article.nlp()
        return article.summary
    except Exception as e:
        print(f"[ERROR] Failed to fetch or summarize article: {e}")
        return "No summary available."

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

def get_market_news(tickers):
    all_news = []
    today = datetime.now().date()
    one_day_ago = today - timedelta(days=1)

    for ticker in tickers:
        print(f"[DEBUG] Fetching news for ticker: {ticker}")
        stock = yf.Ticker(ticker)
        try:
            news = stock.news
            print(f"[DEBUG] Fetched {len(news)} news items for ticker {ticker}")
            for item in news:
                try:
                    publish_timestamp = item.get('providerPublishTime', 0)
                    publish_date = datetime.fromtimestamp(publish_timestamp).date()

                    if publish_date >= one_day_ago and item.get('type', '').lower() == 'story':
                        link = item.get('link', '')
                        summary = fetch_article_summary(link) if link else "No summary available."
                        sentiment_score = calculate_sentiment(summary)
                        sentiment_label = label_sentiment(sentiment_score)

                        news_item = {
                            'ticker': ticker,
                            'title': item.get('title', ''),
                            'summary': summary,
                            'publisher': item.get('publisher', ''),
                            'link': link,
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
            print(f"[ERROR] Error retrieving news for {ticker}: {e}")
    print(f"[DEBUG] Total news items fetched: {len(all_news)}")
    return pd.DataFrame(all_news)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    try:
        if 'category' in df.columns:
            df = df.drop(columns=['category'])
            print("[DEBUG] 'category' column removed from DataFrame.")

        datetime_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in datetime_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        print("[DEBUG] Uploading data to BigQuery.")
        print(f"[DEBUG] DataFrame shape: {df.shape}")
        print(f"[DEBUG] DataFrame columns: {df.columns}")

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()

        print(f"[INFO] Data successfully uploaded and overwritten in BigQuery table: {table_ref}")
    except Exception as e:
        print(f"[ERROR] Failed to upload data to BigQuery: {e}")

def fetch_market_news(request):
    try:
        print("[DEBUG] Cloud Function execution started.")

        indices = ['^IXIC', '^DJI', '^RUT', '^GSPC']
        market_news = get_market_news(tickers=indices)
        print(f"[DEBUG] Market news fetched: {len(market_news)} rows.")
        if not market_news.empty:
            market_news['category'] = 'General'

        tech_stocks = [
            'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
            'BWXT', 'ARBK', 'AMD', 'NVDA', 'BTC', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
            'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META','RGTI','QUBT',
            'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
        ]
        tech_news = get_market_news(tickers=tech_stocks)
        print(f"[DEBUG] Tech news fetched: {len(tech_news)} rows.")
        if not tech_news.empty:
            tech_news['category'] = 'Tech'

        combined_news = pd.concat([market_news, tech_news], ignore_index=True)
        print(f"[DEBUG] Combined news rows: {len(combined_news)}")

        if not combined_news.empty:
            upload_to_bigquery(
                combined_news,
                project_id="trendsense",
                dataset_id="market_data",
                table_id="News_Yahoo_Extract",
            )
            return jsonify({"status": "success", "message": "News data processed and uploaded to BigQuery."}), 200
        else:
            print("[DEBUG] No news data to upload.")
            return jsonify({"status": "success", "message": "No news data to upload."}), 200
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500
