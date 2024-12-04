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
# Log the search paths for nltk resources
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
try:
    nltk.data.find("tokenizers/punkt_tab")
    print("[DEBUG] 'punkt_tab' resource found.")
except LookupError:
    print("[DEBUG] 'punkt_tab' resource not found. Downloading...")
    nltk.download("punkt_tab")




def fetch_article_summary(link):
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
        stock = yf.Ticker(ticker)
        try:
            news = stock.news
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
            print(f"[ERROR] Error retrieving news for {ticker}: {str(e)}")
    return pd.DataFrame(all_news)


def upload_to_bigquery(df, project_id, dataset_id, table_id):
    try:
        # Drop 'category' column if present
        if 'category' in df.columns:
            df = df.drop(columns=['category'])
            print("[DEBUG] 'category' column removed from DataFrame.")

        # Automatically convert datetime columns to string
        datetime_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in datetime_columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # BigQuery client setup
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"

        # Define load job configuration to overwrite the data
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        # Upload data to BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete

        print(f"[INFO] Data successfully uploaded and overwritten in BigQuery table: {table_ref}")
    except Exception as e:
        print(f"[ERROR] Failed to upload data to BigQuery: {e}")



def fetch_market_news(request):
    """
    HTTP-triggered Cloud Function entry point.
    """
    try:
        print("[DEBUG] Function triggered.")
        # Process the request as needed
        indices = ['^IXIC', '^DJI', '^RUT', '^GSPC']
        market_news = get_market_news(tickers=indices)
        if not market_news.empty:
            market_news['category'] = 'General'

        tech_stocks = [
            'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
            'BWXT', 'ARBK', 'AMD', 'NVDA', 'BTC', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
            'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZ', 'CRM', 'NOW', 'CHTR', 'TDS', 'META'
        ]
        tech_news = get_market_news(tickers=tech_stocks)
        if not tech_news.empty:
            tech_news['category'] = 'Tech'

        combined_news = pd.concat([market_news, tech_news], ignore_index=True)

        if not combined_news.empty:
            upload_to_bigquery(
                combined_news,
                project_id="trendsense",
                dataset_id="market_data",
                table_id="News_Yahoo_Extract",
            )
            return jsonify({"status": "success", "message": "News data processed and uploaded to BigQuery."}), 200
        else:
            return jsonify({"status": "success", "message": "No news data to upload."}), 200
    except Exception as e:
        print(f"[ERROR] {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

