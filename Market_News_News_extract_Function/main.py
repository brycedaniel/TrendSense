import functions_framework
import requests
import pandas as pd
import time
from datetime import datetime, timedelta
from textblob import TextBlob
from google.cloud import bigquery
import pytz


# Replace with your NewsAPI key
api_key = 'afc3fe9ac08745439bf521cb5b974fbc'

# BigQuery configuration
project_id = "trendsense"
dataset_id = "market_data"
table_id = "News_News_Extract"

# List of tickers to search news for
tickers = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META'
]

# Get yesterday's date in ISO format
yesterday = datetime.utcnow() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')

# Function for TextBlob sentiment analysis
def textblob_sentiment(text):
    if text:
        return TextBlob(text).sentiment.polarity  # Sentiment polarity from -1 to 1
    return 0

# Function to fetch market news for a specific date
def get_market_news(ticker, date):
    url = (
        f'https://newsapi.org/v2/everything?q={ticker}&from={date}&to={date}&sortBy=publishedAt&apiKey={api_key}'
    )
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get('articles', [])
    elif response.status_code == 429:
        print(f"Rate limit exceeded for {ticker}, retrying after delay...")
        time.sleep(5)
        return []
    else:
        print(f"Error fetching data for {ticker}: {response.status_code}")
        return []

# Function to save data to BigQuery
def save_to_bigquery(data, project_id, dataset_id, table_id):
    from google.cloud import bigquery
    
    client = bigquery.Client()
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Define schema if table doesn't exist
    schema = [
        bigquery.SchemaField("ticker", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("summary_textblob_sentiment", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("publisher", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("publish_date", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    ]
    
    # Check if the table exists
    try:
        client.get_table(table_ref)
    except Exception:
        print(f"Table {table_ref} does not exist. Creating it...")
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_ref} created.")
    
    # Convert publish_date to datetime and then to MST
    data['publish_date'] = pd.to_datetime(data['publish_date'], errors='coerce')
    utc = pytz.utc
    mst = pytz.timezone('US/Mountain')
    data['publish_date'] = data['publish_date'].apply(
        lambda x: x.astimezone(mst) if pd.notnull(x) else None
    )
    
    # Ensure numeric values for sentiment
    data['summary_textblob_sentiment'] = pd.to_numeric(data['summary_textblob_sentiment'], errors='coerce')

    # Log data types to verify
    print("DataFrame dtypes before uploading:")
    print(data.dtypes)

    # Load data into BigQuery
    job = client.load_table_from_dataframe(data, table_ref)
    job.result()  # Wait for the load job to complete
    print(f"Data successfully saved to BigQuery table: {table_ref}")
    
# Cloud Function Entry Point
@functions_framework.http
def main(request):
    all_news = []
    for ticker in tickers:
        print(f"Fetching news for {ticker} from {yesterday_str}...")
        articles = get_market_news(ticker, yesterday_str)

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
                'source': 'NewsAPI',  # Identify source
            }
            all_news.append(news_entry)

        # Avoid rate limiting
        time.sleep(1)

    # Convert data to a DataFrame
    df = pd.DataFrame(all_news)

    if not df.empty:
        # Save to BigQuery
        save_to_bigquery(df, project_id, dataset_id, table_id)
        return {
            "status": "success",
            "message": f"Data saved to BigQuery table: {project_id}.{dataset_id}.{table_id}",
            "total_articles": len(all_news),
        }
    else:
        return {
            "status": "success",
            "message": "No news articles found for yesterday.",
            "total_articles": 0,
        }

