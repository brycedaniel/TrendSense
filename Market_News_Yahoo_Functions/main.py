import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from newspaper import Article
from bs4 import BeautifulSoup
import requests
import nltk

# Ensure the required NLTK data is downloaded
nltk.download('punkt')


def fetch_article_summary(link):
    """
    Fetch and summarize the article content from a URL.
    """
    try:
        # Fetch article using Newspaper3k
        article = Article(link)
        article.download()
        article.parse()
        article.nlp()
        return article.summary
    except Exception as e:
        print(f"[ERROR] Failed to summarize article from {link}: {str(e)}")
        return "No summary available."

def get_market_news(tickers):
    """
    Fetch market news for the current day, capturing all available fields and generating summaries.
    """
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

                    # Filter news to include only today's and yesterday's articles
                    if publish_date >= one_day_ago:
                        # Fetch and summarize article content
                        link = item.get('link', '')
                        summary = fetch_article_summary(link) if link else "No summary available."

                        news_item = {
                            'ticker': ticker,
                            'title': item.get('title', ''),
                            'publisher': item.get('publisher', ''),
                            'link': link,
                            'publish_date': datetime.fromtimestamp(publish_timestamp),
                            'summary': summary,  # Include the generated summary
                            'type': item.get('type', ''),  # Original type from Yahoo API
                            'related_tickers': ', '.join(item.get('relatedTickers', [])),  # Comma-separated related tickers
                        }
                        all_news.append(news_item)
                except Exception as news_item_error:
                    print(f"[ERROR] Error processing news item: {news_item_error}")

        except Exception as e:
            print(f"[ERROR] Error retrieving news for {ticker}: {str(e)}")

    print(f"[INFO] Fetched {len(all_news)} news articles.")
    return pd.DataFrame(all_news)


def save_to_bigquery(df, project_id, dataset_id, table_id):
    """
    Save DataFrame to BigQuery using schema auto-detection.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    try:
        # Ensure DataFrame is not empty
        if df.empty:
            print("DataFrame is empty. No data to save.")
            return False

        # Convert datetime to timestamp
        df['publish_date'] = pd.to_datetime(df['publish_date'])

        # Configure the job to auto-detect schema and append data
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True  # Enable schema auto-detection
        )

        # Load DataFrame directly to BigQuery
        job = client.load_table_from_dataframe(
            df, 
            table_ref, 
            job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        print(f"Successfully saved {len(df)} rows to {table_ref}")
        return True
    
    except Exception as e:
        print(f"[ERROR] BigQuery Save Error: {str(e)}")
        print(f"DataFrame Columns: {df.columns}")
        print(f"DataFrame Sample:\n{df.head()}")
        return False


def main(request=None):
    """
    Main function for fetching and saving market news.
    """
    # Google Cloud configuration
    project_id = "trendsense"
    dataset_id = "market_data"
    table_id = "market_news_yahoo"

    try:
        # Fetch general market news
        indices = ['^IXIC', '^DJI', '^RUT', '^GSPC']
        market_news = get_market_news(tickers=indices)
        if not market_news.empty:
            market_news['category'] = 'General'  # Add category for general market

        # Fetch tech stock news
        tech_stocks = ['AAPL', 'GOOGL', 'MSFT']
        tech_news = get_market_news(tickers=tech_stocks)
        if not tech_news.empty:
            tech_news['category'] = 'Tech'  # Add category for tech stocks

        # Combine news
        combined_news = pd.concat([market_news, tech_news], ignore_index=True)

        # Save to BigQuery
        if not combined_news.empty:
            save_result = save_to_bigquery(combined_news, project_id, dataset_id, table_id)
            return "Data successfully saved to BigQuery.", 200
        else:
            return "No news to save.", 204
    except Exception as e:
        print(f"[ERROR] Error in main function: {e}")
        return f"Internal Server Error: {e}", 500


# Optional: For local testing
if __name__ == "__main__":
    main()
