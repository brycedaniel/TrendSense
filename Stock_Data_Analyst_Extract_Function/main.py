import yfinance as yf
import pandas as pd
from datetime import datetime
import os
import json
import requests
from google.cloud import bigquery

# Replace with your actual API Key
YAHOO_API_KEY = "ee72be2ef9msh532c4fc1a7b7941p1176e1jsn0328598b0245"

# Constants for Google BigQuery
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "stock_analyst"
STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
            'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
            'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META','RGTI','QUBT']

def fetch_latest_yahoo_recommendations(ticker: str) -> tuple:
    """Fetch the latest analyst recommendations from Yahoo Finance."""
    strong_buy, buy, hold, sell, strong_sell = 0, 0, 0, 0, 0
    try:
        url = "https://yahoo-finance166.p.rapidapi.com/api/stock/get-recommendation-trend"
        headers = {
            'x-rapidapi-key': YAHOO_API_KEY,
            'x-rapidapi-host': 'yahoo-finance166.p.rapidapi.com'
        }
        params = {'symbol': ticker, 'region': 'US'}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"API error for {ticker}: {response.status_code} {response.reason}")
            return strong_buy, buy, hold, sell, strong_sell

        parsed_data = response.json()
        recommendations = (
            parsed_data.get('quoteSummary', {})
            .get('result', [])[0]
            .get('recommendationTrend', {})
            .get('trend', [])
        )

        if recommendations:
            latest_trend = recommendations[0]
            strong_buy = latest_trend.get('strongBuy', 0)
            buy = latest_trend.get('buy', 0)
            hold = latest_trend.get('hold', 0)
            sell = latest_trend.get('sell', 0)
            strong_sell = latest_trend.get('strongSell', 0)
    except Exception as e:
        print(f"Error fetching recommendations for {ticker}: {e}")
    return strong_buy, buy, hold, sell, strong_sell

def get_stock_targets(symbols):
    """Fetch stock targets and recommendations for a list of symbols."""
    all_stock_data = []
    for symbol in symbols:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            strong_buy, buy, hold, sell, strong_sell = fetch_latest_yahoo_recommendations(symbol)

            stock_data = {
                'symbol': symbol,
                'fetch_date': datetime.now().strftime('%Y-%m-%d'),
                'current_price': info.get('currentPrice'),
                'target_high_price': info.get('targetHighPrice'),
                'target_low_price': info.get('targetLowPrice'),
                'target_mean_price': info.get('targetMeanPrice'),
                'target_median_price': info.get('targetMedianPrice'),
                'Strong_Buy': strong_buy,
                'Buy': buy,
                'Hold': hold,
                'Sell': sell,
                'Strong_Sell': strong_sell
            }
            all_stock_data.append(stock_data)
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
    return pd.DataFrame(all_stock_data)

def upload_to_bigquery(df):
    """Upload the DataFrame to Google BigQuery."""
    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Ensure fetch_date is converted to datetime.date
    if "fetch_date" in df.columns:
        df["fetch_date"] = pd.to_datetime(df["fetch_date"]).dt.date
        
        
    # Define schema
    schema = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("fetch_date", "DATE"),
        bigquery.SchemaField("current_price", "FLOAT"),
        bigquery.SchemaField("target_high_price", "FLOAT"),
        bigquery.SchemaField("target_low_price", "FLOAT"),
        bigquery.SchemaField("target_mean_price", "FLOAT"),
        bigquery.SchemaField("target_median_price", "FLOAT"),
        bigquery.SchemaField("Strong_Buy", "INTEGER"),
        bigquery.SchemaField("Buy", "INTEGER"),
        bigquery.SchemaField("Hold", "INTEGER"),
        bigquery.SchemaField("Sell", "INTEGER"),
        bigquery.SchemaField("Strong_Sell", "INTEGER"),
    ]

    try:
        # Check if the table exists; create if not
        try:
            client.get_table(table_ref)
        except:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {table_ref}")

        # Upload DataFrame
        job = client.load_table_from_dataframe(df, table_ref)
        job.result()
        print("Data uploaded to BigQuery successfully.")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

def main(event=None, context=None):
    """Cloud Function entry point."""
    try:
        print("Fetching stock data...")
        stock_data = get_stock_targets(STOCK_SYMBOLS)

        if not stock_data.empty:
            print("Uploading data to BigQuery...")
            upload_to_bigquery(stock_data)
            return "Stock data successfully fetched and stored in BigQuery."
        else:
            print("No stock data retrieved.")
            return "No stock data retrieved."

    except Exception as e:
        print(f"Error in main function: {e}")
        return f"Error processing stock data: {e}"

if __name__ == "__main__":
    main()




