import yfinance as yf
import pandas as pd
from datetime import datetime
import os
import json
import requests
from google.cloud import bigquery
from typing import List
import random

# API Keys Configuration
API_KEYS = [
    "1949b7a600msh1fcf25399699fcap11bf4fjsnd87cb8a26731",
    "ee72be2ef9msh532c4fc1a7b7941p1176e1jsn0328598b0245",  
    "bd8ce2e47emshd9b790e712f6f41p195cd7jsnc2d18a4e9d95"
 
]

# Constants for Google BigQuery
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "stock_analyst"
STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
            'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
            'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META','RGTI','QUBT',
            'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
            ]

class APIKeyManager:
    def __init__(self, api_keys: List[str]):
        self.api_keys = api_keys
        self.current_index = 0
        self.failed_keys = set()

    def get_next_key(self) -> str:
        """Get the next available API key."""
        attempts = 0
        while attempts < len(self.api_keys):
            if len(self.failed_keys) == len(self.api_keys):
                raise Exception("All API keys have failed")
            
            key = self.api_keys[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.api_keys)
            
            if key not in self.failed_keys:
                return key
            
            attempts += 1
        
        raise Exception("No valid API keys available")

    def mark_key_failed(self, key: str):
        """Mark an API key as failed."""
        self.failed_keys.add(key)

def fetch_latest_yahoo_recommendations(ticker: str, key_manager: APIKeyManager) -> tuple:
    """Fetch the latest analyst recommendations from Yahoo Finance using rotating API keys."""
    strong_buy, buy, hold, sell, strong_sell = 0, 0, 0, 0, 0
    
    try:
        api_key = key_manager.get_next_key()
        url = "https://yahoo-finance166.p.rapidapi.com/api/stock/get-recommendation-trend"
        headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': 'yahoo-finance166.p.rapidapi.com'
        }
        params = {'symbol': ticker, 'region': 'US'}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:  # Rate limit exceeded
            print(f"Rate limit exceeded for key: {api_key}")
            key_manager.mark_key_failed(api_key)
            return fetch_latest_yahoo_recommendations(ticker, key_manager)  # Retry with next key
            
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

def get_stock_targets(symbols: List[str]) -> pd.DataFrame:
    """Fetch stock targets and recommendations for a list of symbols."""
    key_manager = APIKeyManager(API_KEYS)
    all_stock_data = []
    
    for symbol in symbols:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info

            strong_buy, buy, hold, sell, strong_sell = fetch_latest_yahoo_recommendations(symbol, key_manager)

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

def upload_to_bigquery(df: pd.DataFrame):
    """Upload the DataFrame to Google BigQuery."""
    client = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    if "fetch_date" in df.columns:
        df["fetch_date"] = pd.to_datetime(df["fetch_date"]).dt.date
        
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
        try:
            client.get_table(table_ref)
        except:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {table_ref}")

        job = client.load_table_from_dataframe(df, table_ref)
        job.result()
        print("Data uploaded to BigQuery successfully.")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

def main(event=None, context=None):
    """Cloud Function entry point."""
    try:
        print("Fetching stock data...")
        # Shuffle the stock symbols to distribute load across different API keys
        shuffled_symbols = STOCK_SYMBOLS.copy()
        random.shuffle(shuffled_symbols)
        
        stock_data = get_stock_targets(shuffled_symbols)

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




