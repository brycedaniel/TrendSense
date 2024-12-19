import yfinance as yf
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import pytz

# Define BigQuery dataset and table
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "current_stock_data"

# Define the list of stock tickers
TICKERS = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META','RGTI','QUBT'
]

def fetch_current_stock_data(request):
    """Cloud Function to fetch current stock data and save to BigQuery."""
    # Create a list to store current stock data
    stock_data_list = []

    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info  # Fetch current stock information
            previous_close = info.get("previousClose")
            current_price = info.get("currentPrice")
            
            # Calculate percent difference if both values are available
            if previous_close and current_price:
                percent_difference = ((current_price - previous_close) / previous_close) 
            else:
                percent_difference = None

            mst = pytz.timezone('MST')
            current_data = {
                "Date": datetime.now(pytz.utc).astimezone(mst).strftime('%Y-%m-%d %H:%M:%S'),
                "Ticker": ticker,
                "Current_Price": current_price,
                "Open": info.get("open"),
                "High": info.get("dayHigh"),
                "Low": info.get("dayLow"),
                "Volume": info.get("volume"),
                "Market_Cap": info.get("marketCap"),
                "Previous_Close": previous_close,
                "Percent_Difference": percent_difference  # Add the calculated column
            }
            stock_data_list.append(current_data)
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            continue

    # Convert list to DataFrame
    stock_data_df = pd.DataFrame(stock_data_list)

    # Save to BigQuery
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Define job configuration
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append data if table exists
        autodetect=True  # Automatically detect schema
    )

    # Load data to BigQuery
    job = client.load_table_from_dataframe(
        stock_data_df,
        table_ref,
        job_config=job_config
    )
    job.result()  # Wait for job to complete

    return f"Current stock data successfully saved to {table_ref}."
