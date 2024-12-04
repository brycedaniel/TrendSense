import yfinance as yf
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# Define BigQuery dataset and table
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "stock_data_history"

# Define the list of stock tickers
TICKERS = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META'
]

def extract_stock_close(request):
    """Cloud Function to fetch current day stock data and save to BigQuery."""
    # Define today's date
    today = datetime.today().strftime('%Y-%m-%d')

    # Fetch stock data for the current day using yfinance
    stock_data = yf.download(TICKERS, start=today, end=today, group_by='ticker')

    # Create an empty list to store reformatted data
    formatted_data = []

    # Process each ticker to extract relevant information
    for ticker in TICKERS:
        try:
            if ticker in stock_data.columns.levels[0]:  # Ensure ticker exists in data
                ticker_data = stock_data[ticker].reset_index()  # Reset index for easier processing

                # Calculate percent difference from the previous close
                previous_close = ticker_data["Close"].iloc[-2] if len(ticker_data) > 1 else None
                current_close = ticker_data["Close"].iloc[-1]
                percent_difference = (
                    ((current_close - previous_close) / previous_close)
                    if previous_close else None
                )

                # Append today's data
                formatted_data.append({
                    "Date": today,
                    "Ticker": ticker,
                    "Close": current_close,
                    "Volume": ticker_data["Volume"].iloc[-1],
                    "High": ticker_data["High"].iloc[-1],
                    "Low": ticker_data["Low"].iloc[-1],
                    "Open": ticker_data["Open"].iloc[-1],
                    "Percent_Difference": percent_difference
                })
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue

    # Convert the list of dictionaries to a DataFrame
    reformatted_data = pd.DataFrame(formatted_data)

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
        reformatted_data,
        table_ref,
        job_config=job_config
    )
    job.result()  # Wait for job to complete

    return f"Stock data for {today} successfully saved to {table_ref}."




