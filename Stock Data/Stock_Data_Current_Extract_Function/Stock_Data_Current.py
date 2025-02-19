import yfinance as yf
import pandas as pd
import time
from datetime import datetime
import pytz
from google.cloud import bigquery

# BigQuery Configuration
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "current_stock_data"

# Define stock tickers
TICKERS = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS',
    'META', 'RGTI', 'QUBT', 'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
]

def initialize_bigquery_client():
    """Initialize BigQuery client using default credentials."""
    try:
        return bigquery.Client(project=PROJECT_ID)
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return None

def fetch_stock_data():
    """Fetch stock data efficiently using yfinance batch requests."""
    try:
        print("Fetching stock data in batch...")
        # Use yfinance's bulk request method
        data = yf.download(TICKERS, period="5d", interval="1d", group_by="ticker", progress=False)

        mst = pytz.timezone('MST')
        current_date = datetime.now(pytz.utc).astimezone(mst).strftime('%Y-%m-%d %H:%M:%S')

        stock_data_list = []
        for ticker in TICKERS:
            try:
                if ticker in data and not data[ticker].empty:
                    df = data[ticker]

                    # Ensure there is at least one row of data
                    if df.shape[0] > 0:
                        last_close = df["Close"].iloc[-1] if "Close" in df else None
                        prev_close = df["Close"].iloc[-2] if len(df) > 1 else None
                        percent_diff = ((last_close - prev_close) / prev_close) if prev_close else None

                        stock_data_list.append({
                            "Date": current_date,
                            "Ticker": ticker,
                            "Current_Price": last_close,
                            "Open": df["Open"].iloc[-1] if "Open" in df else None,
                            "High": df["High"].iloc[-1] if "High" in df else None,
                            "Low": df["Low"].iloc[-1] if "Low" in df else None,
                            "Volume": df["Volume"].iloc[-1] if "Volume" in df else None,
                            "Previous_Close": prev_close,
                            "Percent_Difference": percent_diff
                        })
            except Exception as e:
                print(f"Error processing data for {ticker}: {e}")
                continue

        return pd.DataFrame(stock_data_list)

    except Exception as e:
        print(f"Error fetching stock data: {e}")
        return pd.DataFrame()

def upload_to_bigquery(df: pd.DataFrame, client: bigquery.Client):
    """Upload the DataFrame to Google BigQuery."""
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    try:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append data if table exists
            autodetect=True  # Automatically detect schema
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Data successfully uploaded to BigQuery: {table_ref}")
    except Exception as e:
        print(f"Error uploading data to BigQuery: {e}")

def main():
    """Main function to run locally."""
    print("Initializing BigQuery client...")
    client = initialize_bigquery_client()
    if client is None:
        print("Failed to initialize BigQuery client. Exiting...")
        return

    print("Fetching stock data...")
    stock_data = fetch_stock_data()

    if not stock_data.empty:
        print("Uploading data to BigQuery...")
        upload_to_bigquery(stock_data, client)
        print("Process completed successfully.")
    else:
        print("No stock data retrieved.")

if __name__ == "__main__":
    main()

