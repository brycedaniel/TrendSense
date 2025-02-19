import yfinance as yf
import pandas as pd
from datetime import datetime
import pytz
from google.cloud import bigquery
from google.oauth2 import service_account

# BigQuery Configuration
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "current_stock_data"

# Define the list of stock tickers
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


def fetch_current_stock_data():
    """Fetch current stock data and save to BigQuery."""
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

            # Convert to Mountain Standard Time (MST)
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
                "Percent_Difference": percent_difference
            }
            stock_data_list.append(current_data)
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    return pd.DataFrame(stock_data_list)

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

    print("Fetching current stock data...")
    stock_data = fetch_current_stock_data()

    if not stock_data.empty:
        print("Uploading data to BigQuery...")
        upload_to_bigquery(stock_data, client)
        print("Process completed successfully.")
    else:
        print("No stock data retrieved.")

if __name__ == "__main__":
    main()
