import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define BigQuery dataset and table
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "stock_data_history"

# Define stock tickers
TICKERS = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META', 'RGTI', 'QUBT',
    'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
]

# NASDAQ Composite Index Ticker
NASDAQ_TICKER = "^IXIC"

def extract_stock_close(request):
    """Cloud Function to fetch stock data and save to BigQuery."""
    try:
        today = datetime.today()
        start_date = today - timedelta(days=3)  # Normal tickers (last 3 days)
        nasdaq_start_date = timedelta(days=3)  # NASDAQ fetch start date

        logger.info(f"Fetching stock data from {start_date} to {today}")
        logger.info(f"Fetching NASDAQ (^IXIC) data from {nasdaq_start_date} to {today}")

        # Fetch stock data for normal tickers
        try:
            stock_data = yf.download(TICKERS, start=start_date, end=today, group_by='ticker', threads=5)
        except Exception as download_error:
            logger.error(f"Failed to download stock data: {download_error}")
            return f"Failed to download stock data: {download_error}"

        # Fetch NASDAQ Composite Index (^IXIC) data
        try:
            nasdaq_data = yf.download(NASDAQ_TICKER, start=nasdaq_start_date, end=today)
        except Exception as nasdaq_error:
            logger.error(f"Failed to download NASDAQ data: {nasdaq_error}")
            return f"Failed to download NASDAQ data: {nasdaq_error}"

        # 🛠 Flatten NASDAQ MultiIndex Columns
        if isinstance(nasdaq_data.columns, pd.MultiIndex):
            nasdaq_data.columns = [col[0] for col in nasdaq_data.columns]
        nasdaq_data = nasdaq_data.reset_index()  # Convert Date index into a column

        # Detect correct column names dynamically
        column_map = {"Close": None, "Open": None, "High": None, "Low": None, "Volume": None}
        for col in nasdaq_data.columns:
            for key in column_map.keys():
                if key in col:
                    column_map[key] = col
        nasdaq_data = nasdaq_data.rename(columns=column_map)

        formatted_data = []

        # Process normal tickers
        for ticker in TICKERS:
            try:
                if ticker in stock_data.columns.get_level_values(0):
                    ticker_data = stock_data[ticker].iloc[-1]  # Get latest data
                    
                    if pd.notna(ticker_data['Close']):
                        previous_close = stock_data[ticker]['Close'].shift(1).iloc[-1]
                        percent_difference = None
                        
                        if pd.notna(previous_close):
                            percent_difference = ((ticker_data['Close'] - previous_close) / previous_close)

                        formatted_data.append({
                            "Date": today.strftime('%Y-%m-%d'),
                            "Ticker": ticker,
                            "Close": ticker_data['Close'],
                            "Volume": ticker_data['Volume'],
                            "High": ticker_data['High'],
                            "Low": ticker_data['Low'],
                            "Open": ticker_data['Open'],
                            "Percent_Difference": percent_difference
                        })
            except Exception as ticker_error:
                logger.error(f"Error processing {ticker}: {ticker_error}")
                continue

        # Process NASDAQ Data
        for _, row in nasdaq_data.iterrows():
            try:
                previous_close = nasdaq_data["Close"].shift(1).iloc[-1] if len(nasdaq_data) > 1 else None
                percent_difference = None

                if pd.notna(previous_close):
                    percent_difference = ((row["Close"] - previous_close) / previous_close)

                formatted_data.append({
                    "Date": row["Date"].strftime('%Y-%m-%d'),
                    "Ticker": NASDAQ_TICKER,
                    "Close": row["Close"],
                    "Volume": row["Volume"] if "Volume" in nasdaq_data.columns else None,
                    "High": row["High"] if "High" in nasdaq_data.columns else None,
                    "Low": row["Low"] if "Low" in nasdaq_data.columns else None,
                    "Open": row["Open"] if "Open" in nasdaq_data.columns else None,
                    "Percent_Difference": percent_difference
                })
            except Exception as nasdaq_error:
                logger.error(f"Error processing NASDAQ (^IXIC): {nasdaq_error}")
                continue

        # Convert to DataFrame
        reformatted_data = pd.DataFrame(formatted_data)

        if reformatted_data.empty:
            logger.warning(f"No valid stock data available for {today}")
            return f"No valid stock data available for {today}"

        # Save to BigQuery
        try:
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
            
            # Wait for job to complete
            job.result()
            
            logger.info(f"Stock data for {today} successfully saved to {table_ref}")
            return f"Stock data for {today} successfully saved to {table_ref}"

        except Exception as bigquery_error:
            logger.error(f"BigQuery upload failed: {bigquery_error}")
            return f"BigQuery upload failed: {bigquery_error}"

    except Exception as general_error:
        logger.error(f"Unexpected error in extract_stock_close: {general_error}")
        return f"Unexpected error: {general_error}"



