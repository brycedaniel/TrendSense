import yfinance as yf
import pandas as pd
import logging
from datetime import datetime, timedelta
from google.cloud import bigquery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Google Cloud BigQuery settings
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

# Market Index Tickers (NASDAQ and S&P 500)
INDEX_TICKERS = ["^IXIC", "^GSPC"]

def extract_stock_close_daily(request):
    """Google Cloud Function to fetch only today's stock data and append it to BigQuery."""
    try:
        today = datetime.today().date()
        logger.info(f"Fetching daily stock data for {today}")

        # Fetch only today's stock data
        try:
            stock_data = yf.download(TICKERS, start=today, end=today + timedelta(days=1), group_by='ticker', threads=5)
            index_data = yf.download(INDEX_TICKERS, start=today, end=today + timedelta(days=1), group_by='ticker')
        except Exception as error:
            logger.error(f"Failed to download stock data: {error}")
            return f"Failed to download stock data: {error}"

        formatted_data = []

        # Function to find previous available closing price
        def get_previous_close(df, current_date):
            previous_data = df[df["Date"] < current_date].sort_values("Date", ascending=False)
            return previous_data["Close"].iloc[0] if not previous_data.empty else None

        # Process normal tickers
        for ticker in TICKERS:
            try:
                if ticker in stock_data.columns.get_level_values(0):
                    ticker_data = stock_data[ticker].reset_index()

                    for _, row in ticker_data.iterrows():
                        if pd.notna(row['Close']):
                            previous_close = get_previous_close(ticker_data, row["Date"])
                            percent_difference = ((row['Close'] - previous_close) / previous_close) if previous_close else None

                            formatted_data.append({
                                "Date": row["Date"].strftime('%Y-%m-%d'),
                                "Ticker": ticker,
                                "Close": row['Close'],
                                "Volume": row['Volume'],
                                "High": row['High'],
                                "Low": row['Low'],
                                "Open": row['Open'],
                                "Percent_Difference": percent_difference
                            })
            except Exception as ticker_error:
                logger.error(f"Error processing {ticker}: {ticker_error}")
                continue

        # Process NASDAQ and S&P 500 Data
        for ticker in INDEX_TICKERS:
            if ticker in index_data.columns.get_level_values(0):
                index_df = index_data[ticker].reset_index()

                for _, row in index_df.iterrows():
                    try:
                        previous_close = get_previous_close(index_df, row["Date"])
                        percent_difference = ((row["Close"] - previous_close) / previous_close) if previous_close else None

                        formatted_data.append({
                            "Date": row["Date"].strftime('%Y-%m-%d'),
                            "Ticker": ticker,
                            "Close": row["Close"],
                            "Volume": row["Volume"] if "Volume" in index_df.columns else None,
                            "High": row["High"] if "High" in index_df.columns else None,
                            "Low": row["Low"] if "Low" in index_df.columns else None,
                            "Open": row["Open"] if "Open" in index_df.columns else None,
                            "Percent_Difference": percent_difference
                        })
                    except Exception as index_error:
                        logger.error(f"Error processing {ticker}: {index_error}")
                        continue

        # Convert to DataFrame
        reformatted_data = pd.DataFrame(formatted_data)

        if reformatted_data.empty:
            logger.warning(f"No valid stock data available for {today}")
            return f"No valid stock data available for {today}"

        # Append to BigQuery instead of overwriting
        try:
            client = bigquery.Client(project=PROJECT_ID)
            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append new data
                autodetect=True
            )

            job = client.load_table_from_dataframe(
                reformatted_data,
                table_ref,
                job_config=job_config
            )

            job.result()
            logger.info(f"Daily stock data successfully appended to {table_ref}")
            return f"Daily stock data successfully appended to {table_ref}"

        except Exception as bigquery_error:
            logger.error(f"BigQuery append failed: {bigquery_error}")
            return f"BigQuery append failed: {bigquery_error}"

    except Exception as general_error:
        logger.error(f"Unexpected error: {general_error}")
        return f"Unexpected error: {general_error}"





