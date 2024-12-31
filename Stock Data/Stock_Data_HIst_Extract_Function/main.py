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

# Define the list of stock tickers
TICKERS = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META', 'RGTI','QUBT',
    'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO'
]

def extract_stock_close(request):
    """Cloud Function to fetch current day stock data and save to BigQuery."""
    try:
        # Define today's date and previous business day
        today = datetime.today()
        
        # Adjust for weekends and market holidays
        start_date = today - timedelta(days=3)
        end_date = today
        
        logger.info(f"Fetching stock data from {start_date} to {end_date}")
        
        # Fetch stock data using a date range to ensure data availability
        try:
            stock_data = yf.download(TICKERS, start=start_date, end=end_date, group_by='ticker', threads=True)
        except Exception as download_error:
            logger.error(f"Failed to download stock data: {download_error}")
            return f"Failed to download stock data: {download_error}"

        # Check if data was returned
        if stock_data.empty:
            logger.warning(f"No data available for date range {start_date} to {end_date}")
            return f"No data available for date range {start_date} to {end_date}"

        # Create an empty list to store reformatted data
        formatted_data = []

        # Process each ticker to extract relevant information
        for ticker in TICKERS:
            try:
                if ticker in stock_data.columns.get_level_values(0):  # Ensure ticker exists in data
                    # Select the most recent day's data
                    ticker_data = stock_data[ticker].iloc[-1]
                    
                    # Ensure we have valid data for the current day
                    if pd.notna(ticker_data['Close']):
                        # Calculate percent difference from the previous close
                        try:
                            previous_close = stock_data[ticker].iloc[-2]['Close']
                            current_close = ticker_data['Close']
                            percent_difference = ((current_close - previous_close) / previous_close)
                        except (IndexError, TypeError):
                            previous_close = None
                            percent_difference = None

                        # Append today's data
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

        # Convert the list of dictionaries to a DataFrame
        reformatted_data = pd.DataFrame(formatted_data)

        # Check if reformatted data is empty
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
            
            # Wait for job to complete and log any errors
            job.result()
            
            logger.info(f"Stock data for {today} successfully saved to {table_ref}")
            return f"Stock data for {today} successfully saved to {table_ref}"

        except Exception as bigquery_error:
            logger.error(f"BigQuery upload failed: {bigquery_error}")
            return f"BigQuery upload failed: {bigquery_error}"

    except Exception as general_error:
        logger.error(f"Unexpected error in extract_stock_close: {general_error}")
        return f"Unexpected error: {general_error}"

# Note: If this is a Google Cloud Function, you might need to add a trigger
# such as a HTTP trigger or a scheduled cloud function trigger



