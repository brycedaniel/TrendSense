import requests
import json
from datetime import datetime
from google.cloud import bigquery
from flask import Request

# List of company tickers
tickers = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'BTC', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZ', 'CRM', 'NOW', 'CHTR', 'TDS', 'META'
]

# Function to load the company tickers JSON file and create a ticker to CIK mapping
def load_ticker_cik_mapping():
    url = 'https://www.sec.gov/files/company_tickers.json'
    headers = {'User-Agent': 'Bryce Daniel  (bryce.daniel@umconnect.umt.edu)'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        ticker_cik_mapping = {item['ticker']: str(item['cik_str']).zfill(10) for item in data.values()}
        return ticker_cik_mapping
    return {}

# Function to fetch filings for the current date
def fetch_filings_for_today(cik):
    url = f'https://data.sec.gov/submissions/CIK{cik}.json'
    headers = {'User-Agent': 'Bryce Daniel  (bryce.daniel@umconnect.umt.edu)'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        filings = data.get('filings', {}).get('recent', {})
        if filings:
            today = datetime.now().strftime('%Y-%m-%d')
            for i in range(len(filings['filingDate'])):
                if filings['filingDate'][i] == today:
                    return {
                        'form': filings['form'][i],
                        'date': filings['filingDate'][i],
                        'accession_number': filings['accessionNumber'][i]
                    }
    return None

# Function to overwrite data in BigQuery
def overwrite_bigquery_table(data):
    client = bigquery.Client()
    table_id = "trendsense.SEC_data.SEC_Filings"

    # Overwrite the table by using the WRITE_TRUNCATE disposition
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("Ticker", "STRING"),
            bigquery.SchemaField("CIK", "STRING"),
            bigquery.SchemaField("Form_Type", "STRING"),
            bigquery.SchemaField("Filing_Date", "DATE"),
            bigquery.SchemaField("Filing_URL", "STRING"),
        ]
    )

    # Load data into BigQuery
    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

# Main entry point for Cloud Function
def main(request: Request):
    """
    HTTP-triggered Cloud Function entry point.

    Args:
        request (flask.Request): The HTTP request object.
    """
    # Load the ticker to CIK mapping
    ticker_cik_mapping = load_ticker_cik_mapping()

    # Prepare data for BigQuery
    rows_to_insert = []
    for ticker in tickers:
        cik = ticker_cik_mapping.get(ticker.upper())
        if cik:
            latest_filing = fetch_filings_for_today(cik)
            if latest_filing:
                filing_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{latest_filing['accession_number'].replace('-', '')}/{latest_filing['accession_number']}-index.html"
                rows_to_insert.append({
                    "Ticker": ticker,
                    "CIK": cik,
                    "Form_Type": latest_filing['form'],
                    "Filing_Date": latest_filing['date'],
                    "Filing_URL": filing_url
                })

    # Overwrite data in BigQuery
    if rows_to_insert:
        overwrite_bigquery_table(rows_to_insert)

    # Return an HTTP response
    return "SEC Filings for today processed and uploaded to BigQuery", 200


