import requests
import time
from flask import jsonify
from google.cloud import bigquery

# Your FMP API Key
API_KEY = "DKhbgwU29WSYBQlGkdkYjAomzvDQRVE0"

# List of stock symbols
SYMBOLS = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META'
]

# BigQuery table details
PROJECT_ID = "trendsense"  # Replace with your GCP project ID
DATASET_ID = "stock_data"
TABLE_ID = "stock_ratings"


def get_company_rating(symbol):
    """
    Fetch company rating for a given stock symbol from Financial Modeling Prep API
    """
    url = f"https://financialmodelingprep.com/api/v3/rating/{symbol}?apikey={API_KEY}"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if data:
                rating_data = data[0]
                return {
                    'Symbol': symbol,
                    'Date': rating_data.get('date', 'No date available'),
                    'OverallRating': rating_data.get('rating'),
                    'Recommendation': rating_data.get('ratingRecommendation'),
                    'RatingScore': rating_data.get('ratingScore'),
                    'DCFScore': rating_data.get('ratingDetailsDCFScore'),
                    'DCFRecommendation': rating_data.get('ratingDetailsDCFRecommendation'),
                    'ROEScore': rating_data.get('ratingDetailsROEScore'),
                    'ROERecommendation': rating_data.get('ratingDetailsROERecommendation'),
                    'ROAScore': rating_data.get('ratingDetailsROAScore'),
                    'ROARecommendation': rating_data.get('ratingDetailsROARecommendation'),
                    'PEScore': rating_data.get('ratingDetailsPEScore'),
                    'PERecommendation': rating_data.get('ratingDetailsPERecommendation'),
                    'PBScore': rating_data.get('ratingDetailsPBScore'),
                    'PBRecommendation': rating_data.get('ratingDetailsPBRecommendation')
                }
        return None
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


def fetch_ratings():
    """
    Fetch ratings for all symbols and return as a list
    """
    all_ratings = []
    for symbol in SYMBOLS:
        print(f"Fetching rating for {symbol}...")
        rating = get_company_rating(symbol)
        if rating:
            all_ratings.append(rating)
        time.sleep(0.5)  # Pause to avoid hitting API rate limits
    return all_ratings


def create_table_if_not_exists(client, dataset_id, table_id):
    """
    Create a BigQuery table if it does not exist
    """
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        client.get_table(table_ref)  # Check if table exists
        print(f"Table {table_id} already exists.")
    except Exception:
        print(f"Table {table_id} does not exist. Creating it...")
        dataset = bigquery.Dataset(dataset_ref)
        client.create_dataset(dataset, exists_ok=True)  # Ensure dataset exists
        
        # Create an empty table
        schema = []  # Auto-detect schema when loading data
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created successfully.")


def insert_into_bigquery(data):
    """
    Insert data into BigQuery table, creating it if necessary
    """
    client = bigquery.Client(project=PROJECT_ID)
    create_table_if_not_exists(client, DATASET_ID, TABLE_ID)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,  # Automatically detect schema
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append data to table
    )
    
    try:
        load_job = client.load_table_from_json(data, table_ref, job_config=job_config)
        load_job.result()  # Wait for job to complete
        print(f"Successfully inserted {len(data)} rows into BigQuery.")
    except Exception as e:
        print(f"Error while inserting data into BigQuery: {e}")


def main(request):
    """
    Entry point for the Google Cloud Function
    """
    # Fetch ratings
    ratings = fetch_ratings()
    
    if ratings:
        try:
            # Insert ratings into BigQuery
            insert_into_bigquery(ratings)
            return jsonify({
                "message": f"Successfully stored ratings for {len(ratings)} stocks in BigQuery.",
                "status": "success"
            })
        except Exception as e:
            return jsonify({
                "message": "Failed to store data in BigQuery.",
                "error": str(e),
                "status": "error"
            })
    else:
        return jsonify({
            "message": "No ratings were retrieved.",
            "status": "error"
        })

