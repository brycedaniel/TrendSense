import os
import requests
import functions_framework
from google.cloud import bigquery
from datetime import datetime

# Configuration
API_KEY = os.environ.get('FMP_API_KEY')  # Store API key in environment variables
PROJECT_ID = 'trendsense'
DATASET_ID = 'stockdata'
TABLE_ID = 'stock_data_ratings'

# List of stock symbols
SYMBOLS = [
    'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
    'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
    'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS', 'META'
]

def get_company_rating(symbol, api_key):
    """
    Fetch company rating for a given stock symbol from Financial Modeling Prep API
    """
    url = f"https://financialmodelingprep.com/api/v3/rating/{symbol}?apikey={api_key}"
    
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            if data:
                # Extract the first result
                rating_data = data[0]
                return {
                    'symbol': symbol,
                    'fetch_timestamp': datetime.utcnow().isoformat(),
                    'date': rating_data.get('date', 'No date available'),
                    'overall_rating': rating_data.get('rating'),
                    'recommendation': rating_data.get('ratingRecommendation'),
                    'rating_score': rating_data.get('ratingScore'),
                    'dcf_score': rating_data.get('ratingDetailsDCFScore'),
                    'dcf_recommendation': rating_data.get('ratingDetailsDCFRecommendation'),
                    'roe_score': rating_data.get('ratingDetailsROEScore'),
                    'roe_recommendation': rating_data.get('ratingDetailsROERecommendation'),
                    'roa_score': rating_data.get('ratingDetailsROAScore'),
                    'roa_recommendation': rating_data.get('ratingDetailsROARecommendation'),
                    'pe_score': rating_data.get('ratingDetailsPEScore'),
                    'pe_recommendation': rating_data.get('ratingDetailsPERecommendation'),
                    'pb_score': rating_data.get('ratingDetailsPBScore'),
                    'pb_recommendation': rating_data.get('ratingDetailsPBRecommendation')
                }
            else:
                print(f"No company rating data available for {symbol}.")
                return None
        else:
            print(f"Failed to fetch data for {symbol}. HTTP Status Code: {response.status_code}")
            return None
    
    except Exception as e:
        print(f"An error occurred while fetching data for {symbol}: {e}")
        return None

@functions_framework.http
def fetch_and_store_stock_ratings(request):
    """
    Google Cloud Function to fetch stock ratings and store in BigQuery
    """
    # Validate the API key
    if not API_KEY:
        return ('API key is not configured', 500)

    # Initialize BigQuery client
    client = bigquery.Client(project=PROJECT_ID)
    
    # Prepare the table reference
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    table = client.get_table(table_ref)

    # Collect ratings
    ratings_to_insert = []
    error_symbols = []

    # Fetch ratings for each symbol
    for symbol in SYMBOLS:
        try:
            rating = get_company_rating(symbol, API_KEY)
            
            if rating:
                ratings_to_insert.append(rating)
            else:
                error_symbols.append(symbol)
        
        except Exception as e:
            print(f"Error processing {symbol}: {e}")
            error_symbols.append(symbol)

    # Insert rows into BigQuery
    if ratings_to_insert:
        errors = client.insert_rows(table, ratings_to_insert)
        
        if errors:
            print(f"Errors inserting rows: {errors}")
            return (f'Partial failure. Errors inserting rows. Symbols with errors: {error_symbols}', 206)
        else:
            print(f"Successfully inserted {len(ratings_to_insert)} rows")
            return (f'Successfully processed {len(ratings_to_insert)} stocks', 200)
    else:
        return ('No ratings could be retrieved', 404)

# For local testing
if __name__ == "__main__":
    # This block allows for local testing outside of Google Cloud Functions
    import json
    
    # Simulate the function call
    class MockRequest:
        pass
    
    request = MockRequest()
    result, status = fetch_and_store_stock_ratings(request)
    print(f"Status: {status}")
    print(f"Result: {result}")