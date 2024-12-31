from google.cloud import bigquery

# Define BigQuery dataset and table
PROJECT_ID = "trendsense"
DATASET_ID = "stock_data"
TABLE_ID = "current_stock_data"

def clear_stock_data(request):
    """Cloud Function to clear all data from the BigQuery table."""
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Delete all rows in the table
    query = f"DELETE FROM `{table_ref}` WHERE TRUE"
    client.query(query).result()  # Execute the query

    return f"Data cleared from {table_ref}."
