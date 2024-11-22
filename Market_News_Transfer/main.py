from google.cloud import bigquery

def transfer_market_news_to_hist(request):
    """
    Google Cloud Function to transfer data from market_news_yahoo to market_news_yahoo_hist.

    Parameters:
        request (flask.Request): The HTTP request object (not used in this implementation).
    """
    project_id = "trendsense"
    dataset_id = "market_data"
    source_table_id = "market_news_yahoo"
    destination_table_id = "market_news_yahoo_hist"

    client = bigquery.Client(project=project_id)

    source_table = f"{project_id}.{dataset_id}.{source_table_id}"
    destination_table = f"{project_id}.{dataset_id}.{destination_table_id}"

    try:
        # Step 1: Query data from source table
        print(f"Querying data from {source_table}...")
        query = f"SELECT * FROM `{source_table}`"
        source_data = client.query(query).to_dataframe()

        if source_data.empty:
            print("No data found in the source table.")
            return "No data found.", 204

        print(f"Fetched {len(source_data)} rows from {source_table}.")

        # Step 2: Insert data into destination table
        print(f"Inserting data into {destination_table}...")
        job = client.load_table_from_dataframe(
            source_data,
            destination_table,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        job.result()  # Wait for the job to complete
        print(f"Successfully inserted {len(source_data)} rows into {destination_table}.")

        # Step 3: Clear transferred data from source table
        print("Deleting transferred data from the source table...")
        delete_query = f"DELETE FROM `{source_table}` WHERE TRUE"
        client.query(delete_query).result()
        print(f"Cleared data from {source_table}.")

        return "Data successfully transferred.", 200

    except Exception as e:
        print(f"Error transferring data: {str(e)}")
        return f"Error transferring data: {str(e)}", 500
