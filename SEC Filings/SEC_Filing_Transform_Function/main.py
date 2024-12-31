from google.cloud import bigquery
from datetime import datetime

from datetime import date, datetime  # Ensure both date and datetime are imported

def clean_data(data):
    """
    Cleans the data by removing rows with blank cells and formatting the date fields.

    Args:
        data (list of dict): List of rows to clean.

    Returns:
        list of dict: Cleaned data.
    """
    cleaned_data = []
    for row in data:
        # Remove rows with any blank cells
        if any(value is None or value == "" for value in row.values()):
            continue

        # Ensure Filing_Date is in YYYY-MM-DD format
        if isinstance(row["Filing_Date"], date):  # Use `date` from `datetime`
            # Convert datetime.date to string in ISO format
            row["Filing_Date"] = row["Filing_Date"].isoformat()
        else:
            try:
                # Parse Filing_Date if it is not a datetime.date
                row["Filing_Date"] = datetime.strptime(row["Filing_Date"], "%Y-%m-%d").date().isoformat()
            except ValueError:
                continue  # Skip rows with invalid dates

        cleaned_data.append(row)

    return cleaned_data





def move_data_to_history(request):
    """
    Cloud Function to move data from SEC_Filings to SEC_Filing_History.

    Args:
        request (flask.Request): The HTTP request object.

    Returns:
        flask.Response: HTTP response indicating the result of the operation.
    """
    # Configuration
    project_id = "trendsense"
    dataset_id = "SEC_data"
    source_table = "SEC_Filings"
    target_table = "SEC_Filing_History"

    client = bigquery.Client(project=project_id)

    try:
        print("Starting the function...")

        # Query to fetch data from the source table
        source_table_id = f"{project_id}.{dataset_id}.{source_table}"
        print(f"Fetching data from source table: {source_table_id}")
        query = f"SELECT * FROM `{source_table_id}`"
        query_job = client.query(query)
        source_data = [dict(row) for row in query_job.result()]
        print(f"Source data fetched: {len(source_data)} rows")

        # Clean the data
        print("Cleaning the data...")
        cleaned_data = clean_data(source_data)
        print(f"Cleaned data: {len(cleaned_data)} rows after cleaning")

        # Identify rows not already in the target table
        target_table_id = f"{project_id}.{dataset_id}.{target_table}"
        non_duplicate_data = []
        for row in cleaned_data:
            print(f"Checking for duplicates for row: {row}")
            check_query = f"""
            SELECT COUNT(*) as count
            FROM `{target_table_id}`
            WHERE Filing_Date = @filing_date AND Form_Type = @form_type
            """
            query_job = client.query(
                check_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("filing_date", "DATE", row["Filing_Date"]),
                        bigquery.ScalarQueryParameter("form_type", "STRING", row["Form_Type"]),
                    ]
                ),
            )
            count = [r["count"] for r in query_job.result()][0]
            if count == 0:  # If not a duplicate
                print("Row is unique, adding to non-duplicate data")
                non_duplicate_data.append(row)

        print(f"Non-duplicate data: {len(non_duplicate_data)} rows to insert")

        # Insert non-duplicate data into the target table
        if non_duplicate_data:
            print("Inserting non-duplicate rows into target table...")
            errors = client.insert_rows_json(target_table_id, non_duplicate_data)
            if errors:
                print("Errors during insertion:", errors)
                raise RuntimeError(f"Failed to insert rows into {target_table}: {errors}")
            print("Insertion successful")
        else:
            print("No new rows to insert into the history table.")

        return "Data successfully moved to SEC_Filing_History.", 200

    except Exception as e:
        print("An error occurred:", str(e))
        return f"An error occurred: {str(e)}", 500


