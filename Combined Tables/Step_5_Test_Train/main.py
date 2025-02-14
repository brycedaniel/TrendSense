from google.cloud import bigquery
import pandas as pd

def process_data(request):
    client = bigquery.Client()

    # Define source and target tables
    source_table = "trendsense.combined_data.step_3_predictive_1"
    target_table = "trendsense.combined_data.step_4_test_train"

    # Load data from source table
    query = f"SELECT * FROM `{source_table}`"
    df3 = client.query(query).to_dataframe()

    # Drop rows with missing values
    df4 = df3.dropna()

    # Drop rows where any column contains a zero
    df4 = df4[(df4 != 0).all(axis=1)]

    # Define schema based on the processed DataFrame
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite if table exists
        autodetect=True,  # Automatically detect schema from DataFrame
    )

    # Load the processed data to the target table
    job = client.load_table_from_dataframe(df4, target_table, job_config=job_config)
    job.result()  # Wait for the job to complete

    return f"Created {target_table} with {len(df4)} fully populated rows (excluding NaNs and zeros)."
