from google.cloud import bigquery
import pandas as pd

def process_data(request):
    client = bigquery.Client()

    # Define source and target tables
    source_table = "trendsense.combined_data.step_4_final"
    target_table = "trendsense.combined_data.step_4_test_train"

    # Load data from source table
    query = f"SELECT * FROM `{source_table}`"
    df3 = client.query(query).to_dataframe()

    # Remove rows where Avg_Aggregated_Score is NaN or 0
    df3 = df3[df3["Avg_Aggregated_Score"].notna() & (df3["Avg_Aggregated_Score"] != 0)]

    # Define schema based on the processed DataFrame
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite if table exists
        autodetect=True,  # Automatically detect schema from DataFrame
    )

    # Load the processed data to the target table
    job = client.load_table_from_dataframe(df3, target_table, job_config=job_config)
    job.result()  # Wait for the job to complete

    return f"Created {target_table} with {len(df3)} rows after removing NaN and zero Avg_Aggregated_Score values."

