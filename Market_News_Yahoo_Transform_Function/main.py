from google.cloud import bigquery
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def copy_to_history_table_with_autodetect(project_id, dataset_id, source_table_id, target_table_id):
    """
    Copies data from the source table to the target historical table,
    automatically detecting the schema if the target table does not exist.
    Ensures no duplicates based on the publish_date column.
    """
    client = bigquery.Client()

    source_table = f"{project_id}.{dataset_id}.{source_table_id}"
    target_table = f"{project_id}.{dataset_id}.{target_table_id}"

    # Check if target table exists
    try:
        client.get_table(target_table)  # Raises NotFound if the table doesn't exist
        logger.info(f"Target table {target_table} exists.")
    except Exception as e:
        logger.info(f"Target table {target_table} does not exist. Creating it with autodetect schema...")
        # Use a query to copy and create the table with autodetected schema
        create_query = f"""
        CREATE TABLE `{target_table}`
        AS
        SELECT *
        FROM `{source_table}`
        WHERE FALSE
        """
        query_job = client.query(create_query)
        query_job.result()  # Wait for the table to be created
        logger.info(f"Table {target_table} created with autodetected schema.")

    # Insert data while avoiding duplicates based on publish_date
    insert_query = f"""
    INSERT INTO `{target_table}`
    SELECT *
    FROM `{source_table}`
    WHERE publish_date NOT IN (
        SELECT publish_date
        FROM `{target_table}`
    )
    """

    try:
        logger.info(f"Starting copy from {source_table} to {target_table} without duplicates...")
        query_job = client.query(insert_query)
        query_job.result()  # Wait for the query to complete
        logger.info(f"Data copied successfully to {target_table}.")
        return f"Data copied to {target_table} without duplicates.", 200
    except Exception as e:
        logger.error(f"Error copying data: {e}")
        return f"Error copying data: {e}", 500

# Example usage in a Cloud Function
def copy_market_news_with_autodetect(request):
    """
    Cloud Function entry point for copying data to historical table with auto schema detection.
    """
    # Configuration
    project_id = "trendsense"
    dataset_id = "market_data"
    source_table_id = "News_Yahoo_Extract"
    target_table_id = "Market_News_History"

    return copy_to_history_table_with_autodetect(project_id, dataset_id, source_table_id, target_table_id)

