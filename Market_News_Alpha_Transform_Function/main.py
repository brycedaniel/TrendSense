from google.cloud import bigquery
import logging

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def copy_to_history_table(project_id, dataset_id, source_table_id, target_table_id):
    """
    Copies data from the source table to the target historical table,
    ensuring no duplicates based on the publish_date column.
    """
    client = bigquery.Client()

    source_table = f"{project_id}.{dataset_id}.{source_table_id}"
    target_table = f"{project_id}.{dataset_id}.{target_table_id}"

    # Insert data while avoiding duplicates
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
def copy_alpha_news(request):
    """
    Cloud Function entry point for copying data from News_Alpha_Extract to Market_News_History.
    """
    # Configuration
    project_id = "trendsense"
    dataset_id = "market_data"
    source_table_id = "News_Alpha_Extract"
    target_table_id = "Market_News_History"

    return copy_to_history_table(project_id, dataset_id, source_table_id, target_table_id)
