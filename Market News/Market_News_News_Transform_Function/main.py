from google.cloud import bigquery
import pandas as pd
import logging
from typing import Optional

# Class to process news data with transformations and sentiment analysis
class NewsDataProcessor:
    # Define the schema for the target BigQuery table
    SCHEMA = [
        bigquery.SchemaField("ticker", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("summary", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("publisher", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("link", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("publish_date", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("related_tickers", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("overall_sentiment_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("overall_sentiment_label", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("summary_sentiment", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("title_sentiment", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("reliability_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("lexical_diversity", "FLOAT", mode="NULLABLE"),
    ]
    # Schema defines the structure of the target BigQuery table, ensuring consistency.

    def __init__(self):
        self.client = bigquery.Client()
        self.logger = logging.getLogger(__name__)
    # Initialize the BigQuery client and logger for database operations and tracking.

    @staticmethod
    def calculate_sentiment(text: Optional[str]) -> float:
        from textblob import TextBlob

        if not text or not isinstance(text, str):
            return 0.0
        return TextBlob(text).sentiment.polarity
    # Calculate sentiment polarity using TextBlob to gauge the positivity/negativity of text content.

    @staticmethod
    def assess_language_reliability(summary: Optional[str]) -> float:
        polarity = NewsDataProcessor.calculate_sentiment(summary)
        return max(0, 1 - abs(polarity))
    # Assess language reliability by calculating sentiment neutrality (closer to 0 is more neutral/reliable).

    @staticmethod
    def calculate_lexical_diversity(text: Optional[str]) -> float:
        if not text or not isinstance(text, str):
            return 0.0

        try:
            words = text.split()
            unique_words = set(words)
            return len(unique_words) / len(words) if words else 0.0
        except Exception as e:
            logging.error(f"Lexical diversity calculation failed: {e}")
            return 0.0
    # Calculate lexical diversity to measure the richness of the vocabulary in a text.

    def create_table_if_not_exists(self, table_id: str) -> None:
        try:
            self.client.get_table(table_id)
            self.logger.info(f"Target table {table_id} exists.")
        except Exception:
            self.logger.info(f"Target table {table_id} does not exist. Creating it...")
            table = bigquery.Table(table_id, schema=self.SCHEMA)
            self.client.create_table(table)
            self.logger.info(f"Table {table_id} created successfully with defined schema.")
    # Check if the target table exists in BigQuery. If not, create it using the defined schema.

    def filter_existing_data(self, new_data: pd.DataFrame, target_table: str) -> pd.DataFrame:
        """
        Filter out rows that already exist in the target table based on publish_date.

        Args:
            new_data (pd.DataFrame): Incoming new data.
            target_table (str): Full target table reference.

        Returns:
            pd.DataFrame: Filtered dataframe with only new rows.
        """
        if new_data.empty:
            return new_data

        # Ensure publish_date is in string format
        new_data['publish_date'] = new_data['publish_date'].astype(str)

        # Extract project and dataset from target_table
        project, dataset, _ = target_table.split('.')

        # Create a temporary table of publish_date values
        temp_table_name = "temp_publish_dates"
        temp_table_id = f"{project}.{dataset}.{temp_table_name}"
        temp_table = bigquery.Table(temp_table_id)
        temp_table.schema = [
            bigquery.SchemaField("publish_date", "STRING", mode="REQUIRED"),
        ]
        try:
            self.client.delete_table(temp_table_id, not_found_ok=True)  # Ensure the table doesn't already exist
            self.client.create_table(temp_table)
        except Exception as e:
            self.logger.error(f"Failed to create temporary table {temp_table_id}: {e}")
            raise

        # Insert new publish_date values into the temporary table
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        self.client.load_table_from_dataframe(
            new_data[['publish_date']], temp_table_id, job_config=job_config
        ).result()

        # Query existing publish_date values
        existing_dates_query = f"""
        SELECT publish_date
        FROM `{target_table}`
        WHERE publish_date IN (SELECT publish_date FROM `{temp_table_id}`)
        """
        existing_dates = [row['publish_date'] for row in self.client.query(existing_dates_query)]

        # Drop the temporary table
        self.client.delete_table(temp_table_id, not_found_ok=True)

        # Filter out rows with existing publish dates
        filtered_data = new_data[~new_data['publish_date'].isin(existing_dates)]
        self.logger.info(f"Total new rows: {len(filtered_data)} (out of {len(new_data)} original rows)")

        return filtered_data
    # Filters out data that already exists in the target table by comparing `publish_date`.

    def transform_and_load_data(self, source_table: str, target_table: str):
        """
        Transform the data from the source table and load it into the target table.
        """
        query = f"""
            SELECT 
                ticker,
                title,
                summary,
                publisher,
                link,
                publish_date,
                summary_textblob_sentiment AS summary_sentiment,
                source
            FROM `{source_table}`
        """
        source_data = self.client.query(query).to_dataframe()

        # Format publish_date to match the target table's expected format
        source_data['publish_date'] = pd.to_datetime(source_data['publish_date']).dt.strftime('%Y-%m-%d %H:%M:%S')

        # Add new columns
        source_data['type'] = "Story"
        source_data['related_tickers'] = source_data['ticker']
        source_data['overall_sentiment_score'] = None
        source_data['overall_sentiment_label'] = None
        source_data['title_sentiment'] = None

        # Calculate reliability_score and lexical_diversity
        source_data['reliability_score'] = source_data['summary'].apply(self.assess_language_reliability)
        source_data['lexical_diversity'] = source_data['summary'].apply(self.calculate_lexical_diversity)

        # Ensure target table exists
        self.create_table_if_not_exists(target_table)

        # Filter out existing data
        filtered_data = self.filter_existing_data(source_data, target_table)

        # Load transformed data into the target table
        if not filtered_data.empty:
            job = self.client.load_table_from_dataframe(
                filtered_data,
                target_table,
                job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
            )
            job.result()
            self.logger.info(f"Data successfully loaded into {target_table}")
        else:
            self.logger.info("No new rows to load.")
    # Transforms source data, performs calculations, filters duplicates, and loads the result into the target table.

# Entry-point function for Google Cloud Functions
def transform_and_load_data(request):
    processor = NewsDataProcessor()
    source_table = "trendsense.market_data.News_News_Extract"
    target_table = "trendsense.market_data.Market_News_NAPI_Temp"
    processor.transform_and_load_data(source_table, target_table)
    return "Data transformation and load completed successfully."
# The entry point triggers the transformation and loading process, making it compatible with Google Cloud Functions.

