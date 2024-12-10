# Alpha extract good


import logging
from typing import Tuple, Optional
import pytz
from datetime import datetime
from google.cloud import bigquery
from textblob import TextBlob
import pandas as pd


class NewsDataProcessor:
    """
    A class to process news data, perform sentiment analysis, 
    and manage BigQuery table operations.
    """

    def __init__(self, project_id: str, dataset_id: str):
        """
        Initialize the NewsDataProcessor with project and dataset details.

        Args:
            project_id (str): Google Cloud project ID
            dataset_id (str): BigQuery dataset ID
        """
        self.client = bigquery.Client()
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        # Define schema for BigQuery table
        self.SCHEMA = [
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

    @staticmethod
    def calculate_sentiment(text: Optional[str]) -> float:
        """
        Calculate sentiment polarity using TextBlob.
        """
        if not text or not isinstance(text, str):
            return 0.0
        
        try:
            blob = TextBlob(text)
            return blob.sentiment.polarity
        except Exception as e:
            logging.error(f"Sentiment analysis failed: {e}")
            return 0.0

    @staticmethod
    def extract_first_ticker(ticker: Optional[str]) -> str:
        """
        Extract the first ticker when multiple tickers are present.
        """
        if not ticker or not isinstance(ticker, str):
            return ''
        
        # Split by comma and strip whitespace
        tickers = [t.strip() for t in ticker.split(',')]
        
        # Return the first non-empty ticker
        return tickers[0] if tickers else ''

    @staticmethod
    def assess_language_reliability(summary: Optional[str]) -> float:
        """
        Assign a numerical reliability score based on sentiment polarity.
        """
        polarity = NewsDataProcessor.calculate_sentiment(summary)
        return max(0, 1 - abs(polarity))

    @staticmethod
    def calculate_lexical_diversity(text: Optional[str]) -> float:
        """
        Calculate lexical diversity as the ratio of unique words to total words.
        """
        if not text or not isinstance(text, str):
            return 0.0
        
        try:
            words = text.split()
            unique_words = set(words)
            return len(unique_words) / len(words) if words else 0.0
        except Exception as e:
            logging.error(f"Lexical diversity calculation failed: {e}")
            return 0.0

    def create_table_if_not_exists(self, table_id: str) -> None:
        """
        Create the target table with an explicitly defined schema if it does not exist.
        """
        try:
            self.client.get_table(table_id)
            self.logger.info(f"Target table {table_id} exists.")
        except Exception:
            self.logger.info(f"Target table {table_id} does not exist. Creating it...")
            table = bigquery.Table(table_id, schema=self.SCHEMA)
            self.client.create_table(table)
            self.logger.info(f"Table {table_id} created successfully with defined schema.")

    def filter_existing_data(self, new_data: pd.DataFrame, target_table: str) -> pd.DataFrame:
        """
        Filter out rows that already exist in the target table based on publish_date.
        
        Args:
            new_data (pd.DataFrame): Incoming new data
            target_table (str): Full target table reference
        
        Returns:
            pd.DataFrame: Filtered dataframe with only new rows
        """
        # If no data, return empty dataframe
        if new_data.empty:
            return new_data

        # Construct query to find existing publish dates
        existing_dates_query = f"""
        SELECT DISTINCT publish_date 
        FROM `{target_table}`
        WHERE publish_date IN UNNEST(@publish_dates)
        """
        
        # Prepare job config with parameter
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter('publish_dates', 'STRING', new_data['publish_date'].tolist())
            ]
        )

        # Run query to get existing dates
        query_job = self.client.query(existing_dates_query, job_config=job_config)
        existing_dates = [row['publish_date'] for row in query_job]

        # Filter out existing dates
        filtered_data = new_data[~new_data['publish_date'].isin(existing_dates)]
        
        self.logger.info(f"Total new rows: {len(filtered_data)} (out of {len(new_data)} original rows)")
        return filtered_data

    def process_news_data(
        self, 
        source_table_id: str, 
        target_table_id: str
    ) -> Tuple[str, int]:
        """
        Process news data: clean, add sentiment and reliability metrics, 
        and load into target table, avoiding duplicates.
        """
        # Construct full table references
        source_table = f"{self.project_id}.{self.dataset_id}.{source_table_id}"
        target_table = f"{self.project_id}.{self.dataset_id}.{target_table_id}"

        try:
            # Extract data from the source table
            query = f"SELECT * FROM `{source_table}`"
            new_data = self.client.query(query).to_dataframe()

            if new_data.empty:
                self.logger.info("No new data to process.")
                return "No new data to process.", 200
                # Format 'publish_date' to 'yyyymmdd'
          # Convert 'publish_date' to datetime, reformat date to 'yyyy-dd-mm' while retaining the time
            new_data['publish_date'] = pd.to_datetime(new_data['publish_date'], errors='coerce')  # Ensure valid datetime format
            new_data = new_data.dropna(subset=['publish_date'])  # Drop rows with invalid dates

            # Format the datetime: date as 'yyyy-dd-mm' and retain time
            new_data['publish_date'] = new_data['publish_date'].apply(lambda x: f"{x.strftime('%Y-%m-%d')} {x.strftime('%H:%M:%S')}")



       

            # Extract first ticker
            new_data['ticker'] = new_data['ticker'].apply(self.extract_first_ticker)

            # Ensure target table exists
            self.create_table_if_not_exists(target_table)

            # Filter out existing rows
            new_data = self.filter_existing_data(new_data, target_table)

            # Remove rows with blank or null 'ticker'
            new_data = new_data.dropna(subset=['ticker'])
            new_data = new_data[new_data['ticker'].str.strip() != '']

            if new_data.empty:
                self.logger.info("No new unique rows to process.")
                return "No new unique rows to process.", 200

            # Add calculated fields
            new_data['summary_sentiment'] = new_data['summary'].apply(self.calculate_sentiment)
            new_data['title_sentiment'] = new_data['title'].apply(self.calculate_sentiment)
            new_data['reliability_score'] = new_data['summary'].apply(self.assess_language_reliability)
            new_data['lexical_diversity'] = new_data['summary'].apply(self.calculate_lexical_diversity)

            # Load data into target table
            self.logger.info("Loading data into BigQuery...")
            job = self.client.load_table_from_dataframe(new_data, target_table)
            job.result()  # Wait for job completion

            success_msg = f"Updated table saved successfully to {target_table}. Rows added: {len(new_data)}"
            self.logger.info(success_msg)
            return success_msg, 200

        except Exception as e:
            error_msg = f"Error processing news data: {str(e)}"
            self.logger.error(error_msg)
            return error_msg, 500


def update_alpha_news(request):
    """
    Cloud Function entry point for updating the News_Alpha_Extract table
    and saving to Market_News_History_New.
    """
    # Configuration
    project_id = "trendsense"
    dataset_id = "market_data"
    source_table_id = "News_Alpha_Extract"
    target_table_id = "Market_News_History_New"

    # Create processor and process data
    processor = NewsDataProcessor(project_id, dataset_id)
    return processor.process_news_data(source_table_id, target_table_id)




