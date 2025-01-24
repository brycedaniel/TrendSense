import os
import logging
from typing import Optional, Tuple, Dict, Any
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

class NewsDataProcessorError(Exception):
    """Custom exception for NewsDataProcessor errors."""
    pass

class NewsDataProcessor:
    def __init__(self, project_id: str, dataset_id: str, logger: Optional[logging.Logger] = None):
        # Validate inputs and set up logging
        self._validate_input_parameters(project_id, dataset_id)
        self.logger = logger or self._setup_logger()

        # Initialize BigQuery client
        try:
            self.client = bigquery.Client(project=project_id)
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {e}")
            raise NewsDataProcessorError(f"BigQuery client initialization failed: {e}")

        # Store project and dataset IDs
        self.project_id = project_id
        self.dataset_id = dataset_id

        # Initialize VADER sentiment analyzer
        self.vader_analyzer = SentimentIntensityAnalyzer()

    def _validate_input_parameters(self, project_id: str, dataset_id: str):
        """Ensure the provided project and dataset IDs are valid strings."""
        if not project_id or not isinstance(project_id, str):
            raise NewsDataProcessorError("Invalid project_id. Must be a non-empty string.")
        if not dataset_id or not isinstance(dataset_id, str):
            raise NewsDataProcessorError("Invalid dataset_id. Must be a non-empty string.")

    def _setup_logger(self) -> logging.Logger:
        """Set up a logger for debugging and tracking operations."""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)  # Set to DEBUG for detailed logs
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    def calculate_vader_sentiment(self, text: Optional[str]) -> float:
        """Calculate sentiment using VADER sentiment analyzer."""
        if not text or not isinstance(text, str):
            return 0.0
        try:
            sentiment = self.vader_analyzer.polarity_scores(text)
            return sentiment.get("compound", 0.0)  # Return compound sentiment score
        except Exception as e:
            self.logger.warning(f"VADER sentiment analysis failed: {e}")
            return 0.0

    def calculate_textblob_sentiment(self, text: Optional[str]) -> float:
        """Calculate sentiment using TextBlob."""
        if not text or not isinstance(text, str):
            return 0.0
        try:
            return TextBlob(text).sentiment.polarity
        except Exception as e:
            self.logger.warning(f"TextBlob sentiment analysis failed: {e}")
            return 0.0

    def ensure_table_exists(self, table_id: str):
        """Ensure the target BigQuery table exists with the required schema."""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_id}"
        try:
            self.client.get_table(table_ref)
            self.logger.info(f"Table {table_ref} already exists.")
        except Exception:
            self.logger.info(f"Table {table_ref} does not exist. Creating it...")
            schema = [
                bigquery.SchemaField("ticker", "STRING"),
                bigquery.SchemaField("title", "STRING"),
                bigquery.SchemaField("summary", "STRING"),
                bigquery.SchemaField("publisher", "STRING"),
                bigquery.SchemaField("link", "STRING"),
                bigquery.SchemaField("publish_date", "DATETIME"),
                bigquery.SchemaField("type", "STRING"),
                bigquery.SchemaField("related_tickers", "STRING"),
                bigquery.SchemaField("source", "STRING"),
                bigquery.SchemaField("lexical_diversity", "FLOAT"),
                bigquery.SchemaField("reliability_score", "FLOAT"),
                bigquery.SchemaField("textblob_sentiment", "FLOAT"),
                bigquery.SchemaField("vader_sentiment", "FLOAT"),
                bigquery.SchemaField("bert_sentiment", "FLOAT"),
                bigquery.SchemaField("bert_confidence", "FLOAT"),
                bigquery.SchemaField("word_count", "INTEGER"),
                bigquery.SchemaField("headline_sentiment", "FLOAT"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            try:
                self.client.create_table(table)
                self.logger.info(f"Table {table_ref} created successfully.")
            except Exception as e:
                self.logger.error(f"Failed to create table {table_ref}: {e}")
                raise NewsDataProcessorError(f"Table creation failed: {e}")
    def process_and_replace_data(self, source_table_id: str, target_table_id: str, batch_size: int = 1000) -> Dict[str, Any]:
        """Process data from the source table and replace data in the target table."""
        source_table = f"{self.project_id}.{self.dataset_id}.{source_table_id}"
        target_table = f"{self.project_id}.{self.dataset_id}.{target_table_id}"

        try:
            # Query data from the source table
            query = f"""
            SELECT 
                ticker, title, summary, publisher, link, publish_date, type, 
                related_tickers, source, lexical_diversity, reliability_score
            FROM `{source_table}`
            """
            new_data = self.client.query(query).to_dataframe()

            if new_data.empty:
                return {"status": "success", "message": "No new data to process", "rows_processed": 0}

            # Convert publish_date from STRING to DATETIME
            if 'publish_date' in new_data.columns:
                new_data['publish_date'] = pd.to_datetime(new_data['publish_date'], errors='coerce')
            
            # Handle any invalid dates (optional: log or filter out invalid rows)
            invalid_dates = new_data['publish_date'].isnull().sum()
            if invalid_dates > 0:
                self.logger.warning(f"{invalid_dates} rows have invalid publish_date and will be removed.")
                new_data = new_data.dropna(subset=['publish_date'])

            # Perform transformations and calculations
            new_data["word_count"] = new_data["summary"].apply(lambda x: len(str(x).split()))  # Calculate word count
            new_data["headline_sentiment"] = new_data["title"].apply(self.calculate_vader_sentiment)  # Analyze title sentiment
            new_data["vader_sentiment"] = new_data["summary"].apply(self.calculate_vader_sentiment)  # Analyze summary sentiment
            new_data["textblob_sentiment"] = new_data["summary"].apply(self.calculate_textblob_sentiment)  # Analyze summary sentiment using TextBlob

            # Placeholder for BERT sentiment
            new_data["bert_sentiment"] = 0.0
            new_data["bert_confidence"] = 0.0

            # Replace the target table with the new data
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            self.client.load_table_from_dataframe(new_data, target_table, job_config=job_config).result()

            success_msg = f"Data successfully replaced in {target_table}. Rows added: {len(new_data)}"
            self.logger.info(success_msg)

            return {"status": "success", "message": success_msg, "rows_processed": len(new_data)}

        except Exception as e:
            error_msg = f"Error processing data: {e}"
            self.logger.error(error_msg)
            return {"status": "error", "message": error_msg, "rows_processed": 0}


def move_market_news_data(request):
    """
    Google Cloud Function entry point to process and replace market news data.
    """
    # Load configuration from environment variables
    project_id = os.getenv('GCP_PROJECT_ID', 'trendsense')
    dataset_id = os.getenv('BQ_DATASET_ID', 'market_data')
    source_table_id = os.getenv('SOURCE_TABLE_ID', 'Market_News_NAPI_Temp')
    target_table_id = os.getenv('TARGET_TABLE_ID', 'Market_News_NAPI')

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    try:
        # Initialize the processor and ensure the target table exists
        processor = NewsDataProcessor(project_id, dataset_id)
        processor.ensure_table_exists(target_table_id)

        # Process and replace data
        result = processor.process_and_replace_data(source_table_id, target_table_id)

        # Return the result in a response
        return {
            'statusCode': 200 if result['status'] == 'success' else 500,
            'body': result
        }

    except Exception as e:
        logging.error(f"Failed to process market news data: {e}")
        return {
            'statusCode': 500,
            'body': {
                'status': 'error',
                'message': str(e)
            }
        }

