import os
import logging
from typing import Optional, Tuple, Dict, Any
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from transformers import pipeline


class NewsDataProcessorError(Exception):
    """Custom exception for NewsDataProcessor errors."""
    pass


class NewsDataProcessor:
    def __init__(self, project_id: str, dataset_id: str, logger: Optional[logging.Logger] = None):
        self._validate_input_parameters(project_id, dataset_id)
        self.logger = logger or self._setup_logger()
        try:
            self.client = bigquery.Client(project=project_id)
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {e}")
            raise NewsDataProcessorError(f"BigQuery client initialization failed: {e}")
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self._bert_pipeline = None

    def _validate_input_parameters(self, project_id: str, dataset_id: str):
        if not project_id or not isinstance(project_id, str):
            raise NewsDataProcessorError("Invalid project_id. Must be a non-empty string.")
        if not dataset_id or not isinstance(dataset_id, str):
            raise NewsDataProcessorError("Invalid dataset_id. Must be a non-empty string.")

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.DEBUG)  # Set to DEBUG for detailed logs
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    @property
    def bert_pipeline(self):
        if self._bert_pipeline is None:
            self._bert_pipeline = pipeline("sentiment-analysis")
        return self._bert_pipeline

    def calculate_vader_sentiment(self, text: Optional[str]) -> float:
        if not text or not isinstance(text, str):
            return 0.0
        try:
            sentiment = self.vader_analyzer.polarity_scores(text)
            return sentiment.get("compound", 0.0)
        except Exception as e:
            self.logger.warning(f"VADER sentiment analysis failed: {e}")
            return 0.0

    def calculate_bert_sentiment(self, text: Optional[str]) -> Tuple[float, float]:
        if not text or not isinstance(text, str):
            return 0.0, 0.0
        try:
            result = self.bert_pipeline(text)[0]
            # Map BERT sentiment to a range similar to VADER (-1 to 1)
            if result["label"] == "POSITIVE":
                # Scale positive sentiment from 0-1 to 0-1
                sentiment_score = (result["score"] * 2) - 1
            else:
                # Scale negative sentiment from 0-1 to -1-0
                sentiment_score = -((result["score"] * 2) - 1)
            
            confidence = result["score"]
            return sentiment_score, confidence
        except Exception as e:
            self.logger.warning(f"BERT sentiment analysis failed: {e}")
            return 0.0, 0.0

    def ensure_table_exists(self, table_id: str):
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
                # New columns
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

    def filter_existing_data(self, new_data: pd.DataFrame, target_table: str) -> pd.DataFrame:
        """
        Filter out rows that already exist in the target table based on ticker and publish date.

        Args:
            new_data (pd.DataFrame): Incoming new data to be checked for duplicates.
            target_table (str): Fully qualified BigQuery table reference.

        Returns:
            pd.DataFrame: Filtered dataframe with only new rows.
        """
        if new_data.empty:
            self.logger.info("No new data provided for filtering.")
            return new_data

        try:
            # Ensure publish_date is in datetime format
            new_data['publish_date'] = pd.to_datetime(new_data['publish_date'])

            # Ensure target_table is fully qualified
            if not target_table.count('.') >= 2:
                target_table = f"{self.project_id}.{self.dataset_id}.{target_table}"

            # Create a query to check for existing records with ticker and publish date
            existing_records_query = f"""
            WITH new_records AS (
                {self._create_temp_table_query(new_data)}
            )
            SELECT 
                nr.ticker, 
                nr.publish_date
            FROM new_records nr
            JOIN `{target_table}` existing
            ON nr.ticker = existing.ticker 
            AND DATETIME(nr.publish_date) = DATETIME(existing.publish_date)
            """

            # Execute query to find existing records
            query_job = self.client.query(existing_records_query)
            existing_records = query_job.to_dataframe()

            if not existing_records.empty:
                # Create a combined key for filtering
                new_data['duplicate_key'] = new_data.apply(
                    lambda row: (row['ticker'], row['publish_date']), 
                    axis=1
                )
                existing_records['duplicate_key'] = existing_records.apply(
                    lambda row: (row['ticker'], row['publish_date']), 
                    axis=1
                )

                # Filter out duplicate records
                filtered_data = new_data[~new_data['duplicate_key'].isin(existing_records['duplicate_key'])]
                
                # Drop the temporary duplicate_key column
                filtered_data = filtered_data.drop(columns=['duplicate_key'])
                
                self.logger.info(f"Total new rows after filtering: {len(filtered_data)} (from {len(new_data)} original rows)")
            else:
                filtered_data = new_data

            return filtered_data

        except Exception as e:
            self.logger.error(f"Error filtering existing data: {e}")
            # If filtering fails, return the original dataset to prevent data loss
            return new_data

    def _create_temp_table_query(self, new_data: pd.DataFrame) -> str:
        """
        Create a temporary table query for the new data.

        Args:
            new_data (pd.DataFrame): Incoming new data to be checked for duplicates.

        Returns:
            str: SQL query string creating a temporary table with new records.
        """
        # Convert DataFrame to a list of tuples for the query
        records = []
        for _, row in new_data.iterrows():
            # Explicitly convert to DATETIME 
            publish_datetime = row['publish_date'].replace(tzinfo=None)
            records.append(f"STRUCT('{row['ticker']}' AS ticker, DATETIME '{publish_datetime.strftime('%Y-%m-%d %H:%M:%S')}' AS publish_date)")
        
        # Join records into a single string
        records_str = ',\n'.join(records)
        
        # Create the query
        return f"""
        SELECT 
            ticker, 
            publish_date
        FROM UNNEST([
            {records_str}
        ])
        """

    
    def process_and_move_data(self, source_table_id: str, target_table_id: str, batch_size: int = 1000) -> Dict[str, Any]:
        source_table = f"{self.project_id}.{self.dataset_id}.{source_table_id}"
        target_table = f"{self.project_id}.{self.dataset_id}.{target_table_id}"

        try:
            # Query data from the source table excluding unwanted columns
            self.logger.info(f"Querying source table: {source_table}")
            query = f"""
            SELECT 
                ticker, 
                title, 
                summary, 
                publisher, 
                link, 
                publish_date, 
                type, 
                related_tickers, 
                source, 
                lexical_diversity, 
                reliability_score, 
                summary_sentiment
            FROM `{source_table}`
            LIMIT {batch_size}
            """
            new_data = self.client.query(query).to_dataframe()
            self.logger.info(f"Rows retrieved from source table: {len(new_data)}")

            if new_data.empty:
                self.logger.info("No new data to process.")
                return {"status": "success", "message": "No new data", "rows_processed": 0}

            # Rename summary_sentiment to textblob_sentiment
            self.logger.info("Renaming columns...")
            new_data.rename(columns={"summary_sentiment": "textblob_sentiment"}, inplace=True)

            # Ensure publish_date is in datetime format
            new_data['publish_date'] = pd.to_datetime(new_data['publish_date'])

            # Filter out existing rows
            self.logger.info("Filtering existing rows...")
            new_data = self.filter_existing_data(new_data, target_table_id)
            self.logger.info(f"Rows remaining after filtering: {len(new_data)}")

            if new_data.empty:
                self.logger.info("No new unique rows to process after filtering.")
                return {"status": "success", "message": "No new unique rows", "rows_processed": 0}

            # Word Count Calculation
            self.logger.info("Calculating word count for summaries...")
            new_data["word_count"] = new_data["summary"].fillna("").apply(lambda x: len(str(x).split()))

            # Headline Sentiment using VADER
            self.logger.info("Performing VADER sentiment analysis on headlines...")
            new_data["headline_sentiment"] = new_data["title"].apply(self.calculate_vader_sentiment)

            # Existing Sentiment Analyses
            self.logger.info("Performing VADER sentiment analysis on summaries...")
            new_data["vader_sentiment"] = new_data["summary"].apply(self.calculate_vader_sentiment)
            bert_results = new_data["summary"].apply(self.calculate_bert_sentiment).tolist()

            # Validate BERT results
            if len(bert_results) != len(new_data):
                self.logger.error(f"BERT results length mismatch: {len(bert_results)} results for {len(new_data)} rows.")
                raise ValueError("BERT results length mismatch with DataFrame rows.")

            # Unpack BERT results into separate columns
            bert_sentiments, bert_confidences = zip(*bert_results)
            new_data["bert_sentiment"] = bert_sentiments
            new_data["bert_confidence"] = bert_confidences

            # Load data into the target table
            self.logger.info("Loading data into BigQuery...")
            job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
            job = self.client.load_table_from_dataframe(new_data, target_table, job_config=job_config)
            job.result()  # Wait for the job to complete

            success_msg = f"Data successfully moved to {target_table}. Rows added: {len(new_data)}"
            self.logger.info(success_msg)

            return {"status": "success", "message": success_msg, "rows_processed": len(new_data)}

        except Exception as e:
            error_msg = f"Error processing data: {e}"
            self.logger.error(error_msg)
            return {"status": "error", "message": error_msg, "rows_processed": 0}


def move_market_news_data(request):
    """
    Google Cloud Function entry point to process and move market news data.
    """
    # Load configuration from environment variables
    project_id = os.getenv('GCP_PROJECT_ID', 'trendsense')
    dataset_id = os.getenv('BQ_DATASET_ID', 'market_data')
    source_table_id = os.getenv('SOURCE_TABLE_ID', 'Market_News_AY_Temp')
    target_table_id = os.getenv('TARGET_TABLE_ID', 'Market_News_AY')

    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    try:
        # Initialize the processor and ensure the target table exists
        processor = NewsDataProcessor(project_id, dataset_id)
        processor.ensure_table_exists(target_table_id)

        # Process and move data
        result = processor.process_and_move_data(source_table_id, target_table_id)

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