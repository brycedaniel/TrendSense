import logging
from typing import Tuple, Optional, List

from google.cloud import bigquery
from textblob import TextBlob
import pandas as pd


class YahooNewsProcessor:
    """
    A class to process Yahoo News data, perform sentiment analysis, 
    and manage BigQuery table operations.
    """

    def __init__(self, project_id: str, dataset_id: str):
        """
        Initialize the YahooNewsProcessor with project and dataset details.

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

    @staticmethod
    def calculate_sentiment(text: Optional[str]) -> float:
        """
        Calculate sentiment polarity using TextBlob.

        Args:
            text (str, optional): Text to analyze

        Returns:
            float: Sentiment polarity score
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
    def calculate_lexical_diversity(text: Optional[str]) -> float:
        """
        Calculate lexical diversity as the ratio of unique words to total words.

        Args:
            text (str, optional): Text to analyze

        Returns:
            float: Lexical diversity score
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

    @staticmethod
    def assess_reliability(summary: Optional[str]) -> float:
        """
        Assess language reliability as 1 - abs(sentiment polarity).
        Higher absolute sentiment polarity indicates less reliable/sensational language.

        Args:
            summary (str, optional): Text to assess

        Returns:
            float: Reliability score
        """
        try:
            polarity = YahooNewsProcessor.calculate_sentiment(summary)
            return max(0, 1 - abs(polarity))
        except Exception as e:
            logging.error(f"Reliability analysis failed: {e}")
            return 0.0

    def filter_existing_data(
        self, 
        source_table_id: str, 
        target_table_id: str, 
        data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Filter out rows with publish dates that already exist in the target table.

        Args:
            source_table_id (str): Source table name
            target_table_id (str): Target table name
            data (pd.DataFrame): Input DataFrame to filter

        Returns:
            pd.DataFrame: Filtered DataFrame with only new rows
        """
        source_table = f"{self.project_id}.{self.dataset_id}.{source_table_id}"
        target_table = f"{self.project_id}.{self.dataset_id}.{target_table_id}"

        try:
            # Query to get existing publish dates in the target table
            existing_dates_query = f"""
            SELECT DISTINCT publish_date
            FROM `{target_table}`
            """
            existing_dates = self.client.query(existing_dates_query).to_dataframe()

            # Filter out rows with existing publish dates
            new_data = data[~data['publish_date'].isin(existing_dates['publish_date'])]

            # Log filtering results
            total_rows = len(data)
            filtered_rows = len(new_data)
            self.logger.info(f"Data filtering results:")
            self.logger.info(f"Total input rows: {total_rows}")
            self.logger.info(f"Rows after filtering: {filtered_rows}")
            self.logger.info(f"Rows removed: {total_rows - filtered_rows}")

            return new_data

        except Exception as e:
            error_msg = f"Error filtering existing data: {str(e)}"
            self.logger.error(error_msg)
            raise

    def copy_to_history_table_with_processing(
        self, 
        source_table_id: str, 
        target_table_id: str
    ) -> Tuple[str, int]:
        """
        Copies data from the source table to the target historical table,
        ensuring no duplicates and performing data processing.

        Args:
            source_table_id (str): Source table name
            target_table_id (str): Target table name

        Returns:
            Tuple[str, int]: Status message and HTTP-like status code
        """
        source_table = f"{self.project_id}.{self.dataset_id}.{source_table_id}"
        target_table = f"{self.project_id}.{self.dataset_id}.{target_table_id}"

        try:
            # Extract all new data from source table
            query = f"SELECT * FROM `{source_table}`"
            new_data = self.client.query(query).to_dataframe()

            if new_data.empty:
                self.logger.info("No data found in source table.")
                return "No data found in source table.", 200

            self.logger.info("Cleaning data: Removing rows with blank or null 'ticker' column...")
            
            # Clean data
            new_data = new_data.dropna(subset=['ticker'])
            new_data = new_data[new_data['ticker'].str.strip() != '']

            if new_data.empty:
                self.logger.info("All rows were invalid after cleaning. No data to insert.")
                return "All rows were invalid after cleaning. No data to insert.", 200

            # Filter out existing data using the new method
            new_data = self.filter_existing_data(source_table_id, target_table_id, new_data)

            if new_data.empty:
                self.logger.info("No new rows to insert after filtering.")
                return "No new rows to insert after filtering.", 200

            # Calculate additional metrics
            self.logger.info("Processing data: calculating sentiments and metrics...")
            new_data['title_sentiment'] = new_data['title'].apply(self.calculate_sentiment)
            new_data['summary_sentiment'] = new_data['summary'].apply(self.calculate_sentiment)
            new_data['reliability_score'] = new_data['summary'].apply(self.assess_reliability)
            new_data['lexical_diversity'] = new_data['summary'].apply(self.calculate_lexical_diversity)

            # Save the updated data to the target table in BigQuery
            table_ref = self.client.dataset(self.dataset_id).table(target_table_id)
            job = self.client.load_table_from_dataframe(new_data, table_ref)
            job.result()  # Wait for the job to complete

            success_msg = f"Updated table saved successfully to {target_table}."
            self.logger.info(success_msg)
            return success_msg, 200

        except Exception as e:
            error_msg = f"Error processing and updating table: {str(e)}"
            self.logger.error(error_msg)
            return error_msg, 500


def copy_market_news_with_processing(request):
    """
    Cloud Function entry point for copying data to historical table
    with processing for title sentiment, lexical diversity, and reliability.

    Args:
        request: Cloud Function request object (not used in this implementation)

    Returns:
        Tuple containing status message and status code
    """
    # Configuration
    project_id = "trendsense"
    dataset_id = "market_data"
    source_table_id = "News_Yahoo_Extract"
    target_table_id = "Market_News_History_New"

    # Create processor and process data
    processor = YahooNewsProcessor(project_id, dataset_id)
    return processor.copy_to_history_table_with_processing(source_table_id, target_table_id)


