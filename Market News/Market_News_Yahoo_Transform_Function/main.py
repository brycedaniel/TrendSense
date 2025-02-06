import logging
from typing import Tuple, Optional
import pytz

from google.cloud import bigquery
from textblob import TextBlob
import pandas as pd


class YahooNewsProcessor:
    def __init__(self, project_id: str, dataset_id: str):
        self.client = bigquery.Client()
        self.project_id = project_id
        self.dataset_id = dataset_id
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def calculate_sentiment(text: Optional[str]) -> float:
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
        try:
            polarity = YahooNewsProcessor.calculate_sentiment(summary)
            return max(0, 1 - abs(polarity))
        except Exception as e:
            logging.error(f"Reliability analysis failed: {e}")
            return 0.0

    def filter_existing_data(self, target_table_id: str, data: pd.DataFrame) -> pd.DataFrame:
        target_table = f"{self.project_id}.{self.dataset_id}.{target_table_id}"
        try:
            existing_ids_query = f"""
            SELECT DISTINCT CONCAT(ticker, '_', CAST(publish_date AS STRING)) AS unique_id
            FROM `{target_table}`
            """
            existing_ids = self.client.query(existing_ids_query).to_dataframe()
            
            data['unique_id'] = data['ticker'] + '_' + data['publish_date'].astype(str)
            new_data = data[~data['unique_id'].isin(existing_ids['unique_id'])]

            self.logger.info(f"Total input rows: {len(data)}")
            self.logger.info(f"Rows after filtering: {len(new_data)}")
            self.logger.info(f"Rows removed: {len(data) - len(new_data)}")

            return new_data.drop(columns=['unique_id'])
        except Exception as e:
            error_msg = f"Error filtering existing data: {str(e)}"
            self.logger.error(error_msg)
            raise

    def copy_to_history_table_with_processing(self, source_table_id: str, target_table_id: str) -> Tuple[str, int]:
        source_table = f"{self.project_id}.{self.dataset_id}.{source_table_id}"
        target_table = f"{self.project_id}.{self.dataset_id}.{target_table_id}"

        try:
            query = f"SELECT * FROM `{source_table}`"
            new_data = self.client.query(query).to_dataframe()

            if new_data.empty:
                self.logger.info("No data found in source table.")
                return "No data found in source table.", 200

            self.logger.info("Cleaning data: Removing rows with blank or null 'ticker' column...")
            new_data = new_data.dropna(subset=['ticker'])
            new_data = new_data[new_data['ticker'].str.strip() != '']
            
            mst = pytz.timezone('MST')
            new_data['publish_date'] = (
                pd.to_datetime(new_data['publish_date'], errors='coerce')
                .dt.tz_localize('UTC')
                .dt.tz_convert(mst)
                .dt.strftime('%Y-%m-%d %H:%M:%S')
            )

            if new_data.empty:
                self.logger.info("All rows were invalid after cleaning. No data to insert.")
                return "All rows were invalid after cleaning. No data to insert.", 200

            new_data = self.filter_existing_data(target_table_id, new_data)
            if new_data.empty:
                self.logger.info("No new rows to insert after filtering.")
                return "No new rows to insert after filtering.", 200

            self.logger.info("Processing data: calculating sentiments and metrics...")
            new_data['title_sentiment'] = new_data['title'].apply(self.calculate_sentiment)
            new_data['summary_sentiment'] = new_data['summary'].apply(self.calculate_sentiment)
            new_data['reliability_score'] = new_data['summary'].apply(self.assess_reliability)
            new_data['lexical_diversity'] = new_data['summary'].apply(self.calculate_lexical_diversity)

            table_ref = self.client.dataset(self.dataset_id).table(target_table_id)
            job = self.client.load_table_from_dataframe(new_data, table_ref)
            job.result()

            success_msg = f"Updated table saved successfully to {target_table}."
            self.logger.info(success_msg)
            return success_msg, 200

        except Exception as e:
            error_msg = f"Error processing and updating table: {str(e)}"
            self.logger.error(error_msg)
            return error_msg, 500


def copy_market_news_with_processing(request):
    project_id = "trendsense"
    dataset_id = "market_data"
    source_table_id = "News_Yahoo_Extract"
    target_table_id = "Market_News_AY_Temp"

    processor = YahooNewsProcessor(project_id, dataset_id)
    return processor.copy_to_history_table_with_processing(source_table_id, target_table_id)



