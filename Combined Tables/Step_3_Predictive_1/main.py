from google.cloud import bigquery
import pandas as pd
import functions_framework
import os
from flask import Flask, jsonify
from datetime import datetime, timedelta

# Initialize BigQuery client
client = bigquery.Client()

# Source and Target Table References
SOURCE_TABLE = 'trendsense.combined_data.step_2_transform_AI'
TARGET_TABLE = 'trendsense.combined_data.step_3_predictive_1'

app = Flask(__name__)

@functions_framework.http
def process_data(request):
    try:
        # Load source data from BigQuery
        query = f"SELECT * FROM `{SOURCE_TABLE}`"
        df2 = client.query(query).to_dataframe()

        if "Unique_ID" not in df2.columns:
            raise ValueError("Unique_ID column is missing in source table")

        # Define all tickers
        all_tickers = [
            'AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
            'BWXT', 'ARBK', 'AMD', 'NVDA', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
            'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 
            'TDS', 'META', 'RGTI', 'QUBT', 'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 
            'TWLO', '^IXIC', '^GSPC'
        ]

        # Mapping tickers to their categories
        ticker_category_map = {
            'ASTS': 'Space / Communication', 'GSAT': 'Space / Communication', 'CHTR': 'Space / Communication', 
            'TDS': 'Space / Communication', 'ARBK': 'Space / Communication',
            'NVDA': 'Semiconductor', 'AMD': 'Semiconductor', 'MU': 'Semiconductor', 
            'AVGO': 'Semiconductor', 'SMCI': 'Semiconductor', 'GLW': 'Semiconductor',
            'PLTR': 'AI', 'GOOGL': 'AI', 'META': 'AI', 'RGTI': 'AI', 'QUBT': 'AI',
            'SMR': 'Energy', 'OKLO': 'Energy', 'HAL': 'Energy', 'PSIX': 'Energy', 
            'BWXT': 'Energy', 'RTX': 'Energy',
            'TSLA': 'Transportation', 'ACHR': 'Transportation',
            'QFIN': 'Financial', 'LX': 'Financial',
            'NFLX': 'Entertainment', 'GME': 'Entertainment', 'ZG': 'Entertainment',
            'MSFT': 'Software', 'AAPL': 'Software', 'AMZN': 'Software', 'CRM': 'Software', 
            'NOW': 'Software', 'TWLO': 'Software',
            '^IXIC': 'Index', '^GSPC': 'Index'
        }
        
        # Convert publish_date to datetime (explicitly timezone naive)
        df2['publish_date'] = pd.to_datetime(df2['publish_date'], errors='coerce', utc=True).dt.tz_localize(None)
        df2['date'] = pd.to_datetime(df2['publish_date']).dt.date
        
        # Apply the UTC time shift to existing data (still timezone naive)
        df2['publish_date_utc'] = df2['publish_date'].apply(
            lambda x: x + pd.Timedelta(hours=7) if pd.notna(x) else None
        )
        
        # Get date_utc from publish_date_utc for existing data
        df2['date_utc'] = df2['publish_date_utc'].apply(
            lambda x: x.date() if pd.notna(x) else None
        )
        
        # Get all unique dates in the dataset
        min_date = df2['date'].min()
        max_date = df2['date'].max()
        all_dates = pd.date_range(start=min_date, end=max_date, freq='D')
        
        # Create a complete date-ticker grid
        full_df = pd.DataFrame([(date, ticker) for date in all_dates for ticker in all_tickers],
                             columns=['date', 'ticker'])
        
        # Make sure dates are datetime for merging
        full_df['date'] = pd.to_datetime(full_df['date']).dt.tz_localize(None)
        df2['date'] = pd.to_datetime(df2['date']).dt.tz_localize(None)
        
        # Merge with original data
        df_merged = pd.merge(full_df, df2, 
                           how='left', 
                           on=['date', 'ticker'])
        
        # Create publish_date for new records with timestamp at midnight
        df_merged['publish_date'] = df_merged.apply(
            lambda row: row['publish_date'] if pd.notna(row['publish_date']) 
            else pd.Timestamp(row['date'].date()),  # Use date from the merge
            axis=1
        )
        
        # Ensure publish_date is a consistent datetime type (timezone naive)
        df_merged['publish_date'] = pd.to_datetime(df_merged['publish_date'], utc=True).dt.tz_localize(None)
        
        # Calculate publish_date_utc for ALL records
        df_merged['publish_date_utc'] = df_merged['publish_date'] + pd.Timedelta(hours=7)
        
        # Get date_utc from publish_date_utc
        df_merged['date_utc'] = df_merged['publish_date_utc'].apply(
            lambda x: x.date() if pd.notna(x) else None
        )
        
        # Generate Unique_ID for new rows
        df_merged['Unique_ID'] = df_merged['Unique_ID'].fillna(
            df_merged.apply(lambda x: f"{x['ticker']}_{x['date'].strftime('%Y%m%d')}_{hash(str(datetime.now()))}", axis=1)
        )

        # Forward fill within groups
        fill_columns = [
            'AI Score', 'publisher score', 'article_sentiment', 'daily_avg_ticker_sentiment',
            'average_market_sentiment', 'RatingScore', 'analyst_score', 'target_score',
            'Target_Pct_Change', 'Forward_60min_Change_Diff', 'Forward_60min_Change',
            'Daily_Percent_Difference', 'Next_Daily_Percent_Difference'
        ]

        for col in fill_columns:
            if col in df_merged.columns:
                df_merged[col] = df_merged.groupby('ticker')[col].fillna(method='ffill')
                # Backward fill if still have NaN (for the first dates)
                df_merged[col] = df_merged.groupby('ticker')[col].fillna(method='bfill')

        # Add Stock_Category
        df_merged['Stock_Category'] = df_merged['ticker'].map(ticker_category_map)

        # Calculate scores
        df_merged["Initial_AI_Score"] = df_merged["AI Score"] * df_merged["publisher score"]
        df_merged["Daily_Avg_AI_Score"] = df_merged.groupby(['date', 'ticker'])["Initial_AI_Score"].transform('mean')
        df_merged["AI_Score"] = 0.5 * df_merged["Initial_AI_Score"] + 0.5 * df_merged["Daily_Avg_AI_Score"]

        df_merged["Sentiment Score"] = (
            df_merged["article_sentiment"] * 0.50 +
            df_merged["daily_avg_ticker_sentiment"] * 0.30 +
            df_merged["average_market_sentiment"] * 0.20
        )

        df_merged["RatingScore"] = df_merged["RatingScore"].fillna(2)

        df_merged["Health_Score"] = (
            df_merged["RatingScore"] * 100 * 0.25 +  
            df_merged["analyst_score"] * 0.50 +
            df_merged["target_score"] * 10 * 0.125 +
            df_merged["Target_Pct_Change"] * 50 * 0.125
        )

        df_merged["Aggregated_Score"] = df_merged[["AI_Score", "Sentiment Score", "Health_Score"]].mean(axis=1)

        # Rename columns
        df_merged.rename(
            columns={
                "Forward_60min_Change_Diff": "Relative_1HR_Chg",
                "Forward_60min_Change": "Open_1HR_Change"
            }, inplace=True
        )

        # For debugging, print a sample of records to see if publish_date_utc is populated
        print("Sample of df_merged:")
        print(df_merged[['date', 'ticker', 'publish_date', 'publish_date_utc']].head(10))
        print("NaN values in publish_date_utc:", df_merged['publish_date_utc'].isna().sum())

        # Select final columns
        df_final = df_merged[[
            "Unique_ID", "publish_date", "publish_date_utc", "date_utc", "date", "ticker", "Stock_Category", "AI_Score", 
            "Daily_Avg_AI_Score", "Sentiment Score", "Health_Score", "Aggregated_Score",
            "Relative_1HR_Chg", "Open_1HR_Change", "Daily_Percent_Difference",
            "Next_Daily_Percent_Difference"
        ]]

        # Upload to BigQuery
        if not df_final.empty:
            schema = [
                bigquery.SchemaField("Unique_ID", "STRING"),
                bigquery.SchemaField("publish_date", "TIMESTAMP"),
                bigquery.SchemaField("publish_date_utc", "TIMESTAMP"),
                bigquery.SchemaField("date_utc", "DATE"), 
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("ticker", "STRING"),
                bigquery.SchemaField("Stock_Category", "STRING"),
                bigquery.SchemaField("AI_Score", "FLOAT"),
                bigquery.SchemaField("Daily_Avg_AI_Score", "FLOAT"),
                bigquery.SchemaField("Sentiment Score", "FLOAT"),
                bigquery.SchemaField("Health_Score", "FLOAT"),
                bigquery.SchemaField("Aggregated_Score", "FLOAT"),
                bigquery.SchemaField("Relative_1HR_Chg", "FLOAT"),
                bigquery.SchemaField("Open_1HR_Change", "FLOAT"),
                bigquery.SchemaField("Daily_Percent_Difference", "FLOAT"),
                bigquery.SchemaField("Next_Daily_Percent_Difference", "FLOAT")
            ]

            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=schema
            )

            load_job = client.load_table_from_dataframe(df_final, TARGET_TABLE, job_config=job_config)
            load_job.result()

        return jsonify({"message": f"Added {len(df_final)} rows to {TARGET_TABLE}"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return "Function is running"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)