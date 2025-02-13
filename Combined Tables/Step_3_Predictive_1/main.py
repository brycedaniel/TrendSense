from google.cloud import bigquery
import pandas as pd
import functions_framework
import os
from flask import Flask, jsonify

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

        tickers_to_exclude = ["^RUT", "^GSPC", "^DJI"]
        df_filtered = df2[~df2["ticker"].isin(tickers_to_exclude)].copy()

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
            'NOW': 'Software', 'TWLO': 'Software'
        }

        # Add Stock_Category column
        df_filtered['Stock_Category'] = df_filtered['ticker'].map(ticker_category_map).fillna('Unknown')

        # Fill missing or zero values in Next_Daily_Percent_Difference with next available value
        df_filtered = df_filtered.sort_values(by=['ticker', 'publish_date'])
        df_filtered['Next_Daily_Percent_Difference'] = df_filtered['Next_Daily_Percent_Difference'].replace(0, pd.NA)
        df_filtered['Next_Daily_Percent_Difference'] = df_filtered.groupby('ticker')['Next_Daily_Percent_Difference'].fillna(method='bfill')

        # Initial AI Score calculation
        df_filtered["Initial_AI_Score"] = df_filtered["AI Score"] * df_filtered["publisher score"]

        # Calculate daily average AI score
        df_filtered['date'] = pd.to_datetime(df_filtered['publish_date']).dt.date
        df_filtered["Daily_Avg_AI_Score"] = df_filtered.groupby(['date', 'ticker'])["Initial_AI_Score"].transform('mean')

        # Final AI Score with 50-50 weighting
        df_filtered["AI_Score"] = 0.5 * df_filtered["Initial_AI_Score"] + 0.5 * df_filtered["Daily_Avg_AI_Score"]

        # Compute Sentiment Score
        df_filtered["Sentiment Score"] = (
            df_filtered["article_sentiment"] * 0.50 +
            df_filtered["daily_avg_ticker_sentiment"] * 0.30 +
            df_filtered["average_market_sentiment"] * 0.20
        )

        # Replace NaN values in RatingScore with 2
        df_filtered["RatingScore"] = df_filtered["RatingScore"].fillna(2)

        # Compute Health Score
        df_filtered["Health_Score"] = (
            df_filtered["RatingScore"] * 100 * 0.25 +  
            df_filtered["analyst_score"] * 0.50 +
            df_filtered["target_score"] * 10 * 0.125 +
            df_filtered["Target_Pct_Change"] * 50 * 0.125
        )

        # Drop rows where Health_Score is NaN
        df_filtered = df_filtered.dropna(subset=["Health_Score"])

        df_filtered["Aggregated_Score"] = df_filtered[["AI_Score", "Sentiment Score", "Health_Score"]].mean(axis=1)

        df_filtered.rename(
            columns={
                "Forward_60min_Change_Diff": "Relative_1HR_Chg",
                "Forward_60min_Change": "Open_1HR_Change"
            }, inplace=True
        )

        df_final = df_filtered[[
            "Unique_ID", "publish_date", "date", "ticker", "Stock_Category", "AI_Score", "Daily_Avg_AI_Score", 
            "Sentiment Score", "Health_Score", "Aggregated_Score", "Relative_1HR_Chg", "Open_1HR_Change", 
            "Daily_Percent_Difference", "Next_Daily_Percent_Difference"
        ]]

        if not df_final.empty:
            schema = [
                bigquery.SchemaField("Unique_ID", "STRING"),
                bigquery.SchemaField("publish_date", "TIMESTAMP"),
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

        return jsonify({"message": f"Added {len(df_final)} new rows to {TARGET_TABLE}"})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/')
def home():
    return "Function is running"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)

