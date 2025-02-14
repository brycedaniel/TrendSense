from google.cloud import bigquery
import pandas as pd
import functions_framework
from flask import jsonify

# Initialize BigQuery client
client = bigquery.Client()

# Define source and target BigQuery tables
SOURCE_TABLE = 'trendsense.combined_data.step_3_predictive_1'
STOCK_HISTORY_TABLE = 'trendsense.stock_data.stock_data_history'
TARGET_TABLE = 'trendsense.combined_data.step_4_final'

@functions_framework.http
def process_stock_data(request):
    try:
        # Query BigQuery for data
        query = f"""
        WITH base_predictive AS (
          SELECT
            ticker,
            date,
            Stock_Category,
            Aggregated_Score,
            AI_Score,
            `Sentiment Score`,
            Health_Score
          FROM
            `{SOURCE_TABLE}`
        ),

        next_day_data AS (
          SELECT
            ticker,
            date,
            Percent_Difference,
            LEAD(Percent_Difference) OVER (PARTITION BY ticker ORDER BY date) as Next_Day_Percent
          FROM
            `{STOCK_HISTORY_TABLE}`
        )

        SELECT DISTINCT
          hist.Ticker as ticker,
          DATE(hist.Date) as date,
          COALESCE(bp.Stock_Category, 'Unknown') as Stock_Category,
          bp.Aggregated_Score,
          bp.AI_Score,
          bp.`Sentiment Score`,
          bp.Health_Score,
          hist.Close,
          hist.Percent_Difference as Avg_Daily_Percent_Difference,
          nd.Next_Day_Percent as Avg_Next_Daily_Percent_Difference
        FROM
          `{STOCK_HISTORY_TABLE}` hist
        LEFT JOIN
          base_predictive bp
        ON
          LOWER(hist.Ticker) = LOWER(bp.ticker)
          AND DATE(hist.Date) = DATE(bp.date)
        LEFT JOIN
          next_day_data nd
        ON
          LOWER(hist.Ticker) = LOWER(nd.ticker)
          AND DATE(hist.Date) = DATE(nd.date)
        WHERE 
          hist.Date IS NOT NULL
        ORDER BY
          date,
          ticker;
                                        
          """
        # Execute query and load data into a DataFrame
        query_job = client.query(query)
        df = query_job.to_dataframe()

        # Data Processing
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['ticker', 'date'])

        df_grouped = df.groupby(['ticker', 'date', 'Stock_Category']).agg({
            'Aggregated_Score': 'mean',
            'Close': 'last',
            'Avg_Daily_Percent_Difference': 'mean',
            'Avg_Next_Daily_Percent_Difference': 'mean',
            'AI_Score': 'mean',
            'Sentiment Score': 'mean',
            'Health_Score': 'mean'
        }).reset_index()

        df_grouped = df_grouped.rename(columns={
            'Aggregated_Score': 'Avg_Aggregated_Score',
            'AI_Score': 'Avg_AI_Score',
            'Sentiment Score': 'Avg_Sentiment_Score',
            'Health_Score': 'Avg_Health_Score'
        })

        df_grouped['Avg_Daily_Percent_Difference'] = df_grouped['Avg_Daily_Percent_Difference'].fillna(0)

        df_grouped['Rolling_7day_Avg'] = df_grouped.groupby('ticker')['Avg_Aggregated_Score'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )

        df_grouped['Rolling_Avg_Rank'] = df_grouped.groupby('date')['Rolling_7day_Avg'].rank(
            method='min',
            ascending=False
        )

        df_grouped['Pct_Change_From_Previous'] = df_grouped.groupby('ticker')['Avg_Aggregated_Score'].pct_change() * 100
        df_grouped['Pct_Change_Rank'] = df_grouped.groupby('date')['Pct_Change_From_Previous'].rank(
            method='min',
            ascending=False
        )

        df_grouped['Composite_Rank'] = (df_grouped['Rolling_Avg_Rank'] + df_grouped['Pct_Change_Rank']) / 2
        
        # Calculate daily top 10 averages
        daily_top_10_avg_next = (
            df_grouped.groupby('date')
            .apply(lambda x: x.nsmallest(10, 'Composite_Rank')['Avg_Next_Daily_Percent_Difference'].mean())
            .reset_index()
            .rename(columns={0: 'Top_10_Next_Day_Avg'})
        )

        daily_top_10_avg_today = (
            df_grouped.groupby('date')
            .apply(lambda x: x.nsmallest(10, 'Composite_Rank')['Avg_Daily_Percent_Difference'].mean())
            .reset_index()
            .rename(columns={0: 'Top_10_Today_Day_Avg'})
        )

        # Merge the daily averages back
        df_grouped = df_grouped.merge(daily_top_10_avg_next, on='date', how='left')
        df_grouped = df_grouped.merge(daily_top_10_avg_today, on='date', how='left')

        # Calculate cumulative sums for 2025
        df_2025 = df_grouped[df_grouped['date'] >= '2025-01-01'].copy()

        daily_cumulative_next = (
            df_2025.groupby('date')['Top_10_Next_Day_Avg']
            .mean()
            .cumsum()
            .reset_index()
            .rename(columns={'Top_10_Next_Day_Avg': 'Cumulative_Top_10_Score'})
        )

        daily_cumulative_today = (
            df_2025.groupby('date')['Top_10_Today_Day_Avg']
            .mean()
            .cumsum()
            .reset_index()
            .rename(columns={'Top_10_Today_Day_Avg': 'Cumulative_Top_10_Today_Score'})
        )

        # Merge cumulative sums
        df_grouped = df_grouped.merge(daily_cumulative_next, on='date', how='left')
        df_grouped = df_grouped.merge(daily_cumulative_today, on='date', how='left')

        # Fill missing cumulative scores
        df_grouped['Cumulative_Top_10_Score'].fillna(0, inplace=True)
        df_grouped['Cumulative_Top_10_Today_Score'].fillna(0, inplace=True)

        # Final sort
        df_grouped = df_grouped.sort_values(['date'])

        # Save to BigQuery (overwrite existing table)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )

        job = client.load_table_from_dataframe(df_grouped, TARGET_TABLE, job_config=job_config)
        job.result()

        return jsonify({
            "message": f"Data successfully overwritten in {TARGET_TABLE}",
            "row_count": len(df_grouped)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

