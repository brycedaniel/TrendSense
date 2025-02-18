from google.cloud import bigquery
import pandas as pd
import functions_framework
from flask import jsonify
import logging

# Initialize BigQuery client
client = bigquery.Client()

# Define source and target BigQuery tables
SOURCE_TABLE = 'trendsense.combined_data.step_3_predictive_1'
STOCK_HISTORY_TABLE = 'trendsense.stock_data.stock_data_history'
TARGET_TABLE = 'trendsense.combined_data.step_4_final'
REGRESSION_TABLE = 'trendsense.combined_data.step_4_test_train'

@functions_framework.http
def process_stock_data(request):
    try:
        logging.info("Starting data processing...")
        
        # First, get the regression coefficients
        regression_query = f"""
        SELECT *
        FROM `{REGRESSION_TABLE}`
        """
        regression_df = client.query(regression_query).to_dataframe()
        
        # Create a dictionary of regression coefficients for quick lookup
        regression_dict = {
            row['Ticker']: {
                'intercept': row['Intercept'],
                'ai_coef': row['AI_Coefficient'],
                'sentiment_coef': row['Sentiment_Coefficient'],
                'health_coef': row['Health_Coefficient']
            }
            for _, row in regression_df.iterrows()
        }

        # Modified query to use FULL OUTER JOIN
        query = f"""
      WITH stock_history AS (
  SELECT DISTINCT
    Ticker,
    DATE(Date) as Date,
    Close,
    Percent_Difference,
    LEAD(Percent_Difference) OVER (PARTITION BY Ticker ORDER BY Date) as Next_Day_Percent
  FROM `trendsense.stock_data.stock_data_history`
  WHERE ticker != 'GSAT'
),

predictive_data AS (
  SELECT
    ticker,
    DATE(date) as date,
    Stock_Category,
    Aggregated_Score,
    AI_Score,
    `Sentiment Score` as Sentiment_Score,
    Health_Score
  FROM `trendsense.combined_data.step_3_predictive_1`
  WHERE Ticker != 'GSAT'
)

SELECT 
  COALESCE(sh.Date, pd.date) as date,
  COALESCE(sh.Ticker, pd.ticker) as ticker,
  COALESCE(pd.Stock_Category, 'Unknown') as Stock_Category,
  pd.Aggregated_Score,
  pd.AI_Score,
  pd.Sentiment_Score as `Sentiment Score`,
  pd.Health_Score,
  sh.Close,
  sh.Percent_Difference as Avg_Daily_Percent_Difference,
  sh.Next_Day_Percent as Avg_Next_Daily_Percent_Difference
FROM (
  SELECT * FROM stock_history 
  UNION ALL
  SELECT DISTINCT ticker, date, NULL as Close, NULL as Percent_Difference, NULL as Next_Day_Percent 
  FROM predictive_data 
  WHERE date NOT IN (SELECT DISTINCT Date FROM stock_history)
) sh
FULL OUTER JOIN predictive_data pd
ON LOWER(sh.Ticker) = LOWER(pd.ticker)
AND sh.Date = pd.date
ORDER BY
  COALESCE(sh.Date, pd.date),
  COALESCE(sh.Ticker, pd.ticker)
        """
        
        # Rest of the function remains the same
        query_job = client.query(query)
        df = query_job.to_dataframe()

        filter_date = pd.to_datetime('2024-12-1')
        df['date'] = pd.to_datetime(df['date'])
        df = df[df['date'] >= filter_date]
        df = df.sort_values(['ticker', 'date'])
      

        
        # Additional filter for GSAT just in case
        df = df[df['ticker'] != 'GSAT']

        df_grouped = df.groupby(['ticker', 'date', 'Stock_Category']).agg({
            'Aggregated_Score': 'mean',
            'Close': 'last',
            'Avg_Daily_Percent_Difference': 'mean',
            'Avg_Next_Daily_Percent_Difference': 'mean',
            'AI_Score': 'mean',
            'Sentiment Score': 'mean',
            'Health_Score': 'mean'
        }).reset_index()

        # Updated column renaming
        df_grouped = df_grouped.rename(columns={
            'Aggregated_Score': 'TS_Score',
            'Avg_Daily_Percent_Difference': 'Price_Movement_Today',
            'Avg_Next_Daily_Percent_Difference': 'Price_Movement_Tomorrow',
            'AI_Score': 'AI_Score',
            'Sentiment Score': 'Sentiment_Score',
            'Health_Score': 'Health_Score'
        })
        # Convert 'date' in df_grouped to datetime (now it exists)
        df_grouped['date'] = pd.to_datetime(df_grouped['date'], errors='coerce')

        # Compute Week_of_Year with Saturday-Friday weeks
        df_grouped['Week_of_Year'] = (df_grouped['date'] - pd.Timedelta(days=-2)).dt.isocalendar().week
        # Calculate predicted next day percentage
        def predict_next_day(row):
            ticker_coef = regression_dict.get(row['ticker'])
            if ticker_coef:
                prediction = (
                    ticker_coef['intercept'] +
                    ticker_coef['ai_coef'] * row['AI_Score'] +
                    ticker_coef['sentiment_coef'] * row['Sentiment_Score'] +
                    ticker_coef['health_coef'] * row['Health_Score']
                )
                # Clip predictions to reasonable bounds
                return max(min(prediction, 0.05), -0.05)
            return None

        df_grouped['Predicted_Price_Movement'] = df_grouped.apply(predict_next_day, axis=1)

        # Continue with existing calculations
        df_grouped['Price_Movement_Today'] = df_grouped['Price_Movement_Today'].fillna(0)
        
        def calculate_4week_avg(group):
            """Calculate the average TS_Score for the last 4 weeks per ticker, excluding the current week."""
            group = group.sort_values(by="Week_of_Year")  # Ensure correct order

            def last_4_week_avg(row):
                last_4_weeks = group[
                    (group['Week_of_Year'] < row['Week_of_Year']) &  # Exclude current week
                    (group['Week_of_Year'] >= row['Week_of_Year'] - 4)  # Include only last 4 weeks
                ]
                return last_4_weeks['TS_Score'].mean() if not last_4_weeks.empty else None

            group['TS_Score_4Week'] = group.apply(last_4_week_avg, axis=1)

            return group

        # Apply function grouped by ticker
        df_grouped = df_grouped.groupby('ticker', group_keys=False).apply(calculate_4week_avg)

        # Rank stocks based on the 4-week average (higher average = better rank)
        df_grouped['TS_Rank_4Week'] = df_grouped.groupby('date')['TS_Score_4Week'].rank(
            method='min',
            ascending=False  # Higher scores get better ranks
        )

     
        # Calculate week-over-week change (last Friday vs previous Friday)
        def calculate_friday_change(group):
            # Get the last date in the data
            last_date = group['date'].max()
            
            # Find the last Friday
            days_to_friday = (last_date.dayofweek - 4) % 7
            last_friday = last_date - pd.Timedelta(days=days_to_friday)
            
            # Find the previous Friday
            prev_friday = last_friday - pd.Timedelta(weeks=1)
            
            # Get scores for these dates
            last_friday_score = group[group['date'] == last_friday]['TS_Score'].iloc[0] if len(group[group['date'] == last_friday]) > 0 else None
            prev_friday_score = group[group['date'] == prev_friday]['TS_Score'].iloc[0] if len(group[group['date'] == prev_friday]) > 0 else None
            
            if last_friday_score is not None and prev_friday_score is not None:
                pct_change = ((last_friday_score - prev_friday_score) / prev_friday_score) * 100
            else:
                pct_change = 0
                
            # Apply the change to all rows
            return pd.Series(pct_change, index=group.index)

        # Calculate and rank the Friday-to-Friday change
        df_grouped['TS_Friday_Change'] = df_grouped.groupby('ticker').apply(
            calculate_friday_change
        ).reset_index(level=0, drop=True)

        df_grouped['TS_Rank_Friday_Change'] = df_grouped.groupby('date')['TS_Friday_Change'].rank(
            method='min',
            ascending=False  # Higher change gets better ranks (lower numbers)
        )

        # Calculate 4-week composite score (average of both ranks)
        df_grouped['Composite_Rank_4Week'] = (df_grouped['TS_Rank_4Week'] + df_grouped['TS_Rank_Friday_Change']) / 2
        
        #####################################################
        # Get the most recent Composite_Rank_4Week for each ticker in each week
        latest_rank = (
            df_grouped.groupby(['Week_of_Year', 'ticker'])['Composite_Rank_4Week']
            .last()
            .reset_index()
        )

        # Calculate the sum of Price_Movement_Today for each ticker within each week
        weekly_ticker_sums = (
            df_grouped.groupby(['Week_of_Year', 'ticker'])['Price_Movement_Today']
            .sum()
            .reset_index()
        )

        # Merge to get Composite Rank per ticker per week
        weekly_ticker_analysis = weekly_ticker_sums.merge(
            latest_rank, 
            on=['Week_of_Year', 'ticker']
        )

        # Get the top 10 tickers based on Composite_Rank_4Week and their individual sums
        weekly_top_10_sums = (
            weekly_ticker_analysis.groupby('Week_of_Year')
            .apply(lambda x: x.nsmallest(10, 'Composite_Rank_4Week'))
            .reset_index(drop=True)
            .rename(columns={'Price_Movement_Today': 'Weekly_Ticker_Movement'})
        )

        # Calculate the average movement for top 10 tickers per week
        weekly_averages = (
            weekly_top_10_sums.groupby('Week_of_Year')['Weekly_Ticker_Movement']
            .mean()
            .reset_index()
            .rename(columns={'Weekly_Ticker_Movement': 'Weekly_Ticker_Avg_Movement'})
        )

        # Merge the individual ticker movements and weekly averages back to the main dataframe
        df_grouped = df_grouped.merge(
            weekly_top_10_sums[['Week_of_Year', 'ticker', 'Weekly_Ticker_Movement']],
            on=['Week_of_Year', 'ticker'],
            how='left'
        )

        df_grouped = df_grouped.merge(
            weekly_averages,
            on='Week_of_Year',
            how='left'
        )

        # Fill NaN values with 0
        df_grouped['Weekly_Ticker_Movement'] = df_grouped['Weekly_Ticker_Movement'].fillna(0)
        df_grouped['Weekly_Ticker_Avg_Movement'] = df_grouped['Weekly_Ticker_Avg_Movement'].fillna(0)

        # Compute weekly cumulative sum for 2025 weeks based on average movement
        weekly_2025_returns = (
            df_grouped[df_grouped['date'] >= '2025-01-01']
            .groupby('Week_of_Year')['Weekly_Ticker_Avg_Movement']
            .first()
            .reset_index()
        )

        # Calculate cumulative sum of the averages
        weekly_2025_returns['Weekly_Ticker_Cumulative'] = weekly_2025_returns['Weekly_Ticker_Avg_Movement'].cumsum()

        # Merge back to main dataframe
        df_grouped = df_grouped.merge(
            weekly_2025_returns[['Week_of_Year', 'Weekly_Ticker_Cumulative']],
            on='Week_of_Year',
            how='left'
        )

        # Fill NaN values with 0 for pre-2025 dates
        df_grouped['Weekly_Ticker_Cumulative'] = df_grouped.apply(
            lambda row: row['Weekly_Ticker_Cumulative'] if row['date'].year >= 2025 else 0,
            axis=1
        )
        ########################################################
        df_grouped['TS_Score_7'] = df_grouped.groupby('ticker')['TS_Score'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean()
        )

        df_grouped['TS_Rank_7'] = df_grouped.groupby('date')['TS_Score_7'].rank(
            method='min',
            ascending=False
        )

        df_grouped['TS_Change'] = df_grouped.groupby('ticker')['TS_Score'].pct_change() * 100
        df_grouped['TS_Rank_Change'] = df_grouped.groupby('date')['TS_Change'].rank(
            method='min',
            ascending=False
        )

        df_grouped['Composite_Rank'] = (df_grouped['TS_Rank_7'] + df_grouped['TS_Rank_Change']) / 2
        
        # Calculate daily top 10 averages with new column names
       
       
        daily_top_10_avg_next = (
            df_grouped.groupby('date')
            .apply(lambda x: x.nsmallest(10, 'Composite_Rank')['Price_Movement_Tomorrow'].mean())
            .reset_index()
            .rename(columns={0: 'Top_10_Composite_Price_Movement_Tomorrow'})
        )
        
        # Fill NaN values with 0 for Top_10_Composite_Price_Movement_Tomorrow
        daily_top_10_avg_next['Top_10_Composite_Price_Movement_Tomorrow'] = (
            daily_top_10_avg_next['Top_10_Composite_Price_Movement_Tomorrow'].fillna(0)
        )

        daily_top_10_avg_today = (
            df_grouped.groupby('date')
            .apply(lambda x: x.nsmallest(10, 'Composite_Rank')['Price_Movement_Today'].mean())
            .reset_index()
            .rename(columns={0: 'Top_10_Composite_Price_Movement_Today'})
        )

        daily_top_10_predicted = (
            df_grouped.groupby('date')
            .apply(lambda x: x.nsmallest(10, 'Composite_Rank')['Predicted_Price_Movement'].mean())
            .reset_index()
            .rename(columns={0: 'Top_10_Predicted_Price_Movement'})
        )

        # Merge the daily averages back
        df_grouped = df_grouped.merge(daily_top_10_avg_next, on='date', how='left')
        df_grouped = df_grouped.merge(daily_top_10_avg_today, on='date', how='left')
        df_grouped = df_grouped.merge(daily_top_10_predicted, on='date', how='left')

        # Calculate cumulative sums for 2025
        df_2025 = df_grouped[df_grouped['date'] >= '2025-01-01'].copy()

        daily_cumulative_next = (
            df_2025.groupby('date')['Top_10_Composite_Price_Movement_Tomorrow']
            .mean()
            .cumsum()
            .reset_index()
            .rename(columns={'Top_10_Composite_Price_Movement_Tomorrow': 'Top_10_YTD_Cumulative_Tomorrow'})
        )

        daily_cumulative_today = (
            df_2025.groupby('date')['Top_10_Composite_Price_Movement_Today']
            .mean()
            .cumsum()
            .reset_index()
            .rename(columns={'Top_10_Composite_Price_Movement_Today': 'Top_10_YTD_Cumulative_Today'})
        )

        daily_cumulative_predicted = (
            df_2025.groupby('date')['Top_10_Predicted_Price_Movement']
            .mean()
            .cumsum()
            .reset_index()
            .rename(columns={'Top_10_Predicted_Price_Movement': 'Top_10_YTD_Cumulative_Predicted'})
        )

        # Merge cumulative sums
        df_grouped = df_grouped.merge(daily_cumulative_next, on='date', how='left')
        df_grouped = df_grouped.merge(daily_cumulative_today, on='date', how='left')
        df_grouped = df_grouped.merge(daily_cumulative_predicted, on='date', how='left')

        # Fill missing cumulative scores
        df_grouped['Top_10_YTD_Cumulative_Tomorrow'].fillna(0, inplace=True)
        df_grouped['Top_10_YTD_Cumulative_Today'].fillna(0, inplace=True)
        df_grouped['Top_10_YTD_Cumulative_Predicted'].fillna(0, inplace=True)

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
        logging.error(f"Error in processing: {str(e)}")
        return jsonify({"error": str(e)}), 500