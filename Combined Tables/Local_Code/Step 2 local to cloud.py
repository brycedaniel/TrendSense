


from google.cloud import bigquery
import pandas as pd
import numpy as np
from datetime import datetime
import pandas_gbq
import json
import re
from openai import OpenAI
import os



def transform_data():
    # Initialize BigQuery client
    # You'll need to set GOOGLE_APPLICATION_CREDENTIALS in your environment
    client = bigquery.Client()
    openai_client = OpenAI(api_key="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")
    
    # Define schema (same as before)
    schema = [
        bigquery.SchemaField("Unique_ID", "STRING"),
        bigquery.SchemaField("publish_date", "TIMESTAMP"),
        bigquery.SchemaField("ticker", "STRING"),
        bigquery.SchemaField("publisher", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("article_sentiment", "FLOAT"),
        bigquery.SchemaField("article_sentiment_class", "STRING"),
        bigquery.SchemaField("daily_avg_ticker_sentiment", "FLOAT"),
        bigquery.SchemaField("daily_sentiment_class", "STRING"),
        bigquery.SchemaField("average_market_sentiment", "FLOAT"),
        bigquery.SchemaField("average_market_sentiment_class", "STRING"),
        bigquery.SchemaField("average_market_percent_change", "FLOAT"),
        bigquery.SchemaField("RatingScore", "FLOAT"),
        bigquery.SchemaField("RatingScore_Category", "STRING"),
        bigquery.SchemaField("RatingScoreStatus", "STRING"),
        bigquery.SchemaField("analyst_score", "FLOAT"),
        bigquery.SchemaField("AnalystScoreStatus", "STRING"),
        bigquery.SchemaField("PriceChangeStatus", "STRING"),
        bigquery.SchemaField("Target_Pct_Change", "FLOAT"),
        bigquery.SchemaField("target_score", "FLOAT"),
        bigquery.SchemaField("ValuationStatus", "STRING"),
        bigquery.SchemaField("Forward_15min_Change_Diff", "FLOAT"),
        bigquery.SchemaField("Forward_30min_Change_Diff", "FLOAT"),
        bigquery.SchemaField("Forward_45min_Change_Diff", "FLOAT"),
        bigquery.SchemaField("Forward_60min_Change_Diff", "FLOAT"),
        bigquery.SchemaField("Close", "FLOAT"),
        bigquery.SchemaField("Day_Percent_Change", "FLOAT"),
        bigquery.SchemaField("Next_Day_Percent_Change", "FLOAT"),
        bigquery.SchemaField("Forward_60min_Change", "FLOAT"),
        bigquery.SchemaField("AI Score", "FLOAT"),
        bigquery.SchemaField("publisher score", "INTEGER")
    ]
    
    source_table = "trendsense.combined_data.step_1_combine_clean"
    destination_table = "trendsense.combined_data.step_2_transform_AI"
    
    # Read data from source table
    query = f"""
    SELECT t1.*
    FROM `{source_table}` t1
    LEFT JOIN `{destination_table}` t2
    ON t1.Unique_ID = t2.Unique_ID
    WHERE t2.Unique_ID IS NULL
    """
    
    try:
        # Check if destination table exists
        try:
            client.get_table(destination_table)
        except Exception:
            table = bigquery.Table(destination_table, schema=schema)
            client.create_table(table)
            
        df1 = pd.read_gbq(query, project_id='trendsense')
        new_rows = len(df1)

        if new_rows == 0:
            return json.dumps({"message": "No new data to process"}), 200  # ✅ Always returns a valid response    
        
            
        # Create Unique_ID if not present
        if 'Unique_ID' not in df1.columns:
            df1['Unique_ID'] = df1['ticker'] + '_' + pd.to_datetime(df1['publish_date']).dt.strftime('%Y-%m-%d_%H:%M:%S')
        
        df = df1.copy()  # Make a copy to avoid modifying the original
        
        # Clean NaN values in the dataframe
        df = df.replace([np.inf, -np.inf], np.nan)  # Replace infinite values with NaN
        
        # Rename columns
        df.rename(columns={
            'average_sentiment': 'article_sentiment',  
            'daily_average_sentiment': 'average_market_sentiment',  
            'Average_Market_Change': 'average_market_percent_change'  
        }, inplace=True)
        
        # DateTime processing
        df['publish_date'] = pd.to_datetime(df['publish_date'])
        df['publish_date_date'] = df['publish_date'].dt.date
        
        # Calculate daily average ticker sentiment
        if 'article_sentiment' in df.columns and 'ticker' in df.columns:
            df['daily_avg_ticker_sentiment'] = df.groupby(
                ['publish_date_date', 'ticker']
            )['article_sentiment'].transform('mean')
        
        # Sentiment classification function with NaN handling
        def classify_sentiment(value):
            if pd.isna(value):
                return 'Unknown'
            elif value >= 0.4:
                return 'Bullish'
            elif -0.2 <= value < 0.4:
                return 'Neutral'
            else:
                return 'Bearish'
        
        # Add sentiment classifications
        for col in ['article_sentiment', 'daily_avg_ticker_sentiment', 'average_market_sentiment']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['article_sentiment_class'] = df['article_sentiment'].apply(classify_sentiment)
        df['daily_sentiment_class'] = df['daily_avg_ticker_sentiment'].apply(classify_sentiment)
        df['average_market_sentiment_class'] = df['average_market_sentiment'].apply(classify_sentiment)
        
        # Calculate Day Percent Change with NaN handling
        if 'Close' in df.columns and 'Open' in df.columns:
            df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
            df['Open'] = pd.to_numeric(df['Open'], errors='coerce')
            df['Day_Percent_Change'] = ((df['Close'] - df['Open']) / df['Open'] * 100).round(2)
        
        # Calculate Next Day Percent Change
        if 'Day_Percent_Change' in df.columns:
            next_day_change = df.groupby(['ticker', 'publish_date_date'])['Day_Percent_Change'].mean().reset_index()
            next_day_change['Next_Day_Percent_Change'] = next_day_change.groupby('ticker')['Day_Percent_Change'].shift(-1)
            df = df.merge(
                next_day_change[['ticker', 'publish_date_date', 'Next_Day_Percent_Change']],
                on=['ticker', 'publish_date_date'],
                how='left'
            )
        
        # Publisher scores
        publisher_scores = {
            "Yahoo Entertainment": 2,
            "ETF Daily News": 5,
            "Biztoc.com": 4,
            "Decrypt": 5,
            "The Wall Street Journal": 10,
            "Investor's Business Daily": 8,
            "GuruFocus.com": 6,
            "Barrons.com": 9,
            "Yahoo Finance": 8,
            "Fortune": 7,
            "Investopedia": 7,
            "MT Newswires": 6,
            "Benzinga": 7,
            "Reuters": 10,
            "Bloomberg": 10,
            "Motley Fool": 7,
            "Cult of Mac": 4,
            "Macdailynews.com": 3,
            "CNN Business": 8,
            "TheStreet": 7,
            "Forbes": 7,
            "TipRanks": 7,
            "Quartz": 5,
            "Insider Monkey": 5,
            "Zacks": 7,
            "Investing.com": 7,
            "MarketWatch": 8,
            "Observer": 5,
            "CNBC": 9,
            "GlobeNewswire": 5
        }
        
        df["publisher score"] = df["publisher"].map(publisher_scores).fillna(0).astype(int)
        
        def extract_numeric_score(response):
            """Extracts the first valid float from the OpenAI response."""
            match = re.search(r"-?\d+(\.\d+)?", response)
            if match:
                return float(match.group(0))
            return None

        def get_financial_impact(title, ticker, openai_client):
            """Analyzes financial market impact from the perspective of a specific ticker."""
            try:
                completion = openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": "You are an expert financial analyst specializing in stock market movements."},
                        {"role": "user", "content": f"""
                        Evaluate the financial market impact of this headline from the perspective of **{ticker}**. Assign a **precise** numerical score between -10 and 10.

                        **GUIDELINES:**
                        - **Perspective Matters**: The score should reflect how this affects **{ticker}**, not just the industry as a whole.
                        - **Market Sentiment**: Weigh factors like stock movements, investor reactions, regulatory concerns, or sector trends.
                        - **STRICT FORMAT**: The response must contain only a **single** numerical value (e.g., -7.5, 3.2, 0). No explanations or extra text.

                        **News Headline:** "{title}"

                        **ONLY OUTPUT A SINGLE NUMBER:**"""}
                    ],
                    max_tokens=10,
                    temperature=0.5
                )

                raw_response = completion.choices[0].message.content.strip()
                
                score = extract_numeric_score(raw_response)
                if score is not None and -10 <= score <= 10:
                    return score
                else:
                    print(f"Invalid numeric response for '{title}' (Ticker: {ticker}): {raw_response}")
                    return None

            except Exception as e:
                print(f"Error processing title: {title} (Ticker: {ticker})\nError: {e}")
                return None
        
        
        df['AI Score'] = df.apply(lambda row: get_financial_impact(row['title'], row['ticker'], openai_client), axis=1)
        
        # Handle any None values from the API
        df['AI Score'] = df['AI Score'].fillna(0.0)
        
        # Ensure AI Score is float type
        df['AI Score'] = df['AI Score'].astype(float)
        
        # Rating score mapping with NaN handling
        def map_rating_score(value):
            if pd.isna(value):
                return "Unknown"
            elif value == 5:
                return "Great"
            elif value == 4:
                return "Good"
            elif value == 3:
                return "Neutral"
            elif value == 2:
                return "Bad"
            elif value == 1:
                return "Worst"
            else:
                return "Unknown"
        if 'RatingScore' in df.columns:
            df['RatingScore_Category'] = df['RatingScore'].apply(map_rating_score) 
            
        
        # Rating Score Status
        if 'RatingScore' in df.columns:
            df['RatingScore'] = pd.to_numeric(df['RatingScore'], errors='coerce')
            
            daily_avg = df.groupby(['ticker', 'publish_date_date'])['RatingScore'].mean().reset_index()
            daily_avg.rename(columns={'RatingScore': 'Daily_Avg_RatingScore'}, inplace=True)
            daily_avg['Previous_Day_Avg_RatingScore'] = daily_avg.groupby('ticker')['Daily_Avg_RatingScore'].shift(1)
            
            def determine_rating_status(row):
                current = row['Daily_Avg_RatingScore']
                previous = row['Previous_Day_Avg_RatingScore']
                
                if pd.isna(current) or pd.isna(previous):
                    return "Unknown"
                elif current > previous:
                    return "Upgrade"
                elif current < previous:
                    return "Down Grade"
                else:
                    return "No Change"
            
            daily_avg['RatingScoreStatus'] = daily_avg.apply(determine_rating_status, axis=1)
            
            df = df.merge(
                daily_avg[['ticker', 'publish_date_date', 'RatingScoreStatus']], 
                on=['ticker', 'publish_date_date'], 
                how='left'
            )
            df['RatingScoreStatus'] = df['RatingScoreStatus'].fillna('Unknown')
                
        # Analyst Score Status
        if 'analyst_score' in df.columns:
            # Convert to numeric and handle NaN
            df['analyst_score'] = pd.to_numeric(df['analyst_score'], errors='coerce')
            
            daily_analyst_avg = df.groupby(['ticker', 'publish_date_date'])['analyst_score'].mean().reset_index()
            daily_analyst_avg.rename(columns={'analyst_score': 'Daily_Avg_AnalystScore'}, inplace=True)
            daily_analyst_avg['Previous_Day_Avg_AnalystScore'] = daily_analyst_avg.groupby('ticker')['Daily_Avg_AnalystScore'].shift(1)
            
            # Handle NaN in the comparison
            daily_analyst_avg['AnalystScoreStatus'] = daily_analyst_avg.apply(
                lambda row: (
                    "Unknown" if pd.isna(row['Daily_Avg_AnalystScore']) or pd.isna(row['Previous_Day_Avg_AnalystScore']) else
                    "Upgrade" if row['Daily_Avg_AnalystScore'] > row['Previous_Day_Avg_AnalystScore'] else
                    "Down Grade" if row['Daily_Avg_AnalystScore'] < row['Previous_Day_Avg_AnalystScore'] else
                    "No Change"
                ), axis=1
            )
            df = df.merge(
                daily_analyst_avg[['ticker', 'publish_date_date', 'AnalystScoreStatus']],
                on=['ticker', 'publish_date_date'], 
                how='left'
            )
            df['AnalystScoreStatus'] = df['AnalystScoreStatus'].fillna('Unknown')

        # Price Change Status
        if 'target_median_price' in df.columns:
            # Convert to numeric and handle NaN
            df['target_median_price'] = pd.to_numeric(df['target_median_price'], errors='coerce')
            
            daily_price_avg = df.groupby(['ticker', 'publish_date_date'])['target_median_price'].mean().reset_index()
            daily_price_avg.rename(columns={'target_median_price': 'Daily_Avg_TargetMedianPrice'}, inplace=True)
            daily_price_avg['Previous_Day_Avg_TargetMedianPrice'] = daily_price_avg.groupby('ticker')['Daily_Avg_TargetMedianPrice'].shift(1)
            
            # Handle NaN in percentage calculation
            daily_price_avg['Target_Pct_Change'] = np.where(
                (daily_price_avg['Daily_Avg_TargetMedianPrice'].notna()) & 
                (daily_price_avg['Previous_Day_Avg_TargetMedianPrice'].notna()) & 
                (daily_price_avg['Previous_Day_Avg_TargetMedianPrice'] != 0),
                ((daily_price_avg['Daily_Avg_TargetMedianPrice'] - daily_price_avg['Previous_Day_Avg_TargetMedianPrice']) 
                / daily_price_avg['Previous_Day_Avg_TargetMedianPrice'] * 100),
                0
            )
            
            # Handle NaN in status comparison
            daily_price_avg['PriceChangeStatus'] = daily_price_avg.apply(
                lambda row: (
                    "Unknown" if pd.isna(row['Daily_Avg_TargetMedianPrice']) or pd.isna(row['Previous_Day_Avg_TargetMedianPrice']) else
                    "Increase" if row['Daily_Avg_TargetMedianPrice'] > row['Previous_Day_Avg_TargetMedianPrice'] else
                    "Decrease" if row['Daily_Avg_TargetMedianPrice'] < row['Previous_Day_Avg_TargetMedianPrice'] else
                    "No Change"
                ), axis=1
            )
            df = df.merge(
                daily_price_avg[['ticker', 'publish_date_date', 'PriceChangeStatus', 'Target_Pct_Change']],
                on=['ticker', 'publish_date_date'], 
                how='left'
            )
            df['PriceChangeStatus'] = df['PriceChangeStatus'].fillna('Unknown')
            df['Target_Pct_Change'] = df['Target_Pct_Change'].fillna(0)

        # Valuation Status
        if 'target_score' in df.columns:
            # Convert to numeric and handle NaN
            df['target_score'] = pd.to_numeric(df['target_score'], errors='coerce')
    
            def map_valuation_status(value):
                if pd.isna(value):
                    return "Unknown"
                elif 0 <= value <= 3:
                    return "Slightly Overvalued"
                elif value > 3:
                    return "Overvalued"
                elif 0 > value >= -3:
                    return "Slightly Undervalued"
                elif value < -3:
                    return "Undervalued"
                else:
                    return "Unknown"
            
            df['ValuationStatus'] = df['target_score'].apply(map_valuation_status)

        
        # Define columns to include
        columns_to_include = [
            'Unique_ID', 'publish_date', 'ticker', 'publisher', 'title',
            'article_sentiment', 'article_sentiment_class', 'daily_avg_ticker_sentiment',
            'daily_sentiment_class', 'average_market_sentiment',
            'average_market_sentiment_class', 'average_market_percent_change',
            'RatingScore', 'RatingScore_Category', 'RatingScoreStatus',
            'analyst_score', 'AnalystScoreStatus', 'PriceChangeStatus',
            'Target_Pct_Change', 'target_score', 'ValuationStatus',
            'Forward_15min_Change_Diff', 'Forward_30min_Change_Diff',
            'Forward_45min_Change_Diff', 'Forward_60min_Change_Diff',
            'Close', 'Day_Percent_Change', 'Next_Day_Percent_Change',
            'Forward_60min_Change', 'AI Score', 'publisher score'
        ]
        
        # Write to BigQuery with explicit schema
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="APPEND",
        )
        
        # Ensure data types match schema before writing
        df['publish_date'] = pd.to_datetime(df['publish_date'])
        df['AI Score'] = df['AI Score'].astype(int)
        df['publisher score'] = df['publisher score'].astype(int)
        
        # Convert float columns
        float_columns = [
            'article_sentiment', 'daily_avg_ticker_sentiment', 'average_market_sentiment',
            'average_market_percent_change', 'RatingScore', 'analyst_score', 'Target_Pct_Change',
            'target_score', 'Forward_15min_Change_Diff', 'Forward_30min_Change_Diff',
            'Forward_45min_Change_Diff', 'Forward_60min_Change_Diff', 'Close',
            'Day_Percent_Change', 'Next_Day_Percent_Change', 'Forward_60min_Change', 'AI Score'
        ]
        
        for col in float_columns:
            if col in df.columns:
                df[col] = df[col].astype(float)
        
        # Convert string columns
        string_columns = [
            'Unique_ID', 'ticker', 'publisher', 'title', 'article_sentiment_class',
            'daily_sentiment_class', 'average_market_sentiment_class', 'RatingScore_Category',
            'RatingScoreStatus', 'AnalystScoreStatus', 'PriceChangeStatus', 'ValuationStatus'
        ]
        
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
        if isinstance(schema, list) and all(isinstance(field, bigquery.SchemaField) for field in schema):
            column_names = [field.name for field in schema]
        else:
            raise TypeError("Schema is not a valid list of SchemaField objects")

        
        
        
        filtered_columns = [col for col in column_names if col in df.columns]  # Ensure they exist
        filtered_df = df[filtered_columns]  # Select only available columns

        
        print("Writing data to BigQuery...")
        # Write to BigQuery
        pandas_gbq.to_gbq(
            dataframe=filtered_df,
            destination_table='combined_data.step_2_transform_AI',
            project_id='trendsense',
            if_exists='append',
            progress_bar=True  # Changed to True for local monitoring
        )
        
        print(f"Successfully processed and loaded {new_rows} rows to BigQuery")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise e

if __name__ == "__main__":
    transform_data()