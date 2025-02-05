from google.cloud import bigquery
import pandas as pd
import os

def process_data(request):
    """
    Google Cloud Function to process data and export to BigQuery
    
    Args:
        event (dict): Event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    # Initialize BigQuery client
    client = bigquery.Client()

    # SQL query (same as original script)
    query = """
    WITH Combined_Table AS (
        SELECT
            COALESCE(a.ticker, b.ticker) AS ticker,
            COALESCE(a.publish_date, b.publish_date) AS publish_date,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_SECONDS(
                CAST(
                    ROUND(
                        UNIX_SECONDS(TIMESTAMP(COALESCE(a.publish_date, b.publish_date))) / 900
                    ) * 900 AS INT64
                )
            )) AS hourly_date,
            FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_SECONDS(
                CAST(
                    ROUND(
                        UNIX_SECONDS(TIMESTAMP(COALESCE(a.publish_date, b.publish_date))) / 900
                    ) * 900 AS INT64
                )
            )) AS date_only,
            EXTRACT(WEEK FROM TIMESTAMP_SECONDS(
                CAST(
                    ROUND(
                        UNIX_SECONDS(TIMESTAMP(COALESCE(a.publish_date, b.publish_date))) / 900
                    ) * 900 AS INT64
                )
            )) AS week_of_year,
            COALESCE(a.source, b.source) AS source,
            COALESCE(a.publisher, b.publisher) AS publisher,
            COALESCE(a.title, b.title) AS title,
            COALESCE(a.link, b.link) AS link,
            COALESCE(a.textblob_sentiment, b.textblob_sentiment) AS textblob_sentiment,
            COALESCE(a.vader_sentiment, b.vader_sentiment) AS vader_sentiment,
            COALESCE(a.bert_sentiment, b.bert_sentiment) AS bert_sentiment,
            COALESCE(a.bert_confidence, b.bert_confidence) AS bert_confidence,
            COALESCE(a.reliability_score, b.reliability_score) AS reliability_score,
            COALESCE(a.word_count, b.word_count) AS word_count,
            COALESCE(a.related_tickers, b.related_tickers) AS related_tickers,
            COALESCE(a.summary, b.summary) AS summary
        FROM
            `trendsense.market_data.Market_News_AY` AS a
        FULL OUTER JOIN
            `trendsense.market_data.Market_News_NAPI` AS b
        ON
            a.ticker = b.ticker AND a.publish_date = b.publish_date
        WHERE
            COALESCE(a.ticker, b.ticker) IN ('AAPL', 'GOOGL', 'MSFT', 'ASTS', 'PTON', 'GSAT', 'PLTR', 'SMR', 'ACHR',
                                             'BWXT', 'ARBK', 'AMD', 'NVDA', 'BTC', 'GME', 'MU', 'TSLA', 'NFLX', 'ZG',
                                             'AVGO', 'SMCI', 'GLW', 'HAL', 'LMT', 'AMZN', 'CRM', 'NOW', 'CHTR', 'TDS',
                                             'META', 'RGTI', 'QUBT', 'LX', 'OKLO', 'PSIX', 'QFIN', 'RTX', 'TWLO',
                                             '^IXIC', '^DJI', '^RUT', '^GSPC')
    )

    SELECT 
        c.*,
        s1.Current_Price,
        s1.Percent_Difference,
        s2.Percent_Difference as Forward_15min_Change,
        s3.Percent_Difference as Forward_30min_Change,
        s4.Percent_Difference as Forward_45min_Change,
        s5.Percent_Difference as Forward_60min_Change,
        sr.RatingScore,
        sa.Strong_Buy,
        sa.Buy,
        sa.Hold,
        sa.Sell,
        sa.Strong_Sell,
        sdh.Close,
        sdh.Volume,
        sdh.High,
        sdh.Low,
        sdh.Open,
        (
            (sa.Strong_Buy * 100) +
            (sa.Buy * 75) +
            (sa.Hold * 50) +
            (sa.Sell * 25) +
            (sa.Strong_Sell * 0)
        ) / 
        NULLIF(sa.Strong_Buy + sa.Buy + sa.Hold + sa.Sell + sa.Strong_Sell, 0) AS analyst_score,
        (
            (sa.Target_High_Price - s1.Current_Price) /
            NULLIF(s1.Current_Price, 0)
        ) * 100 AS reward_score,
        (
            (sa.Target_Low_Price - s1.Current_Price) /
            NULLIF(s1.Current_Price, 0)
        ) * 100 AS risk_score,
        (
            (sa.Target_Median_Price - s1.Current_Price) /
            NULLIF(s1.Current_Price, 0)
        ) * 100 AS target_score,
        sa.target_median_price
    FROM
        Combined_Table c
    LEFT JOIN
        `trendsense.stock_data.current_stock_data` s1
    ON
        c.ticker = s1.Ticker AND 
        c.hourly_date = FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP(s1.Date))
    LEFT JOIN
        `trendsense.stock_data.current_stock_data` s2
    ON
        c.ticker = s2.Ticker AND 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_ADD(TIMESTAMP(s1.Date), INTERVAL 15 MINUTE)) = 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP(s2.Date))
    LEFT JOIN
        `trendsense.stock_data.current_stock_data` s3
    ON
        c.ticker = s3.Ticker AND 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_ADD(TIMESTAMP(s1.Date), INTERVAL 30 MINUTE)) = 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP(s3.Date))
    LEFT JOIN
        `trendsense.stock_data.current_stock_data` s4
    ON
        c.ticker = s4.Ticker AND 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_ADD(TIMESTAMP(s1.Date), INTERVAL 45 MINUTE)) = 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP(s4.Date))
    LEFT JOIN
        `trendsense.stock_data.current_stock_data` s5
    ON
        c.ticker = s5.Ticker AND 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_ADD(TIMESTAMP(s1.Date), INTERVAL 60 MINUTE)) = 
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP(s5.Date))
    LEFT JOIN
        `trendsense.stock_data.stock_ratings` sr
    ON
        c.ticker = sr.Symbol AND 
        c.date_only = FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(sr.Date))
    LEFT JOIN
        `trendsense.stock_data.stock_analyst` sa
    ON
        c.ticker = sa.symbol AND 
        CAST(c.date_only AS DATE) = sa.fetch_date
    LEFT JOIN
        `trendsense.stock_data.stock_data_history` sdh
    ON
        c.ticker = sdh.Ticker AND 
        c.date_only = FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(sdh.Date))
    ORDER BY
        c.ticker,
        c.hourly_date
    """

    def calculate_daily_pct_change(group, column):
        """
        Calculate daily percentage change for the specified column within the group.
        Ensures 'publish_date' is retained for mapping.
        """
        daily_values = group.groupby(group['publish_date'].dt.date).agg(
            {column: 'first'}
        ).reset_index()
        daily_values.rename(columns={'index': 'publish_date'}, inplace=True)
        
        daily_pct_change = daily_values[column].pct_change(fill_method=None).mul(100).round(2)
        daily_pct_change_map = dict(zip(daily_values['publish_date'], daily_pct_change))
        
        return group['publish_date'].dt.date.map(daily_pct_change_map)

    try:
        print("Executing BigQuery query...")
        # Run the query and convert results to a DataFrame
        job = client.query(query)
        df = job.to_dataframe()
        
        # Initial data processing
        print("Processing initial data...")
        columns_to_fill = ['Close', 'Volume', 'High', 'Low', 'Open']
        df[columns_to_fill] = df[columns_to_fill].ffill()
        
        df = df.sort_values(by=['ticker', 'date_only'])
        df['Daily_Percent_Difference'] = (
            df.groupby('ticker')['Close']
            .transform(lambda x: (x - x.shift(1)) / x.shift(1) * 100)
        )
        df['Daily_Percent_Difference'] = (
            df.groupby(['ticker', 'date_only'])['Daily_Percent_Difference']
            .transform('first')
        )
        
        # Filter and clean data
        print("Filtering and cleaning data...")
        df = df[df['word_count'] > 7]
        
        allowed_publishers = [
            "Yahoo Entertainment", "ETF Daily News", "Biztoc.com", "Decrypt",
            "The Wall Street Journal", "Investor's Business Daily", "GuruFocus.com",
            "Barrons.com", "Yahoo Finance", "Fortune", "Investopedia", "MT Newswires",
            "Benzinga", "Reuters", "Bloomberg", "Motley Fool", "Cult of Mac",
            "Macdailynews.com", "CNN Business", "TheStreet", "Forbes", "TipRanks",
            "Quartz", "Insider Monkey", "Zacks", "Investing.com", "MarketWatch",
            "Observer", "CNBC", "GlobeNewswire"
        ]
        
        if 'publisher' in df.columns:
            df = df[df['publisher'].isin(allowed_publishers)]
        
        # Drop unnecessary columns
        columns_to_drop = ['Strong_Buy', 'Buy', 'Hold', 'Sell', 'Strong_Sell',
                          'hourly_date', 'date_only', 'week_of_year']
        df = df.drop(columns=columns_to_drop, errors='ignore')
        
        # Convert and sort by publish date
        df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
        df = df.sort_values(by=['ticker', 'publish_date'])
        
        # Forward fill missing values
        fill_columns = [
            'RatingScore', 'analyst_score', 'reward_score', 'risk_score',
            'target_score', 'target_median_price', 'Close', 'Volume',
            'High', 'Low', 'Open'
        ]
        fill_columns = [col for col in fill_columns if col in df.columns]
        
        for col in fill_columns:
            df[col] = df.groupby('ticker')[col].transform(lambda group: group.ffill())
        
        # Calculate additional metrics
        print("Calculating additional metrics...")
        df['Day_Percent_Change'] = ((df['Close'] - df['Open']) / df['Open'] * 100).round(2)
        
        forward_columns = [
            'Forward_15min_Change', 'Forward_30min_Change',
            'Forward_45min_Change', 'Forward_60min_Change'
        ]
        
        if 'Percent_Difference' in df.columns:
            for column in forward_columns:
                if column in df.columns:
                    new_column_name = f'{column}_Diff'
                    df[new_column_name] = (df[column] - df['Percent_Difference'])
        
        # Calculate percentage changes
        pct_change_columns = [
            'RatingScore', 'analyst_score', 'target_score', 'target_median_price'
        ]
        
        for column in pct_change_columns:
            if column in df.columns:
                new_column = f'{column}_pct_change'
                df[new_column] = df.groupby('ticker', group_keys=False).apply(
                    lambda x: calculate_daily_pct_change(x, column)
                ).reset_index(level=0, drop=True)
        
        # Calculate sentiment metrics
        df['average_sentiment'] = df[['textblob_sentiment', 'vader_sentiment']].mean(axis=1)
        
        daily_avg_sentiment = df.groupby(df['publish_date'].dt.date)['average_sentiment'].mean().reset_index()
        daily_avg_sentiment.rename(columns={'average_sentiment': 'daily_average_sentiment'}, inplace=True)
        
        df['publish_date_date'] = df['publish_date'].dt.date
        df = df.merge(daily_avg_sentiment, left_on='publish_date_date', right_on='publish_date', how='left')
        df.drop(columns=['publish_date_y'], inplace=True, errors='ignore')
        df.rename(columns={'publish_date_x': 'publish_date'}, inplace=True)
        
        # Calculate market changes
        daily_avg_change = df.groupby(df['publish_date_date'])['Percent_Difference'].mean().reset_index()
        daily_avg_change.rename(columns={'Percent_Difference': 'Average_Market_Change'}, inplace=True)
        
        df = df.merge(daily_avg_change, on='publish_date_date', how='left')
        df.drop(columns=['publish_date_date'], inplace=True)
        
        # Add Unique ID column
        df['Unique_ID'] = df['ticker'] + '_' + df['publish_date'].dt.strftime('%Y-%m-%d_%H:%M:%S')

        # Prepare BigQuery table destination
        table_id = 'trendsense.combined_data.step_1_combine_clean'
        
        # Create a job config with auto-detect schema
        job_config = bigquery.LoadJobConfig(
            autodetect=True,  # Automatically detect schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # Overwrite existing table
        )
        
        
        # Upload DataFrame to BigQuery
        job = client.load_table_from_dataframe(
            dataframe=df, 
            destination=table_id, 
            job_config=job_config
        )
        job.result()  # Wait for the job to complete
        
        print("Data successfully uploaded to BigQuery.")
        return "Data processing completed successfully.", 200
    
    except Exception as e:
        print(f"Error: {e}")
        return f"Error processing data: {str(e)}", 500