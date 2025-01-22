
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime
# Initialize BigQuery client
client = bigquery.Client()

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
    -- Add new columns from stock_data_history
    sdh.Close,
    sdh.Volume,
    sdh.High,
    sdh.Low,
    sdh.Open,
    sdh.Percent_Difference as Daily_Percent_Difference,
    -- Calculate Analyst Score
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
-- Add new join for stock_data_history
LEFT JOIN
    `trendsense.stock_data.stock_data_history` sdh
ON
    c.ticker = sdh.Ticker AND 
    c.date_only = FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP(sdh.Date))
ORDER BY
    c.ticker,
    c.hourly_date
"""
# Run the query
job = client.query(query)

# Convert results to a DataFrame
df = job.to_dataframe()

# Save to CSV locally
df.to_csv('Combined_Raw.csv', index=False)