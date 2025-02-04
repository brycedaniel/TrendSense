import pandas as pd
from datetime import datetime
import os

file_path_1 = "Table_1_Combined_Clean.csv"
file_path_2 = "Table_2_Filter_Transform.csv"

df1 = pd.read_csv(file_path_1)
df2 = pd.read_csv(file_path_2) if os.path.exists(file_path_2) else pd.DataFrame()

# Create Unique_ID in original dataset if not already present
if 'Unique_ID' not in df1.columns:
    df1['Unique_ID'] = df1['ticker'] + '_' + pd.to_datetime(df1['publish_date']).dt.strftime('%Y-%m-%d_%H:%M:%S')

# If Tabel_2 is empty, process entire dataset
if df2.empty:
    df = df1
    new_rows = len(df1)
else:
    # Find rows in df1 not in df2 based on Unique_ID
    df = df1[~df1['Unique_ID'].isin(df2['Unique_ID'])]
    new_rows = len(df)

print(f"Processing {new_rows} new rows")

# Rename columns to reflect their intended meanings
df.rename(columns={
    'average_sentiment': 'article_sentiment',  
    'daily_average_sentiment': 'average_market_sentiment',  
    'Average_Market_Change': 'average_market_percent_change'  
}, inplace=True)

# Ensure publish_date is in datetime format and create a date-only column
df['publish_date'] = pd.to_datetime(df['publish_date'])
df['publish_date_date'] = df['publish_date'].dt.date  # Extract the date for grouping

# Add the daily_avg_ticker_sentiment column
if 'article_sentiment' in df.columns and 'ticker' in df.columns:
    df['daily_avg_ticker_sentiment'] = df.groupby(
        ['publish_date_date', 'ticker']
    )['article_sentiment'].transform('mean')

# Define a function to classify sentiment into categories
def classify_sentiment(value):
    if value >= 0.4:
        return 'Bullish'
    elif -0.2 <= value < 0.4:
        return 'Neutral'
    else:
        return 'Bearish'

# Add classification columns for sentiments
df['article_sentiment_class'] = df['article_sentiment'].apply(classify_sentiment)
df['daily_sentiment_class'] = df['daily_avg_ticker_sentiment'].apply(classify_sentiment)
df['average_market_sentiment_class'] = df['average_market_sentiment'].apply(classify_sentiment)

# Add columns for Close and Day_Percent_Change
if 'Close' in df.columns and 'Open' in df.columns:
    df['Day_Percent_Change'] = ((df['Close'] - df['Open']) / df['Open'] * 100).round(2)

# Add Next_Day_Percent_Change and populate all rows on the same date with the next day's value
if 'Day_Percent_Change' in df.columns:
    next_day_change = df.groupby(['ticker', 'publish_date_date'])['Day_Percent_Change'].mean().reset_index()
    next_day_change['Next_Day_Percent_Change'] = next_day_change.groupby('ticker')['Day_Percent_Change'].shift(-1)
    df = df.merge(
        next_day_change[['ticker', 'publish_date_date', 'Next_Day_Percent_Change']],
        on=['ticker', 'publish_date_date'],
        how='left'
    )
# Add publisher scores dictionary
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

# Map publisher scores to a new column
df["publisher score"] = df["publisher"].map(publisher_scores)


# Add AI Score column (Placeholder with current minute)
df['AI Score'] = datetime.now().minute

# Define the mapping for RatingScore
def map_rating_score(value):
    if value == 5:
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
        return None  # For any unexpected values

# Add the RatingScore_Category column
if 'RatingScore' in df.columns:
    df['RatingScore_Category'] = df['RatingScore'].apply(map_rating_score)

# Add RatingScoreStatus column
if 'RatingScore' in df.columns:
    daily_avg = df.groupby(['ticker', 'publish_date_date'])['RatingScore'].mean().reset_index()
    daily_avg.rename(columns={'RatingScore': 'Daily_Avg_RatingScore'}, inplace=True)
    daily_avg['Previous_Day_Avg_RatingScore'] = daily_avg.groupby('ticker')['Daily_Avg_RatingScore'].shift(1)
    daily_avg['RatingScoreStatus'] = daily_avg.apply(
        lambda row: (
            "Upgrade" if row['Daily_Avg_RatingScore'] > row['Previous_Day_Avg_RatingScore'] else
            "Down Grade" if row['Daily_Avg_RatingScore'] < row['Previous_Day_Avg_RatingScore'] else
            "No Change"
        ), axis=1
    )
    df = df.merge(daily_avg[['ticker', 'publish_date_date', 'RatingScoreStatus']],
                  on=['ticker', 'publish_date_date'], how='left')

# Add AnalystScoreStatus column
if 'analyst_score' in df.columns:
    daily_analyst_avg = df.groupby(['ticker', 'publish_date_date'])['analyst_score'].mean().reset_index()
    daily_analyst_avg.rename(columns={'analyst_score': 'Daily_Avg_AnalystScore'}, inplace=True)
    daily_analyst_avg['Previous_Day_Avg_AnalystScore'] = daily_analyst_avg.groupby('ticker')['Daily_Avg_AnalystScore'].shift(1)
    daily_analyst_avg['AnalystScoreStatus'] = daily_analyst_avg.apply(
        lambda row: (
            "Upgrade" if row['Daily_Avg_AnalystScore'] > row['Previous_Day_Avg_AnalystScore'] else
            "Down Grade" if row['Daily_Avg_AnalystScore'] < row['Previous_Day_Avg_AnalystScore'] else
            "No Change"
        ), axis=1
    )
    df = df.merge(daily_analyst_avg[['ticker', 'publish_date_date', 'AnalystScoreStatus']],
                  on=['ticker', 'publish_date_date'], how='left')

if 'target_median_price' in df.columns:
    daily_price_avg = df.groupby(['ticker', 'publish_date_date'])['target_median_price'].mean().reset_index()
    daily_price_avg.rename(columns={'target_median_price': 'Daily_Avg_TargetMedianPrice'}, inplace=True)
    
    # Create Previous Day's Average Target Median Price
    daily_price_avg['Previous_Day_Avg_TargetMedianPrice'] = daily_price_avg.groupby('ticker')['Daily_Avg_TargetMedianPrice'].shift(1)

    # Calculate Percentage Change
    daily_price_avg['Target_Pct_Change'] = (
        (daily_price_avg['Daily_Avg_TargetMedianPrice'] - daily_price_avg['Previous_Day_Avg_TargetMedianPrice']) 
        / daily_price_avg['Previous_Day_Avg_TargetMedianPrice']
    ) * 100

    # Add Price Change Status
    daily_price_avg['PriceChangeStatus'] = daily_price_avg.apply(
        lambda row: (
            "Increase" if row['Daily_Avg_TargetMedianPrice'] > row['Previous_Day_Avg_TargetMedianPrice'] else
            "Decrease" if row['Daily_Avg_TargetMedianPrice'] < row['Previous_Day_Avg_TargetMedianPrice'] else
            "No Change"
        ), axis=1
    )

    # Merge back into original DataFrame
    df = df.merge(
        daily_price_avg[['ticker', 'publish_date_date', 'PriceChangeStatus', 'Target_Pct_Change']],
        on=['ticker', 'publish_date_date'], how='left'
    )


# Add ValuationStatus column
if 'target_score' in df.columns:
    def map_valuation_status(value):
        if 0 <= value <= 3:
            return "Slightly Overvalued"
        elif value > 3:
            return "Overvalued"
        elif 0 > value >= -3:
            return "Slightly Undervalued"
        elif value < -3:
            return "Undervalued"
        else:
            return None  # For any unexpected values

    df['ValuationStatus'] = df['target_score'].apply(map_valuation_status)

# Define the columns to include in the final cleaned DataFrame, moving Close, Day_Percent_Change, and Next_Day_Percent_Change to the end
columns_to_include = [
    'Unique_ID',
    'publish_date',
    'ticker',
    'publisher',
    'title',
    'article_sentiment',
    'article_sentiment_class',
    'daily_avg_ticker_sentiment',
    'daily_sentiment_class',
    'average_market_sentiment',
    'average_market_sentiment_class',
    'average_market_percent_change',
    'RatingScore',
    'RatingScore_Category',
    'RatingScoreStatus',
    'analyst_score',
    'AnalystScoreStatus',
    'PriceChangeStatus',
    'Target_Pct_Change',
    'target_score',
    'ValuationStatus',
    'Forward_15min_Change_Diff',
    'Forward_30min_Change_Diff',
    'Forward_45min_Change_Diff',
    'Forward_60min_Change_Diff',
    'Close',  # Move to the end
    'Day_Percent_Change',  # Move to the end
    'Next_Day_Percent_Change',  # Move to the end
    'Forward_60min_Change',
    'AI Score',
    'publisher score'
]

# Filter the DataFrame to include only the specified columns
filtered_df = df[columns_to_include]

# Save the cleaned DataFrame to a new CSV file
output_file_path = "Table_2_Filter_Transform.csv"
# Append the DataFrame to the existing CSV file
filtered_df.to_csv(output_file_path, mode='a', index=False, header=not os.path.exists(output_file_path))

print(f"Updated CSV  {output_file_path}")
print(f"Completed processing {new_rows} new rows")