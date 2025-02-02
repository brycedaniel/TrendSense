import pandas as pd

# Load the original CSV file
file_path = "Combined_Clean_2.csv"  # Replace with the actual file path if needed
df = pd.read_csv(file_path)  # Read the dataset into a DataFrame

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
df['daily_ticker_sentiment_class'] = df['daily_avg_ticker_sentiment'].apply(classify_sentiment)
df['average_market_sentiment_class'] = df['average_market_sentiment'].apply(classify_sentiment)

# Use Close and Daily_Percent_Difference directly from the source table
if 'Close' not in df.columns or 'Daily_Percent_Difference' not in df.columns:
    raise ValueError("Source data is missing the required 'Close' or 'Daily_Percent_Difference' columns.")

# Add Next_Day_Percent_Change and populate all rows on the same date with the next day's value
if 'Daily_Percent_Difference' in df.columns:
    next_day_change = df.groupby(['ticker', 'publish_date_date'])['Daily_Percent_Difference'].mean().reset_index()
    next_day_change['Next_Day_Percent_Change'] = next_day_change.groupby('ticker')['Daily_Percent_Difference'].shift(-1)
    df = df.merge(
        next_day_change[['ticker', 'publish_date_date', 'Next_Day_Percent_Change']],
        on=['ticker', 'publish_date_date'],
        how='left'
    )
# Fill zeros in 'Next_Day_Percent_Change' with the following non-zero value
def fill_with_next_value(series):
    # Use forward-fill method to propagate the next non-zero value backward
    filled_series = series.replace(0, method='bfill')
    return filled_series

# Apply the function to 'Next_Day_Percent_Change' for each group
df['Next_Day_Percent_Change'] = (
    df.groupby('ticker', group_keys=False)['Next_Day_Percent_Change']
    .apply(fill_with_next_value)
)



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

# Add PriceChangeStatus based on target_median_price
if 'target_median_price' in df.columns:
    daily_price_avg = df.groupby(['ticker', 'publish_date_date'])['target_median_price'].mean().reset_index()
    daily_price_avg.rename(columns={'target_median_price': 'Daily_Avg_TargetMedianPrice'}, inplace=True)
    daily_price_avg['Previous_Day_Avg_TargetMedianPrice'] = daily_price_avg.groupby('ticker')['Daily_Avg_TargetMedianPrice'].shift(1)
    daily_price_avg['PriceChangeStatus'] = daily_price_avg.apply(
        lambda row: (
            "Increase" if row['Daily_Avg_TargetMedianPrice'] > row['Previous_Day_Avg_TargetMedianPrice'] else
            "Decrease" if row['Daily_Avg_TargetMedianPrice'] < row['Previous_Day_Avg_TargetMedianPrice'] else
            "No Change"
        ), axis=1
    )
    df = df.merge(daily_price_avg[['ticker', 'publish_date_date', 'PriceChangeStatus']],
                  on=['ticker', 'publish_date_date'], how='left')

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

# Define the columns to include in the final cleaned DataFrame, moving Close, Daily_Percent_Difference, and Next_Day_Percent_Change to the end
columns_to_include = [
    'publish_date',
    'ticker',
    'publisher',
    'article_sentiment',
    'article_sentiment_class',
    'daily_avg_ticker_sentiment',
    'daily_ticker_sentiment_class',
    'average_market_sentiment',
    'average_market_sentiment_class',
    'average_market_percent_change',
    'RatingScore',
    'RatingScore_Category',
    'RatingScoreStatus',
    'analyst_score',
    'AnalystScoreStatus',
    'PriceChangeStatus',
    'target_score',
    'ValuationStatus',
    'Forward_15min_Change_Diff',
    'Forward_30min_Change_Diff',
    'Forward_45min_Change_Diff',
    'Forward_60min_Change_Diff',
    'Close',  # Move to the end
    'Daily_Percent_Difference',  # Move to the end
    'Next_Day_Percent_Change'  # Move to the end
]

# Filter the DataFrame to include only the specified columns
filtered_df = df[columns_to_include]

# Save the cleaned DataFrame to a new CSV file
output_file_path = "Per_Article_TableNA.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"Updated CSV file with Close, Daily_Percent_Difference, and Next_Day_Percent_Change saved as {output_file_path}")













