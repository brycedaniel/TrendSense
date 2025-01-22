import pandas as pd

# Load the original CSV file
file_path = "Combined_Clean_1.csv"  # Replace with the actual file path if needed
df = pd.read_csv(file_path)  # Read the dataset into a DataFrame

# Convert 'publish_date' to datetime and extract only the date
# This ensures grouping is done by the date (not time) portion of 'publish_date'
df['publish_date'] = pd.to_datetime(df['publish_date']).dt.date

# Rename columns to reflect their intended meanings
# This ensures column names are consistent with their use and improves clarity for analysis
df.rename(columns={
    'average_sentiment': 'article_sentiment',  # Reflects the sentiment of individual articles
    'daily_average_sentiment': 'average_market_sentiment',  # Reflects the overall market sentiment on a daily basis
    'Average_Market_Change': 'average_market_percent_change'  # Tracks daily percent changes in market indicators
}, inplace=True)

# Define a function to classify sentiment into categories
# This categorization helps in understanding sentiment trends more intuitively
def classify_sentiment(value):
    if value >= 0.4:
        return 'Bullish'  # Indicates strong positive sentiment
    elif -0.2 <= value < 0.4:
        return 'Neutral'  # Indicates moderate or unclear sentiment
    else:
        return 'Bearish'  # Indicates strong negative sentiment

# Add classification columns for sentiments
# These new columns provide actionable insights for decision-making
df['article_sentiment_class'] = df['article_sentiment'].apply(classify_sentiment)
df['average_market_sentiment_class'] = df['average_market_sentiment'].apply(classify_sentiment)

# Define the columns to include in aggregation
# These are the numerical columns for which averages are calculated
columns_to_average = [
    'article_sentiment',  # Individual article sentiment score
    'average_market_sentiment',  # Daily average sentiment across the market
    'average_market_percent_change',  # Daily market percent change
    'RatingScore_pct_change',  # Percentage change in the rating score
    'analyst_score_pct_change',  # Percentage change in the analyst score
    'target_median_price_pct_change',  # Percentage change in the median target price
    'target_score',  # Target score for predictions or evaluations
    'Day_Percent_Change'  # Include daily percentage change
]

# Group by 'publish_date' and 'ticker' and calculate the mean for numerical columns
# This step aggregates data on a per-day and per-ticker basis
daily_ticker_aggregated_df = df.groupby(['publish_date', 'ticker'])[columns_to_average].mean().reset_index()

# Optionally, add classifications for the aggregated sentiment columns
daily_ticker_aggregated_df['article_sentiment_class'] = daily_ticker_aggregated_df['article_sentiment'].apply(classify_sentiment)
daily_ticker_aggregated_df['average_market_sentiment_class'] = daily_ticker_aggregated_df['average_market_sentiment'].apply(classify_sentiment)

# Save the daily and ticker-aggregated DataFrame to a new CSV file
# This step outputs the processed data for further use, ensuring the work is reproducible and accessible
output_file_path = "Daily_Ticker_Table.csv"
daily_ticker_aggregated_df.to_csv(output_file_path, index=False)  # Save without the index column

# Inform the user that the new file has been saved successfully
print(f"New CSV file with daily averages grouped by ticker saved as {output_file_path}")