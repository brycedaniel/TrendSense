import pandas as pd

# Load the original CSV file containing raw data
file_path = "Combined_Clean_1.csv"
df = pd.read_csv(file_path)

# Convert 'publish_date' to datetime and extract only the date for consistent date handling
df['publish_date'] = pd.to_datetime(df['publish_date']).dt.date

# Rename columns to more descriptive and meaningful names for clarity
df.rename(columns={
    'average_sentiment': 'article_sentiment',
    'daily_average_sentiment': 'average_market_sentiment',
    'Average_Market_Change': 'average_market_percent_change',
    'Day_Percent_Change': 'Yesterday_Percent_Change'
}, inplace=True)

# Define sentiment classification function to categorize sentiment scores
# This helps in quickly understanding the sentiment polarity
def classify_sentiment(value):
    if value >= 0.4:
        return 'Bullish'
    elif -0.2 <= value < 0.4:
        return 'Neutral'
    else:
        return 'Bearish'

# Add categorical sentiment columns for easier interpretation of sentiment scores
df['article_sentiment_class'] = df['article_sentiment'].apply(classify_sentiment)
df['average_market_sentiment_class'] = df['average_market_sentiment'].apply(classify_sentiment)

# Define columns to be aggregated, focusing on numerical sentiment and performance metrics
columns_to_average = [
    'article_sentiment',
    'average_market_sentiment',
    'average_market_percent_change',
    'RatingScore_pct_change',
    'analyst_score_pct_change',
    'target_median_price_pct_change',
    'target_score',
    'Yesterday_Percent_Change'
]

# Group data by publish date and ticker, calculating mean values to reduce noise and get daily insights
daily_ticker_aggregated_df = df.groupby(['publish_date', 'ticker'])[columns_to_average].mean().reset_index()

# Sort the dataframe to ensure correct time-series calculations
daily_ticker_aggregated_df.sort_values(['ticker', 'publish_date'], inplace=True)

# Calculate a smoothed market sentiment by incorporating previous day's sentiment
# This helps in capturing sentiment trend rather than daily fluctuations
daily_ticker_aggregated_df['previous_market_sentiment'] = daily_ticker_aggregated_df.groupby('ticker')['average_market_sentiment'].shift(1)
daily_ticker_aggregated_df['average_market_sentiment'] = (daily_ticker_aggregated_df['average_market_sentiment'] + daily_ticker_aggregated_df['previous_market_sentiment']) / 2
daily_ticker_aggregated_df.drop(columns=['previous_market_sentiment'], inplace=True)

# Add Next_Day_Percent_Change column to enable forward-looking analysis
daily_ticker_aggregated_df['Next_Day_Percent_Change'] = daily_ticker_aggregated_df.groupby('ticker')['Yesterday_Percent_Change'].shift(-1)

# Reclassify sentiment for the aggregated data
daily_ticker_aggregated_df['article_sentiment_class'] = daily_ticker_aggregated_df['article_sentiment'].apply(classify_sentiment)
daily_ticker_aggregated_df['average_market_sentiment_class'] = daily_ticker_aggregated_df['average_market_sentiment'].apply(classify_sentiment)

# Remove any rows with blank/NaN cells to ensure data integrity
daily_ticker_aggregated_df.dropna(inplace=True)

# Save the processed and aggregated DataFrame to a new CSV file
output_file_path = "Daily_Ticker_Table.csv"
daily_ticker_aggregated_df.to_csv(output_file_path, index=False)

# Inform the user about successful file creation
print(f"New CSV file with daily averages grouped by ticker saved as {output_file_path}")