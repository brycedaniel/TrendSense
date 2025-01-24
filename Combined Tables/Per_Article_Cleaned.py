import pandas as pd

# Load the original CSV file
file_path = "Combined_Clean_1.csv"  # Replace with the actual file path if needed
df = pd.read_csv(file_path)  # Read the dataset into a DataFrame

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

# Define the columns to include in the final cleaned DataFrame
# This step ensures only the relevant columns are retained for further analysis, improving efficiency
columns_to_include = [
    'publish_date',  # The date the article or data point was published
    'ticker',  # The stock ticker associated with the data
    'publisher',  # The source or publisher of the article
    'article_sentiment',  # Individual article sentiment score
    'average_market_sentiment',  # Daily average sentiment across the market
    'average_market_percent_change',  # Daily market percent change
    'RatingScore_pct_change',  # Percentage change in the rating score
    'analyst_score_pct_change',  # Percentage change in the analyst score
    'target_median_price_pct_change',  # Percentage change in the median target price
    'target_score',  # Target score for predictions or evaluations
    'Forward_15min_Change_Diff',  # Price change difference over 15 minutes
    'Forward_30min_Change_Diff',  # Price change difference over 30 minutes
    'Forward_45min_Change_Diff',  # Price change difference over 45 minutes
    'Forward_60min_Change_Diff',  # Price change difference over 60 minutes
    'article_sentiment_class',  # Classified sentiment for individual articles
    'average_market_sentiment_class'  # Classified sentiment for the overall market
]

# Filter the DataFrame to include only the specified columns
# This focuses the dataset on relevant data, removing unnecessary information
filtered_df = df[columns_to_include]

# Drop rows with any empty cells (NaN)
# This ensures the final dataset is complete and avoids errors in subsequent analysis
#filtered_df = filtered_df.dropna()

# Save the cleaned DataFrame to a new CSV file
# This step outputs the processed data for further use, ensuring the work is reproducible and accessible
output_file_path = "Per_Article_TableNA.csv"
filtered_df.to_csv(output_file_path, index=False)  # Save without the index column

# Inform the user that the new file has been saved successfully
print(f"New CSV file with selected columns and no empty rows saved as {output_file_path}")

