import pandas as pd

# Load the original CSV file
file_path = "Combined_Clean_1.csv"  # Replace with the actual file path if needed
df = pd.read_csv(file_path)

# Ensure the data is sorted by 'ticker' and 'publish_date'
df['publish_date'] = pd.to_datetime(df['publish_date'])  # Ensure datetime format
df = df.sort_values(by=['ticker', 'publish_date'])

# Calculate the average sentiment and add it as a new column
df['average_sentiment'] = df[['textblob_sentiment', 'vader_sentiment']].mean(axis=1)

# Calculate the daily average sentiment
daily_avg_sentiment = df.groupby(df['publish_date'].dt.date)['average_sentiment'].mean().reset_index()
daily_avg_sentiment.rename(columns={'average_sentiment': 'daily_average_sentiment', 'publish_date': 'publish_date'}, inplace=True)

# Merge the daily average sentiment back into the original DataFrame
df['publish_date_date'] = df['publish_date'].dt.date  # Create a column for the date
df = df.merge(daily_avg_sentiment, left_on='publish_date_date', right_on='publish_date', how='left')

# Remove unnecessary columns introduced during the merge
df.drop(columns=['publish_date_y'], inplace=True, errors='ignore')
df.rename(columns={'publish_date_x': 'publish_date'}, inplace=True, errors='ignore')

# Calculate the daily average market change
daily_avg_change = df.groupby(df['publish_date_date'])['Percent_Difference'].mean().reset_index()
daily_avg_change.rename(columns={'Percent_Difference': 'Average_Market_Change', 'publish_date_date': 'publish_date_date'}, inplace=True)

# Merge the daily average market change back into the original DataFrame
df = df.merge(daily_avg_change, left_on='publish_date_date', right_on='publish_date_date', how='left')

# Drop the temporary date column
df.drop(columns=['publish_date_date'], inplace=True)

# Create the new column 'rating_score_change' based on 'percent_diff_7_day_avg_RatingScore'
#df['rating_score_change'] = df['RatingScore_3Day_Percent_Diff'].apply(
#    lambda x: 1 if x > 0 else (0 if x == 0 else -1)
#)

# Create the new column 'analyst_score_change' based on 'percent_diff_7_day_avg_analyst_score'
#df['analyst_score_change'] = df['analyst_score_3Day_Percent_Diff'].apply(
#    lambda x: 1 if x > 0 else (0 if x == 0 else -1)
#)

# Create the new column 'median_price_change' based on 'percent_diff_7_day_avg_target_median_price'
#df['median_price_change'] = df['target_median_price_3Day_Percent_Diff'].apply(
#    lambda x: 1 if x > 0 else (0 if x == 0 else -1)
#)

# Define the columns to include in the new CSV
columns_to_include = [
    'publish_date',
    'ticker',
    'publisher',  # Include the publisher column
    'average_sentiment',  # Include the new average sentiment column
    'daily_average_sentiment',  # Include the daily average sentiment column
    'Average_Market_Change',  # Include the new average market change column
    'RatingScore_pct_change',  # Include the new rating score change column
    'analyst_score_pct_change',  # Include the new analyst score change column
    'target_median_price_pct_change',  # Include the new median price change column
    'target_score',
    'Percent_Difference',
    'Forward_15min_Change',
    'Forward_30min_Change',
    'Forward_45min_Change',
    'Forward_60min_Change',
]

# Filter the DataFrame to include only the specified columns
filtered_df = df[columns_to_include]

# Drop rows with any empty cells (NaN)
filtered_df = filtered_df.dropna()

# Save the cleaned DataFrame to a new CSV file
output_file_path = "Combined_Total_Clean_3.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"New CSV file with selected columns and no empty rows saved as {output_file_path}")
