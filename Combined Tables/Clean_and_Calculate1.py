import pandas as pd
import re

# Load the CSV file
file_path = "Combined_Raw.csv"
df = pd.read_csv(file_path)

# Filter rows where word_count > 7
# Reason: To focus only on rows with meaningful content (articles or entries with sufficient length).
filtered_df = df[df['word_count'] > 7]

# Function to check if a string contains only ASCII characters
# Reason: Non-ASCII characters can cause encoding issues during processing.
def is_ascii(text):
    try:
        return text.encode('ascii').decode() == text
    except UnicodeEncodeError:
        return False

# Filter rows where the title column contains only ASCII characters
# Reason: Ensures compatibility with ASCII-based text processing or analysis.
if 'title' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['title'].apply(is_ascii)]

# Drop specific columns that are unnecessary or irrelevant for the analysis
# Reason: These columns do not contribute to the analysis and are removed to streamline the dataset.
columns_to_drop = [
    'Strong_Buy', 'Buy', 'Hold', 'Sell', 'Strong_Sell',
    'hourly_date', 'date_only', 'week_of_year'
]
filtered_df = filtered_df.drop(columns=columns_to_drop, errors='ignore')

# Ensure the publish_date column is in datetime format
# Reason: Proper datetime format is required for sorting and time-based analysis.
filtered_df['publish_date'] = pd.to_datetime(filtered_df['publish_date'], errors='coerce')

# Sort the data by ticker and publish_date
# Reason: Sorting ensures chronological order within each ticker, necessary for forward filling and time-series calculations.
filtered_df = filtered_df.sort_values(by=['ticker', 'publish_date'])

# Forward fill missing values for selected columns within each ticker group
# Reason: Missing values may occur in financial or analyst data. Forward filling propagates the most recent values to fill gaps.
fill_columns = [
    'RatingScore', 
    'analyst_score', 
    'reward_score', 
    'risk_score', 
    'target_score', 
    'target_median_price',
    'Close',
    'Volume',
    'High',
    'Low',
    'Open'
]
fill_columns = [col for col in fill_columns if col in filtered_df.columns]

if fill_columns:
    filtered_df[fill_columns] = filtered_df.groupby('ticker')[fill_columns].fillna(method='ffill')

# Calculate Day_Percent_Change for market data
# Reason: Provides a quick measure of daily price movement as a percentage change from the opening to the closing price.
filtered_df['Day_Percent_Change'] = ((filtered_df['Close'] - filtered_df['Open']) / filtered_df['Open'] * 100).round(2)

# Calculate raw differences for Forward_*min_Change columns relative to Percent_Difference
# Reason: Quantifies how the short-term forward changes compare to the overall percent difference.
forward_columns = [
    'Forward_15min_Change',
    'Forward_30min_Change',
    'Forward_45min_Change',
    'Forward_60min_Change'
]

if 'Percent_Difference' in filtered_df.columns:
    for column in forward_columns:
        if column in filtered_df.columns:
            new_column_name = f'{column}_Diff'
            filtered_df[new_column_name] = (filtered_df[column] - filtered_df['Percent_Difference'])

# Calculate daily percent changes for specified columns
# Reason: Adds day-over-day percentage changes to analyze trends in financial or analyst data.
pct_change_columns = [
    'RatingScore',
    'analyst_score',
    'target_score',
    'target_median_price'
]

def calculate_daily_pct_change(group, column):
    # Get the first value of the column for each date
    daily_values = group.groupby(group['publish_date'].dt.date)[column].first()
    # Calculate percentage change between consecutive days
    daily_pct_change = daily_values.pct_change().mul(100).round(2)
    # Map these changes back to all rows for each date
    return group['publish_date'].dt.date.map(daily_pct_change)

for column in pct_change_columns:
    if column in filtered_df.columns:
        new_column = f'{column}_pct_change'
        filtered_df[new_column] = filtered_df.groupby('ticker').apply(
            lambda x: calculate_daily_pct_change(x, column)
        ).reset_index(level=0, drop=True)

# Calculate the average sentiment and add it as a new column
# Reason: Combines multiple sentiment scores into a single metric for simplicity.
filtered_df['average_sentiment'] = filtered_df[['textblob_sentiment', 'vader_sentiment']].mean(axis=1)

# Calculate the daily average sentiment
# Reason: Aggregates sentiment scores on a daily basis to analyze overall market sentiment trends.
daily_avg_sentiment = filtered_df.groupby(filtered_df['publish_date'].dt.date)['average_sentiment'].mean().reset_index()
daily_avg_sentiment.rename(columns={'average_sentiment': 'daily_average_sentiment', 'publish_date': 'publish_date'}, inplace=True)

# Merge the daily average sentiment back into the original DataFrame
# Reason: Links the aggregated daily sentiment scores to the detailed dataset for analysis.
filtered_df['publish_date_date'] = filtered_df['publish_date'].dt.date
filtered_df = filtered_df.merge(daily_avg_sentiment, left_on='publish_date_date', right_on='publish_date', how='left')

# Remove unnecessary columns introduced during the merge
# Reason: Clean up temporary columns to maintain dataset integrity and readability.
filtered_df.drop(columns=['publish_date_y'], inplace=True, errors='ignore')
filtered_df.rename(columns={'publish_date_x': 'publish_date'}, inplace=True, errors='ignore')

# Calculate the daily average market change
# Reason: Provides a metric for average market performance across all entries on a given day.
daily_avg_change = filtered_df.groupby(filtered_df['publish_date_date'])['Percent_Difference'].mean().reset_index()
daily_avg_change.rename(columns={'Percent_Difference': 'Average_Market_Change', 'publish_date_date': 'publish_date_date'}, inplace=True)

# Merge the daily average market change back into the original DataFrame
# Reason: Adds the aggregated market performance metric to the detailed dataset.
filtered_df = filtered_df.merge(daily_avg_change, left_on='publish_date_date', right_on='publish_date_date', how='left')

# Drop the temporary date column
# Reason: Temporary columns are no longer needed after merging the aggregated data.
filtered_df.drop(columns=['publish_date_date'], inplace=True)

# Save the resulting dataset
# Reason: Outputs the final cleaned and enriched dataset for further analysis or reporting.
output_file_path = "Combined_Clean_1.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"Filtered dataset with all calculations saved to {output_file_path}")


