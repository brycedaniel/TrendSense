import pandas as pd
import re

# Load the CSV file
file_path = "Combined_Raw.csv"
df = pd.read_csv(file_path)

# Filter rows where word_count > 7
filtered_df = df[df['word_count'] > 7]

# Function to check if a string contains only ASCII characters
def is_ascii(text):
    try:
        return text.encode('ascii').decode() == text
    except UnicodeEncodeError:
        return False

# Filter rows where the title column contains only ASCII characters
if 'title' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['title'].apply(is_ascii)]

# Drop specific columns
columns_to_drop = [
    'Strong_Buy', 'Buy', 'Hold', 'Sell', 'Strong_Sell',
    'hourly_date', 'date_only', 'week_of_year'
]
filtered_df = filtered_df.drop(columns=columns_to_drop, errors='ignore')

# Ensure the publish_date column is in datetime format
filtered_df['publish_date'] = pd.to_datetime(filtered_df['publish_date'], errors='coerce')

# Sort the data by ticker and publish_date
filtered_df = filtered_df.sort_values(by=['ticker', 'publish_date'])

# Define columns to forward fill
fill_columns = [
    # Original columns
    'RatingScore', 
    'analyst_score', 
    'reward_score', 
    'risk_score', 
    'target_score', 
    'target_median_price',
    # Market data columns
    'Close',
    'Volume',
    'High',
    'Low',
    'Open'
]

# Ensure all columns are present in the DataFrame
fill_columns = [col for col in fill_columns if col in filtered_df.columns]

# Forward fill missing values within each ticker group
if fill_columns:
    # Group by ticker and forward fill missing values
    filtered_df[fill_columns] = filtered_df.groupby('ticker')[fill_columns].fillna(method='ffill')

# Calculate Day_Percent_Change for market data
filtered_df['Day_Percent_Change'] = ((filtered_df['Close'] - filtered_df['Open']) / filtered_df['Open'] * 100).round(2)

# Define columns for daily percent change calculation
pct_change_columns = [
    'RatingScore',
    'analyst_score',
    'target_score',
    'target_median_price'
]

# Function to calculate daily percent changes
def calculate_daily_pct_change(group, column):
    # Create a temporary dataframe with just the first row for each date
    daily_values = group.groupby(group['publish_date'].dt.date)[column].first()
    # Calculate percent change between days
    daily_pct_change = daily_values.pct_change().mul(100).round(2)
    # Map these changes back to all rows for each date
    return group['publish_date'].dt.date.map(daily_pct_change)

# Calculate day-over-day percent changes for specified columns
for column in pct_change_columns:
    if column in filtered_df.columns:
        # Create new column name
        new_column = f'{column}_pct_change'
        # Calculate percent change within each ticker group
        filtered_df[new_column] = filtered_df.groupby('ticker').apply(
            lambda x: calculate_daily_pct_change(x, column)
        ).reset_index(level=0, drop=True)

# Save the resulting dataset
output_file_path = "Combined_Clean_1.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"Filtered dataset with all calculations saved to {output_file_path}")

