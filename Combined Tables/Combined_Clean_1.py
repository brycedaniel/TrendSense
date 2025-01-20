import pandas as pd
import re

# Load the CSV file
file_path = "Combined_Raw.csv"  # Replace with the actual path to your CSV file
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

# Sort the data by ticker and publish_date to ensure proper rolling calculation
filtered_df = filtered_df.sort_values(by=['ticker', 'publish_date'])

# Define columns for which to calculate the rolling 7-day average
rolling_columns = [
    'RatingScore', 
    'analyst_score', 
    'reward_score', 
    'risk_score', 
    'target_score', 
    'target_median_price'  # Include the additional column
]

# Ensure all rolling columns are present in the DataFrame
rolling_columns = [col for col in rolling_columns if col in filtered_df.columns]

# Calculate the 7-day rolling average and percent difference for each column
if rolling_columns:
    def calculate_rolling_avg(group):
        group = group.set_index('publish_date')  # Set publish_date as the index
        for col in rolling_columns:
            # Calculate the 7-day rolling average
            avg_col = f'7_day_avg_{col}'
            group[avg_col] = group[col].rolling('7D', closed='left').mean()
            
            # Calculate the percent difference
            diff_col = f'percent_diff_{avg_col}'
            group[diff_col] = group[avg_col].pct_change(fill_method=None) * 100  # Explicitly disable filling
        return group.reset_index()  # Reset index to maintain original structure

    filtered_df = filtered_df.groupby('ticker', group_keys=False).apply(calculate_rolling_avg)

# Save the resulting dataset
output_file_path = "Combined_Clean_1.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"Filtered dataset with 7-day averages and percent differences saved to {output_file_path}")

