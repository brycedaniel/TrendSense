import pandas as pd
import re

# Load the CSV file
file_path = "Combined_Raw.csv"
df = pd.read_csv(file_path)

# Filter rows where word_count > 7
filtered_df = df[df['word_count'] > 7]

# Define the list of publishers to keep
allowed_publishers = [
    "Yahoo Entertainment", "ETF Daily News", "Biztoc.com", "Decrypt",
    "The Wall Street Journal", "Investor's Business Daily", "GuruFocus.com",
    "Barrons.com", "Yahoo Finance", "Fortune", "Investopedia", "MT Newswires",
    "Benzinga", "Reuters", "Bloomberg", "Motley Fool", "Cult of Mac",
    "Macdailynews.com", "CNN Business", "TheStreet", "Forbes", "TipRanks",
    "Quartz", "Insider Monkey", "Zacks", "Investing.com", "MarketWatch",
    "Observer", "CNBC", "GlobeNewswire"
]

# Filter rows where 'publisher' is in the allowed list
if 'publisher' in filtered_df.columns:
    filtered_df = filtered_df[filtered_df['publisher'].isin(allowed_publishers)]

# Drop specific columns that are unnecessary or irrelevant for the analysis
columns_to_drop = [
    'Strong_Buy', 'Buy', 'Hold', 'Sell', 'Strong_Sell',
    'hourly_date', 'date_only', 'week_of_year'
]
filtered_df = filtered_df.drop(columns=columns_to_drop, errors='ignore')

# Ensure the publish_date column is in datetime format
filtered_df['publish_date'] = pd.to_datetime(filtered_df['publish_date'], errors='coerce')

# Sort the data by ticker and publish_date
filtered_df = filtered_df.sort_values(by=['ticker', 'publish_date'])

# Forward fill missing values for selected columns within each ticker group
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
    for col in fill_columns:
        filtered_df[col] = filtered_df.groupby('ticker')[col].transform(lambda group: group.ffill())

# Calculate Day_Percent_Change for market data
filtered_df['Day_Percent_Change'] = ((filtered_df['Close'] - filtered_df['Open']) / filtered_df['Open'] * 100).round(2)

# Calculate raw differences for Forward_*min_Change columns relative to Percent_Difference
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

def calculate_daily_pct_change(group, column):
    """
    Calculate daily percentage change for the specified column within the group.
    Ensures 'publish_date' is retained for mapping.
    """
    # Group by publish_date (date part only) and calculate the first value for each day
    daily_values = group.groupby(group['publish_date'].dt.date).agg(
        {column: 'first'}
    ).reset_index()
    daily_values.rename(columns={'index': 'publish_date'}, inplace=True)

    # Calculate percentage change with no NA fill
    daily_pct_change = daily_values[column].pct_change(fill_method=None).mul(100).round(2)

    # Create a mapping of publish_date to daily percentage change
    daily_pct_change_map = dict(zip(daily_values['publish_date'], daily_pct_change))

    # Map percentage changes back to the original group
    return group['publish_date'].dt.date.map(daily_pct_change_map)


# Calculate daily percent changes for specified columns
pct_change_columns = [
    'RatingScore',
    'analyst_score',
    'target_score',
    'target_median_price'
]

for column in pct_change_columns:
    if column in filtered_df.columns:
        new_column = f'{column}_pct_change'
        filtered_df[new_column] = filtered_df.groupby('ticker', group_keys=False).apply(
            lambda x: calculate_daily_pct_change(x, column)
        ).reset_index(level=0, drop=True)

# Calculate the average sentiment and add it as a new column
filtered_df['average_sentiment'] = filtered_df[['textblob_sentiment', 'vader_sentiment']].mean(axis=1)

# Calculate the daily average sentiment
daily_avg_sentiment = filtered_df.groupby(filtered_df['publish_date'].dt.date)['average_sentiment'].mean().reset_index()
daily_avg_sentiment.rename(columns={'average_sentiment': 'daily_average_sentiment', 'publish_date': 'publish_date'}, inplace=True)

# Merge the daily average sentiment back into the original DataFrame
filtered_df['publish_date_date'] = filtered_df['publish_date'].dt.date
filtered_df = filtered_df.merge(daily_avg_sentiment, left_on='publish_date_date', right_on='publish_date', how='left')

# Remove unnecessary columns introduced during the merge
filtered_df.drop(columns=['publish_date_y'], inplace=True, errors='ignore')
filtered_df.rename(columns={'publish_date_x': 'publish_date'}, inplace=True, errors='ignore')

# Calculate the daily average market change
daily_avg_change = filtered_df.groupby(filtered_df['publish_date_date'])['Percent_Difference'].mean().reset_index()
daily_avg_change.rename(columns={'Percent_Difference': 'Average_Market_Change', 'publish_date_date': 'publish_date_date'}, inplace=True)

# Merge the daily average market change back into the original DataFrame
filtered_df = filtered_df.merge(daily_avg_change, left_on='publish_date_date', right_on='publish_date_date', how='left')

# Drop the temporary date column
filtered_df.drop(columns=['publish_date_date'], inplace=True)

# Save the resulting dataset
output_file_path = "Combined_Clean_2.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"Filtered dataset with all calculations saved to {output_file_path}")


