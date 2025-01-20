import pandas as pd

# Load the original CSV file
file_path = "Combined_Clean_1.csv"  # Replace with the actual file path if needed
df = pd.read_csv(file_path)

# Define the columns to include in the new CSV
columns_to_include = [
    'publish_date',
    'ticker',
    'textblob_sentiment',
    'vader_sentiment',
    'bert_sentiment',
    'bert_confidence',
    'reliability_score',
    'Current_Price',
    'Percent_Difference',
    'Forward_15min_Change',
    'Forward_30min_Change',
    'Forward_45min_Change',
    'Forward_60min_Change',
    'Close',
    'Volume',
    'High',
    'Low',
    'Open',
    'Daily_Percent_Difference',
    '7_day_avg_RatingScore',
    'percent_diff_7_day_avg_RatingScore',
    '7_day_avg_analyst_score',
    'percent_diff_7_day_avg_analyst_score',
    '7_day_avg_reward_score',
    'percent_diff_7_day_avg_reward_score',
    '7_day_avg_risk_score',
    'percent_diff_7_day_avg_risk_score',
    '7_day_avg_target_score',
    'percent_diff_7_day_avg_target_score',
    '7_day_avg_target_median_price',
    'percent_diff_7_day_avg_target_median_price'
]

# Filter the DataFrame to include only the specified columns
filtered_df = df[columns_to_include]

# Drop rows with any empty cells (NaN)
filtered_df = filtered_df.dropna()

# Save the cleaned DataFrame to a new CSV file
output_file_path = "Combined_Total_Clean_2.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"New CSV file with selected columns and no empty rows saved as {output_file_path}")