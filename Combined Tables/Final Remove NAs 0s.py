import pandas as pd

# Load the dataset
file_path = "Per_Article_TableNA.csv"  # Replace with your actual file path
df = pd.read_csv(file_path)

# Ensure publish_date is in datetime format and extract only the date (remove timestamp)
df['publish_date'] = pd.to_datetime(df['publish_date']).dt.date  # Extract just the date

# Define columns to exclude (sentiment columns)
sentiment_columns = [
    'article_sentiment', 
    'daily_avg_ticker_sentiment', 
    'average_market_sentiment',
    'article_sentiment_class',
    'daily_ticker_sentiment_class',
    'average_market_sentiment_class'
]

# Define columns to remove for Table 2
columns_to_remove = [
    'Forward_15min_Change_Diff', 
    'Forward_30min_Change_Diff', 
    'Forward_45min_Change_Diff', 
    'Forward_60min_Change_Diff'
]

# Create non-sentiment column lists for both cases
non_sentiment_columns = [col for col in df.columns if col not in sentiment_columns]
non_sentiment_columns_2 = [col for col in df.columns if col not in sentiment_columns + columns_to_remove]

# ===============================
# Create Table 1: Remove rows with 0 or NaN in non-sentiment columns
# ===============================
table_1 = df.dropna(subset=non_sentiment_columns)  # Remove rows with NaN in non-sentiment columns
table_1 = table_1[(table_1[non_sentiment_columns] != 0).all(axis=1)]  # Remove rows with 0 in non-sentiment columns

# ===============================
# Create Table 2: Drop specified columns, remove rows with 0 or NaN, and aggregate by average per date per ticker
# ===============================
# Drop specified columns for Table 2
table_2 = df.drop(columns=columns_to_remove)

# Remove rows with 0 or NaN in non-sentiment columns
table_2 = table_2.dropna(subset=non_sentiment_columns_2)  # Remove rows with NaN in non-sentiment columns
table_2 = table_2[(table_2[non_sentiment_columns_2] != 0).all(axis=1)]  # Remove rows with 0 in non-sentiment columns

# Ensure grouping happens at the DATE level (since timestamps might be causing issues)
numeric_columns = table_2.select_dtypes(include=['number']).columns  # Select only numeric columns
table_2 = table_2.groupby([table_2['publish_date'], 'ticker'], as_index=False)[numeric_columns].mean()

# ===============================
# Save the tables to CSV
# ===============================
table_1_output_path = "Table_1_No_Zero_Or_Empty.csv"
table_2_output_path = "Table_2_Aggregated_Per_Ticker_Per_Date.csv"

table_1.to_csv(table_1_output_path, index=False)
table_2.to_csv(table_2_output_path, index=False)

print(f"Table 1 saved to {table_1_output_path}")
print(f"Table 2 saved to {table_2_output_path}")
