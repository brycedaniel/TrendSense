import pandas as pd

# Load the original CSV file
file_path = "Combined_Clean_2.csv"  # Replace with the actual file path if needed
df = pd.read_csv(file_path)  # Read the dataset into a DataFrame

# Rename columns to reflect their intended meanings
df.rename(columns={
    'average_sentiment': 'article_sentiment',  
    'daily_average_sentiment': 'average_market_sentiment',  
    'Average_Market_Change': 'average_market_percent_change'  
}, inplace=True)

# Define a function to classify sentiment into categories
def classify_sentiment(value):
    if value >= 0.4:
        return 'Bullish'
    elif -0.2 <= value < 0.4:
        return 'Neutral'
    else:
        return 'Bearish'

# Add classification columns for sentiments
df['article_sentiment_class'] = df['article_sentiment'].apply(classify_sentiment)
df['average_market_sentiment_class'] = df['average_market_sentiment'].apply(classify_sentiment)

# Define the mapping for RatingScore
def map_rating_score(value):
    if value == 5:
        return "Great"
    elif value == 4:
        return "Good"
    elif value == 3:
        return "Neutral"
    elif value == 2:
        return "Bad"
    elif value == 1:
        return "Worst"
    else:
        return None  # For any unexpected values

# Add the RatingScore_Category column
if 'RatingScore' in df.columns:
    df['RatingScore_Category'] = df['RatingScore'].apply(map_rating_score)

# Define the mapping for analyst_score
def map_analyst_score(value):
    if 80 <= value <= 100:
        return "Strong Buy"
    elif 60 <= value < 80:
        return "Buy"
    elif 40 <= value < 60:
        return "Hold"
    elif 20 <= value < 40:
        return "Sell"
    elif 0 <= value < 20:
        return "Strong Sell"
    else:
        return None  # For any unexpected values

# Add the AnalystScore_Category column
if 'analyst_score' in df.columns:
    df['AnalystScore_Category'] = df['analyst_score'].apply(map_analyst_score)

# Add PriceChangeStatus column
if 'target_median_price_pct_change' in df.columns:
    df['PriceChangeStatus'] = df['target_median_price_pct_change'].apply(
        lambda x: "Increase" if x > 0 else ("No Change" if x == 0 else "Decrease")
    )

# Add ValuationStatus column
if 'target_score' in df.columns:
    def map_valuation_status(value):
        if 0 <= value <= 3:
            return "Slightly Overvalued"
        elif value > 3:
            return "Overvalued"
        elif 0 > value >= -3:
            return "Slightly Undervalued"
        elif value < -3:
            return "Undervalued"
        else:
            return None  # For any unexpected values

    df['ValuationStatus'] = df['target_score'].apply(map_valuation_status)

# Define the columns to include in the final cleaned DataFrame
columns_to_include = [
    'publish_date',
    'ticker',
    'publisher',
    'article_sentiment',
    'average_market_sentiment',
    'average_market_percent_change',
    'RatingScore_pct_change',
    'analyst_score_pct_change',
    'target_median_price_pct_change',
    'target_score',
    'Forward_15min_Change_Diff',
    'Forward_30min_Change_Diff',
    'Forward_45min_Change_Diff',
    'Forward_60min_Change_Diff',
    'article_sentiment_class',
    'average_market_sentiment_class',
    'RatingScore_Category',
    'AnalystScore_Category',
    'PriceChangeStatus',  # Newly added column
    'ValuationStatus'     # Newly added column
]

# Filter the DataFrame to include only the specified columns
filtered_df = df[columns_to_include]

# Add RatingScoreStatus column
if 'RatingScore_pct_change' in filtered_df.columns:
    filtered_df['RatingScoreStatus'] = filtered_df['RatingScore_pct_change'].apply(
        lambda x: "Upgrade" if x > 0 else ("No Change" if x == 0 else "Down Grade")
    )

# Add AnalystScoreStatus column
if 'analyst_score_pct_change' in filtered_df.columns:
    filtered_df['AnalystScoreStatus'] = filtered_df['analyst_score_pct_change'].apply(
        lambda x: "Upgrade" if x > 0 else ("No Change" if x == 0 else "Down Grade")
    )

# Save the cleaned DataFrame to a new CSV file
output_file_path = "Per_Article_TableNA.csv"
filtered_df.to_csv(output_file_path, index=False)

print(f"New CSV file with selected columns and no empty rows saved as {output_file_path}")



