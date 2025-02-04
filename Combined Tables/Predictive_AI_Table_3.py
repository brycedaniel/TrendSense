import pandas as pd
import os

# File paths
file_path_2 = "Table_2_Filter_Transform.csv"
file_path_3 = "Table_3_Prediction_Final.csv"

# Load tables
df2 = pd.read_csv(file_path_2) if os.path.exists(file_path_2) else pd.DataFrame()
df3 = pd.read_csv(file_path_3) if os.path.exists(file_path_3) else pd.DataFrame()

# Ensure Unique_ID column exists
if "Unique_ID" not in df2.columns:
    raise ValueError("Unique_ID column is missing in Table_2_Filter_Transform")

# Filter out tickers we don't want
tickers_to_exclude = ["^RUT", "^GSPC", "^DJI"]
df_filtered = df2[~df2["ticker"].isin(tickers_to_exclude)].copy()

# If Table_3_Prediction_Final is not empty, remove rows where Unique_ID already exists
if not df3.empty:
    df_filtered = df_filtered[~df_filtered["Unique_ID"].isin(df3["Unique_ID"])]

# Compute AI_Score
df_filtered["AI_Score"] = df_filtered["AI Score"] * df_filtered["publisher score"]

# Compute Sentiment Score using weighted average
df_filtered["Sentiment Score"] = (
    df_filtered["article_sentiment"] * 0.50 +
    df_filtered["daily_avg_ticker_sentiment"] * 0.30 +
    df_filtered["average_market_sentiment"] * 0.20
)

# Compute Health Score using weighted average
df_filtered["Health_Score"] = (
    df_filtered["RatingScore"] * 100 * 0.25 +  
    df_filtered["analyst_score"] * 0.50 +
    df_filtered["target_score"] * 10 * 0.125 +
    df_filtered["Target_Pct_Change"] * 50 * 0.125
)

# Filter out rows where Health_Score is NaN or missing
df_filtered = df_filtered.dropna(subset=["Health_Score"])

# Compute Aggregated Score as the average of AI_Score, Sentiment Score, and Health_Score
df_filtered["Aggregated_Score"] = df_filtered[["AI_Score", "Sentiment Score", "Health_Score"]].mean(axis=1)

# Rename additional columns
df_filtered.rename(
    columns={
        "Forward_60min_Change_Diff": "Relative_1HR_Chg",
        "Forward_60min_Change": "Open_1HR_Change"
    }, inplace=True
)

# Select final columns, including the newly added ones
df_final = df_filtered[
    ["Unique_ID", "publish_date", "ticker", "AI_Score", "Sentiment Score", "Health_Score", "Aggregated_Score",
     "Relative_1HR_Chg", "Open_1HR_Change", "Day_Percent_Change", "Next_Day_Percent_Change"]
]

# Append new rows to Table_3_Prediction_Final
if not df_final.empty:
    df_final.to_csv(file_path_3, mode="a", header=not os.path.exists(file_path_3), index=False)

print(f"Added {len(df_final)} new rows to Table_3_Prediction_Final with additional columns.")
