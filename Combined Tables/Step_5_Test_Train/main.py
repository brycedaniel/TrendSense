from google.cloud import bigquery
import pandas as pd
from sklearn.linear_model import LinearRegression

def process_data(request):
    client = bigquery.Client()

    # Define source and target tables
    source_table = "trendsense.combined_data.step_4_final"
    regression_table = "trendsense.combined_data.step_4_test_train"  # Storing regression results here

    # Load data from source table
    query = f"SELECT * FROM `{source_table}`"
    df = client.query(query).to_dataframe()

    # Remove rows where any independent or dependent variable is NaN
    df = df.dropna(subset=["Avg_AI_Score", "Avg_Sentiment_Score", "Avg_Health_Score", "Avg_Next_Daily_Percent_Difference"])

    # Filter out extreme Avg_Next_Daily_Percent_Difference values
    df = df[(df["Avg_Next_Daily_Percent_Difference"] >= -0.05) & (df["Avg_Next_Daily_Percent_Difference"] <= 0.05)]

    # Initialize list to store regression results per ticker
    regression_results = []

    # Iterate over each unique ticker and train regression model
    for ticker in df["ticker"].unique():
        df_ticker = df[df["ticker"] == ticker].copy()

        # Ensure enough data points for training
        if len(df_ticker) > 3:  # Needs at least 4 data points to train
            X = df_ticker[['Avg_AI_Score', 'Avg_Sentiment_Score', 'Avg_Health_Score']]
            y = df_ticker['Avg_Next_Daily_Percent_Difference']

            # Train regression model
            model = LinearRegression()
            model.fit(X, y)

            # Store regression coefficients for future predictions
            regression_results.append({
                'Ticker': ticker,
                'Intercept': model.intercept_,
                'AI_Coefficient': model.coef_[0],
                'Sentiment_Coefficient': model.coef_[1],
                'Health_Coefficient': model.coef_[2],
                'Data_Points': len(df_ticker)  # Track number of data points used
            })
        else:
            print(f"⚠️ Skipping {ticker} (Not enough data points)")

    # Convert regression results into a DataFrame
    regression_df = pd.DataFrame(regression_results)

    # Save regression results to BigQuery (Target table is now storing regression data)
    if not regression_df.empty:
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        client.load_table_from_dataframe(regression_df, regression_table, job_config=job_config).result()
        print(f"✅ Regression coefficients saved to {regression_table}")

    return f"Processing complete. Regression coefficients stored in {regression_table}."


