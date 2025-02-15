from google.cloud import bigquery
import pandas as pd
from sklearn.linear_model import LinearRegression
import functions_framework
import logging
import os

# Flask is included in functions-framework
from flask import jsonify

@functions_framework.http
def process_data(request):
    # Handle health checks
    if request.method == 'GET':
        return jsonify({'status': 'healthy'}), 200

    try:
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)
        
        logger.info("Starting data processing...")
        
        # Initialize BigQuery client
        client = bigquery.Client()

        # Define source and target tables
        source_table = "trendsense.combined_data.step_4_final"
        regression_table = "trendsense.combined_data.step_4_test_train"

        # Load data from source table
        query = f"""
        SELECT * 
        FROM `{source_table}`
        WHERE Avg_AI_Score IS NOT NULL 
          AND Avg_Sentiment_Score IS NOT NULL 
          AND Avg_Health_Score IS NOT NULL 
          AND Avg_Next_Daily_Percent_Difference IS NOT NULL
          AND Avg_Next_Daily_Percent_Difference BETWEEN -0.05 AND 0.05
        """
        
        logger.info("Executing BigQuery query...")
        df = client.query(query).to_dataframe()
        logger.info(f"Loaded {len(df)} rows from BigQuery")

        # Initialize list to store regression results
        regression_results = []

        # Iterate over each unique ticker
        unique_tickers = df["ticker"].unique()
        logger.info(f"Processing {len(unique_tickers)} unique tickers")

        for ticker in unique_tickers:
            df_ticker = df[df["ticker"] == ticker].copy()

            # Ensure enough data points for training
            if len(df_ticker) > 3:
                X = df_ticker[['Avg_AI_Score', 'Avg_Sentiment_Score', 'Avg_Health_Score']]
                y = df_ticker['Avg_Next_Daily_Percent_Difference']

                # Train regression model
                model = LinearRegression()
                model.fit(X, y)

                regression_results.append({
                    'Ticker': ticker,
                    'Intercept': float(model.intercept_),
                    'AI_Coefficient': float(model.coef_[0]),
                    'Sentiment_Coefficient': float(model.coef_[1]),
                    'Health_Coefficient': float(model.coef_[2]),
                    'Data_Points': int(len(df_ticker))
                })
            else:
                logger.warning(f"Skipping {ticker} (insufficient data points: {len(df_ticker)})")

        # Convert regression results into a DataFrame
        regression_df = pd.DataFrame(regression_results)

        if not regression_df.empty:
            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=[
                    bigquery.SchemaField("Ticker", "STRING"),
                    bigquery.SchemaField("Intercept", "FLOAT64"),
                    bigquery.SchemaField("AI_Coefficient", "FLOAT64"),
                    bigquery.SchemaField("Sentiment_Coefficient", "FLOAT64"),
                    bigquery.SchemaField("Health_Coefficient", "FLOAT64"),
                    bigquery.SchemaField("Data_Points", "INTEGER")
                ]
            )

            # Load to BigQuery
            logger.info("Saving results to BigQuery...")
            job = client.load_table_from_dataframe(
                regression_df,
                regression_table,
                job_config=job_config
            )
            job.result()  # Wait for the job to complete
            
            logger.info(f"Successfully saved {len(regression_df)} rows to {regression_table}")
            return jsonify({
                "success": True,
                "message": f"Processing complete. {len(regression_df)} models stored in {regression_table}."
            }), 200
        else:
            return jsonify({
                "success": False,
                "message": "No regression results generated"
            }), 400

    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}"
        }), 500

if __name__ == "__main__":
    # This is used when running locally. When deploying to Google Cloud Functions,
    # the functions-framework package will handle this automatically
    port = int(os.getenv("PORT", 8080))
    functions_framework.start(port=port)


