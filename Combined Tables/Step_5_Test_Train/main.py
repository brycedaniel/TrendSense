from google.cloud import bigquery
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import functions_framework
import logging
import os
import numpy as np
from datetime import datetime
import traceback

# Flask is included in functions-framework
from flask import jsonify

@functions_framework.http
def process_data(request):
    # Handle health checks - necessary for Cloud Function reliability monitoring and load balancer checks
    if request.method == 'GET':
        return jsonify({'status': 'healthy'}), 200

    # Configure logging with timestamp - helps trace execution timeline for debugging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Starting data processing job...")
        start_time = datetime.now()  # Track execution time for performance monitoring
        
        # Initialize BigQuery client - connection to Google Cloud BigQuery service
        client = bigquery.Client()

        # Define tables - separation of source data and model outputs allows for clean data architecture
        source_table = "trendsense.combined_data.step_4_final"
        regression_table = "trendsense.combined_data.step_4_test_train"
        metrics_table = "trendsense.combined_data.model_performance_metrics"

        # Query to fetch data - filters ensure we only process valid data points
        # Price_Movement_Tomorrow limited to ±5% to eliminate extreme market events that could skew models
        query = f"""
        SELECT ticker, date, AI_Score, Sentiment_Score, Health_Score, Price_Movement_Tomorrow
        FROM `{source_table}`
        WHERE AI_Score IS NOT NULL 
          AND Sentiment_Score IS NOT NULL 
          AND Health_Score IS NOT NULL 
          AND Price_Movement_Tomorrow IS NOT NULL
          AND Price_Movement_Tomorrow BETWEEN -0.05 AND 0.05
          -- AND date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        """
        
        logger.info("Executing BigQuery query...")
        df = client.query(query).to_dataframe()
        
        # Early exit if no data - prevents attempting to process empty datasets
        if df.empty:
            logger.warning("Query returned no data")
            return jsonify({
                "success": False,
                "message": "No data found matching the criteria"
            }), 404
            
        logger.info(f"Loaded {len(df)} rows from BigQuery")

        # Outlier detection - identifies potential data quality issues before they affect models
        # Using 3 standard deviations as threshold for statistical anomalies
        logger.info("Checking for outliers...")
        for col in ['AI_Score', 'Sentiment_Score', 'Health_Score', 'Price_Movement_Tomorrow']:
            outliers = df[abs(df[col] - df[col].mean()) > 3 * df[col].std()]
            if not outliers.empty:
                logger.warning(f"Found {len(outliers)} outliers in column {col}")
                # We log but don't remove outliers automatically, as they may be valid data points
        
        # Initialize result storage - separating regression coefficients from performance metrics
        regression_results = []
        model_metrics = []
        
        # Get ticker data distribution - helps prioritize tickers with sufficient data
        # This improves overall model quality by focusing on well-represented tickers
        ticker_counts = df.groupby('ticker').size().reset_index(name='count')
        ticker_counts = ticker_counts.sort_values('count', ascending=False)
        
        # Filter for tickers with sufficient data - minimum threshold of 10 data points
        # This ensures statistical validity of regression models (need enough samples)
        viable_tickers = ticker_counts[ticker_counts['count'] >= 10]['ticker'].tolist()
        logger.info(f"Processing {len(viable_tickers)} tickers with sufficient data")
        
        # Track skipped tickers for reporting - helps identify data gaps
        skipped_tickers = ticker_counts[ticker_counts['count'] < 10]['ticker'].tolist()
        if skipped_tickers:
            logger.info(f"Skipping {len(skipped_tickers)} tickers with insufficient data")

        # Batch processing - prevents memory overload in cloud functions
        # Processing all tickers at once could exceed memory limits for large datasets
        batch_size = 50  # Chosen to balance processing efficiency with memory usage
        for i in range(0, len(viable_tickers), batch_size):
            batch_tickers = viable_tickers[i:i+batch_size]
            logger.info(f"Processing batch {i//batch_size + 1} with {len(batch_tickers)} tickers")
            
            # Process each ticker individually - each stock has its own unique model
            for ticker in batch_tickers:
                df_ticker = df[df["ticker"] == ticker].copy()
                
                # Sort by date - essential for time series data to maintain chronological integrity
                if 'date' in df_ticker.columns:
                    df_ticker = df_ticker.sort_values('date')
                
                # Prepare features and target - X variables are the scores, y is the next-day price movement
                X = df_ticker[['AI_Score', 'Sentiment_Score', 'Health_Score']]
                y = df_ticker['Price_Movement_Tomorrow']
                
                # Calculate feature statistics - used for monitoring data drift over time
                # Data drift detection helps ensure model reliability in production
                feature_stats = {}
                for col in X.columns:
                    feature_stats[f"{col}_mean"] = X[col].mean()
                    feature_stats[f"{col}_std"] = X[col].std()
                
                # Handle NaN values - ensures clean data for model training
                # NaN values would cause training failures if not addressed
                if X.isna().any().any() or y.isna().any():
                    logger.warning(f"Found NaN values for {ticker}, applying simple imputation")
                    X = X.fillna(X.mean())  # Mean imputation as a simple strategy
                    y = y.fillna(y.mean())

                try:
                    # Train linear regression model - simple, interpretable model for price movement prediction
                    # Linear regression chosen for interpretability of coefficients
                    model = LinearRegression()
                    model.fit(X, y)
                    
                    # Generate predictions - needed for calculating performance metrics
                    y_pred = model.predict(X)
                    
                    # Calculate model quality metrics - essential for evaluating model reliability
                    # These metrics help identify which ticker models perform best
                    mse = mean_squared_error(y, y_pred)
                    rmse = np.sqrt(mse)  # More intuitive scale than MSE (same units as target)
                    r2 = r2_score(y, y_pred)  # Indicates proportion of variance explained
                    
                    # Store regression coefficients - these will be used for future predictions
                    regression_results.append({
                        'Ticker': ticker,
                        'Intercept': float(model.intercept_),
                        'AI_Coefficient': float(model.coef_[0]),
                        'Sentiment_Coefficient': float(model.coef_[1]),
                        'Health_Coefficient': float(model.coef_[2]),
                        'Data_Points': int(len(df_ticker)),
                        'Last_Updated': datetime.now()  # Timestamp for tracking model freshness
                    })
                    
                    # Store model performance metrics - used for model comparison and monitoring
                    model_metrics.append({
                        'Ticker': ticker,
                        'MSE': float(mse),
                        'RMSE': float(rmse),
                        'R2': float(r2),
                        'Data_Points': int(len(df_ticker)),
                        # Feature statistics help detect data drift between training cycles
                        'AI_Score_Mean': float(feature_stats['AI_Score_mean']),
                        'Sentiment_Score_Mean': float(feature_stats['Sentiment_Score_mean']),
                        'Health_Score_Mean': float(feature_stats['Health_Score_mean']),
                        'AI_Score_Std': float(feature_stats['AI_Score_std']),
                        'Sentiment_Score_Std': float(feature_stats['Sentiment_Score_std']),
                        'Health_Score_Std': float(feature_stats['Health_Score_std']),
                        'Last_Updated': datetime.now()
                    })
                    
                except Exception as model_error:
                    # Isolate model errors - prevents one ticker failure from stopping entire job
                    logger.error(f"Error building model for {ticker}: {str(model_error)}")

        # Convert results to DataFrames - required format for BigQuery loading
        regression_df = pd.DataFrame(regression_results)
        metrics_df = pd.DataFrame(model_metrics)

        # Verify results exist - prevent attempting to upload empty datasets
        if regression_df.empty:
            return jsonify({
                "success": False,
                "message": "No regression models could be built"
            }), 400

        # Define regression result schema - ensures data types are correctly mapped to BigQuery
        logger.info(f"Saving {len(regression_df)} regression models to BigQuery...")
        regression_schema = [
            bigquery.SchemaField("Ticker", "STRING"),
            bigquery.SchemaField("Intercept", "FLOAT64"),
            bigquery.SchemaField("AI_Coefficient", "FLOAT64"),
            bigquery.SchemaField("Sentiment_Coefficient", "FLOAT64"),
            bigquery.SchemaField("Health_Coefficient", "FLOAT64"),
            bigquery.SchemaField("Data_Points", "INTEGER"),
            bigquery.SchemaField("Last_Updated", "TIMESTAMP")
        ]
        
        # Configure upload job - WRITE_TRUNCATE replaces existing data with new results
        regression_job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=regression_schema
        )
        
        # Upload regression results to BigQuery - storing model coefficients for later use
        regression_job = client.load_table_from_dataframe(
            regression_df,
            regression_table,
            job_config=regression_job_config
        )
        regression_job.result()  # Wait for job completion
        
        # Upload model metrics if available - used for model performance tracking
        if not metrics_df.empty:
            logger.info(f"Saving {len(metrics_df)} model metrics to BigQuery...")
            metrics_schema = [
                bigquery.SchemaField("Ticker", "STRING"),
                bigquery.SchemaField("MSE", "FLOAT64"),
                bigquery.SchemaField("RMSE", "FLOAT64"),
                bigquery.SchemaField("R2", "FLOAT64"),
                bigquery.SchemaField("Data_Points", "INTEGER"),
                bigquery.SchemaField("AI_Score_Mean", "FLOAT64"),
                bigquery.SchemaField("Sentiment_Score_Mean", "FLOAT64"),
                bigquery.SchemaField("Health_Score_Mean", "FLOAT64"),
                bigquery.SchemaField("AI_Score_Std", "FLOAT64"),
                bigquery.SchemaField("Sentiment_Score_Std", "FLOAT64"),
                bigquery.SchemaField("Health_Score_Std", "FLOAT64"),
                bigquery.SchemaField("Last_Updated", "TIMESTAMP")
            ]
            
            # Configure metrics upload job
            metrics_job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=metrics_schema
            )
            
            # Upload metrics to BigQuery
            metrics_job = client.load_table_from_dataframe(
                metrics_df,
                metrics_table,
                job_config=metrics_job_config
            )
            metrics_job.result()  # Wait for job completion
        
        # Calculate total processing time - helps track performance
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Return detailed success response - provides insights into the job results
        return jsonify({
            "success": True,
            "message": f"Processing complete. {len(regression_df)} models stored.",
            "processing_time_seconds": processing_time,
            "tickers_processed": len(regression_df),
            "tickers_skipped": len(skipped_tickers),
            "average_r2": metrics_df['R2'].mean() if not metrics_df.empty else None
        }), 200

    except Exception as e:
        # Comprehensive error handling - provides detailed error information for debugging
        # Full stack trace helps identify the source of errors in complex code
        logger.error(f"Error processing data: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return structured error response
        return jsonify({
            "success": False,
            "message": f"Error: {str(e)}",
            "error_type": type(e).__name__
        }), 500

if __name__ == "__main__":
    # Local development entry point - allows testing on local machine
    # Cloud Functions runtime handles this automatically when deployed
    port = int(os.getenv("PORT", 8080))
    functions_framework.start(port=port)

