from google.cloud import bigquery
import os
import json
from datetime import datetime
import smtplib
from email.mime.text import MIMEText

def news_notification(request):
    """
    Google Cloud Function to check specific BigQuery tables for data related to multiple tickers
    and store the results in a new table 'trendsense.notification.Notifications',
    ensuring no duplicate entries based on the 'Info' column and sending an email notification
    when new rows are added.
    """
    # Set up BigQuery client
    client = bigquery.Client()

    # Define the source tables and columns to query
    source_tables = [
        ("trendsense.SEC_data.SEC_Filings", "Ticker", "Filing_URL", "Filing_Date", None),
        ("trendsense.market_data.News_Alpha_Extract", "ticker", "link", "publish_date", "publisher"),
        ("trendsense.market_data.News_News_Extract", "ticker", "link", "publish_date", "publisher"),
        ("trendsense.market_data.News_Yahoo_Extract", "ticker", "link", "publish_date", "publisher")
    ]

    target_tickers = ["ASTS", "TSLA", "AAPL", "GSAT"]  # Add the list of tickers to monitor

    # Define the target table for notifications
    target_table = "trendsense.notification.Notifications"

    # SQL template for querying each table
    sql_template = """
        SELECT {column} AS Ticker, {info_column} AS Info, {date_column} AS Date{source_select}
        FROM `{table}`
        {where_clause}
    """

    # Email configuration
    SMTP_SERVER = "smtp.office365.com"
    SMTP_PORT = 587
    EMAIL_ADDRESS = os.getenv("snipes202@hotmail.com")  # Your email address
    EMAIL_PASSWORD = os.getenv("Linctel4625")  # Your email password
    USER_EMAIL_ADDRESS = "bryceadaniel@hotmail.com"

    # List to store formatted data for the target table
    formatted_data = []

    try:
        # Fetch existing 'Info' values from the target table to avoid duplicates
        existing_infos = set()
        query_existing = f"SELECT Info FROM `{target_table}`"
        existing_results = client.query(query_existing).result()
        for row in existing_results:
            existing_infos.add(row.Info)

        for ticker in target_tickers:
            for table, column, info_column, date_column, source_column in source_tables:
                # Adjust source selection for SEC table
                source_select = f", {source_column} AS Source" if source_column else ", 'SEC' AS Source"
                where_clause = f"WHERE {column} = @target_ticker" if source_column else ""

                # Prepare the query with a parameterized query (only for filtered tables)
                job_config = None
                if source_column:
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("target_ticker", "STRING", ticker)
                        ]
                    )

                query = sql_template.format(
                    table=table,
                    column=column,
                    info_column=info_column,
                    date_column=date_column,
                    source_select=source_select,
                    where_clause=where_clause
                )

                # Execute the query
                query_job = client.query(query, job_config=job_config)

                # Collect and format results
                results = query_job.result()
                for row in results:
                    if row.Info not in existing_infos:  # Avoid duplicates
                        formatted_data.append({
                            "Ticker": row.Ticker,
                            "Notification": "News Notification",
                            "Type": "Informational",
                            "Info": row.Info,
                            "Date": row.Date.isoformat() if isinstance(row.Date, datetime) else str(row.Date),
                            "Source": row.Source
                        })

        # Check if there's data to insert
        if formatted_data:
            # Insert the data into the target table
            table_ref = client.dataset("notification").table("Notifications")

            # Define the schema if required (BigQuery infers if not specified)
            errors = client.insert_rows_json(table_ref, formatted_data)

            if errors:
                return f"Errors occurred while inserting data: {errors}", 500

            # Send an email notification if new rows were added
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
                message_body = f"New notifications added: {len(formatted_data)} entries."
                msg = MIMEText(message_body)
                msg['Subject'] = "New Notifications Added"
                msg['From'] = EMAIL_ADDRESS
                msg['To'] = USER_EMAIL_ADDRESS
                server.sendmail(EMAIL_ADDRESS, USER_EMAIL_ADDRESS, msg.as_string())

        return "Notification data successfully inserted.", 200

    except Exception as e:
        # Log and return any errors
        return f"Error occurred: {str(e)}", 500

# For local testing
if __name__ == "__main__":
    from flask import Request
    request = Request(environ={})
    response = news_notification(request)
    print(response)
