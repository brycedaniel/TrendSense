from google.cloud import bigquery
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from datetime import datetime

def news_notification(request):
    """
    Google Cloud Function to check specific BigQuery tables for data related to multiple tickers,
    store the results in a new table, and send email notifications for new rows.
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

    target_tickers = ["ASTS", "GSAT"]  # Add the list of tickers to monitor
    target_table = "trendsense.notification.Notifications"

    # Email configuration
    SMTP_SERVER = "smtp.zoho.com"
    SMTP_PORT = 587
    EMAIL_ADDRESS = "trendsense@zohomail.com"
    EMAIL_PASSWORD = "pZNUVbUid0tv"
    USER_EMAIL_ADDRESS = "trendsense@zohomail.com"

    sql_template = """
        SELECT {column} AS Ticker, {info_column} AS Info, {date_column} AS Date{source_select}
        FROM `{table}`
        {where_clause}
    """

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
                source_select = f", {source_column} AS Source" if source_column else ", 'SEC' AS Source"
                where_clause = f"WHERE {column} = @target_ticker"

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

                query_job = client.query(query, job_config=job_config)
                results = query_job.result()

                for row in results:
                    if row.Info not in existing_infos:
                        formatted_data.append({
                            "Ticker": row.Ticker,
                            "Notification": "News Notification",
                            "Type": "Informational",
                            "Info": row.Info,
                            "Date": row.Date.isoformat() if isinstance(row.Date, datetime) else str(row.Date),
                            "Source": row.Source
                        })

        if formatted_data:
            # Insert the data into the target table
            table_ref = client.dataset("notification").table("Notifications")
            errors = client.insert_rows_json(table_ref, formatted_data)

            if errors:
                return f"Errors occurred while inserting data: {errors}", 500

            # Send email notifications for new rows
            for data in formatted_data:
                send_email(SMTP_SERVER, SMTP_PORT, EMAIL_ADDRESS, EMAIL_PASSWORD, USER_EMAIL_ADDRESS, data)

        return "Notification data successfully inserted and emails sent.", 200

    except Exception as e:
        return f"Error occurred: {str(e)}", 500


def send_email(smtp_server, smtp_port, email_address, email_password, recipient, data):
    """
    Send an email notification with row data.
    """
    try:
        subject = f"New Notification for {data['Ticker']}"
        body = f"""
        New Notification:
        Ticker: {data['Ticker']}
        Notification: {data['Notification']}
        Type: {data['Type']}
        Info: {data['Info']}
        Date: {data['Date']}
        Source: {data['Source']}
        """
        msg = MIMEMultipart()
        msg["From"] = email_address
        msg["To"] = recipient
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(email_address, email_password)
            server.sendmail(email_address, recipient, msg.as_string())

    except Exception as e:
        print(f"Failed to send email: {str(e)}")

