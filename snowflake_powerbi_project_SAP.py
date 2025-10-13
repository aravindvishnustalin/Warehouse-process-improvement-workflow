

import polars as pl
from msal import PublicClientApplication
import msal
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import requests
import json
import os
import pymysql
import snowflake.connector
import pandas as pd
import logging
from pathlib import Path
from hdbcli import dbapi
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


# ---------------- Logging ----------------
logging.basicConfig(
    filename="etl_pipeline.log",   # logs also stored in a file
    level=logging.INFO,
    filemode="w",
    format="%(asctime)s - %(levelname)s - Line %(lineno)d - %(message)s"
)
logger = logging.getLogger(__name__)


# ---------------- Config ----------------

load_dotenv(override=True)


sf_user = os.getenv("SNOWFLAKE_USER")
sf_password = os.getenv("SNOWFLAKE_PASSWORD")
sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
sf_database = os.getenv("SNOWFLAKE_DATABASE")
# sf_database = os.getenv("SNOWFLAKE_DATABASE_BIGDATA")
sf_schema = os.getenv("SNOWFLAKE_SCHEMA")

FINAL_TABLE = "WT_SILOS_TRANSFORMED"


# ---------------- Extract ----------------
def test_hana_connection():
    # Connect to HANA Cloud
    conn = dbapi.connect(
        address='4c7d1127-8a8d-4915-8056-6346a5f55e7a.hana.trial-us10.hanacloud.ondemand.com',
        port='443',
        user='DBADMIN',
        password='Obsidiape123789!',
        sslValidateCertificate=False
    )

    # Load each table
    wt_silos = pd.read_sql(
        "SELECT * FROM warehouse_task_validation.wt_silos", conn)
    silo_bins = pd.read_sql(
        "SELECT * FROM warehouse_task_validation.silo_bins", conn)
    conn.close()
    return wt_silos, silo_bins


# ---------------- Transform ----------------


def transform_data(wt_silos: pd.DataFrame, silo_bins: pd.DataFrame) -> pd.DataFrame:
    try:
        # =========================
        # Parse dates and times
        # =========================
        wt_silos["Confirmation Date"] = pd.to_datetime(
            wt_silos["Confirmation Date"], format="%m/%d/%Y", errors="coerce"
        )

        wt_silos["Confirmation Time"] = pd.to_datetime(
            wt_silos["Confirmation Time"], format="%I:%M:%S %p", errors="coerce"
        ).dt.strftime("%H:%M:%S")  # HH:MM:SS string

        # =========================
        # Strip text columns
        # =========================
        wt_silos["Whse Process Type"] = wt_silos["Whse Process Type"].astype(
            str).str.strip()
        wt_silos["Product"] = wt_silos["Product"].fillna(
            "").astype(str).str.strip()
        wt_silos["Destination Bin"] = wt_silos["Destination Bin"].astype(
            str).str.strip()

        # =========================
        # Filter rows
        # =========================
        wt_silos = wt_silos[wt_silos["Whse Process Type"] == "9999"]

        wt_silos = wt_silos[
            wt_silos["Destination Bin"].str.startswith("S") |
            wt_silos["Destination Bin"].str.startswith("DECAF") |
            (wt_silos["Destination Bin"] == "COLDBREW")
        ]

        wt_silos = wt_silos[wt_silos["Product"] != ""]

        # =========================
        # Deduplicate silo_bins
        # =========================
        silo_bins = silo_bins[["EWM BIN", "SAP#"]
                              ].drop_duplicates(subset=["EWM BIN"])
        silo_bins["EWM BIN"] = silo_bins["EWM BIN"].astype(str).str.strip()
        silo_bins["SAP#"] = silo_bins["SAP#"].astype(str).str.strip()

        # =========================
        # Merge (join)
        # =========================
        merged = wt_silos.merge(
            silo_bins,
            left_on="Destination Bin",
            right_on="EWM BIN",
            how="left"
        )

        # =========================
        # Count matching rows
        # =========================
        matches = wt_silos[wt_silos["Destination Bin"].isin(
            silo_bins["EWM BIN"])]
        match_rate = len(matches) / len(wt_silos) if len(wt_silos) > 0 else 0
        logging.info(
            f"Matching rows: {len(matches)} of {len(wt_silos)} ({match_rate:.2%} match rate)")

        # =========================
        # Add "Correct usage / Wrong usage" column
        # =========================
        merged["Correct usage/Wrong usage"] = merged.apply(
            lambda row: "Correct usage" if row["Product"] == row["SAP#"] else "Wrong usage",
            axis=1
        )

        # =========================
        # Sort
        # =========================
        merged = merged.sort_values(
            by=["Confirmation Date", "Confirmation Time"])

        logging.info("Transformations applied successfully (Pandas).")
        return merged

    except Exception as e:
        logging.error(f"Transformation failed: {e}")
        raise


# ---------------- Load ----------------


def load_to_snowflake(merged_df, table_name, schema, database):
    import datetime

    """
    Loads a Pandas DataFrame into a Snowflake table.
    Automatically creates or replaces the target table.
    """

    try:
        # 1️⃣ Connect
        sf_conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            role="ACCOUNTADMIN"
        )

        cur = sf_conn.cursor()
        # 2️⃣ Set context (forces correct DB & Schema)
        print(f"Using database: {database}, schema: {schema}")
        cur.execute(f"USE DATABASE {database}")
        cur.execute(f"USE SCHEMA {schema}")
        cur.execute(f"USE WAREHOUSE {sf_warehouse}")

        logging.info(f"Connected to Snowflake: {database}.{schema}")

        # 3️⃣ Map datatypes for table creation
        columns = []
        for col, dtype in merged_df.dtypes.items():
            if "int" in str(dtype):
                sf_type = "INTEGER"
            elif "float" in str(dtype):
                sf_type = "FLOAT"
            elif "datetime" in str(dtype):
                sf_type = "TIMESTAMP_NTZ"
            elif col == "Confirmation Date":
                sf_type = "DATE"
            elif col == "Confirmation Time":
                sf_type = "TIME"
            else:
                sf_type = "STRING"
            columns.append(f'"{col}" {sf_type}')

        # 4️⃣ Create or replace table
        create_sql = f"""
        CREATE OR REPLACE TABLE {schema}.{table_name} (
            {', '.join(columns)}
        )
        """
        cur.execute(create_sql)
        logging.info(f"Created table: {database}.{schema}.{table_name}")
        # 3️⃣ Prepare data for insertion
        insert_values = []
        for row in merged_df.itertuples(index=False, name=None):
            formatted_row = []
            for val in row:
                if isinstance(val, datetime.datetime):
                    formatted_row.append(val.strftime("%Y-%m-%d %H:%M:%S"))
                elif isinstance(val, datetime.date):
                    formatted_row.append(val.strftime("%Y-%m-%d"))
                elif isinstance(val, datetime.time):
                    formatted_row.append(val.strftime("%H:%M:%S"))
                elif pd.isna(val):
                    formatted_row.append(None)
                else:
                    formatted_row.append(val)
            insert_values.append(tuple(formatted_row))

        # 4️⃣ Execute insert with proper placeholders
        placeholders = ", ".join(["%s"] * len(merged_df.columns))
        insert_sql = f'INSERT INTO {table_name} VALUES ({placeholders})'
        cur.executemany(insert_sql, insert_values)
        sf_conn.commit()

        logging.info(
            f"Inserted {len(merged_df)} rows into {table_name} successfully.")
        cur.close()

    except Exception as e:
        logging.error(f"❌ Snowflake load failed: {e}")
        raise


# ---------------- Trigger Power Automate ----------------
"""
def trigger_power_automate():
    try:
        flow_url = os.getenv("POWER_AUTOMATE_URL")  # from your .env
        payload = {
            "status": "success",
            "message": "Snowflake load completed",
            "report_link": "https://app.powerbi.com/links/uX1al-aKSP?ctid=19276e8f-bb6e-4d88-a6dd-701ce9553050&pbi_source=linkShare"
        }
        headers = {"Content-Type": "application/json"}

        response = requests.post(
            flow_url, json=payload, headers=headers)  # <-- changed here

        if response.ok:  # covers 200 and 202
            logger.info("Triggered Power Automate flow successfully.")
        else:
            logger.error(
                f"Failed to trigger flow. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logger.error(f"Error triggering Power Automate flow: {e}")
"""


def send_email_graph():
    try:
        tenant_id = os.getenv("COR_TENANT_ID")
        client_id = os.getenv("COR_CLIENT_ID")
        client_secret = os.getenv("COR_CLIENT_SECRET")
        sender = os.getenv("SENDER_EMAIL")
        recipient = os.getenv("EMAIL_RECIPIENT")
        report_link = "https://app.powerbi.com/links/uX1al-aKSP?ctid=19276e8f-bb6e-4d88-a6dd-701ce9553050&pbi_source=linkShare"

        # 1. Get OAuth2 token
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        token_data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://graph.microsoft.com/.default"
        }
        token_response = requests.post(token_url, data=token_data)
        token_response.raise_for_status()
        access_token = token_response.json()["access_token"]
        logger.info("Obtained access token for Microsoft Graph.")

        # 2. Send email
        url = f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
        headers = {"Authorization": f"Bearer {access_token}",
                   "Content-Type": "application/json"}

        email_msg = {
            "message": {
                "subject": "Snowflake Load Completed",
                "body": {
                    "contentType": "HTML",
                    "content": f"""
                        <p>Hello,</p>
                        <p>✅ The Snowflake load has completed successfully.</p>
                        <p>📊 <a href="{report_link}">View Power BI Report</a></p>
                        <p>Regards,<br>ETL Pipeline</p>
                    """
                },
                "toRecipients": [
                    {"emailAddress": {"address": recipient}}
                ]
            }
        }

        response = requests.post(url, headers=headers, json=email_msg)
        response.raise_for_status()
        logger.info("Email sent successfully via Microsoft Graph.")

    except Exception as e:
        logger.error(f"Error sending email via Graph API: {e}")


def send_email_outlook():
    try:
        sender = os.getenv("SENDER_EMAIL")   # e.g. yourname@outlook.com
        recipient = os.getenv("EMAIL_RECIPIENT")
        # app password, not normal password
        outlook_password = os.getenv("OUTLOOK_APP_PASSWORD")
        report_link = "https://app.powerbi.com/links/..."

        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = recipient
        msg["Subject"] = "Snowflake Load Completed"

        body = f"""
        <p>Hello,</p>
        <p>✅ The Snowflake load has completed successfully.</p>
        <p>📊 <a href="{report_link}">View Power BI Report</a></p>
        <p>Regards,<br>ETL Pipeline</p>
        """
        msg.attach(MIMEText(body, "html"))

        # Connect to Outlook SMTP
        with smtplib.SMTP("smtp.office365.com", 587) as server:
            server.starttls()
            server.login(sender, outlook_password)
            server.sendmail(sender, recipient, msg.as_string())

        logger.info("Email sent successfully via Outlook SMTP.")

    except Exception as e:
        logger.error(f"Error sending email via Outlook SMTP: {e}")


def send_email():
    # Load environment variables
    CLIENT_ID = os.getenv("PER_CLIENT_ID")
    TENANT_ID = "common"
    # for personal Outlook.com
    AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
    # default Mail.Send if not provided
    SCOPES = [os.getenv("SCOPES", "Mail.Send")]
    # "a@gmail.com,b@everforce.com"
    recipients_str = os.getenv("EMAIL_RECIPIENT")
    recipients = [r.strip() for r in recipients_str.split(",")]
    # Not strictly needed, Graph uses logged-in user
    SENDER = os.getenv("SENDER_EMAIL")
    report_link = "https://app.powerbi.com/links/uX1al-aKSP?ctid=19276e8f-bb6e-4d88-a6dd-701ce9553050&pbi_source=linkShare"
    """
    Sends an email using Microsoft Graph API via OAuth2 interactive login.
    """
    # Use PublicClientApplication for delegated (interactive) auth
    app = PublicClientApplication(
        CLIENT_ID, authority=AUTHORITY
    )

    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])
    else:
        result = app.acquire_token_interactive(SCOPES)

    if not result or "access_token" not in result:
        print("❌ Could not obtain access token.")
        print("Error details:", result)
        raise Exception("❌ Could not obtain access token.")
    access_token = result["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}
    logger.info("Acquired access token successfully.")

    # Email subject & body (hardcoded here, can be parameterized later)
    email_msg = {
        "message": {
            "subject": "Warehouse Tasks Validation Power BI Report",
            "body": {
                "contentType": "HTML",
                "content": f"""
                    <p>Hello,</p>
                    <p>📊 <a href="{report_link}">View Power BI Report</a></p>
                    <p>Regards,<br>ETL Pipeline</p>
                """
            },
            "toRecipients": [{"emailAddress": {"address": r}} for r in recipients],
        }
    }

    # Call Graph API
    endpoint = "https://graph.microsoft.com/v1.0/me/sendMail"
    headers = {"Authorization": f"Bearer {access_token}"}
    r = requests.post(endpoint, headers=headers, json=email_msg)

    if r.status_code == 202:
        print(f"✅ Email sent successfully")
    else:
        print(f"❌ Error sending email: {r.status_code} - {r.text}")


# ---------------- Main Orchestration ----------------


def main():
    try:
        logger.info("Pipeline started.")

        # Extract
        wt_silos, silo_bins = test_hana_connection()

        # Transform
        merged_df = transform_data(wt_silos, silo_bins)
        schema = sf_schema
        table_name = 'warehouse_task_validation'
        database = sf_database
        # Load
        load_to_snowflake(merged_df, table_name, schema, database)

        # ✅ Send email notification
        send_email()
        logger.info("Pipeline completed successfully.")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()
