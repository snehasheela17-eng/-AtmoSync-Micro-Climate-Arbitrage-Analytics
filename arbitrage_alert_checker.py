
import os
import sys
import time
import json
import hashlib
import sqlite3
import smtplib
import argparse
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage

import snowflake.connector
from dotenv import load_dotenv


# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

TABLE = "ATMOSYNC_DB.RAW.ATMOSYNC_ARBITRAGE_ANALYSIS"

POLL_SECONDS = 60
FRESHNESS_MINUTES = 10
MAX_ALERTS_PER_EMAIL = 50
MAX_ROWS_PER_CHECK = 500

STATE_DIR = BASE_DIR / "data" / "processed"
STATE_DIR.mkdir(parents=True, exist_ok=True)

STATE_DB = STATE_DIR / "arbitrage_alert_state.sqlite3"


# --------------------------------------------------
# 2. EMAIL CONFIGURATION
# --------------------------------------------------

def get_email_settings():
    sender = os.getenv("ALERT_EMAIL_SENDER", "").strip()
    password = os.getenv(
        "ALERT_EMAIL_APP_PASSWORD", ""
    ).replace(" ", "")
    recipient = os.getenv("ALERT_EMAIL_RECIPIENT", "").strip()

    if not sender or not password or not recipient:
        raise ValueError(
            "Email settings are missing. Check these variables "
            "in your .env file: ALERT_EMAIL_SENDER, "
            "ALERT_EMAIL_APP_PASSWORD, ALERT_EMAIL_RECIPIENT."
        )

    return sender, password, recipient


def send_email(subject, body):
    sender, password, recipient = get_email_settings()

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = recipient
    message.set_content(body)

    with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(sender, password)
        server.send_message(message)

    print(f"Email accepted by Gmail for delivery to {recipient}.")


def send_test_email():
    send_email(
        "[AtmoSync] Email Delivery Test",
        "This is a real email delivery test from AtmoSync.\n\n"
        "This message tests Gmail delivery only. "
        "It does not represent a real arbitrage opportunity."
    )
    print("Test email completed.")


# --------------------------------------------------
# 3. LOCAL DATABASE FOR DUPLICATE PREVENTION
# --------------------------------------------------

def initialize_state_db():
    with sqlite3.connect(STATE_DB) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS sent_alerts (
                fingerprint TEXT PRIMARY KEY,
                sent_at TEXT NOT NULL
            )
        """)


def make_fingerprint(record):
    serialized = json.dumps(
        record,
        sort_keys=True,
        default=str
    )
    return hashlib.sha256(
        serialized.encode("utf-8")
    ).hexdigest()


def already_sent(fingerprint):
    with sqlite3.connect(STATE_DB) as connection:
        result = connection.execute(
            "SELECT 1 FROM sent_alerts WHERE fingerprint = ?",
            (fingerprint,)
        ).fetchone()

    return result is not None


def mark_as_sent(fingerprints):
    with sqlite3.connect(STATE_DB) as connection:
        connection.executemany(
            """
            INSERT OR IGNORE INTO sent_alerts
            (fingerprint, sent_at)
            VALUES (?, ?)
            """,
            [
                (fingerprint, datetime.now().isoformat())
                for fingerprint in fingerprints
            ]
        )


# --------------------------------------------------
# 4. CONNECT TO SNOWFLAKE
# --------------------------------------------------

def connect_to_snowflake():
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]

    missing = [
        key for key in required
        if not os.getenv(key)
    ]

    if missing:
        raise ValueError(
            "Missing Snowflake configuration: "
            + ", ".join(missing)
        )

    config = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "session_parameters": {
            "TIMEZONE": "Asia/Kolkata"
        },
    }
    optional_mapping = {
        "SNOWFLAKE_WAREHOUSE": "warehouse",
        "SNOWFLAKE_DATABASE": "database",
        "SNOWFLAKE_SCHEMA": "schema",
        "SNOWFLAKE_ROLE": "role",
    }

    for env_name, config_name in optional_mapping.items():
        value = os.getenv(env_name)
        if value:
            config[config_name] = value

    return snowflake.connector.connect(**config)


# --------------------------------------------------
# 5. FETCH RECENT HIGH-OPPORTUNITY RECORDS
# --------------------------------------------------

def fetch_recent_opportunities():
    connection = connect_to_snowflake()

    try:
        cursor = connection.cursor()

        try:
            cursor.execute(f"""
                SELECT MAX("TIMESTAMP")
                FROM {TABLE}
            """)

            latest_result = cursor.fetchone()
            latest_timestamp = latest_result[0]

            if latest_timestamp is None:
                print("No records found in the arbitrage table.")
                return []

            cursor.execute(f"""
                SELECT
                    DATEDIFF(
                        'second',
                        MAX("TIMESTAMP"),
                        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                    )
                FROM {TABLE}
            """)

            age_seconds = cursor.fetchone()[0]

            if age_seconds is None or age_seconds > (
                FRESHNESS_MINUTES * 60
            ):
                print(
                    "Skipping alerts: Snowflake data is stale. "
                    f"Latest timestamp: {latest_timestamp}"
                )
                return []

            cursor.execute(f"""
                SELECT
                    "TIMESTAMP",
                    CITY,
                    STATE,
                    PRODUCT,
                    PRICE_INR,
                    PRICE_CHANGE_PCT,
                    SUPPLY_LEVEL,
                    DEMAND_LEVEL,
                    DEMAND_SUPPLY_GAP,
                    ARBITRAGE_OPPORTUNITY
                FROM {TABLE}
                WHERE
                    "TIMESTAMP" >= DATEADD(
                        minute,
                        -{FRESHNESS_MINUTES},
                        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                    )
                    AND ARBITRAGE_OPPORTUNITY = 'HIGH_OPPORTUNITY'
                ORDER BY "TIMESTAMP" DESC
                LIMIT {MAX_ROWS_PER_CHECK}
            """)

            column_names = [
                description[0]
                for description in cursor.description
            ]

            return [
                dict(zip(column_names, row))
                for row in cursor.fetchall()
            ]

        finally:
            cursor.close()

    finally:
        connection.close()


# --------------------------------------------------
# 6. FORMAT AND SEND OPPORTUNITY ALERTS
# --------------------------------------------------

def format_opportunity(record):
    return (
        f"Timestamp: {record.get('TIMESTAMP')}\n"
        f"City: {record.get('CITY')}, "
        f"State: {record.get('STATE')}\n"
        f"Product: {record.get('PRODUCT')}\n"
        f"Price (INR): {record.get('PRICE_INR')}\n"
        f"Price change (%): {record.get('PRICE_CHANGE_PCT')}\n"
        f"Supply level: {record.get('SUPPLY_LEVEL')}\n"
        f"Demand level: {record.get('DEMAND_LEVEL')}\n"
        f"Demand-supply gap: {record.get('DEMAND_SUPPLY_GAP')}\n"
        f"Opportunity: {record.get('ARBITRAGE_OPPORTUNITY')}\n"
    )


def check_and_send_alerts():
    records = fetch_recent_opportunities()

    if not records:
        print("No recent high-opportunity records to alert on.")
        return

    unseen = []

    for record in records:
        fingerprint = make_fingerprint(record)

        if not already_sent(fingerprint):
            unseen.append((fingerprint, record))

    if not unseen:
        print("No new opportunities. Duplicate alerts skipped.")
        return

    # Limit the number sent in one email.
    unseen = unseen[:MAX_ALERTS_PER_EMAIL]

    body = (
        "AtmoSync has detected new high-opportunity records.\n\n"
        "Review the details below in your project dashboard.\n\n"
        + "\n------------------------------\n".join(
            format_opportunity(record)
            for _, record in unseen
        )
        + "\n\nThis is an automated project notification."
    )

    subject = (
        f"[AtmoSync] {len(unseen)} new high-opportunity record(s)"
    )

    # Mark records only after the email send succeeds.
    send_email(subject, body)

    mark_as_sent([fingerprint for fingerprint, _ in unseen])

    print(f"Alert sent for {len(unseen)} new record(s).")


# --------------------------------------------------
# 7. MAIN PROGRAM
# --------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="AtmoSync arbitrage email alert checker"
    )
    parser.add_argument(
        "--test-email",
        action="store_true",
        help="Send a real email delivery test without querying Snowflake."
    )
    args = parser.parse_args()

    try:
        if args.test_email:
            send_test_email()
            return

        initialize_state_db()

        print("AtmoSync arbitrage alert checker started.")
        print(f"Checking every {POLL_SECONDS} seconds.")
        print(
            f"Only data from the last {FRESHNESS_MINUTES} "
            "minutes is eligible for alerts."
        )
        print("Press Ctrl+C to stop.")

        while True:
            try:
                check_and_send_alerts()
            except Exception as error:
                print(
                    f"Check failed: {type(error).__name__}: {error}",
                    file=sys.stderr
                )

            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("\nAtmoSync alert checker stopped.")

    except Exception as error:
        print(
            f"Startup failed: {type(error).__name__}: {error}",
            file=sys.stderr
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
