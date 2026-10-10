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

ARBITRAGE_TABLE = "ATMOSYNC_DB.RAW.ATMOSYNC_ARBITRAGE_ANALYSIS"
WEATHER_TABLE = "ATMOSYNC_DB.RAW.WEATHER_DATA"

POLL_SECONDS = 60
ARBITRAGE_FRESHNESS_MINUTES = 10
WEATHER_FRESHNESS_MINUTES = 20  # Demo setting for a roughly 15-minute producer cadence
MAX_ALERTS_PER_EMAIL = 50
MAX_ROWS_PER_CHECK = 500

# Illustrative demo thresholds only. Calibrate using validated, cargo-specific standards.
HIGH_TEMPERATURE_C = 35.0
HEAVY_PRECIPITATION_MM = 10.0
HIGH_HUMIDITY_PCT = 80.0

STATE_DIR = BASE_DIR / "data" / "processed"
STATE_DIR.mkdir(parents=True, exist_ok=True)
STATE_DB = STATE_DIR / "arbitrage_alert_state.sqlite3"

# --------------------------------------------------
# 2. EMAIL CONFIGURATION
# --------------------------------------------------
def get_email_settings():
    sender = os.getenv("ALERT_EMAIL_SENDER", "").strip()
    password = os.getenv("ALERT_EMAIL_APP_PASSWORD", "").replace(" ", "")
    recipient = os.getenv("ALERT_EMAIL_RECIPIENT", "").strip()
    if not sender or not password or not recipient:
        raise ValueError(
            "Email settings are missing. Check ALERT_EMAIL_SENDER, "
            "ALERT_EMAIL_APP_PASSWORD, and ALERT_EMAIL_RECIPIENT in .env."
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
        "This message tests Gmail delivery only. It does not represent a real "
        "arbitrage opportunity or a weather warning."
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
    serialized = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def already_sent(fingerprint):
    with sqlite3.connect(STATE_DB) as connection:
        result = connection.execute(
            "SELECT 1 FROM sent_alerts WHERE fingerprint = ?", (fingerprint,)
        ).fetchone()
    return result is not None


def mark_as_sent(fingerprints):
    with sqlite3.connect(STATE_DB) as connection:
        connection.executemany(
            "INSERT OR IGNORE INTO sent_alerts (fingerprint, sent_at) VALUES (?, ?)",
            [(fingerprint, datetime.now().isoformat()) for fingerprint in fingerprints],
        )

# --------------------------------------------------
# 4. CONNECT TO SNOWFLAKE
# --------------------------------------------------
def connect_to_snowflake():
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise ValueError("Missing Snowflake configuration: " + ", ".join(missing))

    config = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "session_parameters": {"TIMEZONE": "Asia/Kolkata"},
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
# 5. FETCH RECENT ARBITRAGE OPPORTUNITIES (EXISTING BEHAVIOUR)
# --------------------------------------------------
def fetch_recent_opportunities():
    connection = connect_to_snowflake()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(f'SELECT MAX("TIMESTAMP") FROM {ARBITRAGE_TABLE}')
            latest_timestamp = cursor.fetchone()[0]
            if latest_timestamp is None:
                print("No records found in the arbitrage table.")
                return []

            cursor.execute(f"""
                SELECT DATEDIFF('second', MAX("TIMESTAMP"),
                    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
                FROM {ARBITRAGE_TABLE}
            """)
            age_seconds = cursor.fetchone()[0]
            if age_seconds is None or age_seconds < 0 or age_seconds > ARBITRAGE_FRESHNESS_MINUTES * 60:
                print(f"Skipping arbitrage alerts: data is stale. Latest timestamp: {latest_timestamp}")
                return []

            cursor.execute(f"""
                SELECT "TIMESTAMP", CITY, STATE, PRODUCT, PRICE_INR,
                       PRICE_CHANGE_PCT, SUPPLY_LEVEL, DEMAND_LEVEL,
                       DEMAND_SUPPLY_GAP, ARBITRAGE_OPPORTUNITY
                FROM {ARBITRAGE_TABLE}
                WHERE "TIMESTAMP" >= DATEADD(
                    minute, -{ARBITRAGE_FRESHNESS_MINUTES}, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
                  AND ARBITRAGE_OPPORTUNITY = 'HIGH_OPPORTUNITY'
                ORDER BY "TIMESTAMP" DESC
                LIMIT {MAX_ROWS_PER_CHECK}
            """)
            names = [item[0] for item in cursor.description]
            return [dict(zip(names, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()
    finally:
        connection.close()


def format_opportunity(record):
    return (
        f"Timestamp: {record.get('TIMESTAMP')}\n"
        f"City: {record.get('CITY')}, State: {record.get('STATE')}\n"
        f"Product: {record.get('PRODUCT')}\n"
        f"Price (INR): {record.get('PRICE_INR')}\n"
        f"Price change (%): {record.get('PRICE_CHANGE_PCT')}\n"
        f"Supply level: {record.get('SUPPLY_LEVEL')}\n"
        f"Demand level: {record.get('DEMAND_LEVEL')}\n"
        f"Demand-supply gap: {record.get('DEMAND_SUPPLY_GAP')}\n"
        f"Opportunity: {record.get('ARBITRAGE_OPPORTUNITY')}\n"
    )


def check_and_send_arbitrage_alerts():
    records = fetch_recent_opportunities()
    if not records:
        print("No recent high-opportunity records to alert on.")
        return
    unseen = []
    for record in records:
        fingerprint = make_fingerprint({"type": "arbitrage", **record})
        if not already_sent(fingerprint):
            unseen.append((fingerprint, record))
    if not unseen:
        print("No new arbitrage opportunities. Duplicate alerts skipped.")
        return
    unseen = unseen[:MAX_ALERTS_PER_EMAIL]
    body = (
        "AtmoSync has detected new high-opportunity records.\n\n"
        "Review the details in your project dashboard.\n\n"
        + "\n------------------------------\n".join(format_opportunity(record) for _, record in unseen)
        + "\nThis is an automated project notification.\n"
    )
    send_email(f"[AtmoSync] {len(unseen)} new high-opportunity record(s)", body)
    mark_as_sent([fingerprint for fingerprint, _ in unseen])
    print(f"Arbitrage alert sent for {len(unseen)} new record(s).")

# --------------------------------------------------
# 6. FETCH FRESH WEATHER RECORDS AND DETECT WARNINGS
# --------------------------------------------------
def fetch_fresh_weather():
    """Return each city's newest record only when that record is within the freshness window."""
    connection = connect_to_snowflake()
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(f"""
                SELECT CITY, LATITUDE, LONGITUDE, TEMPERATURE, HUMIDITY,
                       PRECIPITATION, WIND_SPEED, WEATHER, "TIMESTAMP",
                       DATEDIFF('second', "TIMESTAMP", CURRENT_TIMESTAMP()::TIMESTAMP_NTZ) / 60.0 AS AGE_MINUTES
                FROM {WEATHER_TABLE}
                WHERE "TIMESTAMP" >= DATEADD(
                    minute, -{WEATHER_FRESHNESS_MINUTES}, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
                  AND "TIMESTAMP" <= CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                QUALIFY ROW_NUMBER() OVER (PARTITION BY CITY ORDER BY "TIMESTAMP" DESC) = 1
                ORDER BY "TIMESTAMP" DESC
            """)
            columns = [item[0] for item in cursor.description]
            fresh = [dict(zip(columns, row)) for row in cursor.fetchall()]
            for row in fresh:
                if row.get("AGE_MINUTES") is not None:
                    row["AGE_MINUTES"] = round(float(row["AGE_MINUTES"]), 1)
            print(f"Fresh weather records: {len(fresh)} city/cities within {WEATHER_FRESHNESS_MINUTES} minutes.")
            return fresh
        finally:
            cursor.close()
    finally:
        connection.close()


def weather_warning_reasons(record):
    reasons = []
    temp = record.get("TEMPERATURE")
    humidity = record.get("HUMIDITY")
    precipitation = record.get("PRECIPITATION")
    if temp is not None and float(temp) >= HIGH_TEMPERATURE_C:
        reasons.append(f"temperature {float(temp):.1f}°C meets demo threshold ≥ {HIGH_TEMPERATURE_C:.0f}°C")
    if precipitation is not None and float(precipitation) >= HEAVY_PRECIPITATION_MM:
        reasons.append(f"precipitation {float(precipitation):.1f} mm meets demo threshold ≥ {HEAVY_PRECIPITATION_MM:.0f} mm")
    if humidity is not None and float(humidity) >= HIGH_HUMIDITY_PCT:
        reasons.append(f"humidity {float(humidity):.0f}% meets demo threshold ≥ {HIGH_HUMIDITY_PCT:.0f}%")
    return reasons


def find_candidate_destination(source_record, fresh_records):
    """Find a comparatively lower-warning city; this is not a route/safety approval."""
    source_city = str(source_record.get("CITY", "")).strip().casefold()
    candidates = []
    for record in fresh_records:
        city = str(record.get("CITY", "")).strip().casefold()
        if not city or city == source_city:
            continue
        if weather_warning_reasons(record):
            continue
        # Require temperature, humidity, and precipitation values before comparison.
        if any(record.get(key) is None for key in ("TEMPERATURE", "HUMIDITY", "PRECIPITATION")):
            continue
        candidates.append(record)
    if not candidates:
        return None
    # Prefer lower temperature, then lower precipitation and humidity; demo-only ranking.
    return min(candidates, key=lambda r: (float(r["TEMPERATURE"]), float(r["PRECIPITATION"]), float(r["HUMIDITY"])))


def format_weather_alert(record, reasons, candidate):
    lines = [
        f"City: {record.get('CITY')}",
        f"Observation timestamp (Snowflake): {record.get('TIMESTAMP')}",
        f"Data age: {record.get('AGE_MINUTES')} minute(s)",
        f"Temperature: {record.get('TEMPERATURE')} °C",
        f"Humidity: {record.get('HUMIDITY')} %",
        f"Precipitation: {record.get('PRECIPITATION')} mm (source interval/units should be verified)",
        f"Wind speed: {record.get('WIND_SPEED')} (confirm source units)",
        f"Weather code: {record.get('WEATHER')}",
        "Demo warning reason(s): " + "; ".join(reasons),
        "Potential cargo impact: if a shipment contains perishable produce, review its cargo-specific temperature, humidity, packaging, and cold-chain requirements. These weather readings alone do not prove spoilage.",
    ]
    if candidate:
        lines.extend([
            "",
            f"Candidate destination for logistics review: {candidate.get('CITY')}",
            f"Candidate's latest record: {candidate.get('TIMESTAMP')} (age {candidate.get('AGE_MINUTES')} minute(s))",
            f"Candidate weather: {candidate.get('TEMPERATURE')} °C, humidity {candidate.get('HUMIDITY')}%, precipitation {candidate.get('PRECIPITATION')} mm",
            "This candidate only passed the demo weather thresholds. Confirm route feasibility, travel time, destination capacity, handling facilities, and shipment requirements before making any decision. No rerouting has been performed.",
        ])
    else:
        lines.extend([
            "",
            "Alternative destination: No verified candidate is available from the currently fresh weather records.",
            "Do not infer that a stale city is safe. Refresh its weather data and verify route feasibility, capacity, and cargo requirements before considering rerouting.",
        ])
    return "\n".join(lines)


def check_and_send_weather_alerts():
    fresh_records = fetch_fresh_weather()
    if not fresh_records:
        print("No weather alerts evaluated: no city has weather data within the freshness window.")
        return

    alerts = []
    for record in fresh_records:
        reasons = weather_warning_reasons(record)
        if reasons:
            candidate = find_candidate_destination(record, fresh_records)
            alerts.append((record, reasons, candidate))

    if not alerts:
        print("No weather warning thresholds triggered in fresh weather records.")
        return

    unseen = []
    for record, reasons, candidate in alerts:
        fingerprint_record = {
            "type": "weather",
            "CITY": record.get("CITY"),
            "TIMESTAMP": record.get("TIMESTAMP"),
            "TEMPERATURE": record.get("TEMPERATURE"),
            "HUMIDITY": record.get("HUMIDITY"),
            "PRECIPITATION": record.get("PRECIPITATION"),
            "WIND_SPEED": record.get("WIND_SPEED"),
            "WEATHER": record.get("WEATHER"),
            "reasons": reasons,
        }
        fingerprint = make_fingerprint(fingerprint_record)
        if not already_sent(fingerprint):
            unseen.append((fingerprint, record, reasons, candidate))

    if not unseen:
        print("No new weather warnings. Duplicate alerts skipped.")
        return

    unseen = unseen[:MAX_ALERTS_PER_EMAIL]
    body = (
        "AtmoSync detected one or more weather readings that crossed configured DEMO thresholds.\n"
        "These thresholds are illustrative, not certified produce-storage limits.\n\n"
        + "\n\n----------------------------------------\n\n".join(
            format_weather_alert(record, reasons, candidate)
            for _, record, reasons, candidate in unseen
        )
        + "\n\nAutomated notification. Check the live source and cargo-specific guidance before acting."
    )
    send_email(f"[AtmoSync] Weather warning: {len(unseen)} city alert(s)", body)
    mark_as_sent([fingerprint for fingerprint, _, _, _ in unseen])
    print(f"Weather alert email sent for {len(unseen)} city/cities.")

# --------------------------------------------------
# 7. MAIN PROGRAM
# --------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="AtmoSync arbitrage and live weather email alert checker")
    parser.add_argument("--test-email", action="store_true", help="Send a real email delivery test without querying Snowflake")
    parser.add_argument("--check-weather-once", action="store_true", help="Check weather once and send an email only if a fresh record crosses a demo threshold")
    parser.add_argument("--check-arbitrage-once", action="store_true", help="Check arbitrage once and send an email for new qualifying records")
    args = parser.parse_args()

    try:
        if args.test_email:
            send_test_email()
            return

        initialize_state_db()
        if args.check_weather_once:
            check_and_send_weather_alerts()
            return
        if args.check_arbitrage_once:
            check_and_send_arbitrage_alerts()
            return

        print("AtmoSync arbitrage + weather alert checker started.")
        print(f"Polling every {POLL_SECONDS} seconds.")
        print(f"Arbitrage freshness: {ARBITRAGE_FRESHNESS_MINUTES} minutes; weather freshness: {WEATHER_FRESHNESS_MINUTES} minutes.")
        print("Weather warning thresholds are illustrative demo values.")
        print("Press Ctrl+C to stop.")
        while True:
            try:
                check_and_send_arbitrage_alerts()
            except Exception as error:
                print(f"Arbitrage check failed: {type(error).__name__}: {error}", file=sys.stderr)
            try:
                check_and_send_weather_alerts()
            except Exception as error:
                print(f"Weather check failed: {type(error).__name__}: {error}", file=sys.stderr)
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        print("\nAtmoSync alert checker stopped.")
    except Exception as error:
        print(f"Startup failed: {type(error).__name__}: {error}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
