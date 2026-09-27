import json
import csv
import os

import snowflake.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv


# -----------------------------
# Load Environment Variables
# -----------------------------

load_dotenv()


# -----------------------------
# Kafka Configuration
# -----------------------------

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "live_weather"


# -----------------------------
# CSV Configuration
# -----------------------------

OUTPUT_FILE = "data/live_weather.csv"


# -----------------------------
# Snowflake Configuration
# -----------------------------

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")


# -----------------------------
# Connect to Snowflake
# -----------------------------

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE
)

cursor = conn.cursor()

print("Snowflake connected successfully!")


# -----------------------------
# Connect to Kafka
# -----------------------------

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    group_id="live_weather_snowflake_consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Live Kafka Consumer connected!")
print("Waiting for live weather data...")
print("----------------------------------------")


# -----------------------------
# CSV Setup
# -----------------------------

fieldnames = [
    "city",
    "state",
    "country",
    "latitude",
    "longitude",
    "timestamp",
    "temperature",
    "humidity",
    "precipitation",
    "wind_speed",
    "weather_code"
]

file = open(
    OUTPUT_FILE,
    "a",
    newline="",
    encoding="utf-8"
)

writer = csv.DictWriter(
    file,
    fieldnames=fieldnames,
    extrasaction="ignore"
)

if file.tell() == 0:
    writer.writeheader()


# -----------------------------
# Record Counter
# -----------------------------

count = 0


try:

    for message in consumer:

        data = message.value

        # -----------------------------
        # Read identifying fields
        # -----------------------------

        city = data.get("city")
        timestamp = data.get("timestamp")

        # -----------------------------
        # Duplicate Check
        # CITY + TIMESTAMP
        # -----------------------------

        duplicate_query = """
            SELECT COUNT(*)
            FROM WEATHER_DATA
            WHERE CITY = %s
              AND TIMESTAMP = %s
        """

        cursor.execute(
            duplicate_query,
            (city, timestamp)
        )

        existing_count = cursor.fetchone()[0]

        if existing_count > 0:

            print(
                f"Skipped duplicate live record: "
                f"{city} | {timestamp}"
            )

            print("----------------------------------------")

            continue

        # -----------------------------
        # Save to CSV
        # -----------------------------

        writer.writerow(data)
        file.flush()

        # -----------------------------
        # Prepare Snowflake data
        # -----------------------------

        latitude = data.get("latitude")
        longitude = data.get("longitude")
        temperature = data.get("temperature")
        humidity = data.get("humidity")
        precipitation = data.get("precipitation")
        wind_speed = data.get("wind_speed")
        weather = data.get("weather_code")

        # -----------------------------
        # Insert into Snowflake
        # -----------------------------

        insert_query = """
            INSERT INTO WEATHER_DATA
            (
                city,
                latitude,
                longitude,
                temperature,
                humidity,
                precipitation,
                wind_speed,
                weather,
                timestamp
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(
            insert_query,
            (
                city,
                latitude,
                longitude,
                temperature,
                humidity,
                precipitation,
                wind_speed,
                str(weather) if weather is not None else None,
                timestamp
            )
        )

        conn.commit()

        # -----------------------------
        # Display result
        # -----------------------------

        count += 1

        print(f"Live weather record {count} saved!")
        print(data)
        print("----------------------------------------")


except KeyboardInterrupt:

    print("\nLive Kafka Consumer stopped by user.")


finally:

    file.close()

    consumer.close()

    cursor.close()
    conn.close()

    print("Live Kafka Consumer closed.")