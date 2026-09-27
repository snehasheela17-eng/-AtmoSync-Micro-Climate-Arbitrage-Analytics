import json
import csv
import os

import snowflake.connector
from dotenv import load_dotenv
from kafka import KafkaConsumer


# ==============================
# Load .env
# ==============================

load_dotenv()


# ==============================
# Kafka Settings
# ==============================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "historical_weather"
OUTPUT_FILE = "data/kafka_historical_weather.csv"


# ==============================
# Historical Location Settings
# ==============================

LOCATION_FILE = "data/historical_location.json"


# ==============================
# Load Historical Location
# ==============================

with open(LOCATION_FILE, "r", encoding="utf-8") as location_file:

    location = json.load(location_file)


HISTORICAL_CITY = location["city"]
HISTORICAL_STATE = location["state"]
HISTORICAL_LATITUDE = location["latitude"]
HISTORICAL_LONGITUDE = location["longitude"]


print("Historical location loaded successfully!")
print("City:", HISTORICAL_CITY)
print("State:", HISTORICAL_STATE)
print("Latitude:", HISTORICAL_LATITUDE)
print("Longitude:", HISTORICAL_LONGITUDE)


# ==============================
# Snowflake Settings
# ==============================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")


# ==============================
# Connect to Snowflake
# ==============================

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


# ==============================
# Connect to Kafka
# ==============================

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="earliest",
    group_id="historical_snowflake_consumer",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Kafka Consumer connected!")
print("Waiting for historical weather data...")


# ==============================
# CSV setup
# ==============================

fieldnames = None
file = None
writer = None

inserted_count = 0
duplicate_count = 0


try:

    for message in consumer:

        data = message.value


        # ==============================
        # Check for END marker
        # ==============================

        if data.get("message_type") == "END_OF_HISTORICAL_DATA":

            print("\nHistorical END marker received!")
            print("No more historical records to process.")

            break


        # ==============================
        # Create CSV file
        # ==============================

        if fieldnames is None:

            fieldnames = list(data.keys())

            file = open(
                OUTPUT_FILE,
                "w",
                newline="",
                encoding="utf-8"
            )

            writer = csv.DictWriter(
                file,
                fieldnames=fieldnames
            )

            writer.writeheader()


        # ==============================
        # Save Kafka data to CSV
        # ==============================

        writer.writerow(data)
        file.flush()


        # ==============================
        # Prepare Snowflake values
        # ==============================

        city = HISTORICAL_CITY
        latitude = HISTORICAL_LATITUDE
        longitude = HISTORICAL_LONGITUDE

        temperature = data.get("temp")
        humidity = data.get("rhum")
        precipitation = data.get("prcp")
        wind_speed = data.get("wspd")
        weather = data.get("coco")
        timestamp = data.get("timestamp")


        # ==============================
        # Duplicate protection
        # ==============================

        duplicate_check_query = """
            SELECT COUNT(*)
            FROM WEATHER_DATA
            WHERE CITY = %s
              AND TIMESTAMP = %s
        """

        cursor.execute(
            duplicate_check_query,
            (
                city,
                timestamp
            )
        )

        existing_count = cursor.fetchone()[0]


        # ==============================
        # Skip duplicate
        # ==============================

        if existing_count > 0:

            duplicate_count += 1

            print(
                f"Skipped duplicate record: "
                f"{city} | {timestamp}"
            )

            continue


        # ==============================
        # Insert into Snowflake
        # ==============================

        insert_query = """
            INSERT INTO WEATHER_DATA
            (
                CITY,
                LATITUDE,
                LONGITUDE,
                TEMPERATURE,
                HUMIDITY,
                PRECIPITATION,
                WIND_SPEED,
                WEATHER,
                TIMESTAMP
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

        inserted_count += 1

        print(
            f"Processed historical record: "
            f"{inserted_count}"
        )


finally:

    if file:
        file.close()

    consumer.close()
    cursor.close()
    conn.close()


# ==============================
# Final summary
# ==============================

print("\nHistorical weather processing completed!")

print("Historical location:", HISTORICAL_CITY)
print("Historical state:", HISTORICAL_STATE)
print("New records inserted:", inserted_count)
print("Duplicate records skipped:", duplicate_count)
print("CSV output:", OUTPUT_FILE)
print("Snowflake table: ATMOSYNC_DB.RAW.WEATHER_DATA")