import json
from confluent_kafka import Consumer
import snowflake.connector
from getpass import getpass


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "iot-snowflake-consumer",
    "auto.offset.reset": "earliest"
})

topic = "iot_container_telemetry"
consumer.subscribe([topic])


conn = snowflake.connector.connect(
    account="KXSJJKP-YE27378",
    user="snehasheela",
    password=getpass("Enter Snowflake password: "),
    role="ACCOUNTADMIN",
    database="ATMOSYNC_DB",
    schema="RAW"
)

cursor = conn.cursor()

print("Kafka → Snowflake consumer started...")
print("Reading IoT data from Kafka...")

count = 0

try:
    while count < 100:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))

        cursor.execute(
            """
            INSERT INTO IOT_TELEMETRY_STAGE
            (CONTAINER_ID, TIMESTAMP, TEMPERATURE, HUMIDITY, VIBRATION)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                data["Container_ID"],
                data["Timestamp"],
                data["Temperature"],
                data["Humidity"],
                data["Vibration"]
            )
        )

        
        cursor.execute(
            """
            INSERT INTO CONTAINER_TELEMETRY
            (CONTAINER_ID, TIMESTAMP, TEMPERATURE, HUMIDITY, VIBRATION)
            SELECT
                CONTAINER_ID,
                TIMESTAMP,
                TEMPERATURE,
                HUMIDITY,
                VIBRATION
            FROM IOT_TELEMETRY_STAGE
            WHERE CONTAINER_ID = %s
              AND TIMESTAMP = %s
              AND NOT EXISTS (
                  SELECT 1
                  FROM CONTAINER_TELEMETRY
                  WHERE CONTAINER_ID = %s
                    AND TIMESTAMP = %s
              )
            """,
            (
                data["Container_ID"],
                data["Timestamp"],
                data["Container_ID"],
                data["Timestamp"]
            )
        )

        conn.commit()

        count += 1

        print(f"Processed {count}: {data}")

finally:
    consumer.close()
    cursor.close()
    conn.close()

print(f"\nCompleted! {count} IoT records processed.")