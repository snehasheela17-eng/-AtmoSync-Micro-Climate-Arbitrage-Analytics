import pandas as pd
import json
import time
from kafka import KafkaProducer


# ==============================
# File and Kafka settings
# ==============================

CSV_FILE = "data/historical_weather_cleaned.csv"
KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "historical_weather"


# ==============================
# Load cleaned historical data
# ==============================

df = pd.read_csv(CSV_FILE)

print("Historical dataset loaded successfully!")
print("Total records:", len(df))


# ==============================
# Create Kafka producer
# ==============================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Kafka producer connected successfully!")
print("Starting to send historical weather data...")


# ==============================
# Send historical records
# ==============================

for index, row in df.iterrows():

    data = row.where(pd.notna(row), None).to_dict()

    producer.send(
        TOPIC_NAME,
        value=data
    )

    print(
        f"Sent record {index + 1}/{len(df)}"
    )

    # Small delay so we can observe streaming
    time.sleep(0.01)


# ==============================
# Make sure all historical
# records are delivered
# ==============================

producer.flush()

print("\nAll historical weather records sent.")


# ==============================
# Send END marker
# ==============================

end_marker = {
    "message_type": "END_OF_HISTORICAL_DATA"
}

producer.send(
    TOPIC_NAME,
    value=end_marker
)

producer.flush()

print("Historical END marker sent successfully!")


# ==============================
# Close producer
# ==============================

producer.close()

print("\nHistorical weather streaming completed!")
print("Total records sent:", len(df))
print("END marker: END_OF_HISTORICAL_DATA")