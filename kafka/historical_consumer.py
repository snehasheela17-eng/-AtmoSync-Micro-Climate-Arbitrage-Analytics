from confluent_kafka import Consumer
import json
import csv

consumer = Consumer({
    "bootstrap.servers": "127.0.0.1:9092",
    "group.id": "historical-weather-consumer",
    "auto.offset.reset": "earliest"
})

topic = "weather_data"
output_file = "data/processed/historical_weather_from_kafka.csv"

consumer.subscribe([topic])

rows = []

print("Reading historical weather data from Kafka...")
print("Press Ctrl+C when you want to stop receiving data.")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))
        rows.append(data)

        print(f"Received record {len(rows)}")

except KeyboardInterrupt:
    print("\nStopping consumer...")

finally:
    consumer.close()

if rows:
    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"Received {len(rows)} records from Kafka")
    print(f"Saved to: {output_file}")
else:
    print("No records received.")

