import pandas as pd
import json
from confluent_kafka import Producer

df = pd.read_csv("data/processed/all_india_weather.csv")

producer = Producer({
    "bootstrap.servers": "127.0.0.1:9092",
    "queue.buffering.max.messages": 500000
})

topic = "weather_data"

for i, (_, row) in enumerate(df.iterrows(), start=1):

    data = row.to_dict()

    while True:
        try:
            producer.produce(
                topic,
                value=json.dumps(data)
            )
            break
        except BufferError:
            producer.poll(1)

    producer.poll(0)

    if i % 10000 == 0:
        print(f"Sent {i} records...")

producer.flush()

print(f"Sent {len(df)} records to Kafka topic: {topic}")