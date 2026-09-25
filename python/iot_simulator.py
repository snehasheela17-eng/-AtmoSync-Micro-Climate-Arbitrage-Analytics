import random
import json
import time
from datetime import datetime
from confluent_kafka import Producer

# Kafka connection
producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic = "iot_container_telemetry"

containers = ["C001", "C002", "C003", "C004", "C005"]

print("IoT Simulator started...")
print(f"Sending data to Kafka topic: {topic}")

for i in range(100):

    container_id = random.choice(containers)
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    temperature = round(random.uniform(20, 35), 2)
    humidity = round(random.uniform(50, 90), 2)
    vibration = round(random.uniform(0.05, 0.50), 2)

    data = {
        "Container_ID": container_id,
        "Timestamp": timestamp,
        "Temperature": temperature,
        "Humidity": humidity,
        "Vibration": vibration
    }

    producer.produce(
        topic,
        value=json.dumps(data)
    )

    producer.flush()

    print("Sent:", data)

    time.sleep(1)

print("IoT data generation completed!")