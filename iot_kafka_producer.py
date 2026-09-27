import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer


# ==========================================
# Kafka Settings
# ==========================================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "iot_sensor_data"


# ==========================================
# Locations
# ==========================================

LOCATIONS = [
    {
        "city": "Bangalore",
        "state": "Karnataka",
        "latitude": 12.9716,
        "longitude": 77.5946
    },
    {
        "city": "Mysore",
        "state": "Karnataka",
        "latitude": 12.2958,
        "longitude": 76.6394
    },
    {
        "city": "Chennai",
        "state": "Tamil Nadu",
        "latitude": 13.0827,
        "longitude": 80.2707
    },
    {
        "city": "Hyderabad",
        "state": "Telangana",
        "latitude": 17.3850,
        "longitude": 78.4867
    }
]


# ==========================================
# Create Kafka Producer
# ==========================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


print("=" * 60)
print("AtmoSync IoT Kafka Producer")
print("=" * 60)
print(f"Kafka Server: {KAFKA_SERVER}")
print(f"Kafka Topic: {TOPIC_NAME}")
print("Sending IoT sensor data...")
print("Press CTRL+C to stop.")
print()


try:

    while True:

        location = random.choice(LOCATIONS)

        sensor_data = {
            "timestamp": datetime.now().isoformat(),
            "city": location["city"],
            "state": location["state"],
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "temperature_c": round(random.uniform(22, 36), 2),
            "humidity_pct": round(random.uniform(40, 90), 2),
            "soil_moisture_pct": round(random.uniform(25, 80), 2),
            "leaf_wetness_pct": round(random.uniform(10, 90), 2),
            "co2_ppm": round(random.uniform(380, 550), 2),
            "light_intensity_lux": round(random.uniform(100, 1000), 2),
            "wind_speed_kmh": round(random.uniform(0, 25), 2),
            "rainfall_mm": round(random.uniform(0, 15), 2),
            "sensor_status": "OK"
        }

        producer.send(
            TOPIC_NAME,
            value=sensor_data
        )

        producer.flush()

        print(
            f"Sent → {sensor_data['city']} | "
            f"Temp: {sensor_data['temperature_c']}°C | "
            f"Humidity: {sensor_data['humidity_pct']}% | "
            f"Soil: {sensor_data['soil_moisture_pct']}%"
        )

        time.sleep(5)


except KeyboardInterrupt:

    print()
    print("IoT Kafka Producer stopped.")

finally:

    producer.close()