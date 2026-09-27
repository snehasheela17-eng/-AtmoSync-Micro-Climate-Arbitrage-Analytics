import json
import random
import time
from datetime import datetime

# ==========================================
# AtmoSync IoT Sensor Simulator
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


def generate_sensor_data(location):
    temperature = round(random.uniform(22, 36), 2)
    humidity = round(random.uniform(40, 90), 2)
    soil_moisture = round(random.uniform(25, 80), 2)
    leaf_wetness = round(random.uniform(10, 90), 2)
    co2 = round(random.uniform(380, 550), 2)
    light_intensity = round(random.uniform(100, 1000), 2)
    wind_speed = round(random.uniform(0, 25), 2)
    rainfall = round(random.uniform(0, 15), 2)

    sensor_data = {
        "timestamp": datetime.now().isoformat(),
        "city": location["city"],
        "state": location["state"],
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "temperature_c": temperature,
        "humidity_pct": humidity,
        "soil_moisture_pct": soil_moisture,
        "leaf_wetness_pct": leaf_wetness,
        "co2_ppm": co2,
        "light_intensity_lux": light_intensity,
        "wind_speed_kmh": wind_speed,
        "rainfall_mm": rainfall,
        "sensor_status": "OK"
    }

    return sensor_data


print("=" * 60)
print("AtmoSync IoT Sensor Simulator")
print("=" * 60)
print("Generating simulated sensor data...")
print("Press CTRL+C to stop.")
print()

try:
    while True:

        location = random.choice(LOCATIONS)

        sensor_data = generate_sensor_data(location)

        print(json.dumps(sensor_data, indent=2))

        print("-" * 60)

        time.sleep(5)

except KeyboardInterrupt:
    print()
    print("IoT Sensor Simulator stopped.")