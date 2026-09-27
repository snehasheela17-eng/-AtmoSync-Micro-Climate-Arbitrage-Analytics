import json
import os
import snowflake.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

# ==========================================
# Load environment variables
# ==========================================

load_dotenv()

# ==========================================
# Kafka Settings
# ==========================================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "iot_sensor_data"

# ==========================================
# Snowflake Settings
# ==========================================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# ==========================================
# Connect to Snowflake
# ==========================================

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

print("=" * 60)
print("AtmoSync IoT → Snowflake Loader")
print("=" * 60)
print("Connected to Snowflake successfully.")
print("Kafka Topic:", TOPIC_NAME)
print("=" * 60)

# ==========================================
# Kafka Consumer
# ==========================================

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Listening for IoT sensor data...")
print("Press Ctrl+C to stop.")
print("=" * 60)

# ==========================================
# Insert IoT Data into Snowflake
# ==========================================

insert_sql = """
INSERT INTO IOT_SENSOR_DATA (
    TIMESTAMP,
    CITY,
    STATE,
    LATITUDE,
    LONGITUDE,
    TEMPERATURE_C,
    HUMIDITY_PCT,
    SOIL_MOISTURE_PCT,
    LEAF_WETNESS_PCT,
    CO2_PPM,
    LIGHT_INTENSITY_LUX,
    WIND_SPEED_KMH,
    RAINFALL_MM,
    SENSOR_STATUS
)
VALUES (
    %(timestamp)s,
    %(city)s,
    %(state)s,
    %(latitude)s,
    %(longitude)s,
    %(temperature_c)s,
    %(humidity_pct)s,
    %(soil_moisture_pct)s,
    %(leaf_wetness_pct)s,
    %(co2_ppm)s,
    %(light_intensity_lux)s,
    %(wind_speed_kmh)s,
    %(rainfall_mm)s,
    %(sensor_status)s
)
"""

try:

    for message in consumer:

        data = message.value

        cursor.execute(insert_sql, data)

        conn.commit()

        print(
            f"Loaded → "
            f"{data.get('city')} | "
            f"{data.get('timestamp')} | "
            f"Temp: {data.get('temperature_c')}°C"
        )

except KeyboardInterrupt:
    print("\nIoT Snowflake Loader stopped.")
    consumer.close()
    cursor.close()
    conn.close()
