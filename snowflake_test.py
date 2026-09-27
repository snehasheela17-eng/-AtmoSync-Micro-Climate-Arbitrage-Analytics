import os
import snowflake.connector
from dotenv import load_dotenv

# ==============================
# Load .env file
# ==============================

load_dotenv()

# ==============================
# Snowflake Credentials
# ==============================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# ==============================
# Validate required variables
# ==============================

required_variables = {
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
    "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
    "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
    "SNOWFLAKE_ROLE": SNOWFLAKE_ROLE
}

missing_variables = [
    name for name, value in required_variables.items()
    if not value
]

if missing_variables:
    raise ValueError(
        "Missing environment variables: "
        + ", ".join(missing_variables)
    )

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
# Test Insert
# ==============================

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

data = (
    "Bangalore",
    12.9716,
    77.5946,
    25.5,
    70,
    0.0,
    12.5,
    "Cloudy",
    "2026-09-22 22:00:00"
)

cursor.execute(insert_query, data)
conn.commit()

print("Weather data inserted successfully!")

# ==============================
# Close Snowflake Connection
# ==============================

cursor.close()
conn.close()

print("Snowflake connection closed.")

