import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

# ==============================
# Load environment variables
# ==============================

load_dotenv()

# ==============================
# AtmoSync Market → Snowflake
# ==============================

CSV_FILE = "market/market_product_data.csv"

# ==============================
# Snowflake Connection Settings
# ==============================

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# ==============================
# Read CSV
# ==============================

df = pd.read_csv(CSV_FILE)

print("======================================")
print("AtmoSync Market → Snowflake Loader")
print("======================================")
print(f"CSV rows found: {len(df)}")

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

print("Connected to Snowflake successfully.")

# ==============================
# Create Table
# ==============================

cursor.execute("""
CREATE TABLE IF NOT EXISTS MARKET_PRODUCT_DATA (
    DATE DATE,
    CITY VARCHAR,
    STATE VARCHAR,
    PRODUCT VARCHAR,
    CATEGORY VARCHAR,
    SUPPLY_LEVEL FLOAT,
    DEMAND_LEVEL FLOAT,
    PRICE_INR FLOAT,
    PRICE_CHANGE_PCT FLOAT,
    STORAGE_RISK VARCHAR,
    MARKET_STATUS VARCHAR
)
""")

# ==============================
# Prepare Data
# ==============================

df.columns = [col.upper() for col in df.columns]

records = [
    tuple(row)
    for row in df[
        [
            "DATE",
            "CITY",
            "STATE",
            "PRODUCT",
            "CATEGORY",
            "SUPPLY_LEVEL",
            "DEMAND_LEVEL",
            "PRICE_INR",
            "PRICE_CHANGE_PCT",
            "STORAGE_RISK",
            "MARKET_STATUS"
        ]
    ].itertuples(index=False, name=None)
]

# ==============================
# Insert Data in Batch
# ==============================

insert_sql = """
INSERT INTO MARKET_PRODUCT_DATA (
    DATE,
    CITY,
    STATE,
    PRODUCT,
    CATEGORY,
    SUPPLY_LEVEL,
    DEMAND_LEVEL,
    PRICE_INR,
    PRICE_CHANGE_PCT,
    STORAGE_RISK,
    MARKET_STATUS
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

cursor.executemany(insert_sql, records)

conn.commit()

print(f"Loaded {len(records)} market records into Snowflake.")

# ==============================
# Close Connection
# ==============================

cursor.close()
conn.close()

print("Snowflake connection closed.")
print("======================================")
print("Market data loading completed.")
print("======================================")

