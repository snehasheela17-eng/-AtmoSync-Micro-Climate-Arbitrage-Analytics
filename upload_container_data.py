import snowflake.connector
from getpass import getpass

password = getpass("Enter Snowflake password: ")

conn = snowflake.connector.connect(
    account="KXSJJKP-YE27378",
    user="snehasheela",
    password=password,
    role="ACCOUNTADMIN",
    database="ATMOSYNC_DB",
    schema="RAW"
)

cursor = conn.cursor()

file_path = "data/raw/container_telemetry.csv"

cursor.execute(f"""
PUT 'file://{file_path}'
@CONTAINER_STAGE
AUTO_COMPRESS=TRUE
OVERWRITE=TRUE
""")

print("CSV uploaded to Snowflake stage successfully!")

cursor.close()
conn.close()