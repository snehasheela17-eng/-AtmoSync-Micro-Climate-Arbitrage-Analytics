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

cursor.execute("""
COPY INTO CONTAINER_TELEMETRY
FROM @CONTAINER_STAGE
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';
""")

print("Container telemetry data loaded into Snowflake!")

cursor.close()
conn.close()