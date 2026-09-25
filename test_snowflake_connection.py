import snowflake.connector
from getpass import getpass

password = getpass("Enter Snowflake password: ")

conn = snowflake.connector.connect(
    account="KXSJJKP-YE27378",
    user="SNEHASHEELA",
    password=password,
    role="ACCOUNTADMIN",
    database="ATMOSYNC_DB",
    schema="RAW"
)

print("Snowflake connection successful!")

conn.close()