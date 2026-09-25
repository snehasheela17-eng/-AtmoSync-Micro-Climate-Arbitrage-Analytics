import pandas as pd
import numpy as np

weather = pd.read_csv("data/raw/43295.csv")

weather["Timestamp"] = pd.to_datetime(
    weather[["year", "month", "day", "hour"]]
)

weather = weather[
    ["Timestamp", "temp", "rhum"]
].dropna()

containers = ["C001", "C002", "C003", "C004", "C005"]

rows = []

np.random.seed(42)

for _, weather_row in weather.iterrows():

    for container in containers:

        container_offset = {
            "C001": 0.5,
            "C002": 1.5,
            "C003": 2.5,
            "C004": 0.0,
            "C005": 1.0
        }[container]

        temperature = (
            weather_row["temp"]
            + container_offset
            + np.random.normal(0, 1.5)
        )

        humidity = (
            weather_row["rhum"]
            + np.random.normal(0, 4)
        )

        vibration = np.random.uniform(0.05, 0.45)

        rows.append([
            container,
            weather_row["Timestamp"],
            round(temperature, 2),
            round(np.clip(humidity, 20, 100), 2),
            round(vibration, 2)
        ])

container_df = pd.DataFrame(
    rows,
    columns=[
        "Container_ID",
        "Timestamp",
        "Temperature",
        "Humidity",
        "Vibration"
    ]
)

container_df.to_csv(
    "data/raw/container_telemetry.csv",
    index=False
)

print("Container telemetry created successfully.")
print("Rows:", len(container_df))
print("\nFirst 10 rows:")
print(container_df.head(10))