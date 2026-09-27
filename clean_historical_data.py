import pandas as pd
import numpy as np

# Input and output files
INPUT_FILE = "data/Weather_Historical_Dataset.xlsx"
OUTPUT_FILE = "data/historical_weather_cleaned.csv"

# Read the raw historical dataset
df = pd.read_excel(INPUT_FILE, sheet_name="Climate")

print("Raw dataset loaded successfully!")
print("Rows:", len(df))
print("Columns:", len(df.columns))
# -------------------------------
# Clean date and time columns
# -------------------------------

# Convert date/time columns to numeric
for col in ["year", "month", "day", "hour"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Keep only valid calendar ranges
df.loc[~df["month"].between(1, 12), "month"] = np.nan
df.loc[~df["day"].between(1, 31), "day"] = np.nan
df.loc[~df["hour"].between(0, 23), "hour"] = np.nan

# Create a proper timestamp
df["timestamp"] = pd.to_datetime(
    df[["year", "month", "day", "hour"]],
    errors="coerce"
)

print("Date/time cleaning completed!")
print("Invalid timestamps:", df["timestamp"].isna().sum())
# -------------------------------
# Clean numeric weather columns
# -------------------------------

# Replace common text representations of missing values
df = df.replace(
    ["n.a.", "N/A", "NA", "--", "-", ""],
    np.nan
)

# Temperature
df["temp"] = pd.to_numeric(df["temp"], errors="coerce")

# Humidity
df["rhum"] = pd.to_numeric(df["rhum"], errors="coerce")

# Precipitation
df["prcp"] = pd.to_numeric(df["prcp"], errors="coerce")

# Wind speed - remove "kph"
df["wspd"] = (
    df["wspd"]
    .astype(str)
    .str.replace("kph", "", regex=False)
    .str.strip()
)
df["wspd"] = pd.to_numeric(df["wspd"], errors="coerce")

# Pressure - replace comma decimal with dot
df["pres"] = (
    df["pres"]
    .astype(str)
    .str.replace(",", ".", regex=False)
    .str.strip()
)
df["pres"] = pd.to_numeric(df["pres"], errors="coerce")

# Cloud cover - remove %
df["cldc"] = (
    df["cldc"]
    .astype(str)
    .str.replace("%", "", regex=False)
    .str.strip()
)
df["cldc"] = pd.to_numeric(df["cldc"], errors="coerce")

# Weather condition code
df["coco"] = pd.to_numeric(df["coco"], errors="coerce")

print("Numeric weather columns cleaned!")
# -------------------------------
# Validate weather measurements
# -------------------------------

# Humidity must be between 0 and 100%
df.loc[~df["rhum"].between(0, 100), "rhum"] = np.nan

# Precipitation cannot be negative
df.loc[df["prcp"] < 0, "prcp"] = np.nan

# Wind speed cannot be negative
df.loc[df["wspd"] < 0, "wspd"] = np.nan

# Cloud cover should be between 0 and 100%
df.loc[~df["cldc"].between(0, 100), "cldc"] = np.nan

print("Weather measurement validation completed!")
# -------------------------------
# Clean wind direction
# -------------------------------

# Keep the original wind direction
df["wdir_original"] = df["wdir"]

# Convert numeric wind directions to numbers
df["wdir_degrees"] = pd.to_numeric(df["wdir"], errors="coerce")

# Valid wind direction is 0–360 degrees
df.loc[
    ~df["wdir_degrees"].between(0, 360),
    "wdir_degrees"
] = np.nan

print("Wind direction cleaning completed!")
# -------------------------------
# Validate temperature
# -------------------------------

# Keep the original temperature
df["temp_original"] = df["temp"]

# Accept reasonable Celsius temperatures
# Values outside this range are treated as invalid
df.loc[
    ~df["temp"].between(-50, 60),
    "temp"
] = np.nan

print("Temperature validation completed!")
# -------------------------------
# Final cleanup and export
# -------------------------------

# Remove rows where a valid timestamp could not be created
# Clean weather condition codes
df.loc[~df["coco"].isin([0, 1, 2, 3, 5, 7, 8, 9, 17, 18]), "coco"] = np.nan

# Keep only valid historical year
df = df[df["year"] == 2024].copy()

# Remove rows with invalid timestamps
df = df.dropna(subset=["timestamp"]).copy()

# Sort by timestamp
df = df.sort_values("timestamp").reset_index(drop=True)

# Save cleaned dataset
df.to_csv(OUTPUT_FILE, index=False)

print("\nCleaning completed successfully!")
print("Cleaned rows:", len(df))
print("Cleaned columns:", len(df.columns))
print("Output file:", OUTPUT_FILE)