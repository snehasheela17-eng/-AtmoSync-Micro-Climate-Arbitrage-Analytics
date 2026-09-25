import pandas as pd

input_file = "data/raw/43295.csv"
output_file = "data/processed/bangalore_weather.csv"

df = pd.read_csv(input_file)

df.insert(0, "CITY", "Bangalore")

df.to_csv(output_file, index=False)

print("Bangalore city added successfully!")
print("Total records:", len(df))
print("Columns:", list(df.columns))
print("Saved to:", output_file)