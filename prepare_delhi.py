import pandas as pd

input_file = "data/raw/42182-2024.csv.gz"
output_file = "data/processed/delhi_weather.csv"

df = pd.read_csv(input_file, compression="gzip")

df.insert(0, "CITY", "Delhi")

df.to_csv(output_file, index=False)

print("Delhi weather data prepared successfully!")
print("Total records:", len(df))
print("City:", df["CITY"].unique())
print("Saved to:", output_file)