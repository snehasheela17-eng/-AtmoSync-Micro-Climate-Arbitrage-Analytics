import pandas as pd
import requests
import gzip
import io
import os

cities = {
    "Hyderabad": "43128",
    "Kolkata": "42807",
    "Mumbai": "43003",
    "Chennai": "43279",
    "Pune": "43063",
    "Ahmedabad": "42647",
    "Jaipur": "42348",
    "Lucknow": "42369",
    "Kanpur": "42367",
    "Nagpur": "42867",
    "Indore": "42754",
    "Bhopal": "42754",
    "Patna": "42492",
    "Surat": "42840",
    "Visakhapatnam": "43150",
    "Vijayawada": "43185",
    "Kochi": "43353",
    "Coimbatore": "43321",
    "Chandigarh": "42105",
    "Bhubaneswar": "42971",
    "Guwahati": "42516",
    "Ranchi": "42699",
    "Dehradun": "42111"
}

os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

for city, station in cities.items():

    print(f"\nDownloading {city}...")

    url = f"https://data.meteostat.net/hourly/2024/{station}.csv.gz"

    response = requests.get(url)

    if response.status_code != 200:
        print(f"Could not download {city}")
        continue

    with gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8") as file:
        df = pd.read_csv(file)

    df.insert(0, "CITY", city)

    output_file = f"data/processed/{city.lower()}_weather.csv"
    df.to_csv(output_file, index=False)

    print(f"{city}: {len(df)} records")
    print(f"Saved: {output_file}")

print("\nAll available cities processed!")