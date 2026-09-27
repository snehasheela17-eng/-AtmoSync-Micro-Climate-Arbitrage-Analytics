import requests

url = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 12.9716,
    "longitude": 77.5946,
    "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m,weather_code",
    "timezone": "Asia/Kolkata"
}

response = requests.get(url, params=params)

if response.status_code == 200:

    data = response.json()
    current = data["current"]

    print()
    print("Current Weather — Bengaluru")
    print("--------------------------------")
    print(f"{'Field':<22} {'Current Value'}")
    print("--------------------------------")
    print(f"{'Temperature':<22} {current['temperature_2m']} °C")
    print(f"{'Humidity':<22} {current['relative_humidity_2m']} %")
    print(f"{'Precipitation':<22} {current['precipitation']} mm")
    print(f"{'Wind Speed':<22} {current['wind_speed_10m']} km/h")
    print(f"{'Weather Code':<22} {current['weather_code']}")
    print(f"{'Time':<22} {current['time']}")
    print("--------------------------------")

else:
    print("Unable to fetch weather data.")
    print("Status Code:", response.status_code)