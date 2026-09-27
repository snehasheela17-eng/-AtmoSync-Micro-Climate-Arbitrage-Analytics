import requests
import json
import time
from kafka import KafkaProducer

# ============================================================
# Configuration
# ============================================================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "live_weather"


# ============================================================
# Kafka Producer
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

print("Kafka Producer connected successfully!")
print()


# ============================================================
# Find Indian Locations
# ============================================================

def find_locations(city_name):

    geocoding_url = (
        "https://geocoding-api.open-meteo.com/v1/search"
        f"?name={requests.utils.quote(city_name)}"
        "&count=10"
        "&language=en"
        "&format=json"
        "&countryCode=IN"
    )

    try:

        response = requests.get(
            geocoding_url,
            timeout=10
        )

        if response.status_code != 200:

            print(
                "Geocoding API request failed. Status Code:",
                response.status_code
            )

            return []

        result = response.json()

        if "results" not in result or not result["results"]:

            return []

        locations = []

        for location in result["results"]:

            if location.get("country_code") == "IN":

                locations.append({
                    "city": location.get("name"),
                    "state": location.get("admin1"),
                    "country": location.get("country"),
                    "latitude": location.get("latitude"),
                    "longitude": location.get("longitude")
                })

        return locations

    except requests.RequestException as error:

        print("Geocoding API connection error:")
        print(error)

        return []


# ============================================================
# Select Location
# ============================================================

def select_location(city_name):

    locations = find_locations(city_name)

    if not locations:

        print()
        print("No Indian location found for:", city_name)
        print("Please check the city name and try again.")
        return None

    # Remove duplicate city/state combinations
    unique_locations = []

    seen = set()

    for location in locations:

        key = (
            location["city"],
            location["state"]
        )

        if key not in seen:

            seen.add(key)
            unique_locations.append(location)

    print()
    print("Possible Indian locations:")
    print("----------------------------------------")

    for index, location in enumerate(
        unique_locations,
        start=1
    ):

        print(
            f"{index}. "
            f"{location['city']}, "
            f"{location['state']}, "
            f"{location['country']}"
        )

    print("----------------------------------------")

    if len(unique_locations) == 1:

        selected_location = unique_locations[0]

        print()
        print("Only one matching location found.")
        print(
            "Automatically selected:",
            selected_location["city"],
            ",",
            selected_location["state"]
        )

        return selected_location

    while True:

        try:

            selection = int(
                input("Select location number: ").strip()
            )

            if 1 <= selection <= len(unique_locations):

                return unique_locations[selection - 1]

            print(
                "Invalid selection. "
                "Please enter a number from the list."
            )

        except ValueError:

            print(
                "Please enter a valid number."
            )


# ============================================================
# Weather Function
# ============================================================

def get_weather(location):

    latitude = location["latitude"]
    longitude = location["longitude"]

    weather_url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}"
        f"&longitude={longitude}"
        "&current=temperature_2m,relative_humidity_2m,"
        "precipitation,wind_speed_10m,weather_code"
        "&timezone=auto"
    )

    try:

        response = requests.get(
            weather_url,
            timeout=10
        )

        if response.status_code != 200:

            print(
                "Weather API request failed. Status Code:",
                response.status_code
            )

            return None

        result = response.json()

        current = result["current"]

        weather_data = {
            "city": location["city"],
            "state": location["state"],
            "country": location["country"],
            "latitude": result["latitude"],
            "longitude": result["longitude"],
            "timestamp": current["time"],
            "temperature": current["temperature_2m"],
            "humidity": current["relative_humidity_2m"],
            "precipitation": current["precipitation"],
            "wind_speed": current["wind_speed_10m"],
            "weather_code": current["weather_code"]
        }

        return weather_data

    except requests.RequestException as error:

        print("Weather API connection error:")
        print(error)

        return None


# ============================================================
# Display Weather
# ============================================================

def display_weather(weather_data):

    print()
    print("========================================")
    print("       LIVE WEATHER RESULT")
    print("========================================")
    print("City          :", weather_data["city"])
    print("State         :", weather_data["state"])
    print("Country       :", weather_data["country"])
    print("Latitude      :", weather_data["latitude"])
    print("Longitude     :", weather_data["longitude"])
    print("Timestamp     :", weather_data["timestamp"])
    print("Temperature   :", weather_data["temperature"], "°C")
    print("Humidity      :", weather_data["humidity"], "%")
    print("Precipitation :", weather_data["precipitation"], "mm")
    print("Wind Speed    :", weather_data["wind_speed"], "km/h")
    print("Weather Code  :", weather_data["weather_code"])
    print("========================================")
    print()


# ============================================================
# Send Weather to Kafka
# ============================================================

def send_to_kafka(weather_data):

    producer.send(
        TOPIC_NAME,
        value=weather_data
    )

    producer.flush()

    print("Live weather data sent to Kafka successfully!")
    print()


# ============================================================
# Continuous Streaming
# ============================================================

def stream_city():

    city_name = input(
        "Enter city name (Example: Raichur): "
    ).strip()

    if not city_name:

        print("City name cannot be empty.")
        return

    location = select_location(city_name)

    if location is None:
        return

    print()
    print("Location selected successfully!")
    print("City       :", location["city"])
    print("State      :", location["state"])
    print("Country    :", location["country"])
    print("Latitude   :", location["latitude"])
    print("Longitude  :", location["longitude"])
    print()

    print("Starting continuous weather streaming...")
    print("Press Ctrl+C to stop.")
    print("----------------------------------------")

    last_sent_timestamp = None

    try:

        while True:

            weather_data = get_weather(location)

            if weather_data is None:

                print("Unable to fetch weather data.")
                print("Retrying in 60 seconds...")

                time.sleep(60)

                continue

            current_timestamp = weather_data["timestamp"]

            if current_timestamp == last_sent_timestamp:

                print("Same timestamp received.")
                print("Record not sent again.")

            else:

                send_to_kafka(weather_data)

                display_weather(weather_data)

                last_sent_timestamp = current_timestamp

            print("Next API check in 60 seconds...")
            print("----------------------------------------")

            time.sleep(60)

    except KeyboardInterrupt:

        print()
        print("Live weather streaming stopped by user.")


# ============================================================
# Main Menu
# ============================================================

try:

    while True:

        print()
        print("========================================")
        print("       AtmoSync Live Weather")
        print("========================================")
        print("1. Enter city and stream weather")
        print("2. Exit")
        print()

        choice = input("Enter your choice: ").strip()

        if choice == "1":

            stream_city()

        elif choice == "2":

            print()
            print("Exiting Live Weather Producer.")
            break

        else:

            print()
            print("Invalid choice.")
            print("Please enter 1 or 2.")

finally:

    producer.close()

    print()
    print("Kafka Producer closed.")
