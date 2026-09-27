import pandas as pd
import random
from datetime import datetime, timedelta

# ==============================
# AtmoSync Market Data Generator
# ==============================

cities = [
    ("Bangalore", "Karnataka"),
    ("Mysore", "Karnataka"),
    ("Chennai", "Tamil Nadu"),
    ("Hyderabad", "Telangana")
]

products = [
    ("Tomato", "Vegetable"),
    ("Onion", "Vegetable"),
    ("Potato", "Vegetable"),
    ("Mango", "Fruit"),
    ("Banana", "Fruit"),
    ("Spinach", "Leafy Vegetable")
]

start_date = datetime(2026, 1, 1)
rows = []

# Generate 1000 market records
for i in range(1000):

    city, state = random.choice(cities)
    product, category = random.choice(products)

    date = start_date + timedelta(days=random.randint(0, 265))

    supply_level = round(random.uniform(20, 100), 2)
    demand_level = round(random.uniform(30, 100), 2)

    # Base product prices
    base_prices = {
        "Tomato": 35,
        "Onion": 30,
        "Potato": 28,
        "Mango": 90,
        "Banana": 45,
        "Spinach": 25
    }

    base_price = base_prices[product]

    # Price influenced by supply and demand
    price = (
        base_price
        + (demand_level - 50) * 0.20
        - (supply_level - 50) * 0.15
        + random.uniform(-5, 5)
    )

    price = max(price, 5)

    price_change_pct = random.uniform(-15, 20)

    # Products more sensitive to storage/environment
    if product in ["Spinach", "Tomato", "Mango", "Banana"]:
        storage_risk = random.choice(["HIGH", "MEDIUM"])
    else:
        storage_risk = random.choice(["LOW", "MEDIUM"])

    if demand_level > 75 and supply_level < 40:
        market_status = "HIGH_DEMAND_LOW_SUPPLY"
    elif supply_level > 75:
        market_status = "HIGH_SUPPLY"
    elif demand_level > 70:
        market_status = "HIGH_DEMAND"
    else:
        market_status = "NORMAL"

    rows.append({
        "date": date.strftime("%Y-%m-%d"),
        "city": city,
        "state": state,
        "product": product,
        "category": category,
        "supply_level": supply_level,
        "demand_level": demand_level,
        "price_inr": round(price, 2),
        "price_change_pct": round(price_change_pct, 2),
        "storage_risk": storage_risk,
        "market_status": market_status
    })


# Create DataFrame
df = pd.DataFrame(rows)

# Sort data
df = df.sort_values(["date", "city", "product"])

# Save CSV
output_file = "market/market_product_data.csv"
df.to_csv(output_file, index=False)

print("======================================")
print("AtmoSync Market Data Generated")
print("======================================")
print(f"Rows generated: {len(df)}")
print(f"Columns: {len(df.columns)}")
print(f"Saved to: {output_file}")
print()
print(df.head(10))