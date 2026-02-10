"""
Generate realistic Superstore-style sales data for the Data Lakehouse POC.
No external dependencies — uses only Python standard library.

Produces:
  - data/raw/customers.csv
  - data/raw/products.csv
  - data/raw/orders.csv  (fact table source)

Run: python scripts/generate_sample_data.py
"""

import csv
import random
import os
from datetime import datetime, timedelta

random.seed(42)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
    "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
    "Joseph", "Jessica", "Thomas", "Sarah", "Charles", "Karen", "Daniel",
    "Lisa", "Matthew", "Nancy", "Anthony", "Betty", "Mark", "Margaret",
    "Ahmed", "Fatima", "Wei", "Yuki", "Carlos", "Maria", "Raj", "Priya",
    "Omar", "Aisha", "Hans", "Sophie", "Ivan", "Olga", "Kenji", "Sakura",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
    "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
    "Patel", "Khan", "Chen", "Yamamoto", "Santos", "Kumar", "Ali",
]

SEGMENTS = ["Consumer", "Corporate", "Home Office"]
SEGMENT_WEIGHTS = [0.50, 0.30, 0.20]

REGIONS = {
    "East":    ["New York", "Philadelphia", "Boston", "Baltimore", "Hartford"],
    "West":    ["Los Angeles", "San Francisco", "Seattle", "Portland", "Denver"],
    "Central": ["Chicago", "Detroit", "Minneapolis", "St. Louis", "Indianapolis"],
    "South":   ["Houston", "Atlanta", "Miami", "Dallas", "Charlotte"],
}

STATES = {
    "New York": "New York", "Philadelphia": "Pennsylvania", "Boston": "Massachusetts",
    "Baltimore": "Maryland", "Hartford": "Connecticut", "Los Angeles": "California",
    "San Francisco": "California", "Seattle": "Washington", "Portland": "Oregon",
    "Denver": "Colorado", "Chicago": "Illinois", "Detroit": "Michigan",
    "Minneapolis": "Minnesota", "St. Louis": "Missouri", "Indianapolis": "Indiana",
    "Houston": "Texas", "Atlanta": "Georgia", "Miami": "Florida",
    "Dallas": "Texas", "Charlotte": "North Carolina",
}

CATEGORIES = {
    "Technology": {
        "Phones":       [("iPhone 15", 999), ("Galaxy S24", 849), ("Pixel 8", 699), ("OnePlus 12", 599)],
        "Laptops":      [("MacBook Pro 14", 1999), ("ThinkPad X1", 1549), ("Dell XPS 15", 1299), ("HP Spectre", 1199)],
        "Accessories":  [("Wireless Mouse", 29), ("USB-C Hub", 49), ("Keyboard", 79), ("Monitor Stand", 39)],
    },
    "Furniture": {
        "Chairs":       [("Ergonomic Chair", 399), ("Task Chair", 199), ("Executive Chair", 549), ("Stool", 89)],
        "Tables":       [("Standing Desk", 599), ("Conference Table", 899), ("Side Table", 129), ("Folding Table", 79)],
        "Bookcases":    [("5-Shelf Bookcase", 199), ("3-Shelf Bookcase", 119), ("Corner Shelf", 89), ("Wall Mount", 59)],
    },
    "Office Supplies": {
        "Paper":        [("Copy Paper 500pk", 8), ("Legal Pads 12pk", 15), ("Sticky Notes", 5), ("Envelopes 100pk", 12)],
        "Binders":      [("3-Ring Binder", 7), ("Report Cover", 4), ("Clip Folder", 3), ("Presentation Binder", 12)],
        "Pens":         [("Ballpoint 12pk", 6), ("Gel Pens 8pk", 9), ("Markers 10pk", 11), ("Highlighters 6pk", 5)],
    },
}

SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]
SHIP_MODE_WEIGHTS = [0.60, 0.20, 0.15, 0.05]

# ---------------------------------------------------------------
# Generate Customers
# ---------------------------------------------------------------

def generate_customers(n=500):
    customers = []
    used_names = set()

    for i in range(1, n + 1):
        while True:
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)
            name = f"{first} {last}"
            if name not in used_names:
                used_names.add(name)
                break

        segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        region = random.choice(list(REGIONS.keys()))
        city = random.choice(REGIONS[region])
        state = STATES[city]

        customers.append({
            "customer_id": f"CUST-{i:04d}",
            "customer_name": name,
            "segment": segment,
            "region": region,
            "city": city,
            "state": state,
            "country": "United States",
        })

    return customers


# ---------------------------------------------------------------
# Generate Products
# ---------------------------------------------------------------

def generate_products():
    products = []
    pid = 1

    for category, subcats in CATEGORIES.items():
        for sub_category, items in subcats.items():
            for product_name, base_price in items:
                products.append({
                    "product_id": f"PROD-{pid:04d}",
                    "product_name": product_name,
                    "category": category,
                    "sub_category": sub_category,
                    "brand": product_name.split()[0],
                    "base_price": base_price,
                })
                pid += 1

    return products


# ---------------------------------------------------------------
# Generate Orders (Transactions)
# ---------------------------------------------------------------

def generate_orders(customers, products, n=5000):
    orders = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    date_range = (end_date - start_date).days

    for i in range(1, n + 1):
        customer = random.choice(customers)
        product = random.choice(products)

        order_date = start_date + timedelta(days=random.randint(0, date_range))
        ship_mode = random.choices(SHIP_MODES, weights=SHIP_MODE_WEIGHTS, k=1)[0]

        # Ship days based on mode
        ship_days = {"Standard Class": 5, "Second Class": 3, "First Class": 2, "Same Day": 0}
        ship_date = order_date + timedelta(days=ship_days[ship_mode] + random.randint(0, 2))

        quantity = random.choices(
            [1, 2, 3, 4, 5, 6, 7, 8, 10],
            weights=[30, 25, 15, 10, 8, 5, 3, 2, 2],
            k=1
        )[0]

        # Price variation (+/- 10% from base)
        unit_price = round(product["base_price"] * random.uniform(0.90, 1.10), 2)

        # Discount: 70% no discount, 30% discounted
        discount = 0.0
        if random.random() < 0.30:
            discount = random.choice([0.05, 0.10, 0.15, 0.20, 0.25, 0.30])

        gross_revenue = round(quantity * unit_price, 2)
        net_revenue = round(gross_revenue * (1 - discount), 2)
        profit_margin = random.uniform(0.05, 0.45) if discount < 0.20 else random.uniform(-0.10, 0.20)
        profit = round(net_revenue * profit_margin, 2)

        orders.append({
            "order_id": f"ORD-{i:06d}",
            "order_date": order_date.strftime("%Y-%m-%d"),
            "ship_date": ship_date.strftime("%Y-%m-%d"),
            "ship_mode": ship_mode,
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "discount": discount,
            "gross_revenue": gross_revenue,
            "net_revenue": net_revenue,
            "profit": profit,
        })

    return orders


# ---------------------------------------------------------------
# Write CSVs
# ---------------------------------------------------------------

def write_csv(filepath, data, fieldnames):
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"  Written: {filepath} ({len(data)} rows)")


def main():
    print("Generating sample data...\n")

    customers = generate_customers(500)
    products = generate_products()
    orders = generate_orders(customers, products, 5000)

    # Remove base_price from product output (internal only)
    products_out = [{k: v for k, v in p.items() if k != "base_price"} for p in products]

    write_csv(
        os.path.join(OUTPUT_DIR, "customers.csv"),
        customers,
        ["customer_id", "customer_name", "segment", "region", "city", "state", "country"],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "products.csv"),
        products_out,
        ["product_id", "product_name", "category", "sub_category", "brand"],
    )
    write_csv(
        os.path.join(OUTPUT_DIR, "orders.csv"),
        orders,
        ["order_id", "order_date", "ship_date", "ship_mode", "customer_id",
         "product_id", "quantity", "unit_price", "discount", "gross_revenue",
         "net_revenue", "profit"],
    )

    print(f"\nDone! Files saved to: {os.path.abspath(OUTPUT_DIR)}")
    print(f"  - {len(customers)} customers")
    print(f"  - {len(products_out)} products")
    print(f"  - {len(orders)} orders")


if __name__ == "__main__":
    main()
