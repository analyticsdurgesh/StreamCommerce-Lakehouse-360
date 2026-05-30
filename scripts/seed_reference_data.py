from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from faker import Faker

    fake = Faker()
except ImportError:
    import random

    class _FallbackFaker:
        def email(self) -> str:
            return f"user{random.randint(1000, 9999)}@example.com"

        def date_between(self, start_date="-3y", end_date="today"):
            from datetime import date, timedelta

            return date.today() - timedelta(days=random.randint(0, 1000))

        def random_element(self, values):
            return random.choice(list(values))

        def random_int(self, low, high):
            return random.randint(low, high)

        def pyfloat(self, min_value, max_value, right_digits=2):
            return round(random.uniform(min_value, max_value), right_digits)

        def catch_phrase(self):
            return f"Premium Commerce Item {random.randint(100, 999)}"

        def company(self):
            return f"Seller {random.randint(100, 999)} LLC"

        def state(self):
            return random.choice(["CA", "TX", "NY", "WA", "GA", "IL", "CO"])

        def bs(self):
            return f"campaign {random.randint(1, 99)}"

    fake = _FallbackFaker()


def write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def seed_reference_data(root: str = "data/landing/reference") -> dict[str, int]:
    base = Path(root)
    customers = pd.DataFrame(
        [
            {
                "customer_id": f"CUST-{10000 + i}",
                "email": fake.email(),
                "signup_date": fake.date_between(start_date="-3y", end_date="today").isoformat(),
                "loyalty_tier": fake.random_element(["bronze", "silver", "gold", "platinum"]),
                "home_location": fake.random_element(["New York", "Austin", "Seattle", "Chicago", "San Francisco", "Atlanta", "Denver"]),
            }
            for i in range(500)
        ]
    )
    products = pd.DataFrame(
        [
            {
                "product_id": f"SKU-{1000 + i}",
                "product_name": fake.catch_phrase(),
                "category": fake.random_element(["electronics", "home", "fashion", "beauty", "sports", "books", "grocery"]),
                "seller_id": f"SELLER-{fake.random_int(1, 50):03d}",
                "list_price": round(fake.pyfloat(min_value=8, max_value=450, right_digits=2), 2),
                "is_active": True,
            }
            for i in range(100)
        ]
    )
    warehouses = pd.DataFrame([{"warehouse_id": f"WH-{i:02d}", "region": fake.state(), "capacity": fake.random_int(1000, 20000)} for i in range(1, 9)])
    sellers = pd.DataFrame([{"seller_id": f"SELLER-{i:03d}", "seller_name": fake.company(), "seller_rating": round(fake.pyfloat(min_value=3, max_value=5, right_digits=2), 2)} for i in range(1, 51)])
    locations = pd.DataFrame([{"location": city, "country": "US", "timezone": "America/New_York"} for city in ["New York", "Austin", "Seattle", "Chicago", "San Francisco", "Atlanta", "Denver"]])
    campaigns = pd.DataFrame([{"campaign_id": f"CMP-{i:03d}", "channel": fake.random_element(["paid_search", "email", "social", "affiliate"]), "campaign_name": fake.bs()} for i in range(1, 21)])
    frames = {
        "customers.csv": customers,
        "products.csv": products,
        "warehouses.csv": warehouses,
        "sellers.csv": sellers,
        "locations.csv": locations,
        "marketing_campaigns.csv": campaigns,
    }
    for name, df in frames.items():
        write_csv(df, base / name)
    return {name: len(df) for name, df in frames.items()}


if __name__ == "__main__":
    print(seed_reference_data())
