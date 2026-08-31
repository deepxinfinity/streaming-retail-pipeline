"""seed postgres master data
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from faker import Faker

load_dotenv()

SEED = 42
N_SKUS = 1500
N_STORES = 50
N_CUSTOMERS = 25_000
REGIONS = ["North", "South", "East", "West", "Central"]
STORE_FORMATS = ["hypermarket", "supermarket", "express"]
SEGMENTS = ["value", "mainstream", "premium"]
SEGMENT_P = [0.35, 0.45, 0.20]

CATEGORIES: dict[str, list[str]] = {
    "Grocery": ["Pasta & Rice", "Canned Goods", "Baking", "Condiments & Sauces", "Breakfast Cereals"],
    "Fresh": ["Fruit", "Vegetables", "Bakery", "Meat & Poultry", "Fish & Seafood"],
    "Dairy": ["Milk", "Cheese", "Yogurt", "Butter & Spreads", "Eggs"],
    "Beverages": ["Soft Drinks", "Juices", "Coffee", "Tea", "Water"],
    "Household": ["Cleaning", "Laundry", "Paper Goods", "Kitchen Supplies", "Air Care"],
    "Health & Beauty": ["Hair Care", "Skin Care", "Oral Care", "Vitamins", "Bath & Body"],
    "Frozen": ["Frozen Meals", "Ice Cream", "Frozen Vegetables", "Frozen Pizza", "Frozen Desserts"],
    "Snacks": ["Chips & Crisps", "Chocolate", "Candy", "Nuts & Seeds", "Biscuits & Cookies"],
}

BRANDS = [
    "Alderwood", "Bright Basket", "Casa Verde", "Daily Roots", "Everfield", "Firstlight",
    "Golden Gate", "Harvest Lane", "Ironstone", "Juniper & Co", "Kettle Ridge", "Luma",
    "Maple Crown", "Northbay", "Orchard Hill", "Prairie Best", "Quincy's", "Riverbend",
    "Summit", "Trueleaf",
]


def sim_start_date(today: date | None = None) -> date:
    months = int(os.getenv("BACKFILL_MONTHS", "6"))
    today = today or date.today()
    return (today - timedelta(days=30 * months)).replace(day=1)


def build_master_data(rng: np.random.Generator, start: date, today: date) -> dict:
    """All master rows + ground-truth params, pure in-memory (also used by tests)."""
    fake = Faker()
    Faker.seed(SEED)

    # category tree -> SKUs
    cat_list = [(dept, cat) for dept, cats in CATEGORIES.items() for cat in cats]
    per_cat = np.full(len(cat_list), N_SKUS // len(cat_list))
    per_cat[: N_SKUS % len(cat_list)] += 1

    products, sku_params = [], {}
    sku_no = 0
    for (dept, cat), n in zip(cat_list, per_cat):
        cat_id = f"C-{cat_list.index((dept, cat)):03d}"
        median_price = float(np.exp(rng.normal(1.2, 0.6)))            # ~$3.3 median, wide spread
        elast_center = float(rng.uniform(-2.2, -1.0))                 # categories differ
        uplift_center = float(rng.uniform(0.2, 0.5))
        for _ in range(int(n)):
            sku_id = f"SKU-{sku_no:05d}"
            sku_no += 1
            base_price = round(float(np.clip(median_price * np.exp(rng.normal(0, 0.35)), 0.5, 150.0)), 2)
            unit_cost = round(base_price * float(rng.uniform(0.55, 0.8)), 2)
            products.append({
                "sku_id": sku_id,
                "sku_name": f"{rng.choice(BRANDS)} {cat} {rng.integers(100, 999)}",
                "department": dept,
                "category_id": cat_id,
                "category_name": cat,
                "brand": str(rng.choice(BRANDS)),
                "base_price": base_price,
                "unit_cost": unit_cost,
            })
            sku_params[sku_id] = {
                "category_name": cat,
                "department": dept,
                "elasticity": float(np.clip(rng.normal(elast_center, 0.25), -2.5, -0.8)),
                "promo_uplift": float(np.clip(rng.normal(uplift_center, 0.1), 0.05, 0.9)),
                "base_rate": float(np.exp(rng.normal(0.0, 1.0)) * 0.03),  # lambda share weight
            }

    # stores
    stores, store_params = [], {}
    for i in range(N_STORES):
        store_id = f"S-{i + 1:03d}"
        stores.append({
            "store_id": store_id,
            "store_name": f"{fake.city()} Store",
            "region": REGIONS[i % len(REGIONS)],
            "city": fake.city(),
            "store_format": str(rng.choice(STORE_FORMATS, p=[0.2, 0.5, 0.3])),
            "opened_date": (start - timedelta(days=int(rng.integers(365, 3650)))).isoformat(),
        })
        store_params[store_id] = {"traffic": float(np.exp(rng.normal(0.0, 0.25)))}
    stores.append({
        "store_id": "S-ONLINE", "store_name": "Online", "region": "ONLINE",
        "city": "n/a", "store_format": "online",
        "opened_date": (start - timedelta(days=1500)).isoformat(),
    })
    store_params["S-ONLINE"] = {"traffic": 1.0}

    # customers
    physical = [s["store_id"] for s in stores if s["store_id"] != "S-ONLINE"]
    traffic_w = np.array([store_params[s]["traffic"] for s in physical])
    customers = [{
        "customer_id": f"CU-{i:06d}",
        "full_name": fake.name(),
        "segment": str(rng.choice(SEGMENTS, p=SEGMENT_P)),
        "home_store_id": str(rng.choice(physical, p=traffic_w / traffic_w.sum())),
        "signup_date": (start - timedelta(days=int(rng.integers(0, 1460)))).isoformat(),
    } for i in range(N_CUSTOMERS)]

    # price list
    price_rows = [{
        "sku_id": p["sku_id"], "scope": "all",
        "price": p["base_price"], "effective_from": start.isoformat(),
    } for p in products]
    horizon_days = max((today - start).days, 30)
    revised = rng.choice([p["sku_id"] for p in products], size=int(N_SKUS * 0.2), replace=False)
    base_by_sku = {p["sku_id"]: p["base_price"] for p in products}
    for sku_id in revised:
        for _ in range(int(rng.integers(1, 3))):
            eff = start + timedelta(days=int(rng.integers(14, horizon_days)))
            price_rows.append({
                "sku_id": str(sku_id), "scope": "all",
                "price": round(base_by_sku[sku_id] * float(rng.uniform(0.9, 1.1)), 2),
                "effective_from": eff.isoformat(),
            })
    # unique (sku, scope, effective_from)
    seen, deduped = set(), []
    for r in price_rows:
        k = (r["sku_id"], r["scope"], r["effective_from"])
        if k not in seen:
            seen.add(k)
            deduped.append(r)
    price_rows = deduped

    # weekly promo waves
    all_skus = [p["sku_id"] for p in products]
    n_waves = max((today - start).days // 7 + 8, 26)
    promotions, promo_products = [], []
    for w in range(n_waves):
        promo_id = f"PROMO-W{w:03d}"
        wave_start = start + timedelta(days=7 * w)
        promotions.append({
            "promo_id": promo_id,
            "promo_name": f"Weekly Wave {w}",
            "discount_pct": round(float(rng.uniform(0.10, 0.30)), 3),
            "start_date": wave_start.isoformat(),
            "end_date": (wave_start + timedelta(days=6)).isoformat(),
        })
        n_cover = int(N_SKUS * rng.uniform(0.05, 0.10))
        for sku_id in rng.choice(all_skus, size=n_cover, replace=False):
            promo_products.append({"promo_id": promo_id, "sku_id": str(sku_id)})

    params = {
        "seed": SEED,
        "sim_start": start.isoformat(),
        "dow_effect": [0.0, -0.05, -0.05, 0.0, 0.10, 0.35, 0.25],  # Mon..Sun
        "season_amp": 0.15,
        "basket_mean_store": 8.0,
        "basket_mean_online": 3.0,
        "online_orders_per_day": 2000,
        "skus": sku_params,
        "stores": store_params,
    }
    return {
        "products": products, "stores": stores, "customers": customers,
        "price_list": price_rows, "promotions": promotions,
        "promo_products": promo_products, "params": params,
    }


def _insert(cur, table: str, rows: list[dict]) -> None:
    import psycopg2.extras

    cols = list(rows[0].keys())
    psycopg2.extras.execute_values(
        cur,
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s",
        [[r[c] for c in cols] for r in rows],
        page_size=1000,
    )


def main():
    import psycopg2

    rng = np.random.default_rng(SEED)
    today = date.today()
    data = build_master_data(rng, sim_start_date(today), today)

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "retail"),
    )
    with conn, conn.cursor() as cur:
        cur.execute(
            "TRUNCATE products, stores, customers, price_list, promotions, promo_products"
        )
        for table in ("products", "stores", "customers", "price_list", "promotions", "promo_products"):
            _insert(cur, table, data[table])
            print(f"seeded {table:15s} {len(data[table]):>7,} rows")
    conn.close()

    out = Path(__file__).parent / "params.json"
    out.write_text(json.dumps(data["params"], indent=2))
    print(f"ground truth -> {out}  (never ingest this)")


if __name__ == "__main__":
    main()
