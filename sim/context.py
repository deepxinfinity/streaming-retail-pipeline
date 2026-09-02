"""master data from postgres + the ground-truth params, loaded once.

live producers and the backfill both go through this, so history and live
traffic come out of the same demand model.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

from sim import demand

PARAMS_PATH = Path(__file__).parent / "params.json"


def _pg_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "retail"),
    )


@dataclass
class DayState:
    """Everything needed to draw baskets for one calendar day."""
    sku_ids: np.ndarray          # aligned arrays over all active SKUs
    eff_price: np.ndarray        # list price minus promo discount
    promo_id: np.ndarray         # promo id or None per SKU
    lam_base: np.ndarray         # per-SKU lambda at store_traffic=1, day multipliers=1
    dow_mult: float
    season_mult: float


class SimContext:
    def __init__(self) -> None:
        if not PARAMS_PATH.exists():
            raise SystemExit("sim/params.json missing  -  run `make seed` first")
        self.params = json.loads(PARAMS_PATH.read_text())

        with _pg_conn() as conn:
            self.products = pd.read_sql(
                "SELECT sku_id, base_price, unit_cost FROM products WHERE active", conn
            )
            self.stores = pd.read_sql(
                "SELECT store_id, store_format FROM stores WHERE store_id <> 'S-ONLINE'", conn
            )
            self.customers = pd.read_sql(
                "SELECT customer_id, home_store_id FROM customers", conn
            )
            self.price_list = pd.read_sql(
                "SELECT sku_id, scope, price, effective_from FROM price_list ORDER BY effective_from",
                conn,
            )
            self.promos = pd.read_sql(
                """SELECT pp.sku_id, p.promo_id, p.discount_pct, p.start_date, p.end_date
                   FROM promo_products pp JOIN promotions p USING (promo_id)""",
                conn,
            )

        sku_p = self.params["skus"]
        self.products = self.products[self.products.sku_id.isin(sku_p)].reset_index(drop=True)
        self.sku_ids = self.products.sku_id.to_numpy()
        self.base_price = self.products.base_price.astype(float).to_numpy()
        self.elasticity = np.array([sku_p[s]["elasticity"] for s in self.sku_ids])
        self.promo_uplift = np.array([sku_p[s]["promo_uplift"] for s in self.sku_ids])
        self.base_rate = np.array([sku_p[s]["base_rate"] for s in self.sku_ids])
        self.store_traffic = {s: v["traffic"] for s, v in self.params["stores"].items()}
        self._sku_pos = {s: i for i, s in enumerate(self.sku_ids)}

        # customers grouped by home store for realistic store/customer affinity
        self._cust_by_store = {
            k: g.customer_id.to_numpy() for k, g in self.customers.groupby("home_store_id")
        }
        self._all_customers = self.customers.customer_id.to_numpy()

    @lru_cache(maxsize=64)
    def day_state(self, d: date) -> DayState:
        """Effective prices, promo flags and base lambda for one day (cached)."""
        # current list price per sku: last effective_from <= d, scope 'all'
        pl = self.price_list[
            (self.price_list.scope == "all") & (self.price_list.effective_from <= d)
        ]
        cur = pl.groupby("sku_id").last()["price"].astype(float)
        list_price = np.array([cur.get(s, b) for s, b in zip(self.sku_ids, self.base_price)])

        active = self.promos[(self.promos.start_date <= d) & (self.promos.end_date >= d)]
        promo_id = np.full(len(self.sku_ids), None, dtype=object)
        discount = np.zeros(len(self.sku_ids))
        for row in active.itertuples(index=False):
            i = self._sku_pos.get(row.sku_id)
            if i is not None and promo_id[i] is None:
                promo_id[i] = row.promo_id
                discount[i] = float(row.discount_pct)

        eff_price = np.round(list_price * (1.0 - discount), 2)
        on_promo = (discount > 0).astype(float)
        dow_mult, season_mult = demand.day_multipliers(
            d, self.params["dow_effect"], self.params["season_amp"]
        )
        lam = demand.sku_lambda(
            self.base_rate, eff_price, self.base_price, self.elasticity,
            on_promo, self.promo_uplift,
        )
        return DayState(self.sku_ids, eff_price, promo_id, lam, dow_mult, season_mult)

    def make_lines(self, rng: np.random.Generator, state: DayState, size: int) -> list[dict]:
        """One basket -> order lines with qty aggregated per SKU."""
        picks = demand.draw_basket(rng, np.arange(len(state.sku_ids)), state.lam_base, size)
        lines = []
        for n, (i, qty) in enumerate(sorted(picks.items()), start=1):
            lines.append({
                "line_no": n,
                "sku_id": str(state.sku_ids[i]),
                "qty": int(qty),
                "unit_price": float(state.eff_price[i]),
                "promo_id": state.promo_id[i],
            })
        return lines

    def pick_customer(self, rng: np.random.Generator, store_id: str) -> str:
        """70% home-store shoppers, 30% anyone."""
        home = self._cust_by_store.get(store_id)
        pool = home if home is not None and rng.random() < 0.7 else self._all_customers
        return str(pool[rng.integers(0, len(pool))])
