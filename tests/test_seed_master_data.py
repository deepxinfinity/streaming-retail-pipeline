"""master data checks."""

from datetime import date

import numpy as np
import pytest

from sim.seed import N_SKUS, N_STORES, SEED, build_master_data


@pytest.fixture(scope="module")
def data():
    return build_master_data(np.random.default_rng(SEED), date(2026, 1, 1), date(2026, 7, 1))


def test_sku_count_exact(data):
    assert len(data["products"]) == N_SKUS
    assert len(data["params"]["skus"]) == N_SKUS


def test_stores_include_online(data):
    ids = {s["store_id"] for s in data["stores"]}
    assert len(ids) == N_STORES + 1
    assert "S-ONLINE" in ids


def test_cost_below_price(data):
    for p in data["products"]:
        assert 0 < p["unit_cost"] < p["base_price"]


def test_elasticities_in_range(data):
    for sku in data["params"]["skus"].values():
        assert -2.5 <= sku["elasticity"] <= -0.8
        assert sku["promo_uplift"] > 0


def test_price_list_unique_key(data):
    keys = [(r["sku_id"], r["scope"], r["effective_from"]) for r in data["price_list"]]
    assert len(keys) == len(set(keys))


def test_promo_waves_cover_history(data):
    starts = [date.fromisoformat(p["start_date"]) for p in data["promotions"]]
    assert min(starts) == date(2026, 1, 1)
    assert max(starts) >= date(2026, 7, 1)


def test_deterministic(data):
    again = build_master_data(np.random.default_rng(SEED), date(2026, 1, 1), date(2026, 7, 1))
    assert [p["base_price"] for p in again["products"]] == \
           [p["base_price"] for p in data["products"]]
