"""the demand formula. pure functions, no i/o, unit tested.

    log lam(sku, store, day) = log(base_rate_sku) + store_effect + dow_effect(day)
                             + season_effect(week)
                             + e_sku * ln(price / base_price)
                             + uplift_sku * 1[on_promo]
    units ~ Poisson(lam)

baskets: shoppers per store-day ~ Poisson, basket size ~ shifted negative
binomial (mean ~8), items drawn proportional to each sku's lam share.
"""

from __future__ import annotations

import math
from datetime import date

import numpy as np

BASKET_NB_N = 3.0  # NB dispersion for basket size


def day_multipliers(d: date, dow_effect: list[float], season_amp: float) -> tuple[float, float]:
    """(dow multiplier, seasonal multiplier) for a calendar day. Monday=0."""
    dow_mult = math.exp(dow_effect[d.weekday()])
    week = d.isocalendar().week
    season_mult = math.exp(season_amp * math.sin(2.0 * math.pi * week / 52.0))
    return dow_mult, season_mult


def sku_lambda(
    base_rate: np.ndarray,
    price: np.ndarray,
    base_price: np.ndarray,
    elasticity: np.ndarray,
    on_promo: np.ndarray,
    promo_uplift: np.ndarray,
    store_traffic: float = 1.0,
    dow_mult: float = 1.0,
    season_mult: float = 1.0,
) -> np.ndarray:
    """Vectorized expected units per SKU for one store-day."""
    price_eff = np.power(np.asarray(price) / np.asarray(base_price), np.asarray(elasticity))
    promo_eff = np.exp(np.asarray(promo_uplift) * np.asarray(on_promo, dtype=float))
    return np.asarray(base_rate) * store_traffic * dow_mult * season_mult * price_eff * promo_eff


def shopper_count(
    rng: np.random.Generator,
    base_shoppers: float,
    store_traffic: float,
    dow_mult: float,
    season_mult: float,
) -> int:
    return int(rng.poisson(base_shoppers * store_traffic * dow_mult * season_mult))


def basket_size(rng: np.random.Generator, mean: float = 8.0) -> int:
    """Shifted negative binomial: 1 + NB(n, p) with E = mean."""
    extra_mean = max(mean - 1.0, 0.1)
    p = BASKET_NB_N / (BASKET_NB_N + extra_mean)
    return 1 + int(rng.negative_binomial(BASKET_NB_N, p))


def draw_basket(
    rng: np.random.Generator,
    sku_idx: np.ndarray,
    weights: np.ndarray,
    size: int,
) -> dict[int, int]:
    """Draw `size` item picks proportional to lambda share; returns {sku index: qty}."""
    w = np.asarray(weights, dtype=float)
    total = w.sum()
    if total <= 0 or size <= 0:
        return {}
    picks = rng.choice(sku_idx, size=size, replace=True, p=w / total)
    idx, counts = np.unique(picks, return_counts=True)
    return dict(zip(idx.tolist(), counts.tolist()))
