import numpy as np
import pytest

from sim import demand


@pytest.fixture
def base_args():
    return dict(
        base_rate=np.array([1.0, 2.0, 0.5]),
        base_price=np.array([10.0, 5.0, 20.0]),
        elasticity=np.array([-1.5, -2.0, -1.0]),
        on_promo=np.zeros(3),
        promo_uplift=np.array([0.3, 0.4, 0.2]),
    )


def test_raising_price_lowers_expected_units(base_args):
    lam_base = demand.sku_lambda(price=base_args["base_price"], **base_args)
    lam_up = demand.sku_lambda(price=base_args["base_price"] * 1.2, **base_args)
    assert (lam_up < lam_base).all()


def test_lowering_price_raises_expected_units(base_args):
    lam_base = demand.sku_lambda(price=base_args["base_price"], **base_args)
    lam_down = demand.sku_lambda(price=base_args["base_price"] * 0.8, **base_args)
    assert (lam_down > lam_base).all()


def test_promo_raises_expected_units(base_args):
    lam_off = demand.sku_lambda(price=base_args["base_price"], **base_args)
    promo_args = {**base_args, "on_promo": np.ones(3)}
    lam_on = demand.sku_lambda(price=base_args["base_price"], **promo_args)
    assert (lam_on > lam_off).all()


def test_elasticity_exact_at_log_scale(base_args):
    """Doubling price scales lambda by exactly 2^e  -  the log-log identity."""
    lam1 = demand.sku_lambda(price=base_args["base_price"], **base_args)
    lam2 = demand.sku_lambda(price=base_args["base_price"] * 2, **base_args)
    np.testing.assert_allclose(lam2 / lam1, 2.0 ** base_args["elasticity"])


def test_fixed_seed_identical_output(base_args):
    def run(seed):
        rng = np.random.default_rng(seed)
        size = demand.basket_size(rng)
        return demand.draw_basket(rng, np.arange(3), base_args["base_rate"], size)

    assert run(7) == run(7)
    assert demand.basket_size(np.random.default_rng(1)) == demand.basket_size(
        np.random.default_rng(1)
    )


def test_basket_size_mean_close_to_target():
    rng = np.random.default_rng(0)
    sizes = [demand.basket_size(rng, mean=8.0) for _ in range(20_000)]
    assert 7.5 < np.mean(sizes) < 8.5
    assert min(sizes) >= 1


def test_draw_basket_respects_weights():
    rng = np.random.default_rng(0)
    counts = demand.draw_basket(rng, np.arange(2), np.array([100.0, 1.0]), 1000)
    assert counts.get(0, 0) > counts.get(1, 0) * 10


def test_draw_basket_empty_cases():
    rng = np.random.default_rng(0)
    assert demand.draw_basket(rng, np.arange(2), np.zeros(2), 5) == {}
    assert demand.draw_basket(rng, np.arange(2), np.ones(2), 0) == {}


def test_day_multipliers_weekend_boost():
    from datetime import date
    dow = [0.0, -0.05, -0.05, 0.0, 0.10, 0.35, 0.25]
    sat, _ = demand.day_multipliers(date(2026, 7, 18), dow, 0.0)   # Saturday
    tue, _ = demand.day_multipliers(date(2026, 7, 14), dow, 0.0)   # Tuesday
    assert sat > tue
