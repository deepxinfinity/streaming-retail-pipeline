import pytest

from ml.pricing_rules import above_margin_floor, psych_ending, within_move_cap


class TestMoveCap:
    def test_within_cap(self):
        assert within_move_cap(10.0, 10.99)
        assert within_move_cap(10.0, 9.01)

    def test_exceeds_cap(self):
        assert not within_move_cap(10.0, 11.5)
        assert not within_move_cap(10.0, 8.5)

    def test_boundary_inclusive(self):
        assert within_move_cap(10.0, 11.0)


class TestMarginFloor:
    def test_healthy_margin(self):
        assert above_margin_floor(10.0, 6.0)

    def test_below_floor(self):
        assert not above_margin_floor(10.0, 9.8)

    def test_selling_at_cost_fails(self):
        assert not above_margin_floor(10.0, 10.0)

    def test_boundary(self):
        assert above_margin_floor(10.0, 9.5)  # exactly 5%


class TestPsychEnding:
    @pytest.mark.parametrize("price,expected", [
        (12.30, 11.99),
        (12.99, 12.99),
        (13.00, 12.99),
        (0.75, 0.99),   # never below the floor price
        (1.99, 1.99),
    ])
    def test_rounding(self, price, expected):
        assert psych_ending(price) == expected

    def test_never_above_input_except_floor(self):
        for p in [1.5, 3.2, 9.99, 25.10, 149.49]:
            assert psych_ending(p) <= p + 1e-9
