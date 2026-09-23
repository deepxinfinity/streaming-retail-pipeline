"""pricing guardrails. pure functions, no i/o, so they are easy to test."""

# a real retailer would have these per-category and argue about them for weeks
MOVE_CAP = 0.10      # max fraction a price can move in one run
MARGIN_FLOOR = 0.05  # (price - cost) / price must stay above this


# the 1e-9 is so a price exactly on the boundary counts as inside it. float
# arithmetic on 10.0 * 1.1 does not give you 11.0 exactly.
def within_move_cap(current: float, candidate: float, cap: float = MOVE_CAP) -> bool:
    return abs(candidate - current) <= cap * current + 1e-9


def above_margin_floor(price: float, unit_cost: float, floor: float = MARGIN_FLOOR) -> bool:
    return price > 0 and (price - unit_cost) / price >= floor - 1e-9


def psych_ending(price: float) -> float:
    """round down to the nearest .99. 12.30 -> 11.99, 12.99 stays put.

    always rounds down so it can never push a price past the move cap that was
    already checked against it.
    """
    candidate = int(price) + 0.99
    if candidate > price + 1e-9:
        candidate -= 1.0
    return round(max(candidate, 0.99), 2)
