"""replay months of history through kafka
"""

from __future__ import annotations

import argparse
import os
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime

import numpy as np

from sim import demand
from sim.context import SimContext
from sim.producers.common import TOPIC_ONLINE, TOPIC_POS, EmitCounter, make_producer
from sim.producers.pos import DUP_FLUSH_P, STORE_CLOSE_H, STORE_OPEN_H, draw_delay_s
from sim.seed import sim_start_date

# seeds the demand draws, so the same days produce the same units and baskets.
# event_id is a fresh uuid4 every time though, so re-running does NOT reproduce
# the same events - silver sees them as new facts. truncate before you replay.
SEED = 1042


def ts_iso(d: date, seconds_into_day: float) -> str:
    return (datetime.combine(d, dtime.min, tzinfo=UTC)
            + timedelta(seconds=float(seconds_into_day))).isoformat()


def run_day(ctx: SimContext, rng: np.random.Generator, producer, d: date,
            c_online: EmitCounter, c_pos: EmitCounter) -> int:
    state = ctx.day_state(d)
    n_events = 0

    # ---- online: spread over 24h, key = order_id --------------------------
    n_orders = rng.poisson(
        ctx.params["online_orders_per_day"] * state.dow_mult * state.season_mult
    )
    for _ in range(n_orders):
        lines = ctx.make_lines(rng, state, demand.basket_size(rng, ctx.params["basket_mean_online"]))
        if not lines:
            continue
        order = {
            "event_id": str(uuid.uuid4()),
            "order_id": f"O-{uuid.uuid4().hex[:16]}",
            "order_ts": ts_iso(d, rng.uniform(0, 86_400)),
            "customer_id": str(ctx._all_customers[rng.integers(0, len(ctx._all_customers))]),
            "channel": "online",
            "lines": lines,
        }
        producer.send(TOPIC_ONLINE, key=order["order_id"], value=order)
        c_online.add(len(lines))
        n_events += 1

    # POS: per store, till times 08-22, flush batches with delay
    base_shoppers = float(os.getenv("SHOPPERS_PER_STORE_DAY", "150"))
    open_s, close_s = STORE_OPEN_H * 3600, STORE_CLOSE_H * 3600
    for store_id in ctx.stores.store_id:
        n_baskets = demand.shopper_count(
            rng, base_shoppers, ctx.store_traffic[store_id], state.dow_mult, state.season_mult
        )
        if n_baskets == 0:
            continue
        till_times = np.sort(rng.uniform(open_s, close_s, size=n_baskets))
        batch: list[dict] = []
        for t in till_times:
            lines = ctx.make_lines(rng, state, demand.basket_size(rng, ctx.params["basket_mean_store"]))
            if not lines:
                continue
            batch.append({
                "event_id": str(uuid.uuid4()),
                "txn_id": f"T-{uuid.uuid4().hex[:16]}",
                "store_id": str(store_id),
                "pos_ts": ts_iso(d, t),
                "sent_ts": None,
                "customer_id": ctx.pick_customer(rng, str(store_id)) if rng.random() < 0.6 else None,
                "lines": lines,
            })
            # 25 is arbitrary, roughly "the till uploads every so often".
            # started at 5 and the flush logs were unreadable
            if len(batch) >= 25:
                n_events += flush_pos(rng, producer, batch, c_pos)
                batch = []
        n_events += flush_pos(rng, producer, batch, c_pos)
    return n_events


def flush_pos(rng: np.random.Generator, producer, batch: list[dict], counter: EmitCounter) -> int:
    if not batch:
        return 0
    last_pos = datetime.fromisoformat(batch[-1]["pos_ts"])
    sent = (last_pos + timedelta(seconds=draw_delay_s(rng))).isoformat()
    for b in batch:
        b["sent_ts"] = sent
        producer.send(TOPIC_POS, key=b["store_id"], value=b)
        counter.add(len(b["lines"]))
    if rng.random() < DUP_FLUSH_P:
        for b in batch:
            producer.send(TOPIC_POS, key=b["store_id"], value=b)
            counter.add(0, dupe=True)
    return len(batch)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=date.fromisoformat, default=None)
    ap.add_argument("--end", type=date.fromisoformat, default=None,
                    help="exclusive; defaults to today")
    args = ap.parse_args()

    start = args.start or sim_start_date()
    end = args.end or date.today()

    ctx = SimContext()
    rng = np.random.default_rng(SEED)
    producer = make_producer()
    c_online, c_pos = EmitCounter(TOPIC_ONLINE), EmitCounter(TOPIC_POS)

    print(f"backfill {start} -> {end} (exclusive)")
    t0 = time.monotonic()
    total = 0
    d = start
    while d < end:
        n = run_day(ctx, rng, producer, d, c_online, c_pos)
        producer.flush()
        c_online.flush(d.isoformat())
        c_pos.flush(d.isoformat())
        total += n
        elapsed = time.monotonic() - t0
        print(f"  {d}  {n:>7,} events  ({total / max(elapsed, 1e-9):,.0f} ev/s cumulative)")
        d += timedelta(days=1)

    producer.close()
    print(f"done: {total:,} events in {time.monotonic() - t0:,.1f}s")


if __name__ == "__main__":
    main()
