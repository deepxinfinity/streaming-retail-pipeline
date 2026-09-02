"""fake store till: rings a sale up at pos_ts, uploads it minutes to hours
later at sent_ts, and re-sends ~1% of batches with the same event_ids.
"""

from __future__ import annotations

import os
import time
import uuid
from datetime import UTC, datetime

import numpy as np

from sim import demand
from sim.context import SimContext
from sim.producers.common import TOPIC_POS, EmitCounter, make_producer, utc_now_iso

STORE_OPEN_H, STORE_CLOSE_H = 8, 22
DUP_FLUSH_P = 0.01


def draw_delay_s(rng: np.random.Generator) -> float:
    """lognormal flush delay, median 20 min, capped at 6h."""
    # set POS_DELAY_SCALE=0.05 in dev, otherwise you wait 20 real minutes
    # before a single row shows up in silver and assume it's broken
    scale = float(os.getenv("POS_DELAY_SCALE", "1.0"))
    return float(min(rng.lognormal(np.log(20 * 60), 1.0), 6 * 3600)) * scale


def build_basket(ctx: SimContext, rng: np.random.Generator, store_id: str,
                 pos_ts: str | None = None) -> dict:
    state = ctx.day_state(datetime.now(UTC).date())
    size = demand.basket_size(rng, ctx.params["basket_mean_store"])
    return {
        "event_id": str(uuid.uuid4()),
        "txn_id": f"T-{uuid.uuid4().hex[:16]}",
        "store_id": store_id,
        "pos_ts": pos_ts or utc_now_iso(),
        "sent_ts": None,  # stamped at flush
        "customer_id": ctx.pick_customer(rng, store_id) if rng.random() < 0.6 else None,
        "lines": ctx.make_lines(rng, state, size),
    }


def main() -> None:
    ctx = SimContext()
    rng = np.random.default_rng()
    producer = make_producer()
    counter = EmitCounter(TOPIC_POS)
    rate = float(os.getenv("EVENTS_PER_SEC", "20")) * (1 - float(os.getenv("ONLINE_SHARE", "0.3")))
    store_ids = ctx.stores.store_id.to_numpy()
    traffic = np.array([ctx.store_traffic[s] for s in store_ids])
    traffic = traffic / traffic.sum()
    print(f"pos producer -> {TOPIC_POS} at ~{rate:.1f}/s (buffered, late)")

    # buffers: store_id -> list of baskets; flush_at: store_id -> monotonic deadline
    buffers: dict[str, list[dict]] = {}
    flush_at: dict[str, float] = {}
    last_log = time.monotonic()
    try:
        while True:
            now_utc = datetime.now(UTC)
            if STORE_OPEN_H <= now_utc.hour < STORE_CLOSE_H:
                for _ in range(int(rng.poisson(rate))):
                    store = str(rng.choice(store_ids, p=traffic))
                    basket = build_basket(ctx, rng, store)
                    if not basket["lines"]:
                        continue
                    buffers.setdefault(store, []).append(basket)
                    flush_at.setdefault(store, time.monotonic() + draw_delay_s(rng))

            for store in [s for s, t in flush_at.items() if time.monotonic() >= t]:
                batch = buffers.pop(store, [])
                del flush_at[store]
                sent_ts = utc_now_iso()
                for b in batch:
                    b["sent_ts"] = sent_ts
                    producer.send(TOPIC_POS, key=store, value=b)
                    counter.add(len(b["lines"]))
                if batch and rng.random() < DUP_FLUSH_P:  # verbatim re-send, same event_ids
                    for b in batch:
                        producer.send(TOPIC_POS, key=store, value=b)
                        counter.add(0, dupe=True)

            if time.monotonic() - last_log > 10:
                producer.flush()
                counter.flush("live")
                last_log = time.monotonic()
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        for store, batch in buffers.items():
            sent_ts = utc_now_iso()
            for b in batch:
                b["sent_ts"] = sent_ts
                producer.send(TOPIC_POS, key=store, value=b)
                counter.add(len(b["lines"]))
        producer.flush()
        counter.flush("live-final")
        producer.close()


if __name__ == "__main__":
    main()
