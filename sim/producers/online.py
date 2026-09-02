"""live web orders, emitted as soon as they are placed. key = order_id."""
import os
import time
import uuid
from datetime import UTC, datetime

import numpy as np

from sim import demand
from sim.context import SimContext
from sim.producers.common import TOPIC_ONLINE, EmitCounter, make_producer, utc_now_iso


def build_order(ctx: SimContext, rng: np.random.Generator, order_ts: str | None = None) -> dict:
    state = ctx.day_state(datetime.now(UTC).date())
    size = demand.basket_size(rng, ctx.params["basket_mean_online"])
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"O-{uuid.uuid4().hex[:16]}",
        "order_ts": order_ts or utc_now_iso(),
        "customer_id": str(ctx._all_customers[rng.integers(0, len(ctx._all_customers))]),
        "channel": "online",
        "lines": ctx.make_lines(rng, state, size),
    }


def main() -> None:
    ctx = SimContext()
    rng = np.random.default_rng()
    producer = make_producer()
    counter = EmitCounter(TOPIC_ONLINE)
    rate = float(os.getenv("EVENTS_PER_SEC", "20")) * float(os.getenv("ONLINE_SHARE", "0.3"))
    print(f"online producer -> {TOPIC_ONLINE} at ~{rate:.1f}/s")

    last_flush = time.monotonic()
    try:
        while True:
            for _ in range(int(rng.poisson(rate))):
                order = build_order(ctx, rng)
                if not order["lines"]:
                    continue
                producer.send(TOPIC_ONLINE, key=order["order_id"], value=order)
                counter.add(len(order["lines"]))
            if time.monotonic() - last_flush > 10:
                producer.flush()
                counter.flush("live")
                last_flush = time.monotonic()
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        counter.flush("live")
        producer.close()


if __name__ == "__main__":
    main()
