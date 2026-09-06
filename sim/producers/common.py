"""kafka producer setup, plus a tally of what we send. sum
logs/emit_log.jsonl to check later that nothing was lost or double-counted.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

TOPIC_ONLINE = os.getenv("TOPIC_ONLINE", "online_orders")
TOPIC_POS = os.getenv("TOPIC_POS", "store_pos")
EMIT_LOG = Path("logs/emit_log.jsonl")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        acks="all",
        linger_ms=20,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        key_serializer=lambda k: k.encode(),
    )


class EmitCounter:
    """per-topic counts: events sent, sale lines inside them, and deliberate
    re-sends kept separate so they don't inflate events."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.events = self.lines = self.dupes = 0

    def add(self, lines: int, dupe: bool = False) -> None:
        if dupe:
            self.dupes += 1
        else:
            self.events += 1
            self.lines += lines

    def flush(self, label: str = "") -> None:
        if not (self.events or self.dupes):
            return
        EMIT_LOG.parent.mkdir(exist_ok=True)
        with EMIT_LOG.open("a") as f:
            f.write(json.dumps({
                "ts": utc_now_iso(), "label": label, "topic": self.topic,
                "events": self.events, "lines": self.lines, "dupes": self.dupes,
            }) + "\n")
        self.events = self.lines = self.dupes = 0
