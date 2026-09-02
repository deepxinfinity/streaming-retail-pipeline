"""kafka producer setup, plus a tally of what we send.

each producer keeps a running count and appends a line to logs/emit_log.jsonl
as it goes. sum that file and you know exactly what left the simulator:

  events  messages sent
  lines   sale lines inside them - one basket holds several
  dupes   deliberate re-sends, kept separate so they don't inflate events

later, once this data has landed in a table somewhere, that sum is what you
check the table against. without it there is no way to tell a message that got
dropped from one that was never sent in the first place.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

TOPIC_ONLINE = os.getenv("TOPIC_ONLINE", "online_orders")
TOPIC_POS = os.getenv("TOPIC_POS", "store_pos")
EMIT_LOG = Path("logs/emit_log.jsonl")


def utc_now_iso() -> str:
    return datetime.now().isoformat()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        acks="all",
        linger_ms=20,
        value_serializer=lambda v: json.dumps(v, default=str).encode(),
        key_serializer=lambda k: k.encode(),
    )


class EmitCounter:
    """counts what one topic sent, writes a row on flush and resets."""

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
