import json
from collections import defaultdict
from pathlib import Path

LOG = Path("logs/emit_log.jsonl")


def main():
    if not LOG.exists():
        print(f"no emit log at {LOG}  - run producers or a backfill first")
        return
    totals: dict[str, dict[str, int]] = defaultdict(lambda: {"events": 0, "lines": 0, "dupes": 0})
    with LOG.open() as f:
        for row in map(json.loads, f):
            t = totals[row["topic"]]
            t["events"] += row.get("events", 0)
            t["lines"] += row.get("lines", 0)
            t["dupes"] += row.get("dupes", 0)
    for topic, t in sorted(totals.items()):
        print(f"{topic:15s} events={t['events']:>10,} lines={t['lines']:>10,} dupes={t['dupes']:>6,}")
    print("\n expetcations:")
    print("  bronze row count== events + dupes (bronze keeps everything)")
    print("  silver distinct id == lines (MERGE deduped)")


if __name__ == "__main__":
    main()
