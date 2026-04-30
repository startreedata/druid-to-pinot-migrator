#!/usr/bin/env python3
"""Hybrid producer: old batch with historical timestamps, new batch with
recent timestamps.  Both go to the same topic.

Usage:
  produce_hybrid.py old   --topic hybrid_events_topic --n 1000
  produce_hybrid.py new   --topic hybrid_events_topic --n 500
"""
from __future__ import annotations

import argparse
import json
import random
import sys
import time

from kafka import KafkaProducer

REGIONS = ["us-east", "us-west", "eu-central", "apac"]
PLATFORMS = ["desktop", "mobile", "tablet"]
PAGES = ["/home", "/products", "/checkout", "/blog/post-1", "/blog/post-2"]


def build_events(n: int, seed: int, base_ms: int, span_ms: int) -> list[dict]:
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        ts = base_ms + rng.randint(0, max(span_ms, 1))
        rows.append({
            "timestamp": ts,
            "region": rng.choice(REGIONS),
            "platform": rng.choice(PLATFORMS),
            "page": rng.choice(PAGES),
            "user_id": f"user_{rng.randint(1, 200)}",
            "session_ms": rng.randint(1_000, 600_000),
            "bytes_sent": rng.randint(100, 100_000),
        })
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("phase", choices=["old", "new"])
    p.add_argument("--bootstrap", default="localhost:19092")
    p.add_argument("--topic", default="hybrid_events_topic")
    p.add_argument("--n", type=int, default=1000)
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()

    now_ms = int(time.time() * 1000)
    if args.phase == "old":
        # 7 days ago, span 24h
        base_ms = now_ms - 7 * 86_400_000
        span_ms = 86_400_000
        seed = args.seed or 7777
    else:
        # last hour
        base_ms = now_ms - 3_600_000
        span_ms = 3_600_000
        seed = args.seed or 9999

    events = build_events(args.n, seed, base_ms, span_ms)
    events.sort(key=lambda r: r["timestamp"])

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=10,
    )
    for e in events:
        producer.send(args.topic, e)
    producer.flush()
    producer.close()
    print(f"[{args.phase}] produced {len(events)} events to {args.topic} "
          f"(ts range: {events[0]['timestamp']}..{events[-1]['timestamp']})",
          file=sys.stderr)


if __name__ == "__main__":
    main()
