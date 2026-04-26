#!/usr/bin/env python3
"""
Generate a deterministic sample pageviews NDJSON file for the quickstart.

Produces 5,000 events spread across 7 days, 4 regions, 3 platforms,
and 5 page paths.  Output is written to ``data/pageviews.json``.
"""

from __future__ import annotations

import json
import random
from pathlib import Path

OUT = Path(__file__).parent / "pageviews.json"

REGIONS = ["us-east", "us-west", "eu-central", "apac"]
PLATFORMS = ["desktop", "mobile", "tablet"]
PAGES = [
    "/home",
    "/products",
    "/checkout",
    "/blog/post-1",
    "/blog/post-2",
]
START_MS = 1_704_067_200_000  # 2024-01-01T00:00:00Z
DAY_MS = 86_400_000


def main() -> None:
    rng = random.Random(42)  # deterministic
    rows = []
    for i in range(5_000):
        ts = START_MS + rng.randint(0, 7 * DAY_MS - 1)
        rows.append(
            {
                "timestamp": ts,
                "region": rng.choice(REGIONS),
                "platform": rng.choice(PLATFORMS),
                "page": rng.choice(PAGES),
                "user_id": f"user_{rng.randint(1, 500)}",
                "session_ms": rng.randint(500, 60_000),
                "bytes_sent": rng.randint(200, 100_000),
            }
        )
    rows.sort(key=lambda r: r["timestamp"])

    with OUT.open("w") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")
    print(f"Wrote {len(rows)} rows to {OUT}")


if __name__ == "__main__":
    main()
