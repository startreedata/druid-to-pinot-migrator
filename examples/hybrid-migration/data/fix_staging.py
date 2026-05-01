#!/usr/bin/env python3
"""Convert backfill staging NDJSON: __time (ISO string) → timestamp (LONG millis).

dpm's `backfill-batch` exports rows from Druid SQL as NDJSON with `__time`
as an ISO 8601 string. The Pinot schema's time column is `timestamp` of
type LONG/MILLISECONDS:EPOCH. This script bridges the gap.
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path


def to_ms(iso: str) -> int:
    # accepts "2026-04-23T00:15:22.967Z"
    s = iso.rstrip("Z").replace("T", " ")
    if "." in s:
        head, frac = s.split(".")
        frac = (frac + "000000")[:6]  # microseconds
        dt = datetime.datetime.strptime(head + "." + frac, "%Y-%m-%d %H:%M:%S.%f")
    else:
        dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=datetime.timezone.utc)
    return int(dt.timestamp() * 1000)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("input", type=Path)
    p.add_argument("output", type=Path)
    p.add_argument("--time-column", default="timestamp")
    args = p.parse_args()

    n = 0
    with args.input.open() as inp, args.output.open("w") as outp:
        for line in inp:
            row = json.loads(line)
            if "__time" in row:
                row[args.time_column] = to_ms(row.pop("__time"))
            outp.write(json.dumps(row) + "\n")
            n += 1
    print(f"Converted {n} rows: {args.input} → {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
