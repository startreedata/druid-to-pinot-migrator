#!/usr/bin/env python3
"""
Compare aggregate query results between the Druid datasource and the
Pinot table populated by the migration.

Pinot ingests the *raw* events (the same file Druid ingested), while Druid
applies its rollup at HOUR granularity.  Therefore, parity must be expressed
in terms of *aggregates over the source fields*, not row-by-row matching:

    Druid `SUM(events)`        ==  Pinot `COUNT(*)`
    Druid `SUM(session_ms_sum)` ==  Pinot `SUM(session_ms)`
    Druid `SUM(bytes_sent_sum)` ==  Pinot `SUM(bytes_sent)`
    Druid `MAX(session_ms_max)` ==  Pinot `MAX(session_ms)`
    Druid `MIN(bytes_sent_min)` ==  Pinot `MIN(bytes_sent)`

The script exits with a non-zero status if any check fails.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
from typing import Any

DRUID_ROUTER = "http://localhost:8888"
PINOT_BROKER = "http://localhost:8099"

DRUID_DATASOURCE = "pageviews"
PINOT_TABLE = "pageviews"

# ───── colour helpers (no extra deps) ─────────────────────────────────────────
def green(s: str) -> str:  return f"\033[32m{s}\033[0m"
def red(s: str)   -> str:  return f"\033[31m{s}\033[0m"
def cyan(s: str)  -> str:  return f"\033[36m{s}\033[0m"
def bold(s: str)  -> str:  return f"\033[1m{s}\033[0m"


def http_post_json(url: str, payload: dict, timeout: float = 30.0) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def druid_sql(sql: str) -> list[dict]:
    res = http_post_json(
        f"{DRUID_ROUTER}/druid/v2/sql",
        {"query": sql, "resultFormat": "object"},
    )
    return res


def pinot_sql(sql: str) -> dict:
    res = http_post_json(f"{PINOT_BROKER}/query/sql", {"sql": sql})
    if res.get("exceptions"):
        raise RuntimeError(f"Pinot exception: {res['exceptions']}")
    return res


def pinot_scalar(sql: str) -> Any:
    res = pinot_sql(sql)
    return res["resultTable"]["rows"][0][0]


def druid_scalar(sql: str) -> Any:
    res = druid_sql(sql)
    return list(res[0].values())[0]


def wait_for_pinot_rows(expected: int, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            cnt = int(pinot_scalar(f"SELECT COUNT(*) FROM {PINOT_TABLE}"))
            if cnt >= expected:
                return
            print(f"  Pinot row count = {cnt} / {expected} ...")
        except Exception as e:
            print(f"  waiting for Pinot... ({e})")
        time.sleep(5)
    raise TimeoutError(
        f"Pinot table {PINOT_TABLE} never reached {expected} rows"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Checks
# ─────────────────────────────────────────────────────────────────────────────

CHECKS = [
    # (description, druid_sql, pinot_sql)
    (
        "Total event count",
        f'SELECT SUM(events) AS v FROM "{DRUID_DATASOURCE}"',
        f"SELECT COUNT(*) AS v FROM {PINOT_TABLE}",
    ),
    (
        "Total session_ms (SUM)",
        f'SELECT SUM(session_ms_sum) AS v FROM "{DRUID_DATASOURCE}"',
        f"SELECT SUM(session_ms) AS v FROM {PINOT_TABLE}",
    ),
    (
        "Total bytes_sent (SUM)",
        f'SELECT SUM(bytes_sent_sum) AS v FROM "{DRUID_DATASOURCE}"',
        f"SELECT SUM(bytes_sent) AS v FROM {PINOT_TABLE}",
    ),
    (
        "Max session_ms",
        f'SELECT MAX(session_ms_max) AS v FROM "{DRUID_DATASOURCE}"',
        f"SELECT MAX(session_ms) AS v FROM {PINOT_TABLE}",
    ),
    (
        "Min bytes_sent",
        f'SELECT MIN(bytes_sent_min) AS v FROM "{DRUID_DATASOURCE}"',
        f"SELECT MIN(bytes_sent) AS v FROM {PINOT_TABLE}",
    ),
]


GROUP_BY_CHECKS = [
    # (description, dimension, druid_metric_expr, pinot_metric_expr)
    (
        "events grouped by region",
        "region",
        "SUM(events)",
        "COUNT(*)",
    ),
    (
        "events grouped by platform",
        "platform",
        "SUM(events)",
        "COUNT(*)",
    ),
    (
        "session_ms sum grouped by page",
        "page",
        "SUM(session_ms_sum)",
        "SUM(session_ms)",
    ),
]


def run_scalar_checks() -> tuple[int, int]:
    passed = failed = 0
    for desc, dq, pq in CHECKS:
        try:
            dv = druid_scalar(dq)
            pv = pinot_scalar(pq)
            if dv == pv:
                print(f"  {green('PASS')}  {desc:38} druid={dv}  pinot={pv}")
                passed += 1
            else:
                print(f"  {red('FAIL')}  {desc:38} druid={dv}  pinot={pv}")
                failed += 1
        except Exception as e:
            print(f"  {red('ERR ')}  {desc:38} {e}")
            failed += 1
    return passed, failed


def run_groupby_checks() -> tuple[int, int]:
    passed = failed = 0
    for desc, dim, dexpr, pexpr in GROUP_BY_CHECKS:
        dq = (
            f'SELECT "{dim}" AS k, {dexpr} AS v '
            f'FROM "{DRUID_DATASOURCE}" GROUP BY "{dim}" ORDER BY 1'
        )
        pq = (
            f"SELECT {dim} AS k, {pexpr} AS v "
            f"FROM {PINOT_TABLE} GROUP BY {dim} ORDER BY 1"
        )
        try:
            d_rows = druid_sql(dq)
            p_rows = pinot_sql(pq)["resultTable"]["rows"]
            d_map = {r["k"]: r["v"] for r in d_rows}
            p_map = {r[0]: r[1] for r in p_rows}
            if d_map == p_map:
                print(f"  {green('PASS')}  {desc:38} ({len(d_map)} groups)")
                passed += 1
            else:
                print(f"  {red('FAIL')}  {desc:38}")
                print(f"           druid={d_map}")
                print(f"           pinot={p_map}")
                failed += 1
        except Exception as e:
            print(f"  {red('ERR ')}  {desc:38} {e}")
            failed += 1
    return passed, failed


def main() -> int:
    print(bold("\n=== Druid → Pinot Quickstart Parity Check ===\n"))

    # Wait for Pinot to have data
    print(cyan("Waiting for Pinot table to be populated..."))
    wait_for_pinot_rows(expected=1)

    print(cyan("\nScalar aggregate checks:"))
    s_pass, s_fail = run_scalar_checks()

    print(cyan("\nGROUP BY aggregate checks:"))
    g_pass, g_fail = run_groupby_checks()

    total_pass = s_pass + g_pass
    total_fail = s_fail + g_fail
    print(bold(f"\nResult: {green(str(total_pass) + ' passed')}, "
               f"{red(str(total_fail) + ' failed')}\n"))

    return 0 if total_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
