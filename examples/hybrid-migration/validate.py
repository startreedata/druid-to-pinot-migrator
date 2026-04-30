#!/usr/bin/env python3
"""
Hybrid migration parity validator.

Compares aggregates across:
  - Druid datasource `pageviews_hybrid`  (rollup=false, all 1500 events)
  - Pinot hybrid table `pageviews_hybrid` (broker routes OFFLINE+REALTIME)

Each Druid metric column has the same name as in Pinot
(`events`, `session_ms_sum`, `bytes_sent_sum`) — the values must match
exactly because both engines stored each event verbatim (no rollup).
"""
from __future__ import annotations

import json
import sys
import urllib.request

DRUID = "http://localhost:18888/druid/v2/sql"
PINOT = "http://localhost:18099/query/sql"


def post(url: str, body: dict):
    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def druid(sql: str):
    return post(DRUID, {"query": sql})


def pinot(sql: str):
    out = post(PINOT, {"sql": sql})
    if out.get("exceptions"):
        raise RuntimeError(f"Pinot exception: {out['exceptions']}")
    return out["resultTable"]["rows"]


def scalar(label: str, dsql: str, psql: str):
    d = druid(dsql)
    p = pinot(psql)
    dv = list(d[0].values())[0] if d else None
    pv = p[0][0] if p else None
    return dv == pv, f"druid={dv}  pinot={pv}"


def grouped(label: str, dsql: str, psql: str):
    d = druid(dsql)
    p = pinot(psql)
    d_rows = sorted([(r[list(r.keys())[0]], r[list(r.keys())[1]]) for r in d])
    p_rows = sorted([(r[0], r[1]) for r in p])
    return d_rows == p_rows, f"({len(d_rows)} groups)"


checks = [
    ("Total event count",
     'SELECT COUNT(*) AS v FROM "pageviews_hybrid"',
     'SELECT COUNT(*) FROM pageviews_hybrid'),
    ("SUM(events)",
     'SELECT SUM(events) AS v FROM "pageviews_hybrid"',
     'SELECT SUM(events) FROM pageviews_hybrid'),
    ("SUM(session_ms_sum)",
     'SELECT SUM(session_ms_sum) AS v FROM "pageviews_hybrid"',
     'SELECT SUM(session_ms_sum) FROM pageviews_hybrid'),
    ("SUM(bytes_sent_sum)",
     'SELECT SUM(bytes_sent_sum) AS v FROM "pageviews_hybrid"',
     'SELECT SUM(bytes_sent_sum) FROM pageviews_hybrid'),
    ("Distinct user_id (exact)",
     'SELECT COUNT(*) AS v FROM (SELECT "user_id" FROM "pageviews_hybrid" GROUP BY "user_id")',
     'SELECT DISTINCTCOUNT(user_id) FROM pageviews_hybrid'),
    ("MIN(timestamp)",
     'SELECT TIMESTAMP_TO_MILLIS(MIN(__time)) AS v FROM "pageviews_hybrid"',
     'SELECT MIN("timestamp") FROM pageviews_hybrid'),
    ("MAX(timestamp)",
     'SELECT TIMESTAMP_TO_MILLIS(MAX(__time)) AS v FROM "pageviews_hybrid"',
     'SELECT MAX("timestamp") FROM pageviews_hybrid'),
]

groupings = [
    ("events by region",
     'SELECT region, SUM(events) FROM "pageviews_hybrid" GROUP BY region ORDER BY region',
     'SELECT region, SUM(events) FROM pageviews_hybrid GROUP BY region ORDER BY region'),
    ("events by platform",
     'SELECT platform, SUM(events) FROM "pageviews_hybrid" GROUP BY platform ORDER BY platform',
     'SELECT platform, SUM(events) FROM pageviews_hybrid GROUP BY platform ORDER BY platform'),
    ("session_ms_sum by page",
     'SELECT page, SUM(session_ms_sum) FROM "pageviews_hybrid" GROUP BY page ORDER BY page',
     'SELECT page, SUM(session_ms_sum) FROM pageviews_hybrid GROUP BY page ORDER BY page'),
    ("bytes_sent_sum by region",
     'SELECT region, SUM(bytes_sent_sum) FROM "pageviews_hybrid" GROUP BY region ORDER BY region',
     'SELECT region, SUM(bytes_sent_sum) FROM pageviews_hybrid GROUP BY region ORDER BY region'),
]

passed = failed = 0
print("=== HYBRID: pageviews_hybrid (1000 hist + 500 new = 1500 events) ===")
for label, dsql, psql in checks:
    try:
        ok, detail = scalar(label, dsql, psql)
    except Exception as e:
        ok, detail = False, f"ERROR {e}"
    status = "PASS" if ok else "FAIL"
    print(f"  {status}  {label:<32s} {detail}")
    if ok:
        passed += 1
    else:
        failed += 1

for label, dsql, psql in groupings:
    try:
        ok, detail = grouped(label, dsql, psql)
    except Exception as e:
        ok, detail = False, f"ERROR {e}"
    status = "PASS" if ok else "FAIL"
    print(f"  {status}  {label:<32s} {detail}")
    if ok:
        passed += 1
    else:
        failed += 1

print(f"\nResult: {passed} passed, {failed} failed (out of {passed + failed})")
sys.exit(0 if failed == 0 else 1)
