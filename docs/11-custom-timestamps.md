# Tutorial 11 — Custom Timestamp Formats

**Pattern:** Non-standard `format` in `timestampSpec`  
**Risk:** `CUSTOM_TIMESTAMP_FORMAT` (MEDIUM, LIKELY)  
**Typical use cases:** Apache/nginx access logs, legacy application logs, ISO 8601 variants

---

## Standard vs. Custom Formats

The tool handles these Druid timestamp formats natively (no risk):

| Druid format | Pinot format | Example value |
|-------------|-------------|---------------|
| `millis` | `1:MILLISECONDS:EPOCH` | `1709251200000` |
| `seconds` / `posix` | `1:SECONDS:EPOCH` | `1709251200` |
| `iso` | `1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss.SSSZ` | `2024-03-01T00:00:00.000+0000` |
| `micro` | `1:MICROSECONDS:EPOCH` | `1709251200000000` |
| `nano` | `1:NANOSECONDS:EPOCH` | `1709251200000000000` |
| `auto` | `1:MILLISECONDS:EPOCH` | Druid auto-detects; treated as millis |

Any other string in `format` is treated as a **Java SimpleDateFormat pattern** by Druid.
The tool raises `CUSTOM_TIMESTAMP_FORMAT` and generates a best-effort Pinot format string.

---

## Sample Druid Spec

This spec parses Apache Combined Log Format timestamps:

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "access_logs",
      "timestampSpec": {
        "column": "log_time",
        "format": "dd/MMM/yyyy:HH:mm:ss Z",
        "missingValue": "1970-01-01T00:00:00Z"
      },
      "dimensionsSpec": {
        "dimensions": [
          "host", "method", "path", "status_code", "user_agent"
        ]
      },
      "metricsSpec": [
        {"type": "count",   "name": "request_count"},
        {"type": "longSum", "name": "bytes_sent", "fieldName": "bytes"}
      ],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "HOUR",
        "rollup": false,
        "intervals": ["2024-01-01/2025-01-01"]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "local",
        "baseDir": "/var/log/nginx",
        "filter": "*.log"
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

The format `"dd/MMM/yyyy:HH:mm:ss Z"` parses values like:
```
01/Mar/2024:00:00:00 +0000
```

---

## Running the Migration

```bash
dpm generate access_logs_spec.json --out ./output/access_logs
```

Output:

```
Risks detected: 1
  [MEDIUM] CUSTOM_TIMESTAMP_FORMAT
    A non-standard timestamp format string is used in the Druid timestampSpec.
    The generated Pinot dateTimeFieldSpec uses a best-effort mapping; verify that
    the format pattern is valid in Pinot's SIMPLE_DATE_FORMAT.
    Evidence: Column 'log_time' uses format 'dd/MMM/yyyy:HH:mm:ss Z'
    Remediation: Update the dateTimeFieldSpec.format in schema.json to use the
    correct Pinot SIMPLE_DATE_FORMAT pattern.
```

---

## What Gets Generated

### schema.json (dateTimeFieldSpecs)

The tool generates a best-effort Pinot format using `SIMPLE_DATE_FORMAT`:

```json
{
  "name": "log_time",
  "dataType": "LONG",
  "format": "1:MILLISECONDS:SIMPLE_DATE_FORMAT:dd/MMM/yyyy:HH:mm:ss Z",
  "granularity": "1:MILLISECONDS"
}
```

---

## Verifying the Format

Pinot's `SIMPLE_DATE_FORMAT` uses Java's `SimpleDateFormat` syntax — the same as Druid.
However, there are edge cases:

### Month abbreviations (MMM)

`MMM` in SimpleDateFormat is locale-sensitive. `Mar` in English locale, `mär` in German.
Pinot's `DateTimeUtils` uses `Locale.ENGLISH` by default, so `Mar`, `Apr`, `Jun` etc.
will parse correctly if your data uses English month abbreviations.

If your logs use a non-English locale, normalise month names upstream before ingestion.

### Timezone offsets

Druid accepts both `Z` (literal UTC) and `+0000` as timezone designators.
Pinot's `SimpleDateFormat` parser also accepts both. Test with a sample value:

```java
// Test in Pinot console or a small Java snippet:
new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)
  .parse("01/Mar/2024:00:00:00 +0000")
// Should return: Fri Mar 01 00:00:00 UTC 2024
```

### Format pattern reference

Common format patterns:

| Pattern | Meaning | Example |
|---------|---------|---------|
| `yyyy` | 4-digit year | `2024` |
| `MM` | 2-digit month | `03` |
| `MMM` | 3-letter month abbreviation | `Mar` |
| `dd` | 2-digit day | `01` |
| `HH` | Hour (00-23) | `14` |
| `mm` | Minutes | `30` |
| `ss` | Seconds | `45` |
| `SSS` | Milliseconds | `000` |
| `Z` | Timezone offset | `+0000`, `-0500` |
| `z` | Timezone name | `UTC`, `EST` |
| `X` | ISO 8601 timezone | `+00:00` |

---

## Common Custom Formats

### Unix timestamp with milliseconds (fractional)

```json
"format": "posix"  -- or "millis"
```
These are standard — no custom format needed.

### ISO 8601 without milliseconds

Druid's `auto` format handles this. Or use explicitly:
```json
"format": "yyyy-MM-dd'T'HH:mm:ssZ"
```
Pinot: `1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ssZ`

### Date-only (no time)

```json
"format": "yyyy-MM-dd"
```
Pinot: `1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd`

Note: For Pinot, the granularity and unit must match the precision of the format.
`yyyy-MM-dd` has day-level precision, so use `1:DAYS`.

### Ruby / RFC 2822

Druid supports `ruby` as a built-in format (similar to `posix`). The tool treats it
as non-standard and flags it. In Pinot, if your data carries Ruby-style timestamps
(epoch seconds as float), store as epoch seconds:

```json
"format": "1:SECONDS:EPOCH",
"granularity": "1:SECONDS"
```

---

## Pre-Converting Timestamps Upstream

For formats that Pinot does not parse natively, the simplest fix is to convert to
epoch milliseconds upstream and change the format in both the Druid spec and the
generated Pinot schema:

```python
from datetime import datetime

def parse_apache_log_time(ts: str) -> int:
    """Convert '01/Mar/2024:00:00:00 +0000' to epoch milliseconds."""
    dt = datetime.strptime(ts, "%d/%b/%Y:%H:%M:%S %z")
    return int(dt.timestamp() * 1000)
```

If you pre-convert to epoch millis, update the schema:

```json
{
  "name": "log_time",
  "dataType": "LONG",
  "format": "1:MILLISECONDS:EPOCH",
  "granularity": "1:MILLISECONDS"
}
```

This eliminates the `CUSTOM_TIMESTAMP_FORMAT` risk and removes the parsing dependency
from the Pinot ingestion path.

---

## Testing the Format

After deploying the schema, verify timestamp parsing with a test query:

```sql
-- Should return a recognisable timestamp, not 0 or null:
SELECT log_time,
       DATETIMECONVERT(log_time,
                       '1:MILLISECONDS:SIMPLE_DATE_FORMAT:dd/MMM/yyyy:HH:mm:ss Z',
                       '1:MILLISECONDS:EPOCH',
                       '1:MINUTES') AS epoch_ms
FROM access_logs_OFFLINE
LIMIT 5
```

If `epoch_ms` returns `0` or negative values, the format string is incorrect.

---

## See Also

- [Tutorial 02 — Raw Event Table](02-raw-event-table.md) — standard `iso` and `millis` formats
- [Tutorial 16 — Risks and Confidence Scores](16-risks-and-confidence.md) — CUSTOM_TIMESTAMP_FORMAT
- [Reference: Type Mapping](reference/type-mapping.md) — timestamp format table
