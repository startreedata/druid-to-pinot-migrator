# Tutorial 15 — Cloud Storage Sources (GCS, Azure, HTTP)

**Pattern:** `inputSource.type` of `google`, `azure`, or `http`  
**Risk:** Advisory warnings only (no risk IDs raised)  
**Typical use cases:** cloud-native data lakes, multi-cloud architectures

---

## Overview

Druid supports multiple cloud storage providers as input sources. Pinot also supports
cloud storage but uses its own `PinotFS` plugin architecture. The tool detects
non-S3 cloud sources and emits advisory warnings to remind you to configure Pinot
appropriately.

| Druid `inputSource.type` | Tool warning | Pinot equivalent |
|--------------------------|-------------|-----------------|
| `s3` | None (handled natively) | `S3PinotFS` (default) |
| `google` | GCS advisory | `GcsPinotFS` plugin |
| `azure` | Azure advisory | `AzurePinotFS` plugin |
| `http` | HTTP advisory | `HttpPinotFS` plugin |
| `local` | None | Local file (dev/test only) |

---

## Google Cloud Storage (GCS)

### Sample Druid Spec

```json
{
  "type": "index_parallel",
  "spec": {
    "dataSchema": {
      "dataSource": "app_events",
      "timestampSpec": {"column": "timestamp", "format": "iso"},
      "dimensionsSpec": {
        "dimensions": [
          "app_id", "event_name", "user_id",
          "os_version", "app_version", "country_code"
        ]
      },
      "metricsSpec": [
        {"type": "count",     "name": "event_count"},
        {"type": "doubleSum", "name": "revenue",      "fieldName": "revenue_usd"},
        {"type": "longSum",   "name": "session_time", "fieldName": "session_ms"}
      ],
      "granularitySpec": {
        "segmentGranularity": "DAY",
        "queryGranularity": "NONE",
        "rollup": false,
        "intervals": ["2024-01-01/2025-01-01"]
      }
    },
    "ioConfig": {
      "type": "index_parallel",
      "inputSource": {
        "type": "google",
        "uris": ["gs://analytics-bucket/app_events/dt=2024-*/events_*.json.gz"]
      },
      "inputFormat": {"type": "json"}
    }
  }
}
```

### Running the Migration

```bash
dpm generate app_events_spec.json --out ./output/app_events
```

```
Warnings:
  - GCS (google) inputSource detected; Pinot batch job config requires GCS URI
    and appropriate auth
```

No risks are raised — this is a warning, not a risk. The migration proceeds and all
artifacts are generated.

### Configuring Pinot for GCS

Pinot reads from GCS using the `GcsPinotFS` plugin. Configure it in the batch job spec:

```json
{
  "jobType": "SegmentCreationAndTarPush",
  "inputDirURI": "gs://analytics-bucket/app_events/dt=2024-01-01/",
  "outputDirURI": "gs://pinot-segments-bucket/app_events/",
  "overwriteOutput": true,
  "pinotFSSpecs": [
    {
      "scheme": "gs",
      "className": "org.apache.pinot.plugin.filesystem.GcsPinotFS",
      "configs": {
        "projectId": "my-gcp-project",
        "gcpKey": "/path/to/service-account-key.json"
      }
    }
  ],
  "recordReaderSpec": {
    "dataFormat": "json",
    "className": "org.apache.pinot.plugin.inputformat.json.JSONRecordReader"
  },
  "tableSpec": {
    "tableName": "app_events",
    "schemaURI": "http://controller:9000/schemas/app_events",
    "tableConfigURI": "http://controller:9000/tables/app_events"
  }
}
```

### GCS Authentication Options

**Service account JSON key:**
```json
"configs": {
  "projectId": "my-gcp-project",
  "gcpKey": "/path/to/service-account-key.json"
}
```

**Application Default Credentials (running on GCE/GKE):**
```json
"configs": {
  "projectId": "my-gcp-project"
}
```
Leave out `gcpKey` and Pinot will use the instance's service account automatically.

**Workload Identity (GKE):**
Annotate the Kubernetes service account with `iam.gke.io/gcp-service-account`
and leave `gcpKey` absent.

---

## Azure Blob Storage

### Sample Druid Spec

```json
"inputSource": {
  "type": "azure",
  "uris": ["azure://mycontainer/events/dt=2024-*/data.json"]
}
```

### Configuring Pinot for Azure

```json
{
  "inputDirURI": "adl://mycontainer/events/dt=2024-01-01/",
  "outputDirURI": "adl://pinot-output/events/",
  "pinotFSSpecs": [
    {
      "scheme": "adl",
      "className": "org.apache.pinot.plugin.filesystem.AzurePinotFS",
      "configs": {
        "accountName": "mystorageaccount",
        "accessKey": "${AZURE_STORAGE_ACCESS_KEY}"
      }
    }
  ]
}
```

### Azure Authentication Options

**Access key:**
```json
"configs": {
  "accountName": "mystorageaccount",
  "accessKey": "base64-encoded-key=="
}
```

**SAS token:**
```json
"configs": {
  "accountName": "mystorageaccount",
  "sasToken": "?sv=2020-08-04&ss=b&srt=sco&sp=..."
}
```

**Azure AD (service principal):**
```json
"configs": {
  "accountName": "mystorageaccount",
  "tenantId": "tenant-id",
  "clientId": "app-id",
  "clientSecret": "${AZURE_CLIENT_SECRET}"
}
```

---

## HTTP Input Source

### Sample Druid Spec

```json
"inputSource": {
  "type": "http",
  "uris": [
    "https://data-api.example.com/exports/events_2024-01-01.json.gz"
  ]
}
```

### Running the Migration

```
Warnings:
  - HTTP inputSource detected; Pinot batch job supports HTTP input via the
    HTTP pinotFS plugin
```

### Configuring Pinot for HTTP

Pinot can read from HTTP URLs using its built-in HTTP support:

```json
{
  "inputDirURI": "http://data-api.example.com/exports/",
  "pinotFSSpecs": [
    {
      "scheme": "http",
      "className": "org.apache.pinot.plugin.filesystem.HttpPinotFS"
    }
  ]
}
```

For authenticated HTTP endpoints, pass auth headers through environment variables
or Pinot's credentials configuration. Alternatively, download files locally before
batch ingest.

---

## Compressed Files

Druid automatically decompresses gzip (`.gz`), bz2, and other common formats.
Pinot's JSON record reader also handles `.gz` automatically when reading from
file URIs ending in `.gz`.

If your source data uses `.json.gz`:

```json
"recordReaderSpec": {
  "dataFormat": "json",
  "className": "org.apache.pinot.plugin.inputformat.json.JSONRecordReader"
}
```

No additional configuration is needed — the reader detects compression from the file extension.

---

## Glob Patterns

Druid's `inputSource.uris` supports glob patterns for GCS and Azure:
```
gs://bucket/path/dt=2024-*/data_*.json
```

Pinot uses `inputDirURI` + `includeFileNamePattern` for similar functionality:

```json
{
  "inputDirURI": "gs://analytics-bucket/app_events/",
  "includeFileNamePattern": "glob:**/dt=2024-*/**/*.json",
  "excludeFileNamePattern": "glob:**/_SUCCESS"
}
```

Or specify individual URIs in `inputFileURIPrefix`:

```json
{
  "inputFileURIPrefix": "gs://analytics-bucket/app_events/dt=2024-01-01/"
}
```

---

## See Also

- [Tutorial 02 — Raw Event Table](02-raw-event-table.md) — S3 input (default)
- [Tutorial 18 — Production Checklist](18-production-checklist.md) — cloud auth verification
- [Reference: Artifacts](reference/artifacts.md) — full batch-job.json structure
