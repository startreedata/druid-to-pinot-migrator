# Tutorial 15 — Cloud Storage Sources (GCS, Azure, HTTP)

**Pattern:** `inputSource.type` of `s3`, `google`, `azure`, or `http`  
**Risk:** Advisory warnings only (no risk IDs raised)  
**Typical use cases:** cloud-native data lakes, multi-cloud architectures

---

## Overview

Druid supports multiple cloud storage providers as input sources. Pinot also supports
cloud storage but uses its own `PinotFS` plugin architecture. For S3, GCS, and Azure
the tool now **generates the `pinotFSSpecs` block** in `batch-job.json` — the right
plugin class, the right scheme, and a `configs` block with the structural keys Pinot
needs. Values that live in the Druid spec are carried over; the rest are emitted as
loud `REPLACE_WITH_*` placeholders, and a warning names exactly what to fill.

**Credentials are deliberately never written into the job spec.** Access keys, GCS
service-account keys, and Azure SAS tokens come from the Pinot server's ambient
environment (IAM instance role, GKE workload identity, env vars) — committing them
to `batch-job.json` would leak a secret into source control.

| Druid `inputSource.type` | Generated `pinotFSSpecs` | Pinot plugin (scheme) |
|--------------------------|--------------------------|-----------------------|
| `s3` | `configs.region` (derived or placeholder) | `S3PinotFS` (`s3`) |
| `google` | `configs.projectId` (placeholder) | `GcsPinotFS` (`gs`) |
| `azure` | `configs.accountName` + `fileSystemName`; URI rewritten to `adl2://` | `ADLSGen2PinotFS` (`adl2`) |
| `http` | — (no Pinot HTTP filesystem; pre-stage required) | none |
| `local` | — (no configs) | `LocalPinotFS` (`file`) |

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
  - GCS (google) inputSource detected; the generated batch job wires the
    GcsPinotFS plugin with a placeholder pinotFSSpecs.configs.projectId — set
    your GCP projectId (credentials come from the Pinot server's service
    account / workload identity, not the job spec)
```

No risks are raised — this is a warning, not a risk. The migration proceeds and the
batch job is generated with the `GcsPinotFS` plugin already wired; you only need to
replace the `projectId` placeholder.

### Configuring Pinot for GCS

The generated batch job already wires the `GcsPinotFS` plugin. Replace the
`REPLACE_WITH_GCP_PROJECT_ID` placeholder with your project:

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

Pinot reads ADLS Gen2 storage via the `ADLSGen2PinotFS` plugin, which registers the
`adl2` scheme. The tool **rewrites the Druid `azure://` URI to `adl2://`** (so a
PinotFS claims it at deploy time) and derives `fileSystemName` from the container in
the URI. The generated batch job looks like:

```json
{
  "inputDirURI": "adl2://mycontainer/events/dt=2024-01-01/",
  "outputDirURI": "adl2://pinot-output/events/",
  "pinotFSSpecs": [
    {
      "scheme": "adl2",
      "className": "org.apache.pinot.plugin.filesystem.ADLSGen2PinotFS",
      "configs": {
        "accountName": "REPLACE_WITH_AZURE_STORAGE_ACCOUNT",
        "fileSystemName": "mycontainer"
      }
    }
  ]
}
```

Replace `accountName` with your storage account. The access key is **not** written
into the spec — set it on the Pinot server via the `accessKey` config in
`controller.conf` / `server.conf`, or supply it through the environment.

### Azure Authentication Options

Set these on the **Pinot server** (not the generated job spec). The keys
`ADLSGen2PinotFS` recognises:

**Access key:**
```
pinot.controller.storage.factory.adl2.accountName=mystorageaccount
pinot.controller.storage.factory.adl2.accessKey=base64-encoded-key==
pinot.controller.storage.factory.adl2.fileSystemName=mycontainer
```

**Service principal (Azure AD):** configure `clientId` / `clientSecret` / `tenantId`
on the server-side `adl2` storage factory. Keeping these out of the generated artifact
avoids committing a credential to source control.

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
  - HTTP inputSource detected; Pinot has no HTTP PinotFS plugin — stage the
    files to object storage (S3 / GCS / ADLS) or download them locally before
    running the batch ingestion job
```

### Configuring Pinot for HTTP

**Pinot does not ship an HTTP `PinotFS` plugin.** Unlike S3 / GCS / ADLS, there is no
filesystem class that reads `http(s)://` URLs during segment creation, so the tool
does not generate a `pinotFSSpecs` entry for HTTP sources. Two options:

1. **Pre-stage to object storage** (recommended): copy the source files to an S3 /
   GCS / ADLS bucket, then point the batch job at that bucket — which the tool fully
   supports. This keeps the ingestion reproducible.
2. **Download locally first**: pull the files to a local directory and run the batch
   job against `file://` paths (dev / one-shot migrations only).

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
