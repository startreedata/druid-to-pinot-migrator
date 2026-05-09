from __future__ import annotations

import json

from migrator.core.enums import SourceKind
from migrator.core.models import CanonicalMigrationModel


# ─────────────────────────────────────────────────────────────────────────────
# Default-tunable Pinot keys; lifted to module constants so callers can
# inspect or override them and tests can assert against them stably.
# ─────────────────────────────────────────────────────────────────────────────

KAFKA_CONSUMER_FACTORY = "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory"
"""
The ``kafka20`` package (Kafka 2.0 client plugin) is what we emit by default.

Compatibility rationale:
  - Pinot 1.0 – 1.4 ship the ``pinot-kafka-2.0`` plugin; the FQCN works directly.
  - Pinot 1.5 removed the ``pinot-kafka-2.0`` plugin in favour of
    ``pinot-kafka-3.0``, but adds a backward-compatibility alias in
    ``PluginManager`` that transparently maps ``kafka20.KafkaConsumerFactory``
    → ``kafka30.KafkaConsumerFactory`` at deserialise time.

Therefore emitting ``kafka20`` works on every Pinot 1.x release we support,
while ``kafka30`` would break Pinot ≤ 1.3 (the plugin doesn't exist there).
Operators who target Pinot 1.4+ exclusively can rewrite the FQCN by hand
or via a sed / jq post-processing step; the tool defaults to maximum
compatibility.
"""
KAFKA_JSON_DECODER = "org.apache.pinot.plugin.inputformat.json.JSONMessageDecoder"

# Avro Kafka decoders. Two flavours, matching how Druid's
# ``avro_stream`` inputFormat is typically wired:
#
#   - ``KafkaConfluentSchemaRegistryAvroMessageDecoder`` for the common
#     case where producer + consumer share a Confluent-style schema
#     registry. Druid configures this via
#     ``avroBytesDecoder.type=schema_registry``.
#   - ``SimpleAvroMessageDecoder`` when the writer schema is supplied
#     inline (Druid's ``avroBytesDecoder.type=schema_inline``). Pinot
#     wants the schema as ``stream.kafka.decoder.prop.schema``.
KAFKA_AVRO_REGISTRY_DECODER = (
    "org.apache.pinot.plugin.inputformat.avro.confluent."
    "KafkaConfluentSchemaRegistryAvroMessageDecoder"
)
KAFKA_AVRO_SIMPLE_DECODER = (
    "org.apache.pinot.plugin.inputformat.avro.SimpleAvroMessageDecoder"
)

# Protobuf Kafka decoders. Two flavours mirroring Avro:
#   - ``KafkaConfluentSchemaRegistryProtoBufMessageDecoder`` for the
#     common Confluent-registry case. Druid wires it via
#     ``protoBytesDecoder.type=schema_registry``.
#   - ``ProtoBufMessageDecoder`` for the descriptor-file path. Druid's
#     ``protoBytesDecoder.type=file`` carries the ``.desc`` file path
#     and ``protoMessageType``; Pinot wants ``descriptorFile`` (HTTP
#     URL or local path) and ``protoClassName``.
KAFKA_PROTOBUF_REGISTRY_DECODER = (
    "org.apache.pinot.plugin.inputformat.protobuf.confluent."
    "KafkaConfluentSchemaRegistryProtoBufMessageDecoder"
)
KAFKA_PROTOBUF_FILE_DECODER = (
    "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufMessageDecoder"
)
DEFAULT_OFFSET_RESET = "largest"
"""Used when no migration watermark is supplied. Matches the historical
Pinot default for new REALTIME tables."""


def build_realtime_transform_configs(
    canonical: CanonicalMigrationModel,
) -> list[dict]:
    """
    Build the Pinot ``transformConfigs`` for a REALTIME table from the
    canonical Druid metricsSpec.

    Druid's ``metricsSpec`` declares pre-aggregated metric columns, e.g.

        {"type": "count",   "name": "events"}
        {"type": "longSum", "name": "session_ms_sum", "fieldName": "session_ms"}

    Druid applies these aggregations at ingest time: a row with
    ``session_ms=12345`` becomes a row with ``session_ms_sum=12345`` and
    ``events=1``. dpm's generated Pinot schema follows Druid's lead and
    declares the **stored** column names (``events``, ``session_ms_sum``).

    But the raw Kafka stream still contains the *source* fields
    (``session_ms``), not the rolled-up names. Without a Pinot ingestion
    transform mapping raw → rolled, the REALTIME table receives 0 for
    every metric (the rolled column has no matching JSON field, and the
    source field has no matching schema column, so it's silently dropped).

    This function emits exactly the right transformConfigs:

    - ``count`` → ``{columnName: name, transformFunction: "1"}``
    - any other metric where ``field_name`` differs from ``name`` →
      ``{columnName: name, transformFunction: field_name}`` (alias copy)
    - metrics where ``field_name`` is empty (or equal to ``name``) get no
      transform — Druid's pass-through semantics.

    Returns an empty list if no transforms are needed (e.g. no rollup,
    or every metric's stored name matches its source field).
    """
    transforms: list[dict] = []
    for m in canonical.metrics:
        # ``count`` is special: there is no source field, every input row
        # contributes 1 to the metric.
        if m.druid_type.lower() == "count":
            transforms.append({
                "columnName": m.name,
                "transformFunction": "1",
            })
            continue
        # All other aggregations carry a source field name. Emit an alias
        # only when the names differ (a Druid spec is allowed to use
        # name == fieldName for pure rollup with no rename).
        if m.field_name and m.field_name != m.name:
            transforms.append({
                "columnName": m.name,
                "transformFunction": m.field_name,
            })
    return transforms


KINESIS_CONSUMER_FACTORY = (
    "org.apache.pinot.plugin.stream.kinesis.KinesisConsumerFactory"
)
KINESIS_JSON_DECODER = KAFKA_JSON_DECODER  # JSON decoder is stream-agnostic


def _extract_kinesis_region(endpoint: str | None) -> str | None:
    """Pull a region out of a Druid Kinesis endpoint URL, if it follows the
    canonical AWS form ``kinesis.<region>.amazonaws.com``.

    Returns None for non-AWS endpoints (e.g. localhost, kinesis-lite, custom
    proxies); callers must fall back to an explicit region in that case.
    """
    if not endpoint:
        return None
    # Strip protocol if present (Druid often stores hostname-only).
    host = endpoint.split("://", 1)[-1]
    parts = host.split(".")
    # ``kinesis.us-east-1.amazonaws.com`` → parts[1] == "us-east-1"
    if len(parts) >= 4 and parts[0] == "kinesis" and parts[-2:] == ["amazonaws", "com"]:
        return parts[1]
    return None


def build_kinesis_stream_configs(
    *,
    stream_name: str,
    region: str,
    endpoint: str | None = None,
    offset_criteria: str = DEFAULT_OFFSET_RESET,
    decoder_class: str = KINESIS_JSON_DECODER,
    flush_threshold_rows: str = "1000000",
    flush_threshold_time: str = "1h",
) -> dict[str, str]:
    """
    Build a Pinot ``streamConfigs`` dict for a Kinesis REALTIME table.

    Druid's Kinesis indexing service maps to Pinot's
    ``KinesisConsumerFactory`` plugin (shipped with all Pinot 1.x
    releases the migrator targets). The decoder is JSON-only here
    because every Druid Kinesis spec dpm has seen uses
    ``ioConfig.inputFormat.type == "json"``; protobuf / avro require
    additional schema config that's out of scope for the auto-generator.

    AWS credentials are deliberately omitted — production Pinot
    deployments source them from IAM instance profiles or env vars,
    not from the table config (which would commit a secret to source
    control).
    """
    cfg: dict[str, str] = {
        "streamType": "kinesis",
        "stream.kinesis.topic.name": stream_name,
        "stream.kinesis.consumer.type": "lowlevel",
        "stream.kinesis.consumer.factory.class.name": KINESIS_CONSUMER_FACTORY,
        "stream.kinesis.decoder.class.name": decoder_class,
        "stream.kinesis.consumer.prop.auto.offset.reset": offset_criteria,
        "region": region,
        "realtime.segment.flush.threshold.rows": flush_threshold_rows,
        "realtime.segment.flush.threshold.time": flush_threshold_time,
    }
    if endpoint:
        cfg["stream.kinesis.endpoint"] = endpoint
    return cfg


def build_kafka_stream_configs(
    *,
    topic: str,
    broker_list: str,
    offset_criteria: str = DEFAULT_OFFSET_RESET,
    decoder_class: str = KAFKA_JSON_DECODER,
    decoder_props: dict[str, str] | None = None,
    flush_threshold_rows: str = "1000000",
    flush_threshold_time: str = "1h",
) -> dict[str, str]:
    """
    Build a Pinot ``streamConfigs`` dict for a Kafka REALTIME table.

    ``offset_criteria`` accepts any value Pinot ``OffsetCriteria`` understands:

    - ``"smallest"`` / ``"largest"`` — start at topic head/tail.
    - An ISO-8601 timestamp like ``"2024-03-01T00:00:00.000Z"`` — Pinot's
      TIMESTAMP offset criterion. **This is the migration-watermark mode**:
      Pinot starts consumption at this timestamp, so it picks up exactly
      where Druid's REALTIME ingestion stopped.
    - A relative period like ``"7d"`` or ``"4h30m"`` — Pinot's PERIOD
      offset criterion (relative to broker request time).

    ``decoder_props`` (when given) are written as
    ``stream.kafka.decoder.prop.<key>`` entries. Used by the Avro
    schema-registry decoder for ``schema.registry.rest.url`` and by
    the simple-Avro decoder for the inline schema string.

    Pulled out as a free function so the hybrid planner and the existing
    PinotTableGenerator can share one definition.
    """
    cfg: dict[str, str] = {
        "streamType": "kafka",
        "stream.kafka.topic.name": topic,
        "stream.kafka.broker.list": broker_list,
        "stream.kafka.consumer.type": "lowlevel",
        "stream.kafka.consumer.factory.class.name": KAFKA_CONSUMER_FACTORY,
        "stream.kafka.decoder.class.name": decoder_class,
        "stream.kafka.consumer.prop.auto.offset.reset": offset_criteria,
        "realtime.segment.flush.threshold.rows": flush_threshold_rows,
        "realtime.segment.flush.threshold.time": flush_threshold_time,
    }
    for k, v in (decoder_props or {}).items():
        cfg[f"stream.kafka.decoder.prop.{k}"] = v
    return cfg


def _schema_registry_props(decoder_block: dict) -> dict[str, str]:
    """Translate a Druid ``*BytesDecoder`` schema-registry block into
    Pinot ``stream.kafka.decoder.prop.*`` keys.

    Both Avro and Protobuf Confluent decoders share this surface in
    Pinot — the decoder *class* differs, but the prop keys for URL +
    auth + headers are identical. Pulled into a helper so a fix in
    one path automatically propagates.

    Properties handled:

      - ``url`` / ``urls`` → ``schema.registry.rest.url`` (Pinot
        accepts a comma-joined list for HA registries).
      - ``config.basic.auth.credentials.source`` /
        ``config.basicAuthCredentialsSource`` (Druid casing varies)
        → ``basic.auth.credentials.source``.
      - ``config.basic.auth.user.info`` / ``config.basicAuthUserInfo``
        → ``basic.auth.user.info``. Loaded into the registry HTTP
        client; the underlying Kafka client itself is unaffected.
      - ``capacity`` → ``schema.registry.cache.capacity`` when set
        (Druid's local cache size; Pinot has the same knob).

    Anything not listed here is dropped — Pinot's decoder will use
    the SDK defaults, which is what the operator wanted anyway by
    not setting the corresponding Druid field.
    """
    props: dict[str, str] = {}

    # URL (single) wins; ``urls`` array gets comma-joined for the HA
    # case — Pinot's Confluent client honours the comma list.
    url = decoder_block.get("url") or ""
    urls = decoder_block.get("urls") or []
    if url:
        props["schema.registry.rest.url"] = url
    elif urls:
        props["schema.registry.rest.url"] = ",".join(urls)

    # Auth lives under a nested ``config`` block; both camel-case
    # (``basicAuthUserInfo``) and dotted-key (``basic.auth.user.info``)
    # spellings appear in real specs depending on Druid version.
    config = decoder_block.get("config") or {}
    auth_source = (
        config.get("basic.auth.credentials.source")
        or config.get("basicAuthCredentialsSource")
    )
    if auth_source:
        props["basic.auth.credentials.source"] = str(auth_source)
    user_info = (
        config.get("basic.auth.user.info")
        or config.get("basicAuthUserInfo")
    )
    if user_info:
        props["basic.auth.user.info"] = str(user_info)

    capacity = decoder_block.get("capacity")
    if capacity:
        props["schema.registry.cache.capacity"] = str(capacity)

    return props


def avro_decoder_config_from_io(io: dict) -> tuple[str, dict[str, str]]:
    """Pick the right Pinot Avro decoder for a Druid ``ioConfig`` block.

    Druid wires Avro on Kafka via ``inputFormat.type == "avro_stream"``
    plus an ``avroBytesDecoder`` sub-object that says how to find the
    writer schema:

      - ``schema_registry``: pulls URL + (optional) basic-auth
        credentials + capacity out via ``_schema_registry_props`` and
        points Pinot at the Confluent decoder.
      - ``schema_inline``: Druid embeds the schema JSON inline; we map
        to Pinot's ``SimpleAvroMessageDecoder`` and pass the schema
        string through as the decoder prop ``schema``. The operator
        is responsible for verifying the schema renders correctly
        (Druid sometimes accepts variants Pinot's decoder rejects).
      - Anything else / missing: fall back to schema-registry decoder
        with no URL — the normalizer surfaces a warning so the operator
        knows to fill it in post-generation.

    Returned tuple is (decoder_class, decoder_props) in the shape
    ``build_kafka_stream_configs`` expects.
    """
    avro_decoder = (io.get("inputFormat") or {}).get("avroBytesDecoder", {})
    decoder_type = (avro_decoder.get("type") or "").lower()
    if decoder_type == "schema_registry":
        return KAFKA_AVRO_REGISTRY_DECODER, _schema_registry_props(avro_decoder)
    if decoder_type == "schema_inline":
        schema = avro_decoder.get("schema", "")
        # Pinot expects the schema as a JSON string. If Druid stored it
        # as a dict, serialise; otherwise pass through.
        if isinstance(schema, dict):
            schema = json.dumps(schema)
        return KAFKA_AVRO_SIMPLE_DECODER, {"schema": schema} if schema else {}
    # Default: registry decoder with no URL — the simpler ``avro``
    # alias used by some operator-written specs lands here too.
    return KAFKA_AVRO_REGISTRY_DECODER, {}


def protobuf_decoder_config_from_io(io: dict) -> tuple[str, dict[str, str]]:
    """Pick the right Pinot Protobuf decoder for a Druid ``ioConfig``.

    Druid's ``protoBytesDecoder`` mirrors ``avroBytesDecoder`` in
    shape but the field names + the Pinot decoder class differ:

      - ``schema_registry``: uses the same URL/auth/capacity props as
        Avro (registry-side wire format is identical).
        ``schemaName`` (the Protobuf message type) is required by
        Pinot's Confluent decoder; pull it from Druid's
        ``protoMessageType``.
      - ``file``: descriptor-file mode. Druid stores the ``.desc``
        path on ``descriptor`` and the message type on
        ``protoMessageType``; Pinot wants ``descriptorFile`` (path
        or URL) and ``protoClassName``.

    Anything else falls back to the registry decoder, with the
    normalizer surfacing the missing-config warning.
    """
    proto_decoder = (io.get("inputFormat") or {}).get("protoBytesDecoder", {})
    decoder_type = (proto_decoder.get("type") or "").lower()
    if decoder_type == "schema_registry":
        props = _schema_registry_props(proto_decoder)
        # The protobuf message type is mandatory for the Confluent
        # decoder; Druid stores it on ``protoMessageType``.
        message_type = proto_decoder.get("protoMessageType") or ""
        if message_type:
            props["schemaName"] = message_type
        return KAFKA_PROTOBUF_REGISTRY_DECODER, props
    if decoder_type == "file":
        descriptor = proto_decoder.get("descriptor", "")
        message_type = proto_decoder.get("protoMessageType", "")
        props: dict[str, str] = {}
        if descriptor:
            props["descriptorFile"] = descriptor
        if message_type:
            props["protoClassName"] = message_type
        return KAFKA_PROTOBUF_FILE_DECODER, props
    return KAFKA_PROTOBUF_REGISTRY_DECODER, {}


class PinotTableGenerator:
    """Generate Pinot offline and realtime table config dicts."""

    def generate_offline(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate an OFFLINE table configuration."""
        time_column = canonical.time_field.column_name if canonical.time_field else "__time"
        table_name = f"{canonical.datasource_name}_OFFLINE"

        ingestion: dict = {
            "batchIngestionConfig": {
                "segmentIngestionType": "APPEND",
                "segmentIngestionFrequency": "DAILY",
            }
        }
        # Same per-row metric-column-rename trick the REALTIME path
        # uses: when a Druid metricsSpec maps ``SUM(amount) AS
        # amount_sum``, the canonical schema declares ``amount_sum``
        # but the source rows still carry ``amount``. Without a
        # transform, the Pinot column ends up 0/null. Pinot's batch
        # ingestion CAN'T re-execute the GROUP BY (the analyzer
        # warns about this separately via BATCH_AGGREGATION_NOT_REPLAYED),
        # but emitting the rename means each row's source value
        # lands in the right column — so SUM(amount_sum) at query
        # time still produces the correct total, just over more rows.
        transforms = build_realtime_transform_configs(canonical)
        if transforms:
            ingestion["transformConfigs"] = transforms

        return {
            "tableName": table_name,
            "tableType": "OFFLINE",
            "segmentsConfig": {
                "timeColumnName": time_column,
                "timeType": "MILLISECONDS",
                "replication": "1",
                "segmentAssignmentStrategy": "BalanceNumSegmentAssignmentStrategy",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {
                "broker": "DefaultTenant",
                "server": "DefaultTenant",
            },
            "tableIndexConfig": {
                "loadMode": "MMAP",
            },
            "ingestionConfig": ingestion,
            "metadata": {
                "customConfigs": {}
            },
        }

    def generate_realtime(
        self,
        canonical: CanonicalMigrationModel,
        *,
        watermark_iso: str | None = None,
    ) -> dict:
        """
        Generate a REALTIME table configuration.

        If ``watermark_iso`` is provided, the generated stream config uses it
        as the ``auto.offset.reset`` value (Pinot's TIMESTAMP offset criterion),
        so Pinot starts consumption from that point. Use this for hybrid
        Druid → Pinot migrations where Druid has already ingested everything
        before the watermark.
        """
        time_column = canonical.time_field.column_name if canonical.time_field else "__time"
        table_name = f"{canonical.datasource_name}_REALTIME"

        io = canonical.raw_io_config or {}
        offset_criteria = watermark_iso or DEFAULT_OFFSET_RESET

        # Dispatch on ioConfig.type. Kinesis specs declare ``stream``
        # instead of ``topic``; Kafka specs declare ``topic`` and live
        # consumer properties under ``consumerProperties``. Defaulting
        # to Kafka preserves the previous behaviour for any spec whose
        # type is missing or unrecognised.
        io_type = (io.get("type") or "").lower()
        is_kinesis = io_type == "kinesis" or (
            "stream" in io and "topic" not in io
        )
        if is_kinesis:
            stream_name = io.get("stream") or canonical.datasource_name
            endpoint = io.get("endpoint")
            region = io.get("region") or _extract_kinesis_region(endpoint)
            if not region:
                # Pinot won't bring up the table without a region. We
                # emit a placeholder and surface this in the canonical
                # warnings so the operator notices before deploying.
                region = "us-east-1"
            # Druid's ``useEarliestSequenceNumber`` flag is the closest
            # analogue of Pinot's offset criterion: True means "start
            # from the oldest available record" (smallest), False means
            # "start from the latest" (largest). Watermark mode wins
            # when supplied.
            if watermark_iso is None:
                offset_criteria = (
                    "smallest" if io.get("useEarliestSequenceNumber") else "largest"
                )
            stream_configs = build_kinesis_stream_configs(
                stream_name=stream_name,
                region=region,
                endpoint=endpoint,
                offset_criteria=offset_criteria,
            )
        else:
            consumer_props = io.get("consumerProperties", {})
            broker_list = consumer_props.get("bootstrap.servers", "localhost:9092")
            topic = io.get("topic", canonical.datasource_name)
            # Pick the decoder from the canonical input_format. Default
            # is JSON (the v0.10.0 behaviour); ``avro`` swaps in the
            # Confluent or simple Avro decoder, depending on the Druid
            # spec's avroBytesDecoder.type.
            decoder_class = KAFKA_JSON_DECODER
            decoder_props: dict[str, str] = {}
            if canonical.input_format == "avro":
                decoder_class, decoder_props = avro_decoder_config_from_io(io)
            elif canonical.input_format == "protobuf":
                decoder_class, decoder_props = protobuf_decoder_config_from_io(io)
            stream_configs = build_kafka_stream_configs(
                topic=topic,
                broker_list=broker_list,
                offset_criteria=offset_criteria,
                decoder_class=decoder_class,
                decoder_props=decoder_props,
            )

        table: dict = {
            "tableName": table_name,
            "tableType": "REALTIME",
            "segmentsConfig": {
                "timeColumnName": time_column,
                "timeType": "MILLISECONDS",
                "replication": "1",
                "retentionTimeUnit": "DAYS",
                "retentionTimeValue": "365",
            },
            "tenants": {
                "broker": "DefaultTenant",
                "server": "DefaultTenant",
                "tagOverrideConfig": {},
            },
            "tableIndexConfig": {
                "loadMode": "MMAP",
                "streamConfigs": stream_configs,
            },
            "metadata": {
                "customConfigs": {}
            },
        }

        # transformConfigs map raw Kafka field names → rolled-up Druid metric
        # column names so the REALTIME table actually receives metric values.
        # Without this the realtime half ingests dimensions only and every
        # SUM(events)/SUM(session_ms_sum) returns 0 until enough events
        # accumulate to make the divergence obvious. Empty list = no rollup
        # (or no rename) → no ingestionConfig key emitted.
        transforms = build_realtime_transform_configs(canonical)
        if transforms:
            table["ingestionConfig"] = {"transformConfigs": transforms}

        # Upsert: emit ``upsertConfig`` + the strict-replica-group
        # routing knob Pinot requires for upsert tables. The schema
        # generator already wrote ``primaryKeyColumns`` at the schema
        # level. Validation (PK exists, source_kind=stream) happens at
        # the canonical-build / CLI boundary; by this point the
        # canonical is trusted.
        if canonical.upsert.enabled and canonical.upsert.primary_key:
            upsert_block: dict = {
                "mode": canonical.upsert.mode,
            }
            # Default the comparison column to the time field — the
            # most common operator intent ("latest event wins for
            # this PK").
            comparison = (
                canonical.upsert.comparison_column
                or (canonical.time_field.column_name if canonical.time_field else None)
            )
            if comparison:
                upsert_block["comparisonColumns"] = [comparison]
            if canonical.upsert.mode.upper() == "PARTIAL" and canonical.upsert.partial_columns:
                upsert_block["partialUpsertStrategies"] = dict(
                    canonical.upsert.partial_columns
                )
            table["upsertConfig"] = upsert_block
            # Mandatory for upsert: routing must be strict-replica-group
            # so all replicas of a primary key route to the same server,
            # otherwise dedup is broken.
            table["routing"] = {"instanceSelectorType": "strictReplicaGroup"}

        return table

    def generate(self, canonical: CanonicalMigrationModel) -> dict:
        """Generate the appropriate table config based on source kind."""
        if canonical.source_kind == SourceKind.STREAM.value:
            return self.generate_realtime(canonical)
        return self.generate_offline(canonical)
