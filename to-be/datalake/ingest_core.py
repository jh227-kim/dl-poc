"""Spark Structured Streaming ingest 공통 코어 (adapter 주입)."""

from __future__ import annotations

import json
from typing import Callable

from datalake.adapters.base import SupplyAdapter

_ready_kafka_producers: dict[tuple[str, str], object] = {}


def _get_ready_kafka_producer(bootstrap_servers: str, ready_topic: str):
    key = (bootstrap_servers, ready_topic)
    producer = _ready_kafka_producers.get(key)
    if producer is None:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=20,
        )
        _ready_kafka_producers[key] = producer
    return producer


def _publish_ready(
    adapter: SupplyAdapter,
    kafka_bootstrap: str,
    entity_id: str,
    status: str = "ready",
) -> None:
    producer = _get_ready_kafka_producer(kafka_bootstrap, adapter.ready_topic)
    producer.send(adapter.ready_topic, adapter.ready_payload(entity_id, status))
    producer.flush()
    print(
        f"[ingest] -> {adapter.ready_topic} 적재 완료 알림 발행 완료 | "
        f"{adapter.entity_id_field}={entity_id}"
    )


def process_batch(
    adapter: SupplyAdapter,
    kafka_bootstrap: str,
    df,
    epoch_id: int,
) -> None:
    from pyspark.sql import functions as F

    spark = df.sparkSession
    if df.isEmpty():
        return

    raw_events = df.select(F.col("value").cast("string").alias("json")).collect()
    parsed_events: list[dict] = []
    quarantine_rows: list[dict] = []

    for sequence, row in enumerate(raw_events):
        event, quarantine = adapter.parse_event_json(row.json, sequence)
        if quarantine is not None:
            quarantine_rows.append(quarantine)
            continue
        if event is not None:
            parsed_events.append(event)

    events = adapter.dedupe_events(parsed_events)
    if not events and quarantine_rows:
        adapter.save_quarantine(spark, quarantine_rows)
        return
    if not events:
        return

    delete_ids: list[str] = []
    candidate_rows: list[dict] = []
    notifications: list[tuple[str, str]] = []

    for event in events:
        entity_id = event[adapter.entity_id_field]
        if adapter.is_delete(event):
            delete_ids.append(entity_id)
            notifications.append((entity_id, "deleted"))
            continue

        body, fetch_quarantine = adapter.fetch_from_supply(event)
        if fetch_quarantine is not None:
            quarantine_rows.append(fetch_quarantine)
            continue

        valid, reason = adapter.validate_raw(body)
        if not valid:
            mail = (body or {}).get("mail") or {}
            quarantine_rows.append(
                adapter.build_quarantine_row(
                    entity_id,
                    "UPSERT",
                    event["payload"],
                    mail,
                    reason or "validation failed",
                )
            )
            continue

        standardized = adapter.to_canonical(body, event)
        encrypted = adapter.encrypt_canonical(standardized)
        candidate_rows.append(encrypted)
        notifications.append((entity_id, "ready"))

    accepted_rows, stale_rows = adapter.drop_stale(spark, candidate_rows)
    quarantine_rows.extend(stale_rows)

    if delete_ids:
        adapter.delete_entities(spark, sorted(set(delete_ids)))

    if accepted_rows:
        adapter.save_accepted(spark, accepted_rows)

    if quarantine_rows:
        adapter.save_quarantine(spark, quarantine_rows)

    for entity_id, status in notifications:
        _publish_ready(adapter, kafka_bootstrap, entity_id, status=status)

    print(
        f"[ingest] epoch={epoch_id} "
        f"events={len(events)} accepted={len(accepted_rows)} "
        f"deleted={len(set(delete_ids))} quarantined={len(quarantine_rows)}"
    )


def make_process_batch(adapter: SupplyAdapter, kafka_bootstrap: str) -> Callable:
    """Spark foreachBatch에 넘길 (df, epoch_id) 콜러블."""

    def _batch(df, epoch_id: int) -> None:
        process_batch(adapter, kafka_bootstrap, df, epoch_id)

    return _batch
