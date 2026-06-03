"""Spark Structured Streaming ingest 공통 코어 (adapter 주입)."""

from __future__ import annotations

import json
from typing import Callable
import time
import os

from datalake.adapters.base import SupplyAdapter

_ready_kafka_producers: dict[tuple[str, str], object] = {}


class DynamicRateLimiter:
    """
    SUPPLY_SERVICE의 상태(지연, 에러)를 감지하여 
    호출 속도를 실시간으로 동적 튜닝하는 제어기.
    """
    def __init__(self, initial_rps=None, min_rps=None, max_rps=None):
        if initial_rps is not None:
            self.rps = int(initial_rps)
        else:
            self.rps = int(os.getenv("DL_LIMITER_INITIAL_RPS", "50"))

        if min_rps is not None:
            self.min_rps = int(min_rps)
        else:
            self.min_rps = int(os.getenv("DL_LIMITER_MIN_RPS", "5"))

        if max_rps is not None:
            self.max_rps = int(max_rps)
        else:
            self.max_rps = int(os.getenv("DL_LIMITER_MAX_RPS", "200"))

        self.last_update = time.time()

    def throttle(self):
        # 현재 설정된 RPS에 맞춰 의도적인 대기 시간(Sleep)을 발생.
        sleep_time = 1.0 / max(self.rps, self.min_rps)
        time.sleep(sleep_time)

    def report_success(self):
        # 정상 작동 시 1초마다 RPS를 조금씩 올림.
        if time.time() - self.last_update > 1.0:
            self.rps = min(self.rps + 2, self.max_rps)
            self.last_update = time.time()

    def report_failure_or_latency(self):
        # 타임아웃이나 과부하 에러 발생 시 속도를 반(50%)으로 줄임.
        self.rps = max(int(self.rps * 0.5), self.min_rps)
        self.last_update = time.time()


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
    ready_bootstrap = os.environ.get("KAFKA_READY_BOOTSTRAP_SERVERS", kafka_bootstrap)
    producer = _get_ready_kafka_producer(ready_bootstrap, adapter.ready_topic)
    producer.send(adapter.ready_topic, adapter.ready_payload(entity_id, status))
    producer.flush()
    print(
        f"[ingest] -> {adapter.ready_topic} ({ready_bootstrap}) 적재 완료 알림 발행 완료 | "
        f"{adapter.entity_id_field}={entity_id}"
    )


def _publish_dlq(
    adapter: SupplyAdapter,
    kafka_bootstrap: str,
    quarantine_rows: list[dict],
) -> None:
    dlq_topic = getattr(adapter, "dlq_topic", None)
    if not dlq_topic:
        return

    producer = _get_ready_kafka_producer(kafka_bootstrap, dlq_topic)
    for row in quarantine_rows:
        entity_id = row.get(adapter.entity_id_field)
        key_bytes = str(entity_id).encode("utf-8") if entity_id is not None else None

        orig_payload = row.get("payload_json")
        retry_count = 0
        if isinstance(orig_payload, dict):
            retry_count = orig_payload.get("_retry_count", 0)
        elif isinstance(orig_payload, str):
            try:
                parsed_orig = json.loads(orig_payload)
                if isinstance(parsed_orig, dict):
                    retry_count = parsed_orig.get("_retry_count", 0)
            except Exception:
                pass

        payload = {
            "timestamp": float(time.time()),
            "adapter_source": adapter.source,
            "entity_id": entity_id,
            "action": row.get("action"),
            "payload_json": orig_payload,
            "raw_mail_json": row.get("raw_mail_json"),
            "validation_error": row.get("validation_error"),
            "retry_count": retry_count,
        }
        producer.send(dlq_topic, key=key_bytes, value=payload)
    producer.flush()
    print(f"[ingest] -> {dlq_topic} DLQ 메시지 {len(quarantine_rows)}건 발행 완료 (Key 보존, retry_count={retry_count})")


def process_batch(
    adapter: SupplyAdapter,
    kafka_bootstrap: str,
    df,
    epoch_id: int,
) -> None:
    from pyspark.sql import functions as F
    
    # SUPPLY_SERVICE에 부담을 주지 않도록 호출 사이에 대기 시간 삽입을 위한 동적 제어기 초기화
    limiter = DynamicRateLimiter()

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
        _publish_dlq(adapter, kafka_bootstrap, quarantine_rows)
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

        limiter.throttle() # SUPPLY_SERVICE에 부담을 주지 않도록 호출 사이에 대기 시간 삽입

        body, fetch_quarantine = adapter.fetch_from_supply(event)
        if fetch_quarantine is not None:            
            limiter.report_failure_or_latency() # 실패 혹은 과부하 시 감속 피드백
            quarantine_rows.append(fetch_quarantine)
            continue
        else:            
            limiter.report_success() # 성공 시 가속 피드백

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

        refined, refine_error = adapter.refine_raw(body)
        if refine_error:
            mail = (body or {}).get("mail") or {}
            quarantine_rows.append(
                adapter.build_quarantine_row(
                    entity_id,
                    "UPSERT",
                    event["payload"],
                    mail,
                    refine_error,
                )
            )
            continue

        standardized = adapter.to_canonical(refined, event)
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
        _publish_dlq(adapter, kafka_bootstrap, quarantine_rows)

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
