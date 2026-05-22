"""
PoC: mail.events (Kafka) -> fetch mail from supply HTTP -> Iceberg local.db.mail_silver -> mail.ready (Kafka).

Prereq: cd to-be && docker compose up -d; supply on 8100; repo-root venv with pyspark, kafka-python, requests.

Run (from dl-poc repo root):
  venv\\Scripts\\python to-be\\spark_ingest_mail.py

Windows: default checkpoint is MinIO s3a://warehouse/.spark-checkpoints/mail-ingest (avoids NativeIO on local disk).
  Override: MAIL_INGEST_CHECKPOINT or SPARK_CHECKPOINT_LOCATION (e.g. file path or another s3a URI).

Stop with Ctrl+C. Uses processingTime trigger (default 5s).
"""
from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import re
import sys

from datalake.encryption import encrypt_personal_fields
from datalake.layers import (
    SILVER_TABLE,
    create_namespaces,
    create_tables,
    delete_from_silver_and_gold,
    save_to_gold,
    save_to_quarantine,
    save_to_silver,
)
from datalake.validator import validate_mail


# def _mask_personal_info(text: str) -> str:
#     """간단한 정규식을 사용한 개인정보(이메일, 전화번호) 마스킹 처리 함수.
#
#     현재 파이프라인에서는 사용하지 않으나, 추후 마스킹 로직이
#     필요할 경우를 대비하여 유지합니다.
#     """
#     if not text:
#         return ""
#     # 이메일 마스킹 (ex: abcde@company.com -> ab***@company.com)
#     email_pattern = r'([a-zA-Z0-9_.+-]{2})[a-zA-Z0-9_.+-]+@([a-zA-Z0-9-]+\.[a-zA-Z0-9-. ]+)'
#     text = re.sub(email_pattern, r'\1***@\2', text)
#
#     # 전화번호 마스킹 (ex: 010-1234-5678 -> 010-****-5678)
#     phone_pattern = r'(\d{2,3})-(\d{3,4})-(\d{4})'
#     text = re.sub(phone_pattern, r'\1-****-\3', text)
#     return text

# Worker subprocess must use this interpreter; on Windows PATH often has another `python`
# without PySpark → "Python worker failed to connect back" / Accept timed out.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

if os.name == "nt":
    hadoop_home = os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    hadoop_bin = str(Path(hadoop_home) / "bin")
    if Path(hadoop_bin).is_dir():
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

SPARK_VERSION = "3.5.1"
PACKAGES = ",".join(
    [
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.postgresql:postgresql:42.6.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "software.amazon.awssdk:bundle:2.20.18",
        "software.amazon.awssdk:url-connection-client:2.20.18",
    ]
)
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--packages {PACKAGES} pyspark-shell")

JAVA17_OPENS = (
    "-XX:+IgnoreUnrecognizedVMOptions "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "-Dio.netty.tryReflectionSetAccessible=true"
)

SUPPLY_URL = os.environ.get("SUPPLY_SERVICE_URL", "http://127.0.0.1:8100")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INGEST_TOPIC = os.environ.get("KAFKA_INGEST_TOPIC", "mail.events")
READY_TOPIC = os.environ.get("KAFKA_READY_TOPIC", "mail.ready")
TRIGGER_SEC = os.environ.get("SPARK_STREAM_TRIGGER_SEC", "5")
# Streaming checkpoint: on Windows, local disk uses Hadoop RawLocalFileSystem + NativeIO (winutils/JNI pain).
# Default to MinIO (same bucket as Iceberg warehouse) when unset; override with MAIL_INGEST_CHECKPOINT.
DEFAULT_WIN_CHECKPOINT = "s3a://warehouse/.spark-checkpoints/mail-ingest"

ready_kafka_producer = None


def _checkpoint_location() -> str:
    location = (
        os.environ.get("MAIL_INGEST_CHECKPOINT")
        or os.environ.get("SPARK_CHECKPOINT_LOCATION")
        or ""
    ).strip()
    if location:
        return location
    if os.name == "nt":
        return DEFAULT_WIN_CHECKPOINT
    return str(Path(__file__).resolve().parent / ".spark-mail-ingest-cp")

def _get_ready_kafka_producer():
    global ready_kafka_producer

    if ready_kafka_producer is None:
        from kafka import KafkaProducer

        ready_kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=20,
        )
    return ready_kafka_producer


def _publish_ready(mail_id: str, status: str = "ready") -> None:
    producer = _get_ready_kafka_producer()
    producer.send(READY_TOPIC, {"mail_id": mail_id, "status": status})
    producer.flush()
    print(f"[ingest] -> {READY_TOPIC} 적재 완료 알림 발행 완료 | mail_id={mail_id}")

def _parse_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _event_version(payload: dict) -> int:
    raw = payload.get("version", 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0

def _event_action(payload: dict) -> str:
    action = str(payload.get("action", "")).strip().lower()
    if action in {"delete", "deleted", "remove", "removed"}:
        return "DELETE"
    return "UPSERT"

def _event_sort_key(event: dict) -> tuple:
    version = int(event.get("event_version") or 0)
    source_updated_at = event.get("source_updated_at") or datetime.min
    occurred_at = event.get("occurred_at") or datetime.min
    sequence = int(event.get("sequence") or 0)
    return (version, source_updated_at, occurred_at, sequence)

def _dedupe_events(events: list[dict]) -> list[dict]:
    latest_by_mail_id: dict[str, dict] = {}
    for event in events:
        mail_id = event["mail_id"]
        current = latest_by_mail_id.get(mail_id)
        if current is None or _event_sort_key(event) >= _event_sort_key(current):
            latest_by_mail_id[mail_id] = event
    return list(latest_by_mail_id.values())


def _standardize_mail(mail: dict, event: dict) -> dict:
    return {
        "mail_id": str(mail.get("id") or mail.get("mail_id") or event["mail_id"]),
        "title": str(mail.get("title") or ""),
        "body": str(mail.get("body") or ""),
        "sender": str(mail.get("sender") or ""),
        "sender_name": str(mail.get("sender_name") or ""),
        "receiver": str(mail.get("receiver") or ""),
        "receiver_name": str(mail.get("receiver_name") or ""),
        "cc": str(mail.get("cc") or ""),
        "sent_at": _parse_datetime(mail.get("sent_at")),
        "read_yn": bool(mail.get("read_yn") is True),
        "has_attachment": bool(mail.get("has_attachment") is True),
        "event_version": event["event_version"],
        "source_updated_at": event["source_updated_at"],
        "occurred_at": event["occurred_at"],
    }

def _load_existing_versions(spark, mail_ids: list[str]) -> dict[str, int]:
    if not mail_ids:
        return {}
    quoted_ids = ",".join("'" + mail_id.replace("'", "''") + "'" for mail_id in mail_ids)
    query = f"SELECT mail_id, event_version FROM {SILVER_TABLE} WHERE mail_id IN ({quoted_ids})"
    rows = spark.sql(query).collect()
    versions = {}
    for row in rows:
        if row["mail_id"] is not None and row["event_version"] is not None:
            versions[str(row["mail_id"])] = int(row["event_version"])
    return versions


def _drop_stale_rows(spark, rows: list[dict]) -> tuple[list[dict], list[dict]]:
    if not rows:
        return [], []

    existing_versions = _load_existing_versions(spark, [r["mail_id"] for r in rows])
    accepted: list[dict] = []
    rejected: list[dict] = []
    for row in rows:
        current_version = existing_versions.get(row["mail_id"])
        incoming_version = int(row.get("event_version") or 0)
        if current_version is not None and incoming_version < current_version:
            rejected.append(
                {
                    "mail_id": row["mail_id"],
                    "action": "UPSERT",
                    "payload_json": None,
                    "raw_mail_json": None,
                    "validation_error": (
                        f"stale event dropped: incoming_version={incoming_version}, "
                        f"current_version={current_version}"
                    ),
                }
            )
            continue
        accepted.append(row)
    return accepted, rejected


def process_batch(df, epoch_id: int) -> None:
    import requests
    from pyspark.sql import functions as F

    spark = df.sparkSession
    if df.isEmpty():
        return

    raw_events = df.select(F.col("value").cast("string").alias("json")).collect()
    parsed_events: list[dict] = []
    quarantine_rows: list[dict] = []

    for sequence, row in enumerate(raw_events):
        try:
            payload = json.loads(row.json)
        except (TypeError, json.JSONDecodeError):
            quarantine_rows.append(
                {
                    "mail_id": None,
                    "action": "UPSERT",
                    "payload_json": row.json,
                    "raw_mail_json": None,
                    "validation_error": "invalid event payload json",
                }
            )
            continue

        mail_id = payload.get("mail_id")
        if mail_id is None or str(mail_id).strip() == "":
            quarantine_rows.append(
                {
                    "mail_id": None,
                    "action": "UPSERT",
                    "payload_json": payload,
                    "raw_mail_json": None,
                    "validation_error": "event mail_id is missing",
                }
            )
            continue

        parsed_events.append(
            {
                "mail_id": str(mail_id),
                "action": _event_action(payload),
                "event_version": _event_version(payload),
                "source_updated_at": _parse_datetime(payload.get("source_updated_at")),
                "occurred_at": _parse_datetime(payload.get("occurred_at")),
                "payload": payload,
                "sequence": sequence,
            }
        )

    # 동일 mail_id 중복 이벤트 제거
    events = _dedupe_events(parsed_events)
    if not events and quarantine_rows:
        save_to_quarantine(spark, quarantine_rows)
        return
    if not events:
        return

    delete_ids: list[str] = []
    candidate_rows: list[dict] = []
    notifications: list[tuple[str, str]] = []

    for event in events:
        mail_id = event["mail_id"]
        # action == "DELETE" 이면, silver와 gold 레이어에서도 물리적 삭제
        if event["action"] == "DELETE":
            delete_ids.append(mail_id)
            notifications.append((mail_id, "deleted"))
            continue

        try:
            response = requests.get(f"{SUPPLY_URL.rstrip('/')}/mails/{mail_id}", timeout=15)
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            quarantine_rows.append(
                {
                    "mail_id": mail_id,
                    "action": "UPSERT",
                    "payload_json": event["payload"],
                    "raw_mail_json": None,
                    "validation_error": f"fetch failed: {exc}",
                }
            )
            continue

        if body.get("error"):
            quarantine_rows.append(
                {
                    "mail_id": mail_id,
                    "action": "UPSERT",
                    "payload_json": event["payload"],
                    "raw_mail_json": body,
                    "validation_error": f"supply response error: {body.get('error')}",
                }
            )
            continue

        mail = body.get("mail") or {}
        # 데이터 정합성 검증 수행(필수 필드 입력값 체크 등), 정합성 검증에 실패한 데이터는 quarantine 레이어에 쌓임
        valid, reason = validate_mail(mail)
        if not valid:
            quarantine_rows.append(
                {
                    "mail_id": mail_id,
                    "action": "UPSERT",
                    "payload_json": event["payload"],
                    "raw_mail_json": mail,
                    "validation_error": reason or "validation failed",
                }
            )
            continue

        standardized = _standardize_mail(mail, event)
        # 3단계: 개인정보 보호를 위한 데이터 암호화 가공 처리 진행
        encrypted = encrypt_personal_fields(standardized)
        candidate_rows.append(encrypted)
        notifications.append((mail_id, "ready"))

    accepted_rows, stale_rows = _drop_stale_rows(spark, candidate_rows)
    quarantine_rows.extend(stale_rows)

    if delete_ids:
        delete_from_silver_and_gold(spark, sorted(set(delete_ids)))

    # 4단계: 가공 데이터 Iceberg 테이블 멱등성 저장
    if accepted_rows:
        save_to_silver(spark, accepted_rows)
        save_to_gold(spark, accepted_rows)

    if quarantine_rows:
        save_to_quarantine(spark, quarantine_rows)

    # 5단계: Iceberg 적재 확정 후, 후속 소비 시스템을 위한 알림 토픽 일괄 발행 수행
    for mail_id, status in notifications:
        _publish_ready(mail_id, status=status)

    print(
        f"[ingest] epoch={epoch_id} "
        f"events={len(events)} accepted={len(accepted_rows)} "
        f"deleted={len(set(delete_ids))} quarantined={len(quarantine_rows)}"
    )


def main() -> None:
    try:
        import requests # noqa: F401
    except ImportError:
        print("pip install requests", file=sys.stderr)
        sys.exit(1)

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("MailIngestToIcebergLayers")
        .config("spark.jars.packages", PACKAGES)
        # 1. 메인 데이터레이크 카탈로그 설정 (PostgreSQL JDBC + MinIO S3 연동)
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "jdbc")
        .config("spark.sql.catalog.local.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog")
        .config("spark.sql.catalog.local.jdbc.user", "admin")
        .config("spark.sql.catalog.local.jdbc.password", "password")
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.client.region", "us-east-1")
        .config("spark.sql.catalog.local.jdbc.schema-version", "V1")

        # 2. 로컬 테스트 및 네트워크/호환성 에러 방지 워크아라운드
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.master", "local[1]")
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "120s")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")

        # 3. Iceberg SQL 확장 기능 활성화 및 기본 세션 카탈로그 맵핑 최적화
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.as-super-catalog", "local")
        )

    driver_java_opts = JAVA17_OPENS
    if os.name == "nt":
        hadoop_home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop")).as_posix()
        driver_java_opts = f"{JAVA17_OPENS} -Dhadoop.home.dir={hadoop_home}"

    spark = (
        builder.config("spark.sql.catalog.local.s3.endpoint", "http://127.0.0.1:9000")
        .config("spark.sql.catalog.local.s3.access-key-id", "admin")
        .config("spark.sql.catalog.local.s3.secret-access-key", "password")
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.driver.extraJavaOptions", driver_java_opts)
        .config("spark.executor.extraJavaOptions", driver_java_opts)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    create_namespaces(spark)
    create_tables(spark)

    checkpoint = _checkpoint_location()
    print(
        ">>> Stream: "
        f"{INGEST_TOPIC} @ {KAFKA_BOOTSTRAP} -> supply -> validate/encrypt -> silver,gold/quarantine -> {READY_TOPIC}"
    )
    print(f">>> checkpoint: {checkpoint}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INGEST_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("allowAutomaticTopicCreation", "true")
        .load()
    )

    query = (
        raw.writeStream.foreachBatch(process_batch)
        .outputMode("update")
        .trigger(processingTime=f"{TRIGGER_SEC} seconds")
        .option("checkpointLocation", checkpoint)
        .start()
    )
    print(">>> streaming (Ctrl+C to stop)")
    query.awaitTermination()

if __name__ == "__main__":
    main()