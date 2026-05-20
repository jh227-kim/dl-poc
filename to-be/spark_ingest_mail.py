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

import json
import os
import re
import sys
from pathlib import Path

# Worker subprocess must use this interpreter; on Windows PATH often has another `python`
# without PySpark → "Python worker failed to connect back" / Accept timed out.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

if os.name == "nt":
    _hadoop_home = os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    _hadoop_bin = str(Path(_hadoop_home) / "bin")
    if Path(_hadoop_bin).is_dir():
        os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")

_SPARK_VER = "3.5.1"
_PACKAGES = ",".join(
    [
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{_SPARK_VER}",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.postgresql:postgresql:42.6.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "software.amazon.awssdk:bundle:2.20.18",
        "software.amazon.awssdk:url-connection-client:2.20.18",
    ]
)
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--packages {_PACKAGES} pyspark-shell")

_JAVA17_OPENS = (
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
_DEFAULT_WIN_CHECKPOINT = "s3a://warehouse/.spark-checkpoints/mail-ingest"

_ready_kafka_producer = None

def _checkpoint_location() -> str:
    loc = (os.environ.get("MAIL_INGEST_CHECKPOINT") or os.environ.get("SPARK_CHECKPOINT_LOCATION") or "").strip()
    if loc:
        return loc
    if os.name == "nt":
        return _DEFAULT_WIN_CHECKPOINT
    return str(Path(__file__).resolve().parent / ".spark-mail-ingest-cp")

def _get_ready_kafka_producer():
    global _ready_kafka_producer
    if _ready_kafka_producer is None:
        from kafka import KafkaProducer
        _ready_kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=20,
        )
    return _ready_kafka_producer

def _publish_ready(mail_id: str) -> None:
    p = _get_ready_kafka_producer()
    p.send(READY_TOPIC, {"mail_id": mail_id, "status": "ready"})
    p.flush()
    print(f"[ingest] -> {READY_TOPIC} 적재 완료 알림 발행 완료 | mail_id={mail_id}")

def _sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"

def _mask_personal_info(text: str) -> str:
    """간단한 정규식을 사용한 개인정보(이메일, 전화번호) 마스킹 처리 함수"""
    if not text:
        return ""
    # 이메일 마스킹 (ex: abcde@company.com -> ab***@company.com)
    email_pattern = r'([a-zA-Z0-9_.+-]{2})[a-zA-Z0-9_.+-]+@([a-zA-Z0-9-]+\.[a-zA-Z0-9-. ]+)'
    text = re.sub(email_pattern, r'\1***@\2', text)
    
    # 전화번호 마스킹 (ex: 010-1234-5678 -> 010-****-5678)
    phone_pattern = r'(\d{2,3})-(\d{3,4})-(\d{4})'
    text = re.sub(phone_pattern, r'\1-****-\3', text)
    return text

def _register_mail_batch_view(spark, rows: list[tuple[str, str, str, str]]) -> None:
    """Avoid spark.createDataFrame for batch rows: Iceberg write + PySpark local worker often crashes on Windows."""
    tuples_sql = ",\n".join(
        "("
        + ",".join(
            [
                _sql_string_literal(mid),
                _sql_string_literal(title),
                _sql_string_literal(body),
                _sql_string_literal(sender),
            ]
        )
        + ")"
        for mid, title, body, sender in rows
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW _mail_batch AS
        SELECT mail_id, title, body, sender, current_timestamp() AS ingested_at
        FROM VALUES
        {tuples_sql}
        AS v(mail_id, title, body, sender)
        """
    )

def process_batch(df, epoch_id: int) -> None:
    import requests
    from pyspark.sql import functions as F

    spark = df.sparkSession
    if df.isEmpty():
        return

    rows_out: list[tuple[str, str, str, str]] = []
    success_ids: list[str] = []

    for row in df.select(F.col("value").cast("string").alias("json")).collect():
        try:
            payload = json.loads(row.json)
        except (json.JSONDecodeError, TypeError):
            continue
        mid = payload.get("mail_id")
        act = payload.get("action")
        if not mid or not isinstance(mid, str):
            continue
        try:
            r = requests.get(f"{SUPPLY_URL.rstrip('/')}/mails/{mid}", timeout=15)
            data = r.json()
        except Exception as e:
            print(f"[ingest] fetch failed mail_id={mid}: {e}")
            continue
        if data.get("error"):
            print(f"[ingest] supply error mail_id={mid}: {data}")
            continue
            
        mail = data.get("mail") or {}
        
        # 3단계: 개인정보 보호를 위한 데이터 비식별화 가공 처리 진행
        masked_title = _mask_personal_info(str(mail.get("title") or ""))
        masked_body = _mask_personal_info(str(mail.get("body") or ""))
        masked_sender = _mask_personal_info(str(mail.get("sender") or ""))

        rows_out.append((str(mid), masked_title, masked_body, masked_sender))
        success_ids.append(str(mid))
        print(f"[ingest] 가공 완료 (메모리 적재) mail_id={mid} action={act} epoch={epoch_id}")

    if not rows_out:
        return

    # 4단계: 가공 데이터 Iceberg 테이블 멱등성 저장
    _register_mail_batch_view(spark, rows_out)
    # idempotent-ish: delete same mail_id then insert (no Iceberg MERGE SQL extension required)
    ids = ",".join("'" + m.replace("'", "''") + "'" for m in success_ids)
    spark.sql(f"DELETE FROM local.db.mail_silver WHERE mail_id IN ({ids})")
    spark.sql(
        "INSERT INTO local.db.mail_silver (mail_id, title, body, sender, ingested_at) "
        "SELECT mail_id, title, body, sender, ingested_at FROM _mail_batch"
    )
    print(f"[ingest] Iceberg 테이블 저장 트랜잭션 완료 완료. 총 건수: {len(rows_out)}")

    # 5단계: Iceberg 적재 확정 후, 후속 소비 시스템을 위한 알림 토픽 일괄 발행 수행
    for valid_id in success_ids:
        _publish_ready(valid_id)


def main() -> None:
    try:
        import requests  # noqa: F401
    except ImportError:
        print("pip install requests", file=sys.stderr)
        sys.exit(1)

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("MailIngestToIceberg")
        .config("spark.jars.packages", _PACKAGES)
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "jdbc")
        .config("spark.sql.catalog.local.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog")
        .config("spark.sql.catalog.local.jdbc.user", "admin")
        .config("spark.sql.catalog.local.jdbc.password", "password")
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.client.region", "us-east-1")
        .config("spark.sql.catalog.local.jdbc.schema-version", "V1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.master", "local[1]")
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "120s")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )
    driver_java_opts = _JAVA17_OPENS
    if os.name == "nt":
        _hh = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop")).as_posix()
        driver_java_opts = f"{_JAVA17_OPENS} -Dhadoop.home.dir={_hh}"

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

    print(">>> Iceberg table local.db.mail_silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS local.db.mail_silver (
            mail_id STRING,
            title STRING,
            body STRING,
            sender STRING,
            ingested_at TIMESTAMP
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """
    )

    cp = _checkpoint_location()
    print(f">>> Stream: {INGEST_TOPIC} @ {KAFKA_BOOTSTRAP} -> supply -> Iceberg -> {READY_TOPIC}")
    print(f">>> checkpoint: {cp}")
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INGEST_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("allowAutomaticLengthCheck", "true")
        .load()
    )

    q = (
        raw.writeStream.foreachBatch(process_batch)
        .outputMode("update")
        .trigger(processingTime=f"{TRIGGER_SEC} seconds")
        .option("checkpointLocation", cp)
        .start()
    )
    print(">>> streaming (Ctrl+C to stop)")
    q.awaitTermination()

if __name__ == "__main__":
    main()