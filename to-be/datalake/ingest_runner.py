"""Kafka Structured Streaming ingest runner (adapter-driven)."""

from __future__ import annotations

import os

from datalake.adapters.base import SupplyAdapter
from datalake.ingest_core import make_process_batch
from datalake.spark_runtime import create_spark_session


def run_streaming_ingest(
    adapter: SupplyAdapter,
    *,
    checkpoint: str,
    kafka_bootstrap: str | None = None,
    trigger_sec: str | None = None,
    max_offsets_per_trigger: int | None = None,
) -> None:
    bootstrap = kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    trigger = trigger_sec or os.environ.get("SPARK_STREAM_TRIGGER_SEC", "5")
    max_offsets = max_offsets_per_trigger or int(os.environ.get("SPARK_STREAM_MAX_OFFSETS_PER_TRIGGER", "5000"))

    app_name = f"{adapter.source}-ingest"
    spark = create_spark_session(app_name)
    adapter.ensure_tables(spark)

    print(f">>> Stream: {adapter.stream_description(bootstrap)}")
    print(f">>> checkpoint: {checkpoint}")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", adapter.ingest_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("allowAutomaticTopicCreation", "true")
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )

    query = (
        raw.writeStream.foreachBatch(make_process_batch(adapter, bootstrap))
        .outputMode("update")
        .trigger(processingTime=f"{trigger} seconds")
        .option("checkpointLocation", checkpoint)
        .start()
    )
    print(">>> streaming (Ctrl+C to stop)")
    query.awaitTermination()
