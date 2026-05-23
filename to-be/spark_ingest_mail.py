"""
PoC: mail.events (Kafka) -> MailAdapter -> Iceberg silver/gold/quarantine -> mail.ready (Kafka).

Prereq: cd to-be && docker compose up -d; supply on 8100; repo-root venv with pyspark, kafka-python, requests.

Run (from dl-poc repo root):
  venv\\Scripts\\python to-be\\spark_ingest_mail.py

Windows: default checkpoint is MinIO s3a://warehouse/.spark-checkpoints/mail-ingest (avoids NativeIO on local disk).
  Override: MAIL_INGEST_CHECKPOINT or SPARK_CHECKPOINT_LOCATION (e.g. file path or another s3a URI).

Stop with Ctrl+C. Uses processingTime trigger (default 5s).
"""
from __future__ import annotations

import sys
from pathlib import Path

from datalake.adapters.mail import MailAdapter
from datalake.checkpoint import resolve_checkpoint
from datalake.ingest_runner import run_streaming_ingest

DEFAULT_WIN_CHECKPOINT = "s3a://warehouse/.spark-checkpoints/mail-ingest"


def main() -> None:
    try:
        import requests  # noqa: F401
    except ImportError:
        print("pip install requests", file=sys.stderr)
        sys.exit(1)

    adapter = MailAdapter()
    checkpoint = resolve_checkpoint(
        env_keys=("MAIL_INGEST_CHECKPOINT", "SPARK_CHECKPOINT_LOCATION"),
        default_win=DEFAULT_WIN_CHECKPOINT,
        default_local_name=".spark-mail-ingest-cp",
        base_dir=Path(__file__).resolve().parent,
    )
    run_streaming_ingest(adapter, checkpoint=checkpoint)


if __name__ == "__main__":
    main()
