"""
Single-shot: publish one mail event to Kafka (same shape as kafka-smoke).

  pip install kafka-python
  python examples/mail_event_publish.py
  python examples/mail_event_publish.py --mail-id abc12345 --action created

Supply service (FastAPI) should do the equivalent after POST /mails succeeds.
"""
from __future__ import annotations

import argparse
import json
import sys


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092", help="Host: use localhost:9092. Same compose network: kafka:29092")
    p.add_argument("--topic", default="mail.events")
    p.add_argument("--mail-id", default=None, help="8 chars if omitted, random")
    p.add_argument("--action", default="created")
    args = p.parse_args()

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("Install: pip install kafka-python", file=sys.stderr)
        return 1

    mid = args.mail_id or __import__("uuid").uuid4().hex[:8]
    payload = {"mail_id": mid, "action": args.action}

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=10,
    )
    producer.send(args.topic, payload).get(timeout=10)
    producer.flush()
    producer.close()
    print(f"OK sent to {args.topic}: {payload}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
