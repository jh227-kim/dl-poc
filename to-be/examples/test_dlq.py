"""
Manual/Automated Verification Script for Kafka DLQ.
This script publishes:
1. An invalid JSON string.
2. A valid JSON but missing 'mail_id'.
Then it listens to 'mail.events.dlq' to verify they are received with the correct validation_error.
"""
from __future__ import annotations

import json
import sys
import time
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = ["localhost:9092"]
INGEST_TOPIC = "mail.events"
DLQ_TOPIC = "mail.events.dlq"

def main() -> int:
    print(">>> Starting DLQ Verification Test...")
    
    # 1. Initialize Producer
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        linger_ms=10,
    )

    # 2. Publish invalid JSON
    bad_json = b"invalid-raw-string-not-json"
    print(f"Publishing bad JSON to {INGEST_TOPIC}: {bad_json.decode('utf-8')}")
    producer.send(INGEST_TOPIC, bad_json)

    # 3. Publish event with missing mail_id
    bad_payload = {"title": "Missing ID Event", "content": "This event has no mail_id"}
    bad_payload_bytes = json.dumps(bad_payload).encode("utf-8")
    print(f"Publishing event missing mail_id to {INGEST_TOPIC}: {bad_payload}")
    producer.send(INGEST_TOPIC, bad_payload_bytes)
    
    producer.flush()
    producer.close()
    print(">>> Published test events. Waiting for Spark streaming to process...")

    # 4. Consume from DLQ
    print(f"Listening to DLQ topic: {DLQ_TOPIC}...")
    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=30000,  # 30 seconds timeout
    )

    received_bad_json = False
    received_missing_id = False

    for message in consumer:
        val = message.value
        print(f"Received DLQ Message | Key: {message.key} | Value: {val}")
        
        err = val.get("validation_error", "")
        if "invalid event payload json" in err:
            received_bad_json = True
            print("✔ Verified: 'invalid event payload json' DLQ event received!")
        elif "event mail_id is missing" in err:
            received_missing_id = True
            print("✔ Verified: 'event mail_id is missing' DLQ event received!")

        if received_bad_json and received_missing_id:
            print(">>> SUCCESS: All expected DLQ test cases verified!")
            consumer.close()
            return 0

    print(">>> FAILURE: Timeout reached without receiving expected DLQ events.")
    consumer.close()
    return 1

if __name__ == "__main__":
    sys.exit(main())
