"""
Kafka DLQ Processor.
Listens to 'mail.events.dlq', identifies transient failures (e.g., supply API fetch failure),
and automatically republishes them to 'mail.events' after a 5-second delay, incrementing the retry count.
Limits automated retries to 3 times to prevent infinite loops.
"""
from __future__ import annotations

import json
import os
import sys
import time
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "mail.events.dlq")
INGEST_TOPIC = os.environ.get("KAFKA_INGEST_TOPIC", "mail.events")
MAX_RETRIES = 3
RETRY_DELAY_SEC = 5

def main() -> int:
    print(f"============================================================")
    print(f">>> starting Kafka DLQ Processor...")
    print(f">>> bootstrap servers : {BOOTSTRAP_SERVERS}")
    print(f">>> consuming from    : {DLQ_TOPIC}")
    print(f">>> republishing to   : {INGEST_TOPIC}")
    print(f">>> max retries       : {MAX_RETRIES}")
    print(f"============================================================")

    try:
        consumer = KafkaConsumer(
            DLQ_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
    except Exception as exc:
        print(f"[DLQ-FATAL] Failed to initialize KafkaConsumer: {exc}", file=sys.stderr)
        return 1

    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            linger_ms=10,
        )
    except Exception as exc:
        print(f"[DLQ-FATAL] Failed to initialize KafkaProducer: {exc}", file=sys.stderr)
        consumer.close()
        return 1

    print(">>> Listening for failed events...")
    
    try:
        for message in consumer:
            val = message.value
            entity_id = val.get("entity_id")
            validation_error = val.get("validation_error", "")
            retry_count = val.get("retry_count", 0)
            action = val.get("action", "UPSERT")
            orig_payload = val.get("payload_json")

            print(f"\n[DLQ-EVENT-RECEIVED] mail_id={entity_id} | action={action} | retry={retry_count} | error={validation_error}")

            # 1. Check if error is transient (fetch failed)
            is_transient = "fetch failed" in validation_error.lower() or "supply response error" in validation_error.lower()

            if is_transient:
                new_retry = retry_count + 1
                if new_retry <= MAX_RETRIES:
                    print(f"  └─ [REDRIVE] Transient error detected. Retrying for mail_id={entity_id} (Attempt {new_retry}/{MAX_RETRIES}) after {RETRY_DELAY_SEC}s delay...")
                    time.sleep(RETRY_DELAY_SEC)

                    # Reconstruct the raw payload and inject/update the retry count
                    if isinstance(orig_payload, str):
                        try:
                            payload_dict = json.loads(orig_payload)
                        except Exception:
                            payload_dict = {"mail_id": entity_id, "action": action}
                    elif isinstance(orig_payload, dict):
                        payload_dict = orig_payload
                    else:
                        payload_dict = {"mail_id": entity_id, "action": action}

                    payload_dict["_retry_count"] = new_retry

                    # Preserve the original partition key
                    key_bytes = str(entity_id).encode("utf-8") if entity_id is not None else None

                    producer.send(INGEST_TOPIC, key=key_bytes, value=payload_dict)
                    producer.flush()
                    print(f"  └─ [SUCCESS] Successfully republished mail_id={entity_id} to '{INGEST_TOPIC}' with retry_count={new_retry}")
                else:
                    print(f"  └─ [DLQ-FATAL] Max retry count ({MAX_RETRIES}) exceeded for mail_id={entity_id}. Aborting retry. Manual intervention required.")
            else:
                print(f"  └─ [DLQ-FATAL] Permanent error detected for mail_id={entity_id}: '{validation_error}'. Aborting retry. Manual intervention required.")

    except KeyboardInterrupt:
        print("\n>>> DLQ Processor stopped by user.")
    except Exception as exc:
        print(f"[DLQ-FATAL] Error in processor loop: {exc}", file=sys.stderr)
    finally:
        consumer.close()
        producer.close()
        print(">>> DLQ Processor closed.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
