"""
Kafka DLQ Processor.
Listens to multiple DLQ topics (e.g. 'mail.events.dlq', 'schedule.events.dlq'),
identifies transient failures (e.g., supply API fetch failure),
and automatically republishes them to their respective events topics (e.g. 'mail.events')
after a 5-second delay, incrementing the retry count.
Limits automated retries to 3 times to prevent infinite loops.
"""
from __future__ import annotations

import json
import os
import sys
import time
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

# Read comma-separated DLQ topics to support multiple systems (e.g. mail, schedule, approval, board)
DLQ_TOPIC_ENV = os.environ.get("KAFKA_DLQ_TOPIC", "mail.events.dlq")
DLQ_TOPICS = [t.strip() for t in DLQ_TOPIC_ENV.split(",") if t.strip()]

MAX_RETRIES = 3
RETRY_DELAY_SEC = 5

def resolve_ingest_topic(dlq_topic: str) -> str:
    """
    Dynamically resolves the target ingest topic from the DLQ topic name.
    e.g., 'mail.events.dlq' -> 'mail.events'
          'schedule.events.dlq' -> 'schedule.events'
    """
    if dlq_topic.endswith(".dlq"):
        return dlq_topic[:-4]
    return dlq_topic.replace(".dlq", "")

def resolve_entity_id_field(dlq_topic: str) -> str:
    """
    Dynamically determines the correct entity ID key based on the topic prefix/domain.
    e.g., 'mail.events.dlq' -> 'mail_id'
          'schedule.events.dlq' -> 'schedule_id'
    """
    parts = dlq_topic.split(".")
    domain = parts[0] if parts else "entity"
    
    # Override mappings if naming convention differs (e.g. board vs post)
    overrides = {
        "board": "board_id",
        "post": "post_id",
    }
    return overrides.get(domain, f"{domain}_id")

def main() -> int:
    print(f"============================================================")
    print(f">>> starting Kafka DLQ Processor...")
    print(f">>> bootstrap servers : {BOOTSTRAP_SERVERS}")
    print(f">>> consuming from    : {DLQ_TOPICS}")
    print(f">>> max retries       : {MAX_RETRIES}")
    print(f"============================================================")

    try:
        consumer = KafkaConsumer(
            *DLQ_TOPICS,
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
            dlq_topic = message.topic
            
            # Dynamically resolve topic and domain configurations
            target_ingest_topic = resolve_ingest_topic(dlq_topic)
            id_field = resolve_entity_id_field(dlq_topic)
            
            entity_id = val.get("entity_id") or val.get(id_field)
            validation_error = val.get("validation_error", "")
            retry_count = val.get("retry_count", 0)
            action = val.get("action", "UPSERT")
            orig_payload = val.get("payload_json")

            print(f"\n[DLQ-EVENT-RECEIVED] topic={dlq_topic} | entity_id={entity_id} | action={action} | retry={retry_count} | error={validation_error}")

            # 1. Check if error is transient (fetch failed)
            is_transient = "fetch failed" in validation_error.lower() or "supply response error" in validation_error.lower()

            if is_transient:
                new_retry = retry_count + 1
                if new_retry <= MAX_RETRIES:
                    print(f"  └─ [REDRIVE] Transient error detected. Retrying for entity_id={entity_id} (Attempt {new_retry}/{MAX_RETRIES}) after {RETRY_DELAY_SEC}s delay...")
                    time.sleep(RETRY_DELAY_SEC)

                    # Reconstruct the raw payload and inject/update the retry count
                    if isinstance(orig_payload, str):
                        try:
                            payload_dict = json.loads(orig_payload)
                        except Exception:
                            payload_dict = {id_field: entity_id, "action": action}
                    elif isinstance(orig_payload, dict):
                        payload_dict = orig_payload
                    else:
                        payload_dict = {id_field: entity_id, "action": action}

                    # Ensure domain-specific ID is present in payload
                    if entity_id is not None:
                        payload_dict[id_field] = entity_id
                    
                    payload_dict["_retry_count"] = new_retry

                    # Preserve the original partition key
                    key_bytes = str(entity_id).encode("utf-8") if entity_id is not None else None

                    producer.send(target_ingest_topic, key=key_bytes, value=payload_dict)
                    producer.flush()
                    print(f"  └─ [SUCCESS] Successfully republished entity_id={entity_id} to '{target_ingest_topic}' with retry_count={new_retry}")
                else:
                    print(f"  └─ [DLQ-FATAL] Max retry count ({MAX_RETRIES}) exceeded for entity_id={entity_id}. Aborting retry. Manual intervention required.")
            else:
                print(f"  └─ [DLQ-FATAL] Permanent error detected for entity_id={entity_id}: '{validation_error}'. Aborting retry. Manual intervention required.")

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

