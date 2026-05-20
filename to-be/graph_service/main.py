from __future__ import annotations

import json
import os
import threading

import httpx
from fastapi import FastAPI
from kafka import KafkaConsumer

SERVICE_LABEL = "Graph서비스"
CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "graph-to-be")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_READY_TOPIC = os.environ.get("KAFKA_READY_TOPIC", "mail.ready")
SERVING_SERVICE_URL = os.environ.get("SERVING_SERVICE_URL", "http://localhost:8105")
AUTO_OFFSET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest")

app = FastAPI(title=f"{SERVICE_LABEL} (To-Be)")
received_data: list = []
logs: list[str] = []


def handle_event(mail_id: str | None, action: str | None) -> None:
    log = f"[{SERVICE_LABEL}] Kafka 적재 완료 알림 수신 완료 | mail_id={mail_id}"
    print(log)
    logs.append(log)

    log2 = f"[{SERVICE_LABEL}] Data Serving 레이어에 데이터 요청 → GET {SERVING_SERVICE_URL}/mails/{mail_id}"
    print(log2)
    logs.append(log2)

    try:
        response = httpx.get(f"{SERVING_SERVICE_URL}/mails/{mail_id}", timeout=10)
        data = response.json()
        received_data.append(data)
        log3 = (
            f"[{SERVICE_LABEL}] 데이터 수신 완료 | mail_id={mail_id} | "
            f"응답시간: {data.get('response_delay_sec', 0):.1f}초 | "
            f"내용: {data}"
        )
        print(log3)
        logs.append(log3)
    except Exception as e:
        logs.append(f"[{SERVICE_LABEL}] 오류: {e}")


def kafka_loop() -> None:
    consumer = KafkaConsumer(
        KAFKA_READY_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset=AUTO_OFFSET,
        enable_auto_commit=True,
    )
    print(f"[{SERVICE_LABEL}] 구독 중: topic={KAFKA_READY_TOPIC} group={CONSUMER_GROUP}")
    for message in consumer:
        body = message.value
        if not isinstance(body, dict):
            continue
        handle_event(body.get("mail_id"), body.get("status"))


@app.on_event("startup")
def startup() -> None:
    thread = threading.Thread(target=kafka_loop, daemon=True)
    thread.start()


@app.get("/")
def health():
    return {"service": f"{SERVICE_LABEL} (To-Be)", "status": "running", "received_count": len(received_data)}


@app.get("/logs")
def get_logs():
    return {"logs": logs[-50:]}


@app.get("/data")
def get_data():
    return {"data": received_data[-10:]}


@app.post("/reset")
def reset():
    received_data.clear()
    logs.clear()
    return {"message": "초기화 완료"}
