from __future__ import annotations

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from kafka import KafkaProducer
from pydantic import BaseModel

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INGEST_TOPIC = os.environ.get("KAFKA_INGEST_TOPIC", "mail.events")

producer: KafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=20,
    )
    yield
    if producer is not None:
        producer.flush()
        producer.close()


app = FastAPI(title="공급서비스 (To-Be) - Kafka 수집", lifespan=lifespan)

stats = {"total_requests": 0, "direct_requests": 0, "active_requests": 0, "peak_active": 0}

mail_db = {
    "1": {"id": "1", "title": "회의 안내", "body": "내일 오전 10시 회의가 있습니다.", "sender": "hong@company.com"},
    "2": {"id": "2", "title": "프로젝트 공유", "body": "첨부 문서를 확인해주세요.", "sender": "kim@company.com"},
}


class MailCreate(BaseModel):
    title: str
    body: str
    sender: str


def _mail_payload(mail: MailCreate, mail_id: str) -> dict:
    data = mail.model_dump() if hasattr(mail, "model_dump") else mail.dict()
    return {"id": mail_id, **data}


def publish_event(mail_id: str, action: str) -> None:
    assert producer is not None
    payload = {"mail_id": mail_id, "action": action}
    producer.send(KAFKA_INGEST_TOPIC, payload)
    producer.flush()
    print(f"[공급서비스 To-Be] Kafka 발행 -> {KAFKA_INGEST_TOPIC} | {payload}")


@app.get("/")
def health():
    return {"service": "공급서비스 (To-Be)", "status": "running", "stats": stats, "kafka_topic": KAFKA_INGEST_TOPIC}


@app.get("/mails")
def list_mails():
    stats["total_requests"] += 1
    return {"mails": list(mail_db.values())}


@app.get("/mails/{mail_id}")
async def get_mail(mail_id: str):
    stats["total_requests"] += 1
    stats["direct_requests"] += 1
    stats["active_requests"] += 1
    active = stats["active_requests"]
    if active > stats["peak_active"]:
        stats["peak_active"] = active
    print(f"[공급서비스 To-Be] 원본 데이터 직접 요청 수신 | mail_id={mail_id} | 동시 처리 중: {active}건")

    delay = min(0.1 * active, 3.0)
    await asyncio.sleep(delay)
    stats["active_requests"] -= 1

    mail = mail_db.get(mail_id)
    if not mail:
        return {"error": "not found"}
    return {"mail": mail, "response_delay_sec": delay}


@app.post("/mails")
def create_mail(mail: MailCreate):
    mail_id = str(uuid.uuid4())[:8]
    mail_db[mail_id] = _mail_payload(mail, mail_id)
    print(f"[공급서비스 To-Be] 메일 생성 | mail_id={mail_id}")
    publish_event(mail_id, "created")
    return {"mail_id": mail_id, "message": "생성 완료, Kafka 수집 토픽에 발행됨"}


@app.get("/stats")
def get_stats():
    return stats


@app.post("/stats/reset")
def reset_stats():
    stats["total_requests"] = 0
    stats["direct_requests"] = 0
    stats["active_requests"] = 0
    stats["peak_active"] = 0
    mail_db.clear()
    mail_db["1"] = {"id": "1", "title": "회의 안내", "body": "내일 오전 10시 회의가 있습니다.", "sender": "hong@company.com"}
    mail_db["2"] = {"id": "2", "title": "프로젝트 공유", "body": "첨부 문서를 확인해주세요.", "sender": "kim@company.com"}
    return {"message": "통계 초기화 완료"}
