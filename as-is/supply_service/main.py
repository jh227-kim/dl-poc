import asyncio
import uuid
import json
from fastapi import FastAPI
from pydantic import BaseModel
from google.cloud import pubsub_v1

PROJECT_ID = "architect-certification-289902"
TOPICS = ["search-events", "mobile-events", "graph-events"]

app = FastAPI(title="공급서비스 (메일서비스) - As-Is")
publisher = pubsub_v1.PublisherClient()

# 요청 통계 (부하 시연용)
stats = {"total_requests": 0, "direct_requests": 0, "active_requests": 0, "peak_active": 0}

# 메일 데이터 저장소 (인메모리)
mail_db = {
    "1": {"id": "1", "title": "회의 안내", "body": "내일 오전 10시 회의가 있습니다.", "sender": "hong@company.com"},
    "2": {"id": "2", "title": "프로젝트 공유", "body": "첨부 문서를 확인해주세요.", "sender": "kim@company.com"},
}

class MailCreate(BaseModel):
    title: str
    body: str
    sender: str

def publish_event(mail_id: str, action: str):
    event = json.dumps({"mail_id": mail_id, "action": action}).encode()
    for topic in TOPICS:
        topic_path = publisher.topic_path(PROJECT_ID, topic)
        publisher.publish(topic_path, event)
        print(f"[공급서비스] 이벤트 발행 → {topic} | mail_id={mail_id}, action={action}")

@app.get("/")
def health():
    return {"service": "공급서비스 (As-Is)", "status": "running", "stats": stats}

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
    print(f"[공급서비스] 원본 데이터 직접 요청 수신 | mail_id={mail_id} | 동시 처리 중: {active}건")

    # 부하 시뮬레이션: 동시 요청 수에 따라 느려지고, 완료되면 회복
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
    mail_db[mail_id] = {"id": mail_id, **mail.dict()}
    print(f"[공급서비스] 메일 생성 | mail_id={mail_id}")
    publish_event(mail_id, "created")
    return {"mail_id": mail_id, "message": "생성 완료, 이벤트 발행됨"}

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
