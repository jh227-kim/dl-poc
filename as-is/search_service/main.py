import threading
import json
import httpx
from fastapi import FastAPI
from google.cloud import pubsub_v1

PROJECT_ID = "architect-certification-289902"
SUBSCRIPTION = "search-sub"
SUPPLY_SERVICE_URL = "http://localhost:8000"

app = FastAPI(title="검색서비스 - As-Is")
subscriber = pubsub_v1.SubscriberClient()

# 수신한 데이터 저장소
received_data = []
logs = []

def callback(message):
    event = json.loads(message.data.decode())
    mail_id = event.get("mail_id")
    action = event.get("action")
    log = f"[검색서비스] 이벤트 수신 | mail_id={mail_id}, action={action}"
    print(log)
    logs.append(log)

    # ★ As-Is 핵심 문제: 공급서비스에 직접 원본 데이터 요청
    log2 = f"[검색서비스] 공급서비스에 직접 원본 데이터 요청 → GET {SUPPLY_SERVICE_URL}/mails/{mail_id}"
    print(log2)
    logs.append(log2)

    try:
        response = httpx.get(f"{SUPPLY_SERVICE_URL}/mails/{mail_id}", timeout=10)
        data = response.json()
        received_data.append(data)
        log3 = f"[검색서비스] 데이터 수신 완료 | mail_id={mail_id} | 응답시간: {data.get('response_delay_sec', 0):.1f}초"
        print(log3)
        logs.append(log3)
    except Exception as e:
        logs.append(f"[검색서비스] 오류: {e}")

    message.ack()

def start_subscriber():
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION)
    streaming_pull = subscriber.subscribe(subscription_path, callback=callback)
    print(f"[검색서비스] {SUBSCRIPTION} 구독 시작")
    streaming_pull.result()

@app.on_event("startup")
def startup():
    thread = threading.Thread(target=start_subscriber, daemon=True)
    thread.start()

@app.get("/")
def health():
    return {"service": "검색서비스 (As-Is)", "status": "running", "received_count": len(received_data)}

@app.get("/logs")
def get_logs():
    return {"logs": logs[-20:]}

@app.get("/data")
def get_data():
    return {"data": received_data[-10:]}

@app.post("/reset")
def reset():
    received_data.clear()
    logs.clear()
    return {"message": "초기화 완료"}
