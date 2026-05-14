# As-Is (GCP Pub/Sub → 소비 서비스 → Supply GET)

**To-Be / Docker Kafka와 별개입니다.** As-Is는 **Google Cloud Pub/Sub**으로 이벤트를 쏘고, 검색·모바일·그래프가 **각 구독**을 받은 뒤 **공급 서비스에 `GET /mails/{id}`**를 직접 호출하는 데모입니다.

Docker는 **필수 아님**입니다. (To-Be용 Docker와 무관.)

## 추가 패키지

저장소 **루트** 가상환경에서:

```bat
venv\Scripts\pip install google-cloud-pubsub
```

## GCP 준비

1. **인증** (택일): `gcloud auth application-default login` 또는 `GOOGLE_APPLICATION_CREDENTIALS`에 서비스 계정 JSON 경로.
2. **프로젝트 ID**: 코드 기본값은 `architect-certification-289902` 입니다. 본인 프로젝트로 바꿀 경우 `supply_service`, `search_service`, `mobile_service`, `graph_service` 각 `main.py`의 `PROJECT_ID`를 동일하게 맞춥니다.
3. **토픽·구독** 생성 (프로젝트 ID는 예시):

공급이 발행하는 토픽: `search-events`, `mobile-events`, `graph-events`  
구독: `search-sub` → `search-events`, `mobile-sub` → `mobile-events`, `graph-sub` → `graph-events`

```bat
set PROJECT=architect-certification-289902
gcloud pubsub topics create search-events --project=%PROJECT%
gcloud pubsub topics create mobile-events --project=%PROJECT%
gcloud pubsub topics create graph-events --project=%PROJECT%
gcloud pubsub subscriptions create search-sub --topic=search-events --project=%PROJECT%
gcloud pubsub subscriptions create mobile-sub --topic=mobile-events --project=%PROJECT%
gcloud pubsub subscriptions create graph-sub --topic=graph-events --project=%PROJECT%
```

## 기본 포트

| 서비스 | URL |
|--------|-----|
| 공급 | http://localhost:8000 |
| 검색 | http://localhost:8001 |
| 모바일 | http://localhost:8002 |
| 그래프 | http://localhost:8003 |
| 대시보드 | http://localhost:8004 |

소비 서비스의 `SUPPLY_SERVICE_URL` 기본값은 `http://localhost:8000` 입니다.

## 실행 (터미널 5개, 공급 먼저)

저장소 루트에 `venv`가 있다고 가정합니다.

```bat
cd /d path\to\dl-poc\as-is\supply_service
..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8000
```

```bat
cd /d path\to\dl-poc\as-is\search_service
..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8001
```

```bat
cd /d path\to\dl-poc\as-is\mobile_service
..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8002
```

```bat
cd /d path\to\dl-poc\as-is\graph_service
..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8003
```

```bat
cd /d path\to\dl-poc\as-is\dashboard
..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8004
```

## 동작 확인

```bat
curl -X POST http://localhost:8000/mails -H "Content-Type: application/json" -d "{\"title\":\"t\",\"body\":\"b\",\"sender\":\"s@test.com\"}"
```

공급 로그에 Pub/Sub 발행이 찍히고, 소비 터미널에 이벤트·`GET /mails/...` 로그가 보이면 됩니다. 브라우저: `http://localhost:8004`

## Pub/Sub 에뮬레이터 (선택)

[GCP Pub/Sub 에뮬레이터](https://cloud.google.com/pubsub/docs/emulator)를 띄우고 `PUBSUB_EMULATOR_HOST`를 설정한 뒤, 위와 동일한 토픽·구독 이름으로 리소스를 만들면 됩니다. `PROJECT_ID` 문자열은 코드와 맞추면 됩니다.
