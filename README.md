# dl-poc (Data lake PoC)

Kafka 수집 토픽(`mail.events`) → **To-Be 공급 서비스** HTTP로 본문 조회 → **Iceberg**(`local.db.mail_silver`) 적재 → 소비용 토픽(`mail.ready`) 발행을 **로컬 Docker + PySpark**로 재현한 저장소입니다.

## 사전 준비

- **Docker Desktop** (Linux 엔진) 실행 가능 상태
- **JDK 11 또는 17** 권장 (JDK 18도 동작은 하나 Spark 경고가 더 많을 수 있음)
- **Python 3.11+** 권장 (프로젝트에서 쓰는 버전에 맞추면 됨)
- Windows: Spark 스크립트가 `HADOOP_HOME` 기본값 `C:\hadoop`을 참고합니다. 빈 폴더라도 두거나, 실제 Hadoop/winutils 레이아웃에 맞게 환경 변수를 설정하세요.

## 저장소 받기 후 한 번만

저장소 루트(`dl-poc`)에서 가상환경과 패키지를 설치합니다.

**Windows CMD**

```bat
cd /d path\to\dl-poc
py -3 -m venv venv
venv\Scripts\python -m pip install -U pip
venv\Scripts\pip install -r requirements.txt
```

**PowerShell**

```powershell
Set-Location path\to\dl-poc
py -3 -m venv .\venv
.\venv\Scripts\python -m pip install -U pip
.\venv\Scripts\pip install -r requirements.txt
```

## 인프라 기동

```bat
cd /d path\to\dl-poc
docker compose up -d
```

구성: MinIO(9000), Postgres(5432), Kafka(9092), Zookeeper.

## As-Is 로컬 실행 (GCP Pub/Sub)

**To-Be/Kafka PoC와 별개입니다.** As-Is는 **Google Cloud Pub/Sub**으로 이벤트를 쏘고, 검색·모바일·그래프 소비 서비스가 **각각 구독을 받은 뒤 공급 서비스에 `GET /mails/{id}`**를 직접 때리는 데모입니다. `docker compose`의 Kafka는 As-Is에 사용되지 않습니다.

### 추가 패키지

루트 가상환경에서 한 번 설치합니다.

```bat
venv\Scripts\pip install google-cloud-pubsub
```

### GCP 준비

1. **인증** (택일): `gcloud auth application-default login` 또는 서비스 계정 JSON 경로를 `GOOGLE_APPLICATION_CREDENTIALS`에 지정.
2. **프로젝트·리소스**: 코드에 `PROJECT_ID = architect-certification-289902` 가 박혀 있습니다. **본인 GCP 프로젝트로 바꿀 경우** `as-is` 아래 `supply_service`, `search_service`, `mobile_service`, `graph_service` 각각의 `main.py`에서 `PROJECT_ID`(및 필요 시 토픽/구독 이름)를 동일하게 맞춥니다.
3. **토픽·구독**이 없으면 생성합니다. (프로젝트 ID는 예시이며, 실제 값으로 바꿉니다.)

공급이 발행하는 토픽 이름:

- `search-events`, `mobile-events`, `graph-events`

소비 서비스가 기대하는 구독 이름:

- 검색: `search-sub` → 토픽 `search-events`
- 모바일: `mobile-sub` → 토픽 `mobile-events`
- 그래프: `graph-sub` → 토픽 `graph-events`

`gcloud` 예시 (한 프로젝트에 모두 만듭니다):

```bat
set PROJECT=architect-certification-289902
gcloud pubsub topics create search-events --project=%PROJECT%
gcloud pubsub topics create mobile-events --project=%PROJECT%
gcloud pubsub topics create graph-events --project=%PROJECT%
gcloud pubsub subscriptions create search-sub --topic=search-events --project=%PROJECT%
gcloud pubsub subscriptions create mobile-sub --topic=mobile-events --project=%PROJECT%
gcloud pubsub subscriptions create graph-sub --topic=graph-events --project=%PROJECT%
```

### 기본 포트

| 서비스 | URL |
|--------|-----|
| 공급 | http://localhost:8000 |
| 검색 | http://localhost:8001 |
| 모바일 | http://localhost:8002 |
| 그래프 | http://localhost:8003 |
| 대시보드 | http://localhost:8004 |

소비 서비스의 `SUPPLY_SERVICE_URL` 기본값은 `http://localhost:8000` 입니다.

### 실행 순서 (터미널 5개)

공급을 먼저 띄운 뒤, 나머지를 띄웁니다 (각각 백그라운드에서 Pub/Sub 구독 스레드를 붙입니다).

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

### 동작 확인

```bat
curl -X POST http://localhost:8000/mails -H "Content-Type: application/json" -d "{\"title\":\"t\",\"body\":\"b\",\"sender\":\"s@test.com\"}"
```

공급 로그에 Pub/Sub 발행이 찍히고, 검색/모바일/그래프 터미널에 이벤트 수신·`GET /mails/...` 로그가 보이면 됩니다. 대시보드는 브라우저에서 `http://localhost:8004` 등으로 확인합니다.

### Pub/Sub 에뮬레이터만 쓸 때 (선택)

GCP 프로젝트 없이 로컬만 쓰려면 [Pub/Sub 에뮬레이터](https://cloud.google.com/pubsub/docs/emulator)를 띄우고 `PUBSUB_EMULATOR_HOST`(예: `127.0.0.1:8085`)를 설정한 뒤, 위와 동일한 토픽·구독 이름으로 리소스를 만들면 됩니다. 에뮬레이터에서도 `PROJECT_ID` 문자열은 코드와 일치시키면 됩니다.

## Kafka 토픽 (선택)

`mail.events`가 없으면 생성·스모크:

```bat
cd /d path\to\dl-poc
scripts\kafka-smoke.bat
```

## To-Be + Spark end-to-end (터미널 2개 + 선택 consumer)

### 1) To-Be 공급 서비스 (기본 포트 8100)

**CMD**

```bat
cd /d path\to\dl-poc\to-be\supply_service
..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8100
```

### 2) Spark ingest (`mail.events` → Supply GET → Iceberg → `mail.ready`)

**CMD**

```bat
cd /d path\to\dl-poc
venv\Scripts\python spark_ingest_mail.py
```

- 첫 실행 시 Ivy가 JAR을 받아 오므로 시간이 걸릴 수 있습니다.
- **Windows**에서는 스트리밍 체크포인트 기본이 MinIO 경로(`s3a://warehouse/.spark-checkpoints/mail-ingest`)입니다. 로그에 `>>> checkpoint: ...` 가 출력됩니다.
- **Linux/macOS**에서는 기본이 로컬 폴더 `.spark-mail-ingest-cp/` 입니다.
- 체크포인트 위치를 바꾸려면 환경 변수 `MAIL_INGEST_CHECKPOINT` 또는 `SPARK_CHECKPOINT_LOCATION` 을 사용하세요.

### 3) 메일 생성 (이벤트 → Kafka → Spark가 처리)

```bat
curl -X POST http://localhost:8100/mails -H "Content-Type: application/json" -d "{\"title\":\"t\",\"body\":\"b\",\"sender\":\"s@test.com\"}"
```

### (선택) 토픽만 보기

```bat
cd /d path\to\dl-poc
docker compose exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic mail.events --from-beginning
```

`mail.ready`도 동일하게 `--topic mail.ready` 로 확인할 수 있습니다.

## 환경 변수 (선택)

| 변수 | 기본 | 설명 |
|------|------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka (호스트에서 접속) |
| `KAFKA_INGEST_TOPIC` | `mail.events` | 수집 토픽 |
| `KAFKA_READY_TOPIC` | `mail.ready` | 적재 후 알림 토픽 |
| `SUPPLY_SERVICE_URL` | `http://127.0.0.1:8100` | Spark가 메일 본문을 가져올 Supply |
| `SPARK_STREAM_TRIGGER_SEC` | `5` | 마이크로배치 트리거(초) |
| `MAIL_INGEST_CHECKPOINT` | (플랫폼별 기본) | Spark 체크포인트 URI |

## Git으로 팀에 공유하기

1. 원격 저장소(GitHub/GitLab 등)에 빈 저장소를 만듭니다.
2. 로컬에서 (Git 설치된 환경에서):

```bat
cd /d path\to\dl-poc
git init
git add .
git commit -m "Initial dl-poc: Kafka supply ingest and Iceberg PoC"
git branch -M main
git remote add origin https://github.com/YOUR_ORG/dl-poc.git
git push -u origin main
```

`venv/`, 체크포인트, `__pycache__/` 등은 **루트 `.gitignore`**에 포함되어 커밋되지 않습니다. 팀원은 클론 후 위 **가상환경 + pip + docker compose** 순서를 따르면 됩니다.

## 디렉터리 메모

- `to-be/` — Kafka(`mail.events`)로만 발행하는 공급 등 To-Be 레이아웃 (기본 포트 8100~)
- `as-is/` — GCP Pub/Sub + 소비 서비스가 공급에 직접 GET 하는 As-Is 데모 (기본 포트 8000~, **Kafka Docker와 무관**)
- `spark_ingest_mail.py` — 스트리밍 ingest 파이프라인
- `docker-compose.yml` — 로컬 의존성 스택
- `scripts/kafka-smoke.bat` — Windows에서 Kafka 스모크(실행 정책은 bat에서 Bypass)

## 문제가 자주 나는 곳

- **Docker 미기동**: `docker compose` 실패 → Docker Desktop 실행
- **Windows + 로컬 체크포인트 + Hadoop NativeIO**: 스크립트는 기본으로 MinIO 체크포인트를 사용합니다. 여전히 문제면 JDK 17과 `MAIL_INGEST_CHECKPOINT`를 `s3a://...` 로 유지하는 것을 권장합니다.
- **Python worker / Accept timeout**: 스크립트에서 `PYSPARK_PYTHON`을 현재 인터프리터로 맞춥니다. 반드시 `venv\Scripts\python spark_ingest_mail.py` 로 실행하세요.
