# To-Be (Kafka → Supply → Spark → Iceberg → mail.ready)

`mail.events`에 이벤트가 오면 Spark가 **공급 서비스 HTTP**로 본문을 가져와 **Iceberg**(`local.db.mail_silver`)에 넣고, **`mail.ready`**로 알립니다.

**Docker Desktop**이 필요합니다. 설치·기동은 저장소 **루트 `README.md`**의 Docker Desktop 절을 참고하세요.

## 이 폴더에 있는 것

| 경로 | 설명 |
|------|------|
| `docker-compose.yml` | MinIO(9000), Postgres(5432), Kafka(9092), Zookeeper |
| `spark_ingest_mail.py` | Structured Streaming ingest |
| `serving_service/` | 자바(Spark) 없이 Python 네이티브(PyIceberg)로 MinIO Parquet 데이터를 직접 조회하는 서빙 레이어 |
| `app.py` | Iceberg/MinIO 연결만 빠르게 검증할 때 (선택) |
| `examples/mail_event_publish.py` | Kafka에 이벤트 한 건 발행 (선택) |
| `scripts/kafka-smoke.bat` | Windows: 토픽 생성 + produce/consume 스모크 |
| `supply_service/` 등 | FastAPI 마이크로서비스 (기본 포트 8100~) |

## 한 번만: 루트에서 venv + 패키지

저장소 **루트**(`dl-poc`)에서:

```bat
cd /d path\to\dl-poc
py -3 -m venv venv
venv\Scripts\pip install -r requirements.txt
```

## 인프라 (이 폴더에서 Compose)

```bat
cd /d path\to\dl-poc\to-be
docker compose up -d
```

## Kafka 토픽 (선택)

`mail.events`가 없으면, **저장소 루트**에서:

```bat
to-be\scripts\kafka-smoke.bat
```

(내부에서 `to-be`로 이동해 `docker compose`를 실행합니다.)

## MinIO에 Bucket 생성
- http://localhost:9001/ 접속 (Username: admin / Password: password)
- Create Bucket 클릭
  - bucket명: warehouse



## end-to-end 최소 (터미널 2개 + 선택 consumer)

### 1) 공급 서비스 (8100)

```bat
cd /d path\to\dl-poc\to-be\supply_service
..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8100
```

### 2) Spark ingest

**저장소 루트**에서:

```bat
cd /d path\to\dl-poc
venv\Scripts\python to-be\spark_ingest_mail.py
```

- 첫 실행 시 JAR 다운로드로 시간이 걸릴 수 있습니다.
- **Windows**: 기본 체크포인트는 MinIO `s3a://warehouse/.spark-checkpoints/mail-ingest` (로그에 `>>> checkpoint:`).
- **Linux/macOS**: 기본은 이 폴더 아래 `.spark-mail-ingest-cp/`.
- 바꾸기: `MAIL_INGEST_CHECKPOINT` 또는 `SPARK_CHECKPOINT_LOCATION`.

### 3) 메일 생성

```bat
curl -X POST http://localhost:8100/mails -H "Content-Type: application/json" -d "{\"title\":\"t\",\"body\":\"b\",\"sender\":\"s@test.com\"}"
```

### (선택) Kafka tail

```bat
cd /d path\to\dl-poc\to-be
docker compose exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic mail.events --from-beginning
```

`mail.ready`도 `--topic mail.ready` 로 동일.

### (선택) 이벤트만 Kafka에 넣기

```bat
cd /d path\to\dl-poc
venv\Scripts\python to-be\examples\mail_event_publish.py
```

### (선택) Iceberg 스모크만

```bat
cd /d path\to\dl-poc
venv\Scripts\python to-be\app.py
```

## 환경 변수 (선택)

| 변수 | 기본 | 설명 |
|------|------|------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka (호스트) |
| `KAFKA_INGEST_TOPIC` | `mail.events` | 수집 토픽 |
| `KAFKA_READY_TOPIC` | `mail.ready` | 적재 후 알림 |
| `SUPPLY_SERVICE_URL` | `http://127.0.0.1:8100` | Spark → Supply |
| `SPARK_STREAM_TRIGGER_SEC` | `5` | 트리거(초) |
| `MAIL_INGEST_CHECKPOINT` | (플랫폼별) | Spark 체크포인트 URI |
|`ICEBERG_CATALOG_URI`|`postgresql://admin:password@localhost:5432/iceberg_catalog`|카탈로그 메타데이터 상태 관리 DB 주소|
|`MINIO_ENDPOINT`|`http://127.0.0.1:9000`|물리 데이터 저장 레이어 (Parquet IO 대상)|

## Windows 참고

- `HADOOP_HOME` 기본 `C:\hadoop` — Spark 스크립트가 참고합니다.
- Spark 실행은 **`venv\Scripts\python to-be\spark_ingest_mail.py`** 로 통일하면 `PYSPARK_PYTHON`이 맞습니다.

## 전체 To-Be 서비스 (대시보드 포함)

```bat
cd to-be\supply_service  && ..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8100
cd to-be\search_service  && ..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8101
cd to-be\mobile_service  && ..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8102
cd to-be\graph_service   && ..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8103
cd to-be\dashboard       && ..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8104
cd to-be\serving_service && ..\..\venv\Scripts\uvicorn main:app --host 0.0.0.0 --port 8105
```

## 문제가 자주 나는 곳

- Docker 미기동 → Docker Desktop 실행 후 `to-be`에서 `docker compose up -d`
- Windows NativeIO / 체크포인트 → 위 기본 MinIO 체크포인트 유지 또는 JDK 17
- Python worker 타임아웃 → 반드시 **루트 venv의 python**으로 `spark_ingest_mail.py` 실행

짧은 메모는 [notes.txt](notes.txt)에 있습니다.
