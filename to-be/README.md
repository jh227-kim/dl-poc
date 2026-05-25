# To-Be (Kafka → Supply → Spark → Iceberg → mail.ready)

`mail.events`에 이벤트가 오면 Spark가 **공급 서비스 HTTP**로 본문을 가져와 **Iceberg**(`local.db.mail_silver`)에 넣고, **`mail.ready`**로 알립니다.

**Docker Desktop**이 필요합니다. 설치·기동은 저장소 **루트 `README.md`**의 Docker Desktop 절을 참고하세요.

## 데이터 레이크 ingest 구조

adapter / 공통 ingest 파이프라인 / 새 도메인 추가 방법은 **[datalake/README.md](datalake/README.md)** 를 참고하세요.

## 이 폴더에 있는 것

| 경로 | 설명 |
|------|------|
| `docker-compose.yml` | MinIO, Postgres, Kafka/ZK, Kafka-UI, Kong API Gateway 및 소비시스템 mock 3종 일괄 정의 |
| `kong.yml` | Kong API Gateway용 선언적 라우팅 및 역할 기반 ACL(접근 제어) 설정 |
| `spark_ingest_mail.py` | Structured Streaming ingest (메일 진입점) |
| `datalake/` | adapter, ingest_core, layers — [README](datalake/README.md) |
| `serving_service/` | 자바(Spark) 없이 Python 네이티브(PyIceberg)로 MinIO Parquet 데이터를 직접 조회하는 서빙 레이어 (Port 8105) |
| `consumer_mock_service/` | Docker Compose 내부에서 다중 인스턴스로 기동되는 소비시스템(모바일, 검색, Graph) 통합 mock |
| `supply_service/` | FastAPI 메일 공급/생성 마이크로서비스 (Port 8100) |
| `dashboard/` | 수집 현황 및 소비시스템 로그 모니터링 웹 대시보드 (Port 8104) |
| `app.py` | Iceberg/MinIO 연결만 빠르게 검증할 때 (선택) |
| `examples/mail_event_publish.py` | Kafka에 이벤트 한 건 발행 (선택) |
| `scripts/kafka-smoke.bat` | Windows: 토픽 생성 + produce/consume 스모크 |

## 한 번만: 루트에서 venv + 패키지

저장소 **루트**(`dl-poc`)에서:

```bat
cd /d path\to\dl-poc
py -3 -m venv venv
venv\Scripts\pip install -r requirements.txt
```

## 인프라 및 게이트웨이/Mock 실행 (이 폴더에서 Compose)

현재는 Docker Compose 기동 시 **스토리지만 아니라 Kong API Gateway 및 소비시스템 Mock 3종, 공급 Mock 서비스, 그리고 모니터링 대시보드까지 모두 포함**되어 함께 기동됩니다.

```bat
cd /d path\to\dl-poc\to-be
docker compose up -d
```

### 💡 Docker 이미지 빌드 및 업데이트 가이드

이 프로젝트의 Mock 서비스들과 대시보드는 로컬 코드가 아닌 **Docker 이미지**를 빌드하여 컨테이너 환경에서 작동합니다. 

#### 1) 최초 실행할 때 (빌드 자동 수행)
- 처음 `docker compose up -d`를 실행하면, 별도의 빌드 명령어를 치지 않아도 Docker Compose가 각 폴더의 `Dockerfile`을 감지하여 **자동으로 이미지를 빌드한 뒤 실행**합니다.

#### 2) Mock 서비스 코드를 수정했을 때 (수동 재빌드 필요) ⚠️ 중요
- `consumer_mock_service`, `supply_mock_service`, `dashboard` 폴더 내의 **Python 코드나 설정을 수정한 경우**, 이미 생성된 이미지에는 그 변경사항이 반영되지 않습니다.
- 따라서 코드를 수정했다면 반드시 **재빌드 및 재구동** 명령을 실행해 주어야 합니다.
  ```bat
  # 방법 A: 전체 서비스를 재빌드하고 실행 (가장 안전하고 권장됨)
  docker compose up -d --build

  # 방법 B: 수정한 특정 서비스만 타겟팅해서 빠르게 재빌드 (시간 단축)
  docker compose up -d --build [서비스명]
  # 예: 소비 Mock 서비스 코드를 고쳤을 때
  docker compose up -d --build search-consumer-mock mobile-consumer-mock graph-consumer-mock
  ```

#### 3) 도커 상태 확인 및 문제 해결 꿀팁
- **컨테이너 상태 확인** (정상 구동 중인지 보기):
  ```bat
  docker compose ps
  ```
- **실시간 로그 확인** (에러 추적 및 디버깅):
  ```bat
  # 특정 서비스 로그 보기 (예: 대시보드 로그)
  docker compose logs -f dashboard
  
  # 전체 서비스 로그 보기
  docker compose logs -f
  ```
- **완전 초기화 후 재시작** (이상하게 꼬였을 때 최후의 수단):
  ```bat
  # 실행 중인 컨테이너를 중지하고 생성된 데이터 볼륨까지 깔끔하게 삭제
  docker compose down -v
  
  # 이후 다시 빌드하며 깨끗하게 구동
  docker compose up -d --build
  ```

> [!TIP]
> **Docker Compose에 탑재되어 자동으로 뜨는 서비스들:**
> - 데이터 레이크 인프라: MinIO (`9000`/`9001`), PostgreSQL (`5432`), Kafka (`9092`), ZooKeeper
> - 편의 도구: Kafka-UI (`8632`)
> - 보안 & 라우팅: **Kong API Gateway** (`8106`)
> - 공급 Mock 서비스: `supply-mock-service` (`8100` -> 컨테이너 `8000`)
> - 소비시스템 Mock 3종: 
>   - 검색 Mock 서비스 (`8101` -> 컨테이너 `8000`)
>   - 모바일 Mock 서비스 (`8102` -> 컨테이너 `8000`)
>   - Graph Mock 서비스 (`8103` -> 컨테이너 `8000`)
> - 모니터링 대시보드: `dashboard` (`8104` -> 컨테이너 `8000`)
> 


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



## end-to-end 최소 흐름 테스트 (터미널 1개 필요)

소비시스템 Mock들이 Kafka의 `mail.ready` 알림을 받으면 즉시 **게이트웨이(Kong)를 거쳐 데이터 서빙 레이어(`serving_service`)에 실제 데이터를 요청**합니다. `serving_service`를 포함한 모든 서비스들이 Docker Compose를 통해 백그라운드 컨테이너(Port 8105)로 구동 중이므로, 로컬 터미널에서는 Spark 수집 엔진만 가동해 주면 됩니다.

### 1) Spark Ingest 실행 (메일 수집 엔진)

**저장소 루트**에서:

```bat
cd /d path\to\dl-poc
venv\Scripts\python to-be\spark_ingest_mail.py
```

- 첫 실행 시 JAR 다운로드로 시간이 걸릴 수 있습니다.
- **Windows**: 기본 체크포인트는 MinIO `s3a://warehouse/.spark-checkpoints/mail-ingest` (로그에 `>>> checkpoint:`).
- **Linux/macOS**: 기본은 이 폴더 아래 `.spark-mail-ingest-cp/`.

### 2) 메일 생성 (E2E 테스트 트리거)

```bat
curl -X POST http://localhost:8100/mails -H "Content-Type: application/json" -d "{\"title\":\"t\",\"body\":\"b\",\"sender\":\"s@test.com\",\"receiver\":\"r@test.com\"}"
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
|`SPARK_STREAM_MAX_OFFSETS_PER_TRIGGER` | `5000` | 마이크로 배치(Trigger)가 Kafka에서 읽어올 최대 메시지 개수 |
|`DL_LIMITER_INITIAL_RPS` | `50` | SUPPLY_SERVICE 초당 호출 제한 시작값 |
|`DL_LIMITER_MIN_RPS` | `5` |  SUPPLY_SERVICE 초당 호출 최소 |
|`DL_LIMITER_MAX_RPS` | `200` | SUPPLY_SERVICE 초당 호출 제한 최대  |


## Windows 참고

- `HADOOP_HOME` 기본 `C:\hadoop` — Spark 스크립트가 참고합니다.
- Spark 실행은 **`venv\Scripts\python to-be\spark_ingest_mail.py`** 로 통일하면 `PYSPARK_PYTHON`이 맞습니다.

## 전체 To-Be 서비스 기동 요약 (대시보드 포함)

**`serving-service`, `supply-mock-service`, `search-consumer-mock`, `mobile-consumer-mock`, `graph-consumer-mock` 및 `dashboard`는 `docker compose up -d`를 통해 백그라운드 컨테이너로 항상 작동**하므로, 로컬 터미널에서 수동으로 임의의 서비스를 직접 띄울 필요가 전혀 없습니다.

기동 후 [대시보드(http://localhost:8104)](http://localhost:8104)에 접속하면, Docker Compose 내부에서 수신되는 소비시스템들의 실시간 로그 수신 상태 및 공급 통계를 직관적으로 모니터링할 수 있습니다.


## 문제가 자주 나는 곳

- Docker 미기동 → Docker Desktop 실행 후 `to-be`에서 `docker compose up -d`
- Windows NativeIO / 체크포인트 → 위 기본 MinIO 체크포인트 유지 또는 JDK 17
- Python worker 타임아웃 → 반드시 **루트 venv의 python**으로 `spark_ingest_mail.py` 실행

짧은 메모는 [notes.txt](notes.txt)에 있습니다.
