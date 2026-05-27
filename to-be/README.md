# To-Be 데이터 파이프라인 수집 플랫폼 (Kafka → Spark → Iceberg)

본 플랫폼은 원천 시스템에서 발생하는 데이터 변경 이벤트를 유실 없이 순차적으로 수집하고, 실시간 네트워크 장애 극복을 위한 **자가 복구형 DLQ(Dead Letter Queue) 시스템** 및 **Apache Iceberg 분산 테이블** 적재를 완벽하게 제어하는 고성능 데이터 파이프라인 플랫폼입니다.

아키텍처 설계 배경 및 품질 속성에 대해서는 **[아키텍처 결정서 (ADR)](architecture_evaluation.md)**를 참고하세요.

---

## 1. 4중 격리 디렉터리 아키텍처 구조

아키텍처의 역할 격리와 컨테이너 빌드 환경 최적화를 위해 다음과 같은 **4중 격리(apps / mocks / libs / scripts)** 디렉터리 아키텍처로 선명하게 정돈되었습니다.

| 대분류 디렉터리 | 세부 구성요소 | 역할 및 설명 |
|:---|:---|:---|
| **`apps/`** <br>(운영 코어 앱) | `serving_service/` | Python 네이티브(PyIceberg)로 Parquet 데이터를 고속 서빙하는 코어 API |
| | `spark/` | Spark Structured Streaming 실시간 수집 엔진 및 DLQ 재처리 데몬 |
| **`mocks/`** <br>(테스트 시뮬레이터) | `dashboard/` | 수집 속도 및 실시간 적재 흐름 모니터링 대시보드 화면 |
| | `consumer_mock_service/` | `mail.ready` 수집 완료 알림을 받아 서빙 API를 자동 호출하는 Mock 소비군 |
| | `supply_mock_service/` | E2E 테스트를 트리거하기 위해 모의 메일 이벤트를 발행하는 공급 서비스 |
| **`libs/`** <br>(공유 라이브러리) | `datalake/` | 어댑터, 암호화(PII), canonical 정제 필터가 내장된 공통 핵심 소스 패키지 |
| **`scripts/`** <br>(개발자 유틸리티) | `sanity_check.py` | 로컬 Iceberg + MinIO 연결성을 빠르게 검증하는 sanity 스크립트 |
| | `test_dlq.py` | Kafka 고의 장애를 통한 실시간 DLQ 유입 및 자가 복구 검증 모듈 |
| | `mail_event_publish.py` | Kafka 브로커에 1회성 이벤트를 직송출하는 테스트 도구 |
| | `kafka-smoke.*` | OS별 로컬 카프카 토픽 검증용 스크립트 모음 |
| **`helm/`** <br>(K8s 배포 패키지) | `dl-poc/` | 네임스페이스 격리 및 GCP Managed/로컬 인프라를 동적 스위칭하는 Helm Chart |

---

## 2. 로컬 개발 환경 실행 (Docker Compose)

로컬 빌드 환경 및 모의 컴포넌트들을 한 번에 기동시켜 테스트합니다.

### 1) 인프라 및 전체 서비스 기동
`to-be` 폴더에서 아래 명령어를 구동하면 백그라운드에 인프라와 어플리케이션 및 모의 서비스 13종이 일제히 기동됩니다.
```bash
$ docker compose up -d --build
```
> [!TIP]
> **로컬 구동 시 자동 매핑되는 포트 목록:**
> * 데이터레이크 스토리지: MinIO Console (`http://localhost:9001`) / API (`9000`)
> * 카프카 모니터링: Kafka-UI (`http://localhost:8632`)
> * API Gateway Proxy: Kong API Gateway (`http://localhost:8106`)
> * 모니터링 화면: POC Dashboard (`http://localhost:8104`)

### 2) MinIO 초기 버킷 준비 (최초 1회)
* [MinIO Console](http://localhost:9001)에 접속합니다 (아이디: `admin` / 비밀번호: `password`).
* **Create Bucket**을 클릭하여 **`warehouse`**라는 이름의 버킷을 신규 생성합니다.

### 3) E2E 수집 흐름 수동 기동 및 테스트
로컬 터미널에서 Spark 스트리밍 엔진을 구동하고 수동으로 API를 호출하여 데이터 흐름을 E2E로 검증합니다.

```bash
# 1. 메일 수집 Spark Ingest 엔진 기동 (저장소 루트 기준)
$ venv/Scripts/python to-be/apps/spark/spark_ingest_mail.py

# 2. 다른 터미널에서 모의 메일 생성 트리거 송출 (E2E 이벤트 발행)
$ curl -X POST http://localhost:8100/mails \
  -H "Content-Type: application/json" \
  -d "{\"title\":\"Hello\",\"body\":\"This is PoC body\",\"sender\":\"s@test.com\",\"receiver\":\"r@test.com\"}"
```
* **결과 확인**: Spark 터미널 로그에 `[ingest] accepted=1` 통계가 찍히고, [Dashboard](http://localhost:8104)에 실시간 데이터 카운트가 정상 업데이트됩니다.

---

## 3. Kubernetes 운영 환경 배포 (Helm Chart)

작성된 **[dl-poc]** Helm Chart를 사용하면 네임스페이스 격리는 물론, 온프레미스/GCP Managed 클라우드 환경으로의 다이내믹 이식이 가능합니다.

### 1) Helm 설치 및 네임스페이스 격리 배포
```bash
# dl-poc 전용 격리 네임스페이스를 생성하며 Helm 차트 설치
$ helm install dl-poc ./helm/dl-poc -n dl-poc --create-namespace
```

### 2) GCP 클라우드 서비스 연동 토글 스위치 (`values.yaml`)
PostgreSQL, MinIO, Kafka를 K8s 클러스터 내부에 파드로 직접 프로비저닝할지, 아니면 GCP Cloud SQL/GCS/Managed Kafka를 바인딩하여 사용할지 스위치 제어 한 개로 완벽 대응합니다.
* **GCP Managed 연동 시 values.yaml 설정 예시:**
  ```yaml
  postgres:
    enabled: false # 로컬 Postgres 설치 안 함
  minio:
    enabled: false # 로컬 MinIO 설치 안 함
  kafka:
    enabled: false # 로컬 Kafka/ZK 설치 안 함

  externalPostgres:
    host: "gcp-cloud-sql-ip"
    user: "postgres-admin-user"
    # ... 외부 인프라 접속 정보 입력
  ```

### 3) 통합 Ingress 접속 도메인 바인딩
헬름 차트 내부에 통합 Ingress 컨트롤러(`ingress.yaml`)가 정의되어 있어 브라우저 접속 엔드포인트들을 아래 서브도메인 형태로 다이내믹 분기합니다:
* MinIO Console: `minio.dl-poc.local`
* Kafka-UI 모니터링: `kafka-ui.dl-poc.local`
* Kong API Gateway: `api.dl-poc.local`
* POC 대시보드: `dashboard.dl-poc.local`

**로컬 DNS 바인딩 가이드 (/etc/hosts):**
```bash
# Ingress External IP 조회
$ kubectl get ingress -n dl-poc

# 호스트 OS의 /etc/hosts 파일 맨 하단에 아래 라인 추가
<조회된_INGRESS_IP> minio.dl-poc.local kafka-ui.dl-poc.local api.dl-poc.local dashboard.dl-poc.local
```
바인딩 후 브라우저를 켜고 `http://dashboard.dl-poc.local` 과 같이 도메인 주소로 즉시 테스트가 가능합니다.

---

## 4. 신규 수집 시스템 추가 가이드 (일정, 결재, 게시 등)

본 플랫폼은 어댑터 패턴 기반으로 격리 설계되어 있어 새로운 데이터 공급 시스템을 손쉽게 확장할 수 있습니다. 예를 들어 **일정(Schedule)** 시스템을 추가하는 절차는 다음과 같습니다.

### 1단계: 도메인 어댑터 구현 (`libs/datalake/adapters/`)
`libs/datalake/adapters/schedule.py` 파일을 생성하고 `SupplyAdapter` 인터페이스를 구현합니다.
- `source`: `"schedule-supply-default"`
- `ingest_topic`: `"schedule.events"`
- `ready_topic`: `"schedule.ready"`
- `dlq_topic`: `"schedule.events.dlq"`
- `entity_id_field`: `"schedule_id"`
- `fetch_from_supply()` 및 `validate_raw()` 등 추상 메서드들의 비즈니스 로직 작성.

### 2단계: 도메인 Iceberg 저장 레이어 구현 (`libs/datalake/layers/`)
`libs/datalake/layers/schedule.py` 파일을 생성하고 일정 도메인에 대한 Iceberg 테이블 스키마 DDL 정의 및 `save_to_silver`, `save_to_gold`, `save_to_quarantine` 등의 영속화 헬퍼 함수를 구현합니다.

### 3단계: Spark 스트리밍 실행기 생성 (`apps/spark/`)
`apps/spark/spark_ingest_schedule.py` 파일을 작성하여 일정 어댑터(`ScheduleAdapter`)를 주입하고 스트리밍 질의를 기동시킵니다.
```python
from datalake.adapters.schedule import ScheduleAdapter
from datalake.ingest_runner import run_streaming_ingest

def main():
    adapter = ScheduleAdapter()
    run_streaming_ingest(adapter, checkpoint="s3a://warehouse/.spark-checkpoints/schedule-ingest")
```

### 4단계: 다중 테넌트 DLQ 프로세서 토픽 설정
`spark-dlq-processor`의 환경 변수 `KAFKA_DLQ_TOPIC`에 콤마로 구분하여 신규 DLQ 토픽을 등록합니다.
- **Docker Compose (`docker-compose.yml`):**
  ```yaml
  spark-dlq-processor:
    environment:
      - KAFKA_BOOTSTRAP_SERVERS=kafka:29092
      - KAFKA_DLQ_TOPIC=mail.events.dlq,schedule.events.dlq,approval.events.dlq,board.events.dlq
  ```
설정 등록 후 재시작 시, 프로세서 데몬은 자동으로 추가된 모든 DLQ 토픽들을 동시 모니터링하며, 장애 복구 시 원천 토픽으로 메시지를 동적 재송출(Redrive)합니다.

