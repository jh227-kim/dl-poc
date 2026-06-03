# 외부 API 장애 격리 및 자가 복구 시나리오 명세 (Self-Healing Flow)

본 문서는 통합 데이터 플랫폼의 핵심 기능 중 하나인 **자가 복구(Self-Healing) 및 에러 격리 파이프라인**의 흐름을 설명하는 기술 문서입니다. 이 명세는 [dql-senariotxt](file:///home/ubuntu/a-cert/dl-poc/to-be/docs/dql-senariotxt)에 정의된 `sequencediagram.org` 기반 시퀀스 다이어그램을 바탕으로 각 단계를 세부적으로 설명합니다.



## 1. 시퀀스 다이어그램 소스 코드
본 시나리오의 시퀀스 다이어그램 스크립트는 [dql-senariotxt](file:///home/ubuntu/a-cert/dl-poc/to-be/docs/dql-senariotxt)에 저장되어 있으며, [SequenceDiagram.org](https://sequencediagram.org/) 에디터에 그대로 복사하여 시각화할 수 있습니다.



## 2. 단계별 상세 흐름 설명

### 1단계: 외부 API 장애 상황 및 격리 (API Failure & Isolation)

원천 Knox 서비스의 순간 장애나 타임아웃 발생 시 메인 수집 엔진(`spark-ingest`)이 이를 격리하고 이벤트를 DLQ로 우회하는 흐름입니다.

| 순서 | 컴포넌트 간 상호작용 | 동작 내용 |
|:---:|:---|:---|
| **1** | `Ingest` $\rightarrow$ `Inbound` | **배치 이벤트 컨슘 (raw_json)**<br>`spark-ingest`가 `Inbound Topic`에서 원천 시스템 변경 이벤트 패킷을 컨슘하고 활성화 상태가 됩니다. |
| **2** | `Ingest` $\rightarrow$ `Knox` | **상세 조회 시도 및 실패**<br>Knox 서비스 상세 API(`GET /mails/{mail_id}`)를 호출하지만 원천 서버 장애로 인해 `HTTP 500` 또는 `Connection Timeout` 장애가 발생합니다. |
| **-** | `Ingest` (내부) | **수집 스로틀링 작동 (DynamicRateLimiter)**<br>장애 또는 지연이 검출되면 `DynamicRateLimiter.report_failure_or_latency()`를 실행하여 즉시 플랫폼 전체의 인입 속도(RPS)를 낮춰 장애 확산을 방어합니다. |
| **3** | `Ingest` $\rightarrow$ `MinIO` | **격리 영역 저장 (Quarantine 쓰기)**<br>정상 가공 처리가 불가능하므로, 실패한 원본 메시지를 MinIO의 **Quarantine(격리) 영역**에 Parquet 형식 파일로 다이렉트 기록합니다. |
| **4** | `Ingest` $\rightarrow$ `Catalog` | **Quarantine 메타데이터 기록 (JDBC)**<br>추후 추적 및 분석을 위해 PostgreSQL 카탈로그 DB에 격리 적재 이력 메타데이터를 등록합니다. |
| **5** | `Ingest` $\rightarrow$ `DLQ` | **DLQ 메시지 발행 (retry_count=0)**<br>해당 실패 이벤트를 재시도 처리를 위해 `DLQ Topic (mail.events.dlq)`으로 전송합니다. 최초 실패이므로 `retry_count=0` 및 구체적인 에러 로그(`fetch failed`)를 페이로드에 동봉하며, 이후 메인 수집 라이프라인은 활성화 상태를 종료합니다. |



### 2단계: 자가 복구 프로세서 구동 (Self-Healing Daemon Run)

독립된 프로세스인 `spark-dlq-processor`가 DLQ에 적재된 장애 건을 분석하고 적절한 지연 대기 후 재진입(Redrive) 시키는 자가 복구 흐름입니다.

| 순서 | 컴포넌트 간 상호작용 | 동작 내용 |
|:---:|:---|:---|
| **6** | `DLQProc` $\rightarrow$ `DLQ` | **DLQ 토픽 컨슘**<br>자가복구 데몬인 `spark-dlq-processor`가 `DLQ Topic`에서 에러 원인이 담긴 장애 메시지를 컨슘합니다. |
| **7** | `DLQProc` (내부) | **오류 분석, 재시도 검증 및 대기 (Backoff)**<br>장애 원인이 일시적인 통신/네트워크 에러인지 검증하고, 현재 재시도 횟수(`retry_count <= 3`)를 판별합니다. 유효성 검증을 통과하면 즉각적인 재전송으로 인한 원천 시스템 부하 가중을 차단하기 위해 **5초의 대기 지연(Backoff)**을 수행합니다. |
| **8** | `DLQProc` $\rightarrow$ `Inbound` | **Inbound Topic 재진입 (Redrive)**<br>지연 시간이 만료되면 페이로드 내에 재시도 횟수를 `_retry_count=1`로 1씩 증가시킨 뒤, 메시지 정렬(Ordering)을 위해 원래의 Key(`mail_id`) 바이트를 유지한 상태로 `Inbound Topic`에 이벤트를 밀어 넣습니다(Redrive). |



### 3단계: 공급 API 정상 복구 이후 (After API Recovery & Complete)

원천 서비스 장애가 해소된 후, 재진입한 이벤트가 메인 파이프라인에서 완수되는 최종 단계입니다.

| 순서 | 컴포넌트 간 상호작용 | 동작 내용 |
|:---:|:---|:---|
| **9** | `Ingest` $\rightarrow$ `Inbound` | **재인입 이벤트 컨슘**<br>`spark-ingest`가 `_retry_count=1` 마킹이 추가된 재인입 이벤트를 Inbound Topic으로부터 컨슘합니다. |
| **10** | `Ingest` $\rightarrow$ `Knox` | **상세 조회 재시도 (GET /mails/{mail_id})**<br>정상화된 Knox 서비스에 다시 데이터 상세 조회를 동기적으로 요청하여 `200 OK` 정상 데이터 응답을 획득합니다. |
| **11** | `Ingest` $\rightarrow$ `MinIO` | **Silver/Gold 영역 적재 완료 (S3 API)**<br>취득한 데이터를 정제 및 비식별화 과정을 거쳐 MinIO의 정상 적재 영역(Silver/Gold)에 Parquet 쓰기 완료하고 최종 트랜잭션을 마칩니다. |



## 3. 핵심 아키텍처적 포인트

* **독립형 DLQ Processor 구조:**
  * 메인 스트리밍 엔진(`spark-ingest`)과 독립적인 데몬(`spark-dlq-processor`)으로 기동하여, 재처리 지연 대기(5초 Backoff) 시에도 메인 수집 파이프라인에 병목 현상(Head-of-Line Blocking)이 전파되지 않도록 설계적 **결합도를 격리**하였습니다.
* **에러 격리 및 관찰성 (Quarantine & Meta-logging):**
  * 일시적인 네트워크 장애뿐만 아니라 영구적 장애 발생 가능성에 대비해 MinIO 스토리지의 `Quarantine` 디렉토리에 대상 데이터를 물리적으로 즉각 백업하고, PostgreSQL DB에 기록함으로써 운영자 검토가 용이하게 조치되었습니다.
* **순서 정합성 및 메시지 키 보존:**
  * 재처리 흐름으로 우회되더라도 Kafka 메시지 Key인 `mail_id` 바이트를 그대로 유지한 채 Inbound로 유입함으로써 파티션 단위 메시지 보장 메커니즘을 훼손하지 않습니다.
* **백오프 및 최대 재시도 제약:**
  * 무한 루프 장애 유발을 방지하기 위해 `MAX_RETRIES = 3` 상한과 지수 또는 고정 백오프(`5s`)가 적용되었습니다.
