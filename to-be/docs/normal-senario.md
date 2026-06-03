# 평시 실시간 정상 수집 및 데이터 서빙 시나리오 명세 (Normal Ingestion & Serving Flow)

본 문서는 통합 데이터 플랫폼의 실시간 데이터 수집 및 데이터 서빙의 Happy Path 흐름을 설명하는 기술 문서입니다. 이 명세는 [normal-senario.txt](file:///home/ubuntu/a-cert/dl-poc/to-be/docs/normal-senario.txt)에 정의된 `sequencediagram.org` 기반 시퀀스 다이어그램을 바탕으로 각 단계를 세부적으로 설명합니다.



## 1. 시퀀스 다이어그램 소스 코드
본 시나리오의 시퀀스 다이어그램 스크립트는 [normal-senario.txt](file:///home/ubuntu/a-cert/dl-poc/to-be/docs/normal-senario.txt)에 저장되어 있으며, [SequenceDiagram.org](https://sequencediagram.org/) 에디터에 그대로 복사하여 시각화할 수 있습니다.



## 2. 단계별 상세 흐름 설명

### 1단계: 실시간 수집 및 적재 (Ingestion Phase)

이 단계에서는 원천 시스템의 데이터 변경 이벤트를 감지하여 데이터 레이크에 저장하고 Catalog 메타데이터를 갱신하기까지의 실시간 흐름을 다룹니다.

| 순서 | 컴포넌트 간 상호작용 | 동작 내용 |
|:---:|:---|:---|
| **1** | `Knox` $\rightarrow$ `Inbound` | **변경 이벤트 발행 (mail_id)**<br>원천 시스템(`Knox`)에서 데이터 변경이 발생하면 데이터 식별자(`mail_id`)를 포함한 경량의 변경 이벤트를 Kafka의 `Inbound Topic`으로 비동기 발행합니다. |
| **2** | `Ingest` $\rightarrow$ `Inbound` | **배치 이벤트 컨슘 (raw_json)**<br>`spark-ingest` 수집기가 Kafka 브로커로부터 배치 단위로 이벤트를 컨슘합니다. |
| **3** | `Ingest` (내부) | **스키마 검증, 중복 배제 및 처리 속도 조절**<br>컨슘한 메시지의 JSON 스키마를 유효성 검증하고, 동일 배치 내 중복된 `mail_id`를 배제(`dedupe_events`)합니다. 또한 `DynamicRateLimiter.throttle()`을 실행하여 원천 시스템의 부하를 고려해 수집 속도(RPS)를 제어합니다. |
| **4** | `Ingest` $\rightarrow$ `Knox` | **원천 데이터 상세 조회**<br>식별자(`mail_id`)를 활용하여 Knox API(`GET /mails/{mail_id}`)를 동기식으로 호출해 대용량의 raw JSON 데이터를 획득합니다. 조회가 성공하면 `DynamicRateLimiter.report_success()`를 호출하여 스로틀링 임계치를 조절합니다. |
| **5** | `Ingest` (내부) | **정제 및 비식별화**<br>비즈니스 유효성 검증과 텍스트 정제를 거친 후, 개인정보(PII) 대상 필드를 단방향 또는 양방향 암호화 처리합니다. |
| **6** | `Ingest` $\rightarrow$ `Catalog` | **버전 비교 조회 (SELECT event_version)**<br>동일 데이터에 대한 처리 순서 뒤바뀜을 방지하기 위해, PostgreSQL 카탈로그 DB(Primary)에 접속하여 기존 적재 데이터의 버전을 조회합니다. |
| **7** | `Ingest` (내부) | **버전 검증 (drop_stale)**<br>조회한 기존 버전과 현재 이벤트의 버전을 비교하여, 이미 더 최신 데이터가 적재되어 있다면 현재 이벤트를 버리는(Drop) 정합성 제어를 수행합니다. |
| **8** | `Ingest` $\rightarrow$ `MinIO` | **데이터 레이크 적재 (S3 API)**<br>정제가 완료된 대용량 데이터를 Apache Iceberg의 물리 파일 형식인 Parquet로 MinIO 스토리지(Silver/Gold 영역)에 저장합니다. |
| **9** | `Ingest` $\rightarrow$ `Catalog` | **Catalog 메타데이터 업데이트 (JDBC)**<br>트랜잭션 일관성을 확보하기 위해 PostgreSQL Iceberg Catalog DB에 최신 메타데이터 스냅샷 경로를 갱신 및 커밋합니다. |
| **10** | `Ingest` $\rightarrow$ `Outbound` | **적재 완료 알림 발행 (mail.ready)**<br>모든 수집 및 적재 절차가 완료되면 Kafka `Outbound Topic`에 `mail_id`와 `ready` 상태를 담은 경량 알림(Claim-Check)을 발행하고 수집 트랜잭션을 마칩니다. |



### 2단계: 온디맨드 데이터 서빙 (Serving Phase)

적재 완료 알림을 트리거로 삼아 소비 시스템이 외부 API Gateway 및 Serving 서비스를 통해 안전하게 정제된 비식별 데이터를 동적으로 조회하는 흐름입니다.

| 순서 | 컴포넌트 간 상호작용 | 동작 내용 |
|:---:|:---|:---|
| **11** | `Consumer` $\rightarrow$ `Outbound` | **적재 완료 알림 컨슘**<br>소비 시스템(검색, 모바일 등)이 `Outbound Topic`으로부터 특정 데이터의 적재 완료 알림을 받아 후속 비즈니스 로직을 트리거합니다. |
| **12** | `Consumer` $\rightarrow$ `Kong` | **데이터 조회 요청 (GET /mails/{mail_id})**<br>소비 시스템이 `api-gateway (Kong)`에 인증 토큰/API Key 헤더를 포함하여 동기식으로 상세 데이터 조회를 요청합니다. |
| **13** | `Kong` (내부) | **게이트웨이 통제**<br>Kong API Gateway 전면에서 API Key 인가 여부를 판단하고, 디바이스/사용자별 Dynamic Rate Limiting을 통제합니다. |
| **14** | `Kong` $\rightarrow$ `Serving` | **요청 라우팅**<br>검증된 요청을 실제 비즈니스 로직을 처리하는 `serving-service` 인스턴스로 라우팅합니다. |
| **15** | `Serving` (내부) | **ACL 검증 (Access Control List)**<br>**`serving-service`가 직접** 호출자에 대한 세부 리소스 접근 권한(ACL)을 검증하여 보안 요건을 통제합니다. |
| **16** | `Serving` $\rightarrow$ `Catalog` | **Catalog 조회 (JDBC - Read Replica)**<br>수집 세션과의 경합 및 락 지연을 피하기 위해 PostgreSQL의 **Read Replica DB**에 연결하여 Gold 테이블 구조와 최신 스냅샷 메타데이터 위치를 조회합니다. |
| **17** | `Serving` $\rightarrow$ `MinIO` | **Iceberg table.scan() 요청 (S3 API)**<br>PyIceberg 라이브러리의 `table.scan()` 기능을 사용하여 MinIO 스토리지를 호출합니다. |
| **18** | `MinIO` (내부) | **S3 Storage 필터 푸시다운**<br>불필요한 전체 파일을 메모리에 로드하지 않고, S3 스토리지 수준에서 조건에 맞는 대상 Parquet 슬롯만을 다이렉트로 스캔하여 `serving-service`에 메모리 DataFrame 형태로 고속 반환합니다. |
| **19** | `Serving` $\rightarrow$ `Kong` $\rightarrow$ `Consumer` | **최종 비식별화 데이터 반송**<br>`serving-service`가 데이터를 JSON 형태로 가공하여 반환하며, 최종적으로 Kong 게이트웨이를 거쳐 소비 시스템으로 전송됩니다.



## 3. 핵심 아키텍처적 포인트

* **Claim-Check 패턴 적용:** 
  1MB를 초과할 수 있는 대용량 이메일/문서 데이터를 Kafka 메시지에 직접 담지 않고, Kafka에는 오직 식별키(`mail_id`)만 발행합니다. 상세 내용은 REST API 및 S3 API를 통해 동기적으로 풀링(Pull)함으로써 Kafka 클러스터의 디스크 및 네트워크 병목을 원천 방지합니다.
* **보안 통제의 이원화:**
  * **API Gateway (Kong):** 인증(Authentication), 기본적인 인가 및 트래픽 제어(Rate Limiting) 담당.
  * **Serving Service:** 비즈니스와 밀접하게 연관된 상세 자원 접근 권한(**ACL 검증**) 및 비식별화(PII masking) 제어 담당.
* **읽기/쓰기 카탈로그 분리:**
  * 데이터 적재 시에는 Catalog **Primary DB**를 업데이트하여 쓰기 일관성을 확보합니다.
  * 데이터 서빙(조회) 시에는 Catalog **Read Replica DB**를 활용하여 데이터 제공성능을 확보하고 트랜잭션 경합을 차단합니다.
