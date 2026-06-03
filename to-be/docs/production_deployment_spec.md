# Production Deployment Specification (운영 배포 정의서)

본 정의서는 [external_entities.md](file:///home/ubuntu/a-cert/dl-poc/to-be/docs/external_entities.md)의 원천 시스템별 이벤트 생산 데이터를 토대로 산정된 데이터 처리량 및 스토리지 사용량을 기반으로 설계되었습니다. 대규모 엔터프라이즈 환경에서의 무중단 고가용성(HA), 자원 예측 가능성(QoS Guaranteed), 물리적 격리를 지원하기 위한 Kubernetes 및 Kafka 클러스터 사이징 가이드라인을 제공합니다.



## 1. 데이터 트래픽 분석 및 인프라 사이징 근거

### A. 원천 이벤트 데이터 생산 분석

| 원천 서비스 | 월 발생 건수 | 건당 평균 크기 | 월간 총 데이터량 | 트래픽 비중 |
| :--- | ---: | ---: | ---: | ---: |
| **메일 서비스** | 16억 건 | 1 MB | **1,600 TB** | **99.9%** |
| 결재 서비스 | 600 건 | 5 MB | 0.003 TB | ~0% |
| 일정 서비스 | 600 건 | 1 MB | 0.0006 TB | ~0% |
| **합계** | **16억 600 건** | - | **≈ 1.6 PB** | **100%** |

> **메일 서비스**가 전체 트래픽의 99.9% 이상을 차지하므로 메일 트래픽을 주 기준으로 사이징을 진행합니다. 결재/일정 서비스는 월 수백 건 수준으로 무시 가능한 수치입니다.

### B. 피크타임 및 처리량(Throughput) 분석
* 전체 데이터의 **30%**가 피크 타임인 하루 2시간(오전 8-9시, 오후 1-2시)에 집중되어 발생합니다.
* **피크타임 데이터 발생 비율:** 월간 전체 1.6 PB 중 30%인 **480 TB**가 피크타임 총 60시간 동안 집중됩니다. (30일 $\times$ 2시간 = 60시간/월)
* **피크 타임 처리량:**
  * **시간당 데이터 속도:** $480\text{ TB} \div 60\text{시간} = 8\text{ TB/시간}$
  * **초당 데이터 속도 (Throughput):** $8,000\text{ GB} \div 3,600\text{초} \approx \mathbf{2.22\text{ GB/sec}}\ (17.78\text{ Gbps})$
  * **초당 처리 건수 (Peak TPS):** $480,000,000\text{건} \div 216,000\text{초} \approx \mathbf{2,222\text{ TPS}}$
* **평시(Non-Peak) 타임 처리량:**
  * 나머지 70%인 1,120 TB가 평시 총 660시간 동안 균등 분산됩니다.
  * **초당 데이터 속도 (Throughput):** $1,120\text{ TB} \div (660\text{시간} \times 3,600\text{초}) \approx \mathbf{471\text{ MB/sec}}\ (3.77\text{ Gbps})$
  * **초당 처리 건수 (Average TPS):** $1,120,000,000\text{건} \div 2,376,000\text{초} \approx \mathbf{471\text{ TPS}}$



## 2. Kubernetes 클러스터 하드웨어 요구사항

피크타임 2.22 GB/sec 트래픽과 CPU 집약적인 암호화 및 유효성 검증을 원활히 처리하기 위해 다음과 같이 클러스터를 구성해야 합니다.

### A. 네트워크 대역폭 요구사항
* **최소 내부 네트워크 요구사항:** 피크타임 Ingress 속도가 17.78 Gbps에 달하므로 클러스터 내 모든 노드는 최소 **25 Gbps** 혹은 **100 Gbps** 인터페이스를 갖춘 물리 장비로 구성되어야 합니다. (Kafka 복제 트래픽 3배를 고려하면 실질 대역폭은 50 Gbps 이상이 요구됨)

### B. 전체 클러스터 노드 스펙 및 구성
가상화로 인한 오버헤드를 배제하기 위하여 **Bare-Metal 쿠버네티스 노드** 사용을 적극 권장합니다.

본 정의서는 수집(Ingest) 워크로드 그룹과 조회(Serving) 워크로드 그룹을 물리 노드 수준에서 완전히 분리하는 **Ingest/Serving 이원화 아키텍처**를 채택합니다. 공유 인프라(Postgres, MinIO)는 별도 노드로 독립 운영합니다.

| 분류 | 노드 역할 | 노드 수 | CPU (Node당) | RAM (Node당) | 스토리지 (Node당) | 네트워크 성능 |
| :--- | :--- | :---: | :--- | :--- | :--- | :--- |
| **Control Plane** | 쿠버네티스 마스터 | 3 | 16 Cores | 32 GB | 200 GB SSD (OS용) | 10 Gbps |
| **Worker Nodes (Ingest)** | Ingest Kafka, Ingest Zookeeper, Spark Engine | 12 | 64 Cores | 256 GB | 60 TB NVMe (RAID 10) | 25 Gbps Dual (Bonding) |
| **Worker Nodes (Serving)** | Ready Kafka, Ready Zookeeper, serving-service, api-gateway | 6 | 16 Cores | 64 GB | 5 TB NVMe + 500 GB SSD | 25 Gbps |
| **Worker Nodes (Shared Infra)** | Postgres (Primary + Replica), MinIO | 4 | 32 Cores | 128 GB | 30 TB NVMe (RAID 10) | 25 Gbps |
| **합계** | - | **25** | **1,040 Cores** | **4,064 GB** | **870 TB NVMe + 3.6 TB SSD** | - |

#### 노드 그룹별 수용 워크로드 설명

| 노드 그룹 | 수용 워크로드 | 사이징 근거 |
| :--- | :--- | :--- |
| **Worker Nodes (Ingest)** 12대 | Ingest Kafka Broker × 7, Ingest Zookeeper × 3, Spark Driver × 1, Spark Executor × 36 (노드당 3개) | Spark Executor 1개당 16C/64Gi, 노드당 3개 = 48C/192Gi 점유. 64C/256Gi 노드 기준 12C/64Gi 여유 (OS·Kafka ZK 수용 가능). Kafka 7개 브로커를 7대 노드에 1개씩 분산. |
| **Worker Nodes (Serving)** 6대 | Ready Kafka Broker × 3, Ready Zookeeper × 3, serving-service × 3, api-gateway × 3 | Ready Kafka 1개당 8C/32Gi. serving-service·api-gateway 각 4C/8Gi × 3 = 24C/48Gi. 6대 노드로 Ready Kafka·Zookeeper·Serving Pod를 분산 수용. |
| **Worker Nodes (Shared Infra)** 4대 | Postgres Primary × 1, Postgres Read-Replica × 1, MinIO × 4노드 분산 | MinIO Multi-Node Erasure Coding 4대 이상 필수. Postgres Primary/Replica 이중화. |



## 3. Kubernetes Namespace 및 자원 할당 정책

클러스터 자원의 안정적인 고립을 보장하기 위해 다음과 같이 Namespace를 격리하고 자원 쿼터(Quota)를 제한합니다.

### A. Namespace별 정량적 자원 할당량 (ResourceQuota)

네임스페이스 간의 예기치 못한 자원 잠식(Resource Contention)을 방지하고 상호 간섭을 차단하기 위해, 각 네임스페이스별로 기동되는 핵심 컴포넌트의 CPU/Memory Limits 스펙의 총합에 **약 10%의 운영 안정 마진(Margin)**을 더해 다음과 같이 물리적 자원 쿼터 한도액을 정의합니다.

| 분류 | 네임스페이스 | 관리 대상 서비스 컴포넌트 | 정밀 합산 스펙 (Limits 합계) | 최종 ResourceQuota 할당 한도 |
| :--- | :--- | :--- | :--- | :--- |
| **공유 데이터** | **`ns-dl-infra`** | Postgres Primary(1), Postgres Read-Replica(1), MinIO(4), Kafka UI(1) | CPU 25 Cores <br>Memory 50 GiB | ➔ **CPU 30 Cores** <br>➔ **Memory 60 GiB** |
| **실시간 수집** | **`ns-dl-pipeline`** | Spark Ingest Driver(1), Spark Ingest Executors(36), spark-dlq-processor(1), Ingest Kafka Broker(7), Ingest Zookeeper(3) | CPU 706 Cores <br>Memory 2,796 GiB | ➔ **CPU 780 Cores** <br>➔ **Memory 3,100 GiB** |
| **조회/서비스** | **`ns-dl-serving`** | serving-service(3), api-gateway(3), Ready Kafka Broker(3), Ready Zookeeper(3) | CPU 60 Cores <br>Memory 168 GiB | ➔ **CPU 70 Cores** <br>➔ **Memory 190 GiB** |
| **테스트/모의** | **`ns-dl-mocks`** | supply-mock-service(1), dashboard(1), mock consumers(3) | CPU ~3 Cores <br>Memory ~1.5 GiB | ➔ **CPU 8 Cores** <br>➔ **Memory 4 GiB** <br>*(시뮬레이터 폭주 차단용)* |

### B. 자원 쿼터 (ResourceQuota) 운영 통제 정책

클러스터 운영자는 본 정의서에 제시된 네임스페이스별 자원 한도(ResourceQuota) 상한선을 시스템에 공식 등록하여 강력하게 통제합니다.
* **통제 목적**: 임의의 네임스페이스에서 비정상적으로 자원을 과도하게 요구하더라도 지정된 상한선(예: 실시간 수집망 CPU 600 Cores)을 절대 넘지 못하도록 시스템 레벨에서 원천 제어합니다. 이를 통해 대고객 서비스망(`ns-dl-serving`)을 위한 하드웨어 성능 영역을 항시 침범 불가능한 안전지대로 영구 사수합니다.

### C. 자원 한도 설정 범위 (LimitRange) 안정화 정책

모든 핵심 서비스 컨테이너가 자원 고갈 시 시스템에 의해 임의 축출(Eviction)되는 최악의 상용 장애를 방지하기 위해 네임스페이스 수준의 **자원 한도 설정 범위(LimitRange) 정책**을 수립합니다.

* **도입 정책의 핵심 효과**:
  1. **QoS Guaranteed(안정 보장 등급) 자동 강제**: 배포 엔지니어가 개별 서비스 배포 시 실수로 리소스 크기(Requests/Limits) 설정을 빠뜨리더라도, 시스템이 네임스페이스 레벨에서 **동일한 기본 리소스 요청값과 한계값(Requests = Limits)**을 자동으로 안전하게 강제 주입해 줍니다. 이를 통해 리소스 규격을 지정을 누락하더라도 자동으로 최고 등급의 '안정 보장(QoS Guaranteed) 자격'을 확보하여 예기치 못한 노드 축출 위험을 완벽히 방어합니다.
  2. **개별 컨테이너 자원 독점 방지**: 특정 단일 컨테이너가 시스템 한계 이상으로 자원을 무제한 소모하여 네임스페이스의 쿼터 전체를 순식간에 고갈시키는 사고를 미연에 제약하고, 쿼터 내에서 리소스가 평등하게 분배되도록 제어합니다.



## 4. 고가용성(HA) 및 토폴로지 설정 (Pod Anti-Affinity)

하드웨어 장애가 발생해도 서비스가 정지되지 않도록 **replica를 최소 2개 이상** 설정하고, 동일 서비스의 Pod들이 서로 다른 물리 노드에 분산되도록 **자체 안티어피니티(Self-Anti-Affinity)**를 강제 적용합니다.

### A. 자체 안티어피니티(Self-Anti-Affinity) 설정 정책 (하드 디바이스 수준 격리)
모든 컴포넌트의 Helm 차트 배포 시 물리 노드별 격리 규칙(물리 호스트명 기준의 Hard Anti-Affinity)을 적용합니다.
* **동작 목적**: 시스템이 동일 서비스의 복제본(Pod)들을 여러 물리 서버 노드에 강제로 나누어 배치하도록 통제합니다. 이를 통해 특정 서버 장치 장애 시에도 남은 복제본이 정상 작동하여 완벽한 서비스 무중단 고가용성(HA)을 보장합니다.

### B. 주요 서비스별 HA 설계 구성안

| 컴포넌트 명 | 최소 Replica | Affinity 강도 | HA 및 분산 배포 전략 |
| :--- | :---: | :--- | :--- |
| **Ingest Kafka Broker** | 7 | Hard (Required) | 브로커당 각기 다른 물리 Ingest 노드에 분산 배포. 복제 계수(RF) 3 설정. |
| **Ready Kafka Broker** | 3 | Hard (Required) | 브로커당 서로 다른 Serving 노드 배포. 복제 계수 3 설정. |
| **Ingest Zookeeper** | 3 | Hard (Required) | 각 Zookeeper Pod는 별도의 3개 Ingest 노드로 강제 격리되어 홀수 노드 쿼럼 구성. |
| **Ready Zookeeper** | 3 | Hard (Required) | 각 Zookeeper Pod는 별도의 3개 Serving 노드로 강제 격리되어 홀수 노드 쿼럼 구성. |
| **Spark Driver** | 1 | Soft (Preferred) | Active-Standby 이중화는 불필요하나 드라이버 노드 장애 시 재생성 스케줄링 수행. |
| **Spark Executor** | 36 | Soft (Preferred) | 12대의 Ingest 노드에 고르게 부하를 분산시키기 위해 노드당 3개 Executor 분산 배포. |
| **serving-service** | 3 | Hard (Required) | 3개의 독립적인 Pod를 3대의 Serving 노드에 강하게 Anti-Affinity로 분산. |
| **api-gateway (Kong)** | 3 | Hard (Required) | 로드밸런서의 엔드포인트 세 개가 물리 노드 장애 시에도 살아있도록 분산 배포. |

### C. 수집(Ingest) 그룹과 조회(Serving) 그룹 간의 전략적 물리 격리 (Node Taints & Tolerations)

대용량 데이터 수집 작업과 최종 사용자의 조회 성능은 서로 리소스 사용 패턴이 다르고 간섭을 최소화해야 하므로, **Inter-Pod Anti-Affinity 대신 노드 레벨의 격리 정책(Node Taints/Tolerations 및 Node Affinity)을 적용**하여 물리 노드 수준에서 완벽하게 분리 배포합니다.

> [!NOTE]
> **Inter-Pod Anti-Affinity를 사용하지 않는 이유:**
> Ingest 그룹과 Serving 그룹은 이미 서로 다른 전용 노드 그룹(`Worker Nodes (Ingest)` vs `Worker Nodes (Serving)`)으로 하드웨어 수준에서 격리되어 있습니다. 여기에 불필요하게 스케줄링 연산 비용이 매우 큰 `Inter-Pod Anti-Affinity` (특히 Hard 규칙)를 추가로 적용하면, 대량의 Spark Executor(36개)가 동적 생성/소멸될 때 쿠버네티스 스케줄러의 연산 오버헤드로 인해 스케줄링 지연(Latency)이 발생할 수 있습니다. 따라서 노드 격리가 확실하게 보장된 본 아키텍처에서는 이를 제거하고 노드 수준 격리 정책으로 일원화합니다.

#### 수집/조회 전략적 물리 격리 및 노드 격리 정책 통합 정의

| 격리 그룹 | 매핑 노드 그룹 | 노드 Taint 설정 | 배포 대상 Pod (Tolerations & Node Affinity 적용) | 대상 네임스페이스 | 그룹별 자원 특성 및 격리 목적 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Ingest 그룹** | **Worker Nodes (Ingest)** <br>(12대) | `dedicated=ingest:NoSchedule` | `spark-mail-ingest` (Driver + Executors), `spark-dlq-processor`, Ingest Kafka Broker × 7, Ingest Zookeeper × 3 | `ns-dl-pipeline` | **[Resource/Network Heavy]** <br>초당 2.22 GB(17.78 Gbps) 유입 데이터 검증·암호화로 극심한 CPU 부하 및 네트워크 대역폭 점유. **Hard 격리**를 통해 Serving 노드 성능 오염 원천 차단. |
| **Serving 그룹** | **Worker Nodes (Serving)** <br>(6대) | `dedicated=serving:NoSchedule` | `serving-service` × 3, `api-gateway` × 3, Ready Kafka Broker × 3, Ready Zookeeper × 3 | `ns-dl-serving` | **[Latency Sensitive]** <br>최종 데이터 소비자(Personal Agent, Copilot 등)에게 밀리초(ms) 지연 시간으로 데이터 서빙. **Hard 격리**를 통해 안정적인 대고객 Low-Latency 대역 확보. |
| **Shared Infra** | **Worker Nodes (Shared Infra)** <br>(4대) | `dedicated=shared-infra:NoSchedule` | Postgres Primary, Postgres Read-Replica, MinIO × 4 | `ns-dl-infra` | **[Data/Storage I/O]** <br>공유 데이터 스택(메타데이터 DB, 분산 오브젝트 스토리지). **Hard 격리**를 통해 수집/조회 컴포넌트와의 물리 I/O 경합 격리. |

> **모든 YAML 배포 매니페스트(Helm Value 포함) 작성 시, 해당 노드 그룹에 맞는 `nodeSelector` 또는 `nodeAffinity`와 `tolerations`가 1:1로 엄격히 매핑되도록 보장합니다.**



## 5. QoS (Quality of Service) Guaranteed 및 자원 산정

운영환경에서는 Pod의 자원 고갈로 인한 불시 축출(Eviction)을 완전 차단하기 위해 **모든 핵심 구성요소에 리소스 요청값(Requests)과 제한값(Limits)이 1:1로 정확하게 일치하는 QoS Guaranteed 클래스를 100% 적용**하여 물리적 연산 자원을 안정 보장(Lock-in)합니다.

### A. 핵심 구성요소별 세부 리소스 산정 (Guaranteed Class)

| 격리 그룹 | 컴포넌트 명 | Replica 수 | CPU (Req / Limit) | Memory (Req / Limit) | Storage (개당) | 비고 |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- |
| **Ingest** | **Ingest Kafka Broker** | 7 | 16 Cores / 16 Cores | 64 GiB / 64 GiB | 60 TB NVMe | JVM Heap 32GB + Page Cache 32GB. 7일 Retention 기준 총 1.23 PB 확보 |
| **Ingest** | **Ingest Zookeeper** | 3 | 4 Cores / 4 Cores | 8 GiB / 8 GiB | - | Ingest Kafka 전용 쿼럼 코디네이터 |
| **Ingest** | **Spark Ingest Driver** | 1 | 4 Cores / 4 Cores | 16 GiB / 16 GiB | - | Structured Streaming 작업 조율 |
| **Ingest** | **Spark Ingest Executor** | 36 | 16 Cores / 16 Cores | 64 GiB / 64 GiB | - | 이벤트 검증, AES 암호화, Iceberg 스트리밍 적재 |
| **Ingest** | **spark-dlq-processor** | 1 | 2 Cores / 2 Cores | 4 GiB / 4 GiB | - | DLQ 실패 이벤트 재처리 전용 프로세서 |
| **Serving** | **Ready Kafka Broker** | 3 | 8 Cores / 8 Cores | 32 GiB / 32 GiB | 5 TB NVMe | Ready 알림 용도 인덱스 키만 저장 (초경량) |
| **Serving** | **Ready Zookeeper** | 3 | 4 Cores / 4 Cores | 8 GiB / 8 GiB | - | Ready Kafka 전용 쿼럼 코디네이터 |
| **Serving** | **serving-service** | 3 | 4 Cores / 4 Cores | 8 GiB / 8 GiB | - | 대고객 조회 API (Latency Sensitive) |
| **Serving** | **api-gateway (Kong)** | 3 | 4 Cores / 4 Cores | 8 GiB / 8 GiB | - | API 관문, DB-Less Mode, Role-Based ACL |
| **Shared Infra** | **Postgres Primary** | 1 | 4 Cores / 4 Cores | 8 GiB / 8 GiB | - | Iceberg Catalog 메타데이터 Write |
| **Shared Infra** | **Postgres Read-Replica** | 1 | 4 Cores / 4 Cores | 8 GiB / 8 GiB | - | Iceberg Catalog 메타데이터 Read-Only |
| **Shared Infra** | **MinIO** | 4 | 4 Cores / 4 Cores | 8 GiB / 8 GiB | 30 TB NVMe | Multi-Node Erasure Coding 분산 |
| **Shared Infra** | **kafka-ui** | 1 | 1 Core / 1 Core | 2 GiB / 2 GiB | - | Kafka 클러스터 모니터링 웹 콘솔 |



## 6. 테스트용 시뮬레이터 및 대시보드 최소화 정책 (ns-dl-mocks)

테스트 및 개발 모니터링용 시뮬레이터 컴포넌트들은 상용 하드웨어 자원의 낭비를 막고 물리적 격리 원칙을 준수하기 위해 다음과 같이 배포 및 스케줄링 정책을 규정합니다.

### A. 운영(Production) 클러스터 배포 원칙
* **기본 배포 정책: [배포 제외 (Exclusion by Default)]**
  * 부하 발생기(`supply-mock-service`) 및 테스트용 소비자(`mock consumers`)는 운영 서버의 오작동 및 Noisy Neighbor 유발을 완벽히 차단하기 위해 **실운영(Production) 클러스터 배포 시 비활성화(Helm `enabled: false`)하는 것을 원칙**으로 삼습니다. 해당 컴포넌트들은 오직 개발(Dev) 및 검증(Staging) 환경에서만 배포 및 활성화됩니다.

### B. 불가피한 상용 배포 시 노드 스케줄링 정책 (Fallback Guide)
* 만약 운영 환경 내 모니터링 대시보드 기동이나 상용 연동 테스트 목적 등으로 인해 배포가 불가피한 경우, 다음 규칙을 만족하여 **`Worker Nodes (Serving)` 노드 그룹의 유휴 공간에 격리 편입**시킵니다.
* **대상 노드 지정 및 Toleration:**
  * Serving 노드 그룹(6대)에 얹힐 수 있도록 **`dedicated=serving:NoSchedule` Toleration**을 부여하고, **Node Affinity**를 통해 Serving 노드에만 스케줄링되도록 강제합니다. (Ingest 노드 및 Shared Infra 노드 침범 금지)
* **QoS 및 스케줄링 자격 제한:**
  * 조회계 서비스의 안정성 사수를 위해 **QoS Burstable 등급**을 강제 주입하며, CPU/Memory Request를 극도로 낮게 주되 Limit를 타이트하게 잠가 성능 침해를 차단합니다.
  * **HA 정책:** Replica 1로 고정하여 단일 노드 점유를 최소화합니다.

### C. 세부 리소스 산정 및 스케줄링 규칙 표

| 서비스 컴포넌트 명 | 기본 Replica | CPU Request / Limit | Memory Request / Limit | 대상 물리 노드 그룹 | Tolerations & Affinity | 비고 |
| :--- | :---: | :---: | :---: | :--- | :--- | :--- |
| **supply-mock-service** | 1 | 100m / 500m | 128Mi / 256Mi | Worker Nodes (Serving) | `dedicated=serving:NoSchedule` | 테스트용 단일 이벤트 공급용 |
| **search-consumer-mock** | 1 | 100m / 500m | 128Mi / 256Mi | Worker Nodes (Serving) | `dedicated=serving:NoSchedule` | 테스트용 검색 연동 소비자 |
| **mobile-consumer-mock** | 1 | 100m / 500m | 128Mi / 256Mi | Worker Nodes (Serving) | `dedicated=serving:NoSchedule` | 테스트용 모바일 연동 소비자 |
| **graph-consumer-mock** | 1 | 100m / 500m | 128Mi / 256Mi | Worker Nodes (Serving) | `dedicated=serving:NoSchedule` | 테스트용 Graph 연동 소비자 |
| **dashboard** | 1 | 200m / 1000m | 256Mi / 512Mi | Worker Nodes (Serving) | `dedicated=serving:NoSchedule` | 테스트 모니터링 대시보드 웹앱 |



## 7. 기타 운영 배포 고려 사항

### A. 영속성 볼륨 (PV/PVC) 및 스토리지 클래스 (StorageClass)
* **Ingest Kafka/Zookeeper:** 스토리지 IOPS 병목을 예방하기 위해 Ingest 전용 노드의 로컬 NVMe 디스크를 쿠버네티스 PV로 직접 마운트하는 **Local SSD StorageClass**를 사용합니다.
* **Ready Kafka/Zookeeper:** Serving 전용 노드의 로컬 NVMe 디스크를 PV로 마운트하여 Ready 그룹의 스토리지 I/O를 Ingest 그룹과 물리적으로 완전 격리합니다.
* **Iceberg 적재용 오브젝트 스토리지:** 사내 온프레미스 오브젝트 스토리지(MinIO High Performance Multi-Node cluster)를 Shared Infra 노드에 단독 연동하여 쓰기 병목을 제거합니다.

### B. 파티션 및 로드밸런싱 최적화
* **`mail.events` 파티션 수:** 병렬로 가동되는 36개의 Spark Executor 내 태스크들이 대기 없이 1:1에 가깝게 대량 처리할 수 있도록, Ingest Topic의 파티션 수를 **256개 이상**으로 지정하여 동시 분산 로드밸런싱 성능을 극대화합니다.

### C. 커널 파라미터 튜닝 정책
모든 Kafka 워커 노드에는 안정적인 네트워크 전송과 대용량 파일 관리를 위해 다음과 같은 호스트 수준 튜닝이 필수적입니다.
* `vm.max_map_count = 262144` 이상 설정 (JVM 메모리 맵 한도 확장)
* `sysctl fs.file-max = 1000000` 설정 (파일 오픈 제약 극복)
* 모든 Pod에 `securityContext`를 통한 `runAsNonRoot: true` 적용으로 시스템 보안 컴플라이언스 준수.



## 8. 수집(Ingest)과 조회(Serving)의 추가 격리 및 이원화 아키텍처 가이드

본 플랫폼은 데이터의 실시간 일관성을 완벽히 만족하면서도, 피크타임(2.22 GB/s) 시 물리적인 저장 장치와 컴퓨팅 리소스의 Noisy Neighbor 병목 현상을 원천적으로 회피할 수 있는 현실적인 인프라 레벨의 물리 격리 방안을 정의합니다. (배포 복잡성 완화를 위해 추가 분산 캐시 스택 도입은 철저히 배제하였습니다.)

### A. PostgreSQL 데이터베이스 (Iceberg Catalog) 물리 격리
수집 영역과 조회 영역은 동일한 `iceberg_catalog` 테이블 메타데이터를 실시간으로 공유하여야만 정합성이 유지됩니다. 이를 위해 **PostgreSQL의 DB 커넥션 분리 및 이중화 구조**를 적용합니다. Postgres Primary와 Read-Replica는 **Shared Infra 노드**에 배포되어 두 그룹 모두로부터 네트워크 홉을 균등하게 유지합니다.

* **수집(Ingest) 측 경로 (Primary DB Connection)**:
  * Spark Ingest Engine 및 DLQ 프로세서는 메타데이터 작성 권한이 있는 **PostgreSQL Primary(Master) DB 인스턴스**에 연결되어 Iceberg 테이블의 신규 파티션과 스키마 변경 사항을 지속적으로 반영(Write)합니다.
* **조회(Serving) 측 경로 (Read-Replica DB Connection)**:
  * 대고객 API를 처리하는 Serving Service는 Primary DB를 실시간 스트리밍으로 동기화하는 **Read-Replica(읽기 전용) DB 인스턴스**에 연결되어 Iceberg 메타데이터 정보를 안전하게 읽어옵니다(Read).
* **아키텍처적 기대 효과**:
  * 피크 타임의 극심한 데이터 인입 시 발생하는 Catalog 테이블 락(Lock) 경합 및 CPU 부하가 조회 API의 지연시간(Latency)을 악화시키거나 DB Connection Timeout 장애를 유발하는 것을 원천 차단합니다.

### B. MinIO / 오브젝트 스토리지의 디스크 I/O 물리 스펙 최적화
수집과 조회는 동일한 Parquet 데이터 파일 버킷 공간을 바라보아야 합니다. 추가적인 분산 캐시(Redis, Alluxio 등)가 없는 상태에서 디스크 I/O 병목을 해결하기 위해 **스토리지 디바이스의 물리 사양을 고도화**하여 병목을 흡수합니다.

* **NVMe SSD RAID 10 고성능 설계**:
  * MinIO 물리 스토리지 노드는 초당 2.22 GB/s의 고속 쓰기와 대고객 무작위 읽기(Random Read)를 동시에 소화할 수 있도록 **NVMe SSD로 구성된 RAID 10(Mirroring + Striping)** 방식을 설계 규격으로 채택합니다.
* **Multi-Node Scale-Out 분산 배포**:
  * MinIO 클러스터를 최소 4대 이상의 독립 물리 Shared Infra 노드로 다중화(Erasure Coding 설정)하여 단일 디스크 컨트롤러에 걸리는 I/O 요청을 분산함으로써 I/O 대기시간(I/O Wait)을 최소화합니다.

### C. 네트워크 대역폭 및 서브넷(Subnet/VLAN) 격리
17.78 Gbps에 달하는 수집 네트워크 대역폭 점유가 Serving 조회 API 패킷 전송을 저해하지 않도록 **물리/논리적 네트워크 망을 완벽히 분리**합니다.

* **수집 내부 네트워크 (Ingest Subnet)**:
  * Ingest Kafka Broker, Spark Engine Pod, MinIO 백엔드 노드 간의 트래픽은 최소 **25/100 Gbps 대역의 전용 고속 내부 백본 스위치 및 Subnet(VLAN)**을 통해 유통되도록 설계합니다.
* **조회 외부 네트워크 (Serving Subnet)**:
  * Serving Service 및 API Gateway(Kong)가 대고객 클라이언트 및 외부 연동 시스템의 요청을 받고 응답하는 경로는 **25 Gbps 대역의 독립된 서비스망 Subnet**으로 분리 배포합니다.
* **효과**:
  * 원천 데이터 폭주 시 수집단의 네트워크 백플레인 대역폭 포화(Saturation) 현상이 대고객 서비스 조회망에 영향을 주지 않으므로, 대고객 API의 안정적인 가용 대역을 100% 보장합니다.



본 정의서는 대용량 데이터 파이프라인의 실시간 연계를 위한 물리 노드 인프라 설계 지침서로 활용될 수 있습니다.
