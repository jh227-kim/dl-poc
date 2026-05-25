# 📝 Pull Request Template

## 📌 PR 제목
`[Feat] Kong API Gateway 도입 및 소비시스템별 다중 API ACL 권한 통합 구축`

---

## 📌 PR 본문

### 1. 개요 (Overview)
기존에 개별 소비시스템(`mobile_service`, `search_service`, `graph_service`)이 데이터 서빙 레이어(`serving_service`)를 직접 호출하던 아키텍처에서 발생할 수 있는 보안 취약점과 자원 보호 문제를 해결하기 위해 **Kong API Gateway (DB-less)**를 도입하였습니다.

이번 작업을 통해 **1) 단일 진입점을 통한 API Key 통합 인증**, **2) 저장소 스캔 부하 분산 및 보호**, **3) 소비시스템별 다중 API 접근 권한 통제(ACL)** 및 **4) 복호화 특권 헤더 은닉** 체계를 성공적으로 구축하였습니다.

---

### 2. 주요 변경 사항 (Key Changes)
- **API Gateway 컨테이너 추가 (`docker-compose.yml`)**:
  - 선언적 DB-less 모드로 작동하는 `kong:latest` 이미지 기반 `api-gateway` 서비스 구축.
  - 컨테이너 내부에서 호스트의 서빙 레이어에 도달하기 위한 `host.docker.internal` 브리지 네트워크 연동.
- **선언적 라우팅 및 인가 설정 (`kong.yml`)**:
  - `/mails`, `/users`, `/statistics` 경로에 대한 API Key 검증(`key-auth`) 및 접근 통제(`acl`) 일괄 구축.
  - `mobile_service` 소비자 전용 복호화 마스터 키(`x-consumer-privileged-key`) 주입 플러그인 설정.
- **소비자 서비스 코드 보완 (`mobile_service`, `search_service`, `graph_service`)**:
  - 서빙 호출 대상을 Gateway 주소(`http://localhost:8106`)로 통일하고 요청 시 자신의 고유 `apikey` 헤더를 탑재하도록 보완.
- **서빙 서비스 검증 엔드포인트 신설 (`serving_service`)**:
  - 헤더 주입 및 인가 적절성을 모니터링하기 위한 `/mails/debug/headers` 디버그 API 추가.

---

### 📌 3. API별 소비시스템 접근 권한 매핑 표 (Authorization Matrix)
소비시스템별로 할당된 API Key 및 역할 그룹(ACL Group)에 따라 게이트웨이가 제어하는 최종 권한 매핑 정보입니다.

| API 엔드포인트 | 역할 (ACL Group) | 모바일 서비스 (`mobile`) | 검색 서비스 (`search`) | Graph 서비스 (`graph`) |
| :--- | :---: | :---: | :---: | :---: |
| **API Key (apikey)** | *-* | `mobile-secret-key-123` | `search-secret-key-456` | `graph-secret-key-789` |
| **`/mails`** (메일 조회) | `mails-group` | **허용 (복호화 평문)** | **허용 (비식별 암호화)** | **허용 (비식별 암호화)** |
| **`/users`** (유저 정보) | `users-group` | **허용** | **허용** | 🛑 **차단 (403 Forbidden)** |
| **`/statistics`** (통계) | `admin-group` | 🛑 **차단 (403 Forbidden)** | 🛑 **차단 (403 Forbidden)** | **허용** |

> [!NOTE]
> - **복호화 특권 정책**: `/mails` 호출 시 오직 **모바일 서비스**로의 요청인 경우에만 게이트웨이가 복호화 마스터 권한 헤더를 자동 탑재하며, 다른 시스템의 요청은 개인정보가 안전하게 보호된 상태(비식별 암호화 상태)로 반환됩니다.
> - **인증 정책**: 유효하지 않은 API Key 혹은 Key가 누락된 모든 요청은 진입점에서 즉시 🛑 **`401 Unauthorized`**로 차단됩니다.

---

### 4. 검증 결과 및 증적 (Verification & Test Results)

#### A. API Key 미인증 차단 (401)
```bash
$ curl -s -I http://localhost:8106/mails/debug/headers
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Key
Server: kong/3.9.1
```

#### B. 모바일 권한 특권 주입 동작 검증 (200 OK + Privileged)
```bash
$ curl -s http://localhost:8106/mails/debug/headers -H "apikey: mobile-secret-key-123"
{"x_consumer_privileged_key":"super-secret-privileged-key","is_privileged":true}
```

#### C. 검색/Graph 권한 일반 조회 검증 (200 OK + Standard)
```bash
$ curl -s http://localhost:8106/mails/debug/headers -H "apikey: search-secret-key-456"
{"x_consumer_privileged_key":null,"is_privileged":false}
```

#### D. 미허가 서비스 ACL 차단 검증 (403 Forbidden)
* **검색 서비스**가 비인가 경로인 `/statistics` 접근 시 차단:
```bash
$ curl -s -I http://localhost:8106/statistics -H "apikey: search-secret-key-456"
HTTP/1.1 403 Forbidden
Server: kong/3.9.1
```
