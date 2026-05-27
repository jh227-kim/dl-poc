# Data lake ingest (adapter)

To-Be 데이터 레이크 **ingest** 코드는 `libs/datalake/` 아래에 모여 있습니다.  
**한 플랫폼** 안에 메일·결재·일정 등 **서비스(도메인)** 가 여러 개 있고, Kafka → Spark → Iceberg(silver/gold) → ready → Serving 흐름은 **공통**, **도메인별 차이는 adapter + layers** 로 plug-in 합니다.

실행 절차(Docker, Supply, Spark 명령)는 [`../README.md`](../README.md)를 참고하세요.

---

## 왜 구조를 바꿨는지

### Before (리팩터 전)

- `spark_ingest_mail.py`에 Spark 설정, Supply Pull, 검증, layers 저장이 **한 파일에 혼재**
- `layers.py`가 **메일 전용**만 존재
- 새 도메인(결재·일정 등) 추가 시 ingest 코어까지 **복붙·수정** 필요

### After (리팩터 후)

| 구분 | 위치 |
|------|------|
| **공통 플랫폼** | `ingest_core`, `ingest_runner`, `spark_runtime`, `checkpoint` |
| **메일 plug-in** | `adapters/mail.py`, `layers/mail.py` |
| **메일 진입점** | `../../apps/spark/spark_ingest_mail.py` (~45줄) |

### 목표

- ingest **파이프라인은 고정**, 도메인은 **adapter + layers** 만 추가
- 도메인별 Spark job **분리** (재시작·장애 격리 — 한 job 재시작 시 다른 도메인 ingest 유지)
- **같은 도메인에 공급 여러 개** (source/registry)는 **범위外**

---

## 전체 흐름

```text
{domain}_service (공급)
    → Kafka {domain}.events
        → spark_ingest_{domain}.py
            → {Domain}Adapter
            → ingest_runner → ingest_core
                → Supply HTTP Pull
                → validate → refine → encrypt → silver/gold (또는 quarantine)
            → Kafka {domain}.ready
                → 소비 서비스 → Serving GET
```

### 메일 (현재 구현)

```text
Supply(8100) → mail.events → spark_ingest_mail.py → MailAdapter
    → local.silver/gold.mail → mail.ready → Serving GET /mails/{id}
```

---

## 디렉터리 / 파일 역할

| 경로 | 역할 |
|------|------|
| `adapters/base.py` | `SupplyAdapter` 인터페이스 |
| `adapters/common.py` | event action / version / datetime 공통 파싱 |
| `adapters/mail.py` | 메일 Supply Pull, validate, refine, canonical, encrypt, stale |
| `ingest_core.py` | `process_batch` — adapter 주입 **공통 파이프라인** |
| `ingest_runner.py` | Kafka readStream + foreachBatch 기동 |
| `spark_runtime.py` | SparkSession, Iceberg catalog, MinIO 공통 설정 |
| `checkpoint.py` | streaming checkpoint 경로 resolve |
| `layers/_common.py` | SQL literal, layer namespace 헬퍼 |
| `layers/mail.py` | `local.silver/gold/quarantine.mail` DDL·저장 |
| `encryption.py` | PII 필드 AES-256-GCM |
| `validator.py` | 메일 검증 (향후 `validators/{domain}.py` 분리 가능) |
| `refiners/mail.py` | 메일 HTML/MIME 본문 정제 |
| `../../apps/spark/spark_ingest_mail.py` | **메일 ingest 진입점** |

**import 호환:** `from datalake.layers import save_to_silver` — `layers/__init__.py`가 `layers/mail.py`를 re-export 합니다.

---

## ingest_core 파이프라인 (고정 순서)

모든 도메인 adapter는 아래 순서에 맞춰 `SupplyAdapter` 메서드를 구현합니다.

1. Kafka JSON → `adapter.parse_event_json`
2. `adapter.dedupe_events` (배치 내 동일 entity_id 최신 1건)
3. `adapter.is_delete` → `adapter.delete_entities`
4. UPSERT → `adapter.fetch_from_supply` (**Supply HTTP Pull**)
5. `adapter.validate_raw` — 실패 시 quarantine
6. `adapter.refine_raw` — HTML/MIME 정제 (기본 adapter는 no-op)
7. `adapter.to_canonical` → `adapter.encrypt_canonical`
8. `adapter.drop_stale` — stale는 quarantine
9. `adapter.save_accepted` (silver + gold)
10. `adapter.save_quarantine`
11. `adapter.ready_payload` → Kafka ready 토픽 publish

---

## 레이어 (Silver / Gold / Quarantine)

| Layer | 역할 (메일 PoC) |
|-------|------------------|
| **Silver** | 검증·정제·암호화 통과 canonical (`local.silver.mail`) |
| **Gold** | Serving·소비용 (`local.gold.mail`) — **현 PoC는 silver와 동일 row** |
| **Quarantine** | JSON 파싱 실패, Supply fetch 실패, 검증 실패, stale (Kafka DLQ 아님) |

- **DELETE** 이벤트: quarantine이 아니라 silver·gold에서 **물리 삭제**
- 향후: Gold 서빙 전용 shape 분리 가능

---

## 실행 (메일 — 변경 없음)

저장소 **루트**(`dl-poc`)에서:

```bat
venv\Scripts\python to-be\apps\spark\spark_ingest_mail.py
```

- Supply **8100**, Docker(Kafka / MinIO / Postgres), MinIO **`warehouse`** 버킷
- 성공 로그: `[ingest] epoch=... accepted=1 ...`

**소비 쪽:** adapter 리팩터와 무관 — `mail.ready` → Serving `GET http://localhost:8105/mails/{mail_id}`

---

## 새 도메인 추가 시

**`ingest_core`, `spark_runtime`, `ingest_runner`는 수정하지 않습니다.**

### 체크리스트

- [ ] `{domain}_service/` — API, `{domain}.events` 발행, `GET /.../{id}` (Spark Pull)
- [ ] `validators/{domain}.py` (또는 `validator_{domain}.py`)
- [ ] `layers/{domain}.py` — `local.silver/gold/quarantine.{domain}`
- [ ] `adapters/{domain}.py` — `SupplyAdapter` 구현
- [ ] `../../apps/spark/spark_ingest_{domain}.py` — adapter + checkpoint + `run_streaming_ingest`
- [ ] Kafka `{domain}.events`, `{domain}.ready`
- [ ] Serving `GET /.../{id}` → `local.gold.{domain}`
- [ ] (선택) 소비 — `{domain}.ready` 구독 + Serving GET

### 진입점 템플릿

```python
from pathlib import Path

from datalake.adapters.approval import ApprovalAdapter  # 예시
from datalake.checkpoint import resolve_checkpoint
from datalake.ingest_runner import run_streaming_ingest

def main() -> None:
    adapter = ApprovalAdapter()
    checkpoint = resolve_checkpoint(
        env_keys=("APPROVAL_INGEST_CHECKPOINT", "SPARK_CHECKPOINT_LOCATION"),
        default_win="s3a://warehouse/.spark-checkpoints/approval-ingest",
        default_local_name=".spark-approval-ingest-cp",
        base_dir=Path(__file__).resolve().parent,
    )
    run_streaming_ingest(adapter, checkpoint=checkpoint)

if __name__ == "__main__":
    main()
```

### 검증

- 이벤트/POST → Spark `[ingest] ... accepted=1`
- Serving GET 200
- **기존 메일** `spark_ingest_mail.py`는 **별도 프로세스** — 영향 없어야 함

---

## 암호화·Serving (참고)

- PII: `encryption.py` — `sender`, `receiver` 등 → `*_enc` (AES-256-GCM)
- Serving 복호화: env `SERVING_PRIVILEGED_KEY` + 요청 헤더 `X-Consumer-Privileged-Key` (서버 env만으로는 자동 복호화 안 됨)
- 소비 기본 응답: `sender_enc` 등 (평문 DTO는 Serving/소비 개선 시)

---

## 리팩터 이력 (요약)

| 단계 | 내용 |
|------|------|
| **1단계** | `MailAdapter` + `ingest_core` 분리 (동작 동일) |
| **A단계** | `spark_runtime`, `ingest_runner`, `layers/mail` 패키지화, `spark_ingest_mail.py` 슬림화 |
