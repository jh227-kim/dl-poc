# dl-poc (Data lake PoC)

**As-Is**와 **To-Be** 두 가지 데모가 한 저장소에 있습니다. 실행 방법·스택이 다르므로 **각 폴더의 README**를 먼저 보는 것을 권장합니다.

| 구분 | 설명 | 문서 |
|------|------|------|
| **To-Be** | Kafka `mail.events` → 공급 HTTP → Iceberg → `mail.ready` (Docker: Kafka·MinIO·Postgres + PySpark) | [to-be/README.md](to-be/README.md) |
| **As-Is** | GCP Pub/Sub → 소비 서비스가 공급에 직접 `GET /mails/{id}` (Docker **불필요**) | [as-is/README.md](as-is/README.md) |

## 공통 사전 준비

### Docker Desktop (To-Be만 해당)

To-Be 파이프라인은 **Kafka·MinIO·Postgres**를 Docker로 띄웁니다. PC에 Docker가 없다면:

1. [Docker Desktop for Windows](https://docs.docker.com/desktop/setup/install/windows-install/) (또는 [Mac](https://docs.docker.com/desktop/setup/install/mac-install/) / [Linux](https://docs.docker.com/desktop/setup/install/linux/)) 설치
2. 설치 후 **Docker Desktop을 실행**하고, 설정에서 **Linux 컨테이너(WSL2 백엔드)** 가 켜져 있는지 확인
3. 터미널에서 `docker compose version` 이 동작하는지 확인

As-Is는 GCP Pub/Sub만 사용하므로 **Docker Desktop 없이**도 동작할 수 있습니다.

### Python 가상환경 (권장: 저장소 루트)

```bat
cd /d path\to\dl-poc
py -3 -m venv venv
venv\Scripts\python -m pip install -U pip
venv\Scripts\pip install -r requirements.txt
```

루트 `requirements.txt`는 To-Be용 의존성(`pyspark`, `requests` 등)을 포함합니다. As-Is의 `google-cloud-pubsub`는 [as-is/README.md](as-is/README.md)에 따라 추가 설치합니다.

### JDK (To-Be Spark / Iceberg)

Spark·Iceberg를 쓰는 스크립트는 **JDK 11 또는 17**을 권장합니다. (JDK 18도 동작은 하나 경고가 더 나올 수 있습니다.)

---

## Git으로 공유

```bat
cd /d path\to\dl-poc
git remote add origin https://github.com/YOUR_USER/dl-poc.git
git push -u origin main
```

`venv/`, `**/.spark-mail-ingest-cp/` 등은 `.gitignore`에 있습니다.

## 디렉터리

- `to-be/` — Kafka·Iceberg·Spark ingest, `docker-compose.yml`, `spark_ingest_mail.py`, `scripts/kafka-smoke.*`
- `as-is/` — Pub/Sub 기반 As-Is 마이크로서비스 데모
