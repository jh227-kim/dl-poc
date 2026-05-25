from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import sys

from fastapi import FastAPI, Header, HTTPException
from pyiceberg.catalog import load_catalog

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from datalake.encryption import decrypt_personal_fields

app = FastAPI(title="Data Serving API 서비스")

# 1. 환경 변수를 통한 인프라 Endpoint 동적 바인딩 설정
ICEBERG_CATALOG_URI = os.environ.get(
    "ICEBERG_CATALOG_URI",
    "postgresql://admin:password@localhost:5432/iceberg_catalog",
)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
PRIVILEGED_KEY = os.environ.get("SERVING_PRIVILEGED_KEY", "")

print(u">>> [Serving] PyIceberg 엔진 초기화 중...")
print(f">>> Catalog URI: {ICEBERG_CATALOG_URI}")
print(f">>> MinIO Endpoint: {MINIO_ENDPOINT}")

# 2. 자바(JVM) 없이 SQL Catalog + S3A 파일 스토리지 스캔 세션 수립
# PyIceberg 엔진이 내부적으로 PyArrowFileIO를 탑재하여 Parquet 이진 바이너리를 다이렉트로 파싱.

catalog = load_catalog(
    "local",
    **{
        "type": "sql",
        "uri": ICEBERG_CATALOG_URI,
        "s3.endpoint": MINIO_ENDPOINT,
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO", # 실제 데이터 파일 직접 파싱용 코어 엔진
    },
)

def _safe_text(value) -> str:
    if value is None:
        return ""
    return str(value)


def _safe_bool(value) -> bool:
    return bool(value is True)


def _safe_timestamp(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)

def _is_privileged(privileged_key: str | None) -> bool:
    if not PRIVILEGED_KEY:
        return False
    return privileged_key == PRIVILEGED_KEY


@app.get("/")
def health():
    return {"service": "Data Serving API", "status": "running", "source_table": "local.gold.mail"}


@app.get("/mails/{mail_id}")
def get_lake_mail(mail_id: str, x_consumer_privileged_key: str | None = Header(default=None)):
    """
    운영 DB 개입 차단.
    Postgres 카탈로그 메타데이터 정보를 추적하여 MinIO 실제 데이터 저장 레이어에서
    비식별화 완료 레코드 단 1건만 실시간 초고속 스캔
    """
    try:
        # 3. 카탈로그 마스터에서 네임스페이스 경로 상의 테이블 정의 아티팩트 로드
        table = catalog.load_table("gold.mail")

        # 4. S3 Storage 레이어 필터 푸시다운 연산 가동
        # pyiceberg가 현재 테이블의 메타 스냅샷(Avro)을 탐색해 mail_id 조건에 매핑되는
        # 실제 물리 Parquet 파일 슬롯만 지정 타격하여 인메모리 판다스 데이터프레임으로 압축 로드.
        rows = table.scan(row_filter=f"mail_id == '{mail_id}'").to_pandas()

        # 5. 검색 결과 유무에 따른 예외 분기 정의
        if rows.empty:
            raise HTTPException(status_code=404, detail=f"mail_id not found: {mail_id}")

        # 6. 첫 번째 행 레코드 추출 및 데이터 도메인 맵 변환
        row = rows.iloc[0]
        mail = {
            "id": _safe_text(row.get("mail_id")),
            "title": _safe_text(row.get("title")),
            "body": _safe_text(row.get("body")),
            "sender_enc": _safe_text(row.get("sender_enc")),
            "sender_name_enc": _safe_text(row.get("sender_name_enc")),
            "receiver_enc": _safe_text(row.get("receiver_enc")),
            "receiver_name_enc": _safe_text(row.get("receiver_name_enc")),
            "cc_enc": _safe_text(row.get("cc_enc")),
            "sent_at": _safe_timestamp(row.get("sent_at")),
            "read_yn": _safe_bool(row.get("read_yn")),
            "has_attachment": _safe_bool(row.get("has_attachment")),
            "event_version": int(row.get("event_version") or 0),
            "source_updated_at": _safe_timestamp(row.get("source_updated_at")),
            "occurred_at": _safe_timestamp(row.get("occurred_at")),
        }

        privileged = _is_privileged(x_consumer_privileged_key)
        if privileged:
            mail = decrypt_personal_fields(mail)

        return {
            "mail_id": _safe_text(row.get("mail_id")),
            "mail": mail,
            "source": "local.gold.mail",
            "privileged": privileged,
        }
    except HTTPException:
        raise
    except Exception as exc:
        print(f">>> [Serving Error] Gold 레이어 데이터 조회 및 처리(복호화) 중 예기치 않은 에러 발생: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to process gold layer data: {exc}")


@app.get("/mails/debug/headers")
def debug_headers(x_consumer_privileged_key: str | None = Header(default=None)):
    return {
        "x_consumer_privileged_key": x_consumer_privileged_key,
        "is_privileged": _is_privileged(x_consumer_privileged_key)
    }