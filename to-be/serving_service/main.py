from __future__ import annotations

import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pyiceberg.catalog import load_catalog

app = FastAPI(title="Data Serving API 서비스")

# 1. 환경 변수를 통한 인프라 Endpoint 동적 바인딩 설정
ICEBERG_CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI", "postgresql://admin:password@localhost:5432/iceberg_catalog")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")

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
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"  # 실제 데이터 파일 직접 파싱용 코어 엔진
    }
)

class MailDetail(BaseModel):
    id: str
    title: str
    body: str
    sender: str

class ServingResponse(BaseModel):
    mail_id: str
    mail: MailDetail
    source: str

@app.get("/")
def health():
    return {"service": "Data Serving API 레이어", "status": "running", "engine": "PyIceberg Native Data File IO"}

@app.get("/mails/{mail_id}", response_model=ServingResponse)
def get_lake_mail(mail_id: str):
    """
    운영 DB 개입 차단. 
    Postgres 카탈로그 메타데이터 정보를 추적하여 MinIO 실제 데이터 저장 레이어에서 
    비식별화 완료 레코드 단 1건만 실시간 초고속 스캔
    """
    try:
        # 3. 카탈로그 마스터에서 네임스페이스 경로 상의 테이블 정의 아티팩트 로드
        table = catalog.load_table("db.mail_silver")
        
        # 4. S3 Storage 레이어 필터 푸시다운 연산 가동
        # pyiceberg가 현재 테이블의 메타 스냅샷(Avro)을 탐색해 mail_id 조건에 매핑되는 
        # 실제 물리 Parquet 파일 슬롯만 지정 타격하여 인메모리 판다스 데이터프레임으로 압축 로드.
        scan_result = table.scan(row_filter=f"mail_id == '{mail_id}'").to_pandas()
        
        # 5. 검색 결과 유무에 따른 예외 분기 정의
        if scan_result.empty:
            raise HTTPException(status_code=404, detail=f"데이터 레이크(MinIO) 내에 해당 메일 ID [{mail_id}]가 존재하지 않습니다.")
            
        # 6. 첫 번째 행 레코드 추출 및 데이터 도메인 맵 변환
        row = scan_result.iloc[0]
        
        return ServingResponse(
            mail_id=str(row["mail_id"]),
            mail=MailDetail(
                id=str(row["mail_id"]),
                title=str(row["title"]),
                body=str(row["body"]),
                sender=str(row["sender"])
            ),
            source="Apache Iceberg Native Storage Layer (Direct MinIO Parquet I/O Complete)"
        )
        
    except HTTPException as he:
        raise he
    except Exception as e:
        print(f">>> [Serving Error] 레이크 저장소 다이렉트 파싱 중 치명적 에러 발생: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"인프라 파일 시스템(MinIO/Iceberg) 직접 물리 조회 실패: {str(e)}"
        )