# ==========================================
# Stage 1: Spark 외부 패키지(Jar) 캐시 빌더
# ==========================================
FROM apache/spark:3.5.1-python3 AS jar-builder

USER root

# Spark 런타임과 동기화된 패키지 명세
ENV INGEST_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18"

# 빌드 타임에 외부 Jar 패키지를 다운로드받기 위한 초경량 더미 파이썬 스크립트 작성
RUN echo "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate()" > /tmp/dummy.py

# 실제 Spark 작업을 가상으로 1회 구동시켜 Maven 패키지 다운로드 유도
RUN /opt/spark/bin/spark-submit \
    --conf "spark.jars.ivy=/tmp/.ivy2" \
    --packages $INGEST_PACKAGES \
    /tmp/dummy.py

# ==========================================
# Stage 2: 최적화된 스트리밍 실행 런타임
# ==========================================
FROM apache/spark:3.5.1-python3

USER root
WORKDIR /app

# 빌더 스테이지에서 다운로드 완료된 Ivy 캐시 구조를 그대로 복사하여 런타임 100% 오프라인 캐싱 해소 지원
COPY --from=jar-builder /tmp/.ivy2 /root/.ivy2

# 파이썬 라이브러리 설치 (requirements.txt + requests 패키지 + pyspark==3.5.1 명시적 설치)
# 공식 Spark 이미지 내장 Python 3.8 환경과의 호환을 위해 pandas 버전을 2.0.3(Python 3.8 최종 지원 버전)으로 강제 변환 후 설치
COPY requirements.txt .
RUN sed -i 's/pandas>=3.0.3/pandas>=2.0.0,<2.1.0/g' requirements.txt && \
    pip install --no-cache-dir -r requirements.txt requests pyspark==3.5.1

# 소스코드 복사
COPY . /app

# 파이썬 패키지 경로 및 드라이버 환경설정
ENV PYTHONPATH="/app"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 스트리밍 서비스 실행
CMD ["python3", "apps/spark/spark_ingest_mail.py"]
