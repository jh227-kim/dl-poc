"""
One-off Iceberg + MinIO sanity (creates local.db.poc_table).

Prereq: from repo root, `cd to-be` then `docker compose up -d`.
Run: `venv\\Scripts\\python to-be\\app.py` (repo root).
"""
import os
import subprocess
# Hadoop (winutils 등) — Python import 경로에 bin 넣지 않음
if os.name == "nt":
    os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")

# PySpark JVM 기동 시 패키지 의존성을 먼저 등록 (일부 환경에서 드라이버 클래스패스 보강)
_PACKAGES = ",".join(
    [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.postgresql:postgresql:42.6.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "software.amazon.awssdk:bundle:2.20.18",
        "software.amazon.awssdk:url-connection-client:2.20.18",
    ]
)
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--packages {_PACKAGES} pyspark-shell")

# JDK 17+ 모듈 경고/리플렉션 — Spark 권장 add-opens (Windows + 최신 JDK에서 안정성에 도움)
_JAVA17_OPENS = (
    "-XX:+IgnoreUnrecognizedVMOptions "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "-Dio.netty.tryReflectionSetAccessible=true"
)


def _java_major() -> int | None:
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False)
        out = (proc.stdout or "") + (proc.stderr or "")
    except OSError:
        return None
    # e.g. 'version "18.0.2.1"' or 'openjdk version "17.0.x"'
    for line in out.splitlines():
        if "version" in line:
            start = line.find('"')
            end = line.find('"', start + 1)
            if start != -1 and end != -1:
                token = line[start + 1 : end]
                if token.startswith("1."):
                    parts = token.split(".")
                    if len(parts) >= 2 and parts[1].isdigit():
                        return int(parts[1])
                ver = token.split(".")[0]
                if ver.isdigit():
                    return int(ver)
    return None


from pyspark.sql import SparkSession

_java = _java_major()
if _java is not None and _java > 11 and os.name == "nt":
    print(
        f"[WARN] Java {_java}: Spark 3.5 on Windows may log BlockManager NPE (SPARK-53042). "
        "Use JDK 11 for a clean local PoC."
    )

spark = (
    SparkSession.builder.appName("IcebergLocalPoC")
    .config("spark.jars.packages", _PACKAGES)
    # MERGE INTO / CALL 등 Iceberg SQL 확장이 필요하면 주석 해제 + JDK 11 또는 spark-submit 권장
    # .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "jdbc")
    .config("spark.sql.catalog.local.uri", "jdbc:postgresql://localhost:5432/iceberg_catalog")
    .config("spark.sql.catalog.local.jdbc.user", "admin")
    .config("spark.sql.catalog.local.jdbc.password", "password")
    .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
    .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.local.client.region", "us-east-1")
    .config("spark.sql.catalog.local.jdbc.schema-version", "V1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    # Windows local 모드에서 멀티 스레드 executor가 불안정할 때가 있어 단일 슬롯으로 고정
    .config("spark.master", "local[1]")
    .config("spark.sql.catalog.local.s3.endpoint", "http://127.0.0.1:9000")
    .config("spark.sql.catalog.local.s3.access-key-id", "admin")
    .config("spark.sql.catalog.local.s3.secret-access-key", "password")
    .config("spark.sql.catalog.local.s3.path-style-access", "true")
    .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Windows에서 winutils 없이 S3A가 로컬 디스크 스테이징할 때 NativeIO 오류 방지
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
    .config("spark.driver.extraJavaOptions", _JAVA17_OPENS)
    .config("spark.executor.extraJavaOptions", _JAVA17_OPENS)
    .getOrCreate()
)

try:
    print(">>> 네임스페이스 / 테이블 준비...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    spark.sql("DROP TABLE IF EXISTS local.db.poc_table")
    spark.sql(
        "CREATE TABLE local.db.poc_table (id BIGINT, data STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )

    print(">>> 샘플 데이터 적재...")
    spark.sql("INSERT INTO local.db.poc_table VALUES (1, 'Hello Iceberg!'), (2, 'Local PoC Testing')")

    print(">>> 결과 조회:")
    spark.sql("SELECT * FROM local.db.poc_table").show()
except Exception as e:
    print(f"에러 발생: {e}")
    raise
finally:
    spark.stop()
