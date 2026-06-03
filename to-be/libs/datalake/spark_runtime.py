"""Shared Spark + Iceberg + MinIO session bootstrap for ingest jobs."""

from __future__ import annotations

import os
from pathlib import Path
import sys

SPARK_VERSION = "3.5.1"
INGEST_PACKAGES = ",".join(
    [
        f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "org.postgresql:postgresql:42.6.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "software.amazon.awssdk:bundle:2.20.18",
        "software.amazon.awssdk:url-connection-client:2.20.18",
    ]
)

JAVA17_OPENS = (
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


def configure_python_worker() -> None:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def configure_hadoop_windows() -> None:
    if os.name != "nt":
        return
    hadoop_home = os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")
    hadoop_bin = str(Path(hadoop_home) / "bin")
    if Path(hadoop_bin).is_dir():
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")


def configure_spark_packages() -> None:
    os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--packages {INGEST_PACKAGES} pyspark-shell")


def _driver_java_options() -> str:
    if os.name == "nt":
        hadoop_home = Path(os.environ.get("HADOOP_HOME", r"C:\hadoop")).as_posix()
        return f"{JAVA17_OPENS} -Dhadoop.home.dir={hadoop_home}"
    return JAVA17_OPENS


def create_spark_session(app_name: str, *, enable_iceberg_sql_extensions: bool = True):
    from pyspark.sql import SparkSession

    configure_python_worker()
    configure_hadoop_windows()
    configure_spark_packages()

    catalog_uri = os.environ.get(
        "ICEBERG_CATALOG_JDBC_URI",
        "jdbc:postgresql://localhost:5432/iceberg_catalog",
    )
    warehouse = os.environ.get("ICEBERG_WAREHOUSE", "s3a://warehouse/")
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://127.0.0.1:9000")
    minio_access_key = os.environ.get("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.environ.get("MINIO_SECRET_KEY", "password")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", INGEST_PACKAGES)
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "jdbc")
        .config("spark.sql.catalog.local.uri", catalog_uri)
        .config("spark.sql.catalog.local.jdbc.user", os.environ.get("ICEBERG_JDBC_USER", "admin"))
        .config("spark.sql.catalog.local.jdbc.password", os.environ.get("ICEBERG_JDBC_PASSWORD", "password"))
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.client.region", os.environ.get("AWS_REGION", "us-east-1"))
        .config("spark.sql.catalog.local.jdbc.schema-version", "V1")
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
        .config("spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1"))
        .config("spark.python.worker.reuse", "true")
        .config("spark.network.timeout", "120s")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.streaming.backpressure.initialRate", "1000")
        .config("spark.streaming.kafka.maxRatePerPartition", "1000")
    )

    spark_master = os.environ.get("SPARK_MASTER", "local[1]")
    builder = builder.config("spark.master", spark_master)

    if spark_master.startswith("k8s://"):
        builder = (
            builder
            .config("spark.executor.instances", os.environ.get("SPARK_EXECUTOR_INSTANCES", "1"))
            .config("spark.kubernetes.container.image", os.environ.get("SPARK_IMAGE", "spark-mail-ingest:latest"))
            .config("spark.kubernetes.executor.request.cores", os.environ.get("SPARK_EXECUTOR_CORES", "1"))
            .config("spark.kubernetes.executor.limit.cores", os.environ.get("SPARK_EXECUTOR_CORES", "1"))
            .config("spark.kubernetes.executor.request.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1Gi"))
            .config("spark.kubernetes.executor.limit.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1Gi"))
            .config("spark.kubernetes.namespace", os.environ.get("SPARK_POD_NAMESPACE", "ns-dl-pipeline"))
            .config("spark.kubernetes.authenticate.driver.serviceAccountName", os.environ.get("SPARK_DRIVER_SERVICE_ACCOUNT", "default"))
        )

    if enable_iceberg_sql_extensions:
        builder = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.as-super-catalog", "local")
        )

    driver_java_opts = _driver_java_options()
    spark = (
        builder.config("spark.sql.catalog.local.s3.endpoint", minio_endpoint)
        .config("spark.sql.catalog.local.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.local.s3.secret-access-key", minio_secret_key)
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.driver.extraJavaOptions", driver_java_opts)
        .config("spark.executor.extraJavaOptions", driver_java_opts)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
