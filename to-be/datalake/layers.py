"""Silver / Gold / Quarantine 레이크 레이어의 테이블 관리 모듈입니다."""

from __future__ import annotations

import json
from datetime import datetime

SILVER_TABLE = "local.silver.mail"
GOLD_TABLE = "local.gold.mail"
QUARANTINE_TABLE = "local.quarantine.mail"

_MAIL_SCHEMA_DDL = """(
    mail_id STRING,
    title STRING,
    body STRING,
    sender_enc STRING,
    sender_name_enc STRING,
    receiver_enc STRING,
    receiver_name_enc STRING,
    cc_enc STRING,
    sent_at TIMESTAMP,
    read_yn BOOLEAN,
    has_attachment BOOLEAN,
    event_version BIGINT,
    source_updated_at TIMESTAMP,
    occurred_at TIMESTAMP,
    ingested_at TIMESTAMP
)"""

_QUARANTINE_SCHEMA_DDL = """(
    mail_id STRING,
    action STRING,
    payload_json STRING,
    raw_mail_json STRING,
    validation_error STRING,
    quarantined_at TIMESTAMP
)"""


def _sql_string_literal(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _sql_bool_literal(value) -> str:
    if value is None:
        return "NULL"
    return "true" if bool(value) else "false"

def _sql_bigint_literal(value) -> str:
    if value is None:
        return "NULL"
    return str(int(value))


def _sql_timestamp_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    return f"TIMESTAMP '{value}'"


def create_namespaces(spark) -> None:
    for namespace in ("local.silver", "local.gold", "local.quarantine"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def create_tables(spark) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE}
        {_MAIL_SCHEMA_DDL}
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE}
        {_MAIL_SCHEMA_DDL}
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE}
        {_QUARANTINE_SCHEMA_DDL}
        USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """
    )

def _build_mail_values_sql(rows: list[dict]) -> str:
    tuples = []
    for row in rows:
        parts = [
            _sql_string_literal(row.get("mail_id")),
            _sql_string_literal(row.get("title")),
            _sql_string_literal(row.get("body")),
            _sql_string_literal(row.get("sender_enc")),
            _sql_string_literal(row.get("sender_name_enc")),
            _sql_string_literal(row.get("receiver_enc")),
            _sql_string_literal(row.get("receiver_name_enc")),
            _sql_string_literal(row.get("cc_enc")),
            _sql_timestamp_literal(row.get("sent_at")),
            _sql_bool_literal(row.get("read_yn")),
            _sql_bool_literal(row.get("has_attachment")),
            _sql_bigint_literal(row.get("event_version")),
            _sql_timestamp_literal(row.get("source_updated_at")),
            _sql_timestamp_literal(row.get("occurred_at")),
            "current_timestamp()",
        ]
        tuples.append("(" + ",".join(parts) + ")")
    return ",\n".join(tuples)

def _build_quarantine_values_sql(rows: list[dict]) -> str:
    tuples = []
    for row in rows:
        raw_mail_json = row.get("raw_mail_json")
        if raw_mail_json is not None and not isinstance(raw_mail_json, str):
            raw_mail_json = json.dumps(raw_mail_json, ensure_ascii=False)

        payload_json = row.get("payload_json")
        if payload_json is not None and not isinstance(payload_json, str):
            payload_json = json.dumps(payload_json, ensure_ascii=False)

        parts = [
            _sql_string_literal(row.get("mail_id")),
            _sql_string_literal(row.get("action")),
            _sql_string_literal(payload_json),
            _sql_string_literal(raw_mail_json),
            _sql_string_literal(row.get("validation_error")),
            "current_timestamp()",
        ]
        tuples.append("(" + ",".join(parts) + ")")
    return ",\n".join(tuples)

def _save_mail_rows(spark, table_name: str, rows: list[dict]) -> None:
    if not rows:
        return

    values_sql = _build_mail_values_sql(rows)
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW _mail_batch AS
        SELECT
            mail_id, title, body, sender_enc, sender_name_enc, receiver_enc, receiver_name_enc,
            cc_enc, sent_at, read_yn, has_attachment, event_version, source_updated_at, occurred_at, ingested_at
        FROM VALUES
        {values_sql}
        AS v(
            mail_id, title, body, sender_enc, sender_name_enc, receiver_enc, receiver_name_enc,
            cc_enc, sent_at, read_yn, has_attachment, event_version, source_updated_at, occurred_at, ingested_at
        )
        """
    )

    ids = ",".join(_sql_string_literal(r.get("mail_id")) for r in rows)
    spark.sql(f"DELETE FROM {table_name} WHERE mail_id IN ({ids})")
    spark.sql(
        f"""
        INSERT INTO {table_name}
        (
            mail_id, title, body, sender_enc, sender_name_enc, receiver_enc, receiver_name_enc,
            cc_enc, sent_at, read_yn, has_attachment, event_version, source_updated_at, occurred_at, ingested_at
        )
        SELECT
            mail_id, title, body, sender_enc, sender_name_enc, receiver_enc, receiver_name_enc,
            cc_enc, sent_at, read_yn, has_attachment, event_version, source_updated_at, occurred_at, ingested_at
        FROM _mail_batch
        """
    )

def save_to_silver(spark, rows: list[dict]) -> None:
    _save_mail_rows(spark, SILVER_TABLE, rows)


def save_to_gold(spark, rows: list[dict]) -> None:
    _save_mail_rows(spark, GOLD_TABLE, rows)


def save_to_quarantine(spark, rows: list[dict]) -> None:
    if not rows:
        return

    values_sql = _build_quarantine_values_sql(rows)
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW _quarantine_batch AS
        SELECT mail_id, action, payload_json, raw_mail_json, validation_error, quarantined_at
        FROM VALUES
        {values_sql}
        AS v(mail_id, action, payload_json, raw_mail_json, validation_error, quarantined_at)
        """
    )

    spark.sql(
        f"""
        INSERT INTO {QUARANTINE_TABLE}
        (mail_id, action, payload_json, raw_mail_json, validation_error, quarantined_at)
        SELECT mail_id, action, payload_json, raw_mail_json, validation_error, quarantined_at
        FROM _quarantine_batch
        """
    )

def delete_from_silver_and_gold(spark, mail_ids: list[str]) -> None:
    if not mail_ids:
        return
    ids = ",".join(_sql_string_literal(mail_id) for mail_id in mail_ids)
    spark.sql(f"DELETE FROM {SILVER_TABLE} WHERE mail_id IN ({ids})")
    spark.sql(f"DELETE FROM {GOLD_TABLE} WHERE mail_id IN ({ids})")
