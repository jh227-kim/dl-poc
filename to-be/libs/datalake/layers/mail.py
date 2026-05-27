"""Mail domain Iceberg silver / gold / quarantine tables."""

from __future__ import annotations

from datalake.layers._common import (
    ensure_layer_namespaces,
    json_field_literal,
    sql_bigint_literal,
    sql_bool_literal,
    sql_string_literal,
    sql_timestamp_literal,
)

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


def create_namespaces(spark) -> None:
    ensure_layer_namespaces(spark)


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
            sql_string_literal(row.get("mail_id")),
            sql_string_literal(row.get("title")),
            sql_string_literal(row.get("body")),
            sql_string_literal(row.get("sender_enc")),
            sql_string_literal(row.get("sender_name_enc")),
            sql_string_literal(row.get("receiver_enc")),
            sql_string_literal(row.get("receiver_name_enc")),
            sql_string_literal(row.get("cc_enc")),
            sql_timestamp_literal(row.get("sent_at")),
            sql_bool_literal(row.get("read_yn")),
            sql_bool_literal(row.get("has_attachment")),
            sql_bigint_literal(row.get("event_version")),
            sql_timestamp_literal(row.get("source_updated_at")),
            sql_timestamp_literal(row.get("occurred_at")),
            "current_timestamp()",
        ]
        tuples.append("(" + ",".join(parts) + ")")
    return ",\n".join(tuples)


def _build_quarantine_values_sql(rows: list[dict]) -> str:
    tuples = []
    for row in rows:
        parts = [
            sql_string_literal(row.get("mail_id")),
            sql_string_literal(row.get("action")),
            sql_string_literal(json_field_literal(row.get("payload_json"))),
            sql_string_literal(json_field_literal(row.get("raw_mail_json"))),
            sql_string_literal(row.get("validation_error")),
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

    ids = ",".join(sql_string_literal(r.get("mail_id")) for r in rows)
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
    ids = ",".join(sql_string_literal(mail_id) for mail_id in mail_ids)
    spark.sql(f"DELETE FROM {SILVER_TABLE} WHERE mail_id IN ({ids})")
    spark.sql(f"DELETE FROM {GOLD_TABLE} WHERE mail_id IN ({ids})")
