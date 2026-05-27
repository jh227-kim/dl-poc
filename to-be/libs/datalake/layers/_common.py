"""Iceberg layer SQL helpers shared across domains."""

from __future__ import annotations

import json
from datetime import datetime


def sql_string_literal(value) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def sql_bool_literal(value) -> str:
    if value is None:
        return "NULL"
    return "true" if bool(value) else "false"


def sql_bigint_literal(value) -> str:
    if value is None:
        return "NULL"
    return str(int(value))


def sql_timestamp_literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    return f"TIMESTAMP '{value}'"


def ensure_layer_namespaces(spark) -> None:
    for namespace in ("local.silver", "local.gold", "local.quarantine"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def json_field_literal(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)
