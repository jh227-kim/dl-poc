"""메일 공급 시스템 adapter (Supply GET /mails/{id} → local.silver/gold.mail)."""

from __future__ import annotations

import json
import os
from typing import Any

import requests

from datalake.adapters.base import SupplyAdapter
from datalake.adapters.common import event_action, event_sort_key, event_version, parse_datetime
from datalake.encryption import PERSONAL_FIELDS, encrypt_personal_fields
from datalake.layers import (
    SILVER_TABLE,
    create_namespaces,
    create_tables,
    delete_from_silver_and_gold,
    save_to_gold,
    save_to_quarantine,
    save_to_silver,
)
from datalake.validator import validate_mail


class MailAdapter(SupplyAdapter):
    def __init__(
        self,
        *,
        supply_url: str | None = None,
        ingest_topic: str | None = None,
        ready_topic: str | None = None,
        source: str = "mail-supply-default",
    ) -> None:
        self._supply_url = (supply_url or os.environ.get("SUPPLY_SERVICE_URL", "http://127.0.0.1:8100")).rstrip("/")
        self._ingest_topic = ingest_topic or os.environ.get("KAFKA_INGEST_TOPIC", "mail.events")
        self._ready_topic = ready_topic or os.environ.get("KAFKA_READY_TOPIC", "mail.ready")
        self._source = source

    @property
    def source(self) -> str:
        return self._source

    @property
    def ingest_topic(self) -> str:
        return self._ingest_topic

    @property
    def ready_topic(self) -> str:
        return self._ready_topic

    @property
    def entity_id_field(self) -> str:
        return "mail_id"

    def parse_event_json(self, raw_json: str, sequence: int) -> tuple[dict | None, dict | None]:
        try:
            payload = json.loads(raw_json)
        except (TypeError, json.JSONDecodeError):
            return None, self.build_quarantine_row(
                None, "UPSERT", raw_json, None, "invalid event payload json"
            )

        mail_id = payload.get("mail_id")
        if mail_id is None or str(mail_id).strip() == "":
            return None, self.build_quarantine_row(
                None, "UPSERT", payload, None, "event mail_id is missing"
            )

        return (
            {
                "mail_id": str(mail_id),
                "action": event_action(payload),
                "event_version": event_version(payload),
                "source_updated_at": parse_datetime(payload.get("source_updated_at")),
                "occurred_at": parse_datetime(payload.get("occurred_at")),
                "payload": payload,
                "sequence": sequence,
            },
            None,
        )

    def dedupe_events(self, events: list[dict]) -> list[dict]:
        latest_by_mail_id: dict[str, dict] = {}
        for event in events:
            mail_id = event["mail_id"]
            current = latest_by_mail_id.get(mail_id)
            if current is None or event_sort_key(event) >= event_sort_key(current):
                latest_by_mail_id[mail_id] = event
        return list(latest_by_mail_id.values())

    def is_delete(self, event: dict) -> bool:
        return event["action"] == "DELETE"

    def fetch_from_supply(self, event: dict) -> tuple[dict | None, dict | None]:
        mail_id = event["mail_id"]
        try:
            response = requests.get(f"{self._supply_url}/mails/{mail_id}", timeout=15)
            response.raise_for_status()
            body = response.json()
        except Exception as exc:
            return None, self.build_quarantine_row(
                mail_id, "UPSERT", event["payload"], None, f"fetch failed: {exc}"
            )

        if body.get("error"):
            return None, self.build_quarantine_row(
                mail_id,
                "UPSERT",
                event["payload"],
                body,
                f"supply response error: {body.get('error')}",
            )
        return body, None

    def validate_raw(self, raw: dict) -> tuple[bool, str | None]:
        mail = raw.get("mail") or {}
        return validate_mail(mail)

    def to_canonical(self, raw: dict, event: dict) -> dict:
        mail = raw.get("mail") or {}
        return {
            "mail_id": str(mail.get("id") or mail.get("mail_id") or event["mail_id"]),
            "title": str(mail.get("title") or ""),
            "body": str(mail.get("body") or ""),
            "sender": str(mail.get("sender") or ""),
            "sender_name": str(mail.get("sender_name") or ""),
            "receiver": str(mail.get("receiver") or ""),
            "receiver_name": str(mail.get("receiver_name") or ""),
            "cc": str(mail.get("cc") or ""),
            "sent_at": parse_datetime(mail.get("sent_at")),
            "read_yn": bool(mail.get("read_yn") is True),
            "has_attachment": bool(mail.get("has_attachment") is True),
            "event_version": event["event_version"],
            "source_updated_at": event["source_updated_at"],
            "occurred_at": event["occurred_at"],
        }

    def encrypt_canonical(self, row: dict) -> dict:
        return encrypt_personal_fields(row, fields=PERSONAL_FIELDS)

    def _load_existing_versions(self, spark, mail_ids: list[str]) -> dict[str, int]:
        if not mail_ids:
            return {}
        quoted_ids = ",".join("'" + mail_id.replace("'", "''") + "'" for mail_id in mail_ids)
        query = f"SELECT mail_id, event_version FROM {SILVER_TABLE} WHERE mail_id IN ({quoted_ids})"
        rows = spark.sql(query).collect()
        versions = {}
        for row in rows:
            if row["mail_id"] is not None and row["event_version"] is not None:
                versions[str(row["mail_id"])] = int(row["event_version"])
        return versions

    def drop_stale(self, spark, rows: list[dict]) -> tuple[list[dict], list[dict]]:
        if not rows:
            return [], []

        existing_versions = self._load_existing_versions(spark, [r["mail_id"] for r in rows])
        accepted: list[dict] = []
        rejected: list[dict] = []
        for row in rows:
            current_version = existing_versions.get(row["mail_id"])
            incoming_version = int(row.get("event_version") or 0)
            if current_version is not None and incoming_version < current_version:
                rejected.append(
                    self.build_quarantine_row(
                        row["mail_id"],
                        "UPSERT",
                        None,
                        None,
                        (
                            f"stale event dropped: incoming_version={incoming_version}, "
                            f"current_version={current_version}"
                        ),
                    )
                )
                continue
            accepted.append(row)
        return accepted, rejected

    def ensure_tables(self, spark) -> None:
        create_namespaces(spark)
        create_tables(spark)

    def delete_entities(self, spark, entity_ids: list[str]) -> None:
        delete_from_silver_and_gold(spark, entity_ids)

    def save_accepted(self, spark, rows: list[dict]) -> None:
        save_to_silver(spark, rows)
        save_to_gold(spark, rows)

    def save_quarantine(self, spark, rows: list[dict]) -> None:
        save_to_quarantine(spark, rows)

    def build_quarantine_row(
        self,
        entity_id: str | None,
        action: str,
        payload_json: Any,
        raw_mail_json: Any,
        validation_error: str,
    ) -> dict:
        return {
            "mail_id": entity_id,
            "action": action,
            "payload_json": payload_json,
            "raw_mail_json": raw_mail_json,
            "validation_error": validation_error,
        }

    def ready_payload(self, entity_id: str, status: str) -> dict:
        return {"mail_id": entity_id, "status": status}
