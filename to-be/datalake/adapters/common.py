"""공급 adapter 간 공통 이벤트 파싱 헬퍼."""

from __future__ import annotations

from datetime import datetime


def parse_datetime(value) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def event_version(payload: dict) -> int:
    raw = payload.get("version", 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def event_action(payload: dict) -> str:
    action = str(payload.get("action", "")).strip().lower()
    if action in {"delete", "deleted", "remove", "removed"}:
        return "DELETE"
    return "UPSERT"


def event_sort_key(event: dict) -> tuple:
    version = int(event.get("event_version") or 0)
    source_updated_at = event.get("source_updated_at") or datetime.min
    occurred_at = event.get("occurred_at") or datetime.min
    sequence = int(event.get("sequence") or 0)
    return (version, source_updated_at, occurred_at, sequence)
