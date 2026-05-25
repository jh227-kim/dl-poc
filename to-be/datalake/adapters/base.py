"""공급 시스템 adapter 인터페이스 (1단계: 메일 구현체 기준)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class SupplyAdapter(ABC):
    """Kafka 이벤트 → Supply Pull → canonical → layer 적재 파이프라인용 adapter."""

    @property
    @abstractmethod
    def source(self) -> str:
        """이 adapter가 처리하는 공급 source 식별자 (2단계 registry용)."""

    @property
    @abstractmethod
    def ingest_topic(self) -> str:
        ...

    @property
    @abstractmethod
    def ready_topic(self) -> str:
        ...

    @property
    def dlq_topic(self) -> str | None:
        """이 어댑터가 처리 도중 에러가 난 이벤트를 전송할 Kafka DLQ 토픽. None이면 비활성화."""
        return None

    @property
    @abstractmethod
    def entity_id_field(self) -> str:
        """ready 알림 payload 키 (예: mail_id)."""

    @abstractmethod
    def parse_event_json(self, raw_json: str, sequence: int) -> tuple[dict | None, dict | None]:
        """(parsed_event, quarantine_row) — 둘 중 하나만 non-None."""

    @abstractmethod
    def dedupe_events(self, events: list[dict]) -> list[dict]:
        ...

    @abstractmethod
    def is_delete(self, event: dict) -> bool:
        ...

    @abstractmethod
    def fetch_from_supply(self, event: dict) -> tuple[dict | None, dict | None]:
        """(supply_body, quarantine_row) — fetch 실패 시 quarantine."""

    @abstractmethod
    def validate_raw(self, raw: dict) -> tuple[bool, str | None]:
        ...

    @abstractmethod
    def to_canonical(self, raw: dict, event: dict) -> dict:
        ...

    @abstractmethod
    def encrypt_canonical(self, row: dict) -> dict:
        ...

    @abstractmethod
    def drop_stale(self, spark, rows: list[dict]) -> tuple[list[dict], list[dict]]:
        ...

    @abstractmethod
    def ensure_tables(self, spark) -> None:
        """namespace / iceberg table 생성."""

    @abstractmethod
    def delete_entities(self, spark, entity_ids: list[str]) -> None:
        ...

    @abstractmethod
    def save_accepted(self, spark, rows: list[dict]) -> None:
        ...

    @abstractmethod
    def save_quarantine(self, spark, rows: list[dict]) -> None:
        ...

    @abstractmethod
    def build_quarantine_row(
        self,
        entity_id: str | None,
        action: str,
        payload_json: Any,
        raw_mail_json: Any,
        validation_error: str,
    ) -> dict:
        ...

    @abstractmethod
    def ready_payload(self, entity_id: str, status: str) -> dict:
        ...

    def stream_description(self, kafka_bootstrap: str) -> str:
        return (
            f"{self.ingest_topic} @ {kafka_bootstrap} -> supply -> "
            f"validate/encrypt -> silver,gold/quarantine -> {self.ready_topic}"
        )
