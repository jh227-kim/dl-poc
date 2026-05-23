"""Lake layer tables — mail domain re-exports (backward compatible imports)."""

from datalake.layers.mail import (
    GOLD_TABLE,
    QUARANTINE_TABLE,
    SILVER_TABLE,
    create_namespaces,
    create_tables,
    delete_from_silver_and_gold,
    save_to_gold,
    save_to_quarantine,
    save_to_silver,
)

__all__ = [
    "SILVER_TABLE",
    "GOLD_TABLE",
    "QUARANTINE_TABLE",
    "create_namespaces",
    "create_tables",
    "save_to_silver",
    "save_to_gold",
    "save_to_quarantine",
    "delete_from_silver_and_gold",
]
