"""Streaming checkpoint location helpers."""

from __future__ import annotations

import os
from pathlib import Path


def resolve_checkpoint(
    *,
    env_keys: tuple[str, ...],
    default_win: str,
    default_local_name: str,
    base_dir: Path | None = None,
) -> str:
    for key in env_keys:
        location = os.environ.get(key, "").strip()
        if location:
            return location
    if os.name == "nt":
        return default_win
    root = base_dir or Path.cwd()
    return str(root / default_local_name)
