"""개인정보(PII) 컬럼의 필드 단위 암호화를 위한 헬퍼 함수입니다."""

from __future__ import annotations

import base64
import json
import os
from typing import Iterable

from Crypto.Cipher import AES

PERSONAL_FIELDS = ("sender", "sender_name", "receiver", "receiver_name", "cc")
ALG = "AES-256-GCM"

_DEFAULT_KEY = b"team-platform-local-dev-key-32-bytes!"
_CACHED_KEY: bytes | None = None


def _load_key() -> bytes:
    global _CACHED_KEY
    if _CACHED_KEY is not None:
        return _CACHED_KEY

    key_b64 = os.environ.get("TEAM_PLATFORM_FIELD_ENCRYPTION_KEY_B64", "").strip()
    if key_b64:
        raw = base64.b64decode(key_b64)
        if len(raw) != 32:
            raise ValueError("TEAM_PLATFORM_FIELD_ENCRYPTION_KEY_B64 must decode to 32 bytes")
        _CACHED_KEY = raw
        return _CACHED_KEY

    legacy_key = os.environ.get("ENCRYPTION_KEY", "").encode("utf-8")
    if legacy_key:
        _CACHED_KEY = legacy_key[:32].ljust(32, b"\0")
        return _CACHED_KEY

    _CACHED_KEY = _DEFAULT_KEY[:32]
    return _CACHED_KEY

def _key_id() -> str:
    return os.environ.get("TEAM_PLATFORM_FIELD_ENCRYPTION_KEY_ID", "local-dev-key")

def encrypt_field(plaintext: str) -> str:
    if plaintext is None or plaintext == "":
        return ""

    key = _load_key()
    nonce = os.urandom(12)
    cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
    ciphertext, tag = cipher.encrypt_and_digest(plaintext.encode("utf-8"))

    payload = {
        "alg": ALG,
        "key_id": _key_id(),
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ciphertext).decode("ascii"),
        "tag": base64.b64encode(tag).decode("ascii"),
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def decrypt_field(encrypted_payload: str) -> str:
    if encrypted_payload is None or encrypted_payload == "":
        return ""

    payload = json.loads(encrypted_payload)
    nonce = base64.b64decode(payload["nonce"])
    ciphertext = base64.b64decode(payload["ciphertext"])
    tag = base64.b64decode(payload["tag"])

    cipher = AES.new(_load_key(), AES.MODE_GCM, nonce=nonce)
    plaintext = cipher.decrypt_and_verify(ciphertext, tag)
    return plaintext.decode("utf-8")


def encrypt_personal_fields(data: dict, fields: Iterable[str] = PERSONAL_FIELDS) -> dict:
    """평문 형태의 개인정보(PII) 필드를 *_enc 필드로 대체한 데이터 복사본을 반환합니다."""
    result = dict(data)
    for field in fields:
        value = result.pop(field, None)
        enc_field = f"{field}_enc"
        if value is None or str(value).strip() == "":
            result[enc_field] = None
            continue
        result[enc_field] = encrypt_field(str(value))
    return result


def decrypt_personal_fields(data: dict, fields: Iterable[str] = PERSONAL_FIELDS) -> dict:
    """*_enc 필드의 값을 평문(plaintext)으로 복호화한 데이터 복사본을 반환합니다."""
    result = dict(data)
    for field in fields:
        enc_field = f"{field}_enc"
        token = result.get(enc_field)
        if token is None or token == "":
            continue
        try:
            result[field] = decrypt_field(token)
        except Exception:
            # 복호화 실패 시 기존의 암호화된 데이터를 그대로 유지합니다.
            pass
    return result