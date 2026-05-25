"""Silver/Gold 계층 적재 전 메일 데이터 입력값 검증 모듈입니다."""

from __future__ import annotations

from datetime import datetime
import re

EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")


def _is_iso_datetime(value: str) -> bool:
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False

def validate_mail(data: dict) -> tuple[bool, str | None]:
    mail_id = data.get("id") or data.get("mail_id")
    if mail_id is None or str(mail_id).strip() == "":
        return False, "mail_id is required"

    title = data.get("title")
    if title is None or str(title).strip() == "":
        return False, "title is required"

    body = data.get("body")
    if body is None or str(body).strip() == "":
        return False, "body is required"

    sender = data.get("sender")
    if sender is None or str(sender).strip() == "":
        return False, "sender is required"
    if not EMAIL_PATTERN.match(str(sender).strip()):
        return False, f"invalid sender email: {sender}"

    receiver = data.get("receiver")
    if receiver is None or str(receiver).strip() == "":
        return False, "receiver is required"
    if not EMAIL_PATTERN.match(str(receiver).strip()):
        return False, f"invalid receiver email: {receiver}"

    cc = data.get("cc")
    if cc not in (None, "") and not EMAIL_PATTERN.match(str(cc).strip()):
        return False, f"invalid cc email: {cc}"

    sent_at = data.get("sent_at")
    if sent_at not in (None, "") and not _is_iso_datetime(str(sent_at)):
        return False, f"invalid sent_at datetime: {sent_at}"

    read_yn = data.get("read_yn")
    if read_yn not in (None, True, False):
        return False, "read_yn must be boolean"

    has_attachment = data.get("has_attachment")
    if has_attachment not in (None, True, False):
        return False, "has_attachment must be boolean"

    return True, None