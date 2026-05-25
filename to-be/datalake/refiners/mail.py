"""메일 본문 HTML/MIME 정제."""

from __future__ import annotations

import html
import re
from email import message_from_string
from html.parser import HTMLParser

_HTML_TAG_PATTERN = re.compile(r"<[a-zA-Z][^>]*>")


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return "".join(self._parts)


def strip_html(text: str) -> str:
    parser = _HTMLTextExtractor()
    parser.feed(text)
    parser.close()
    return parser.get_text()


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def decode_entities(text: str) -> str:
    return html.unescape(text)


def _looks_like_mime(text: str) -> bool:
    head = text.lstrip()[:500]
    return head.startswith("Content-Type:") or "MIME-Version:" in head


def _decode_part_payload(part) -> str | None:
    payload = part.get_payload(decode=True)
    if payload is None:
        raw = part.get_payload()
        return raw if isinstance(raw, str) else None
    charset = part.get_content_charset() or "utf-8"
    return payload.decode(charset, errors="replace")


def extract_mime_plaintext(body: str) -> str | None:
    try:
        msg = message_from_string(body)
    except Exception:
        return None

    if msg.is_multipart():
        html_part: str | None = None
        for part in msg.walk():
            content_type = part.get_content_type()
            if content_type == "text/plain":
                decoded = _decode_part_payload(part)
                if decoded:
                    return decoded
            if content_type == "text/html" and html_part is None:
                html_part = _decode_part_payload(part)
        if html_part:
            return strip_html(html_part)
        return None

    decoded = _decode_part_payload(msg)
    if decoded is None:
        return None
    if msg.get_content_type() == "text/html":
        return strip_html(decoded)
    return decoded


def refine_text(text: str) -> str:
    if not text:
        return text

    working = text
    if _looks_like_mime(working):
        extracted = extract_mime_plaintext(working)
        if extracted is not None:
            working = extracted

    if _HTML_TAG_PATTERN.search(working):
        working = strip_html(working)

    working = decode_entities(working)
    return normalize_whitespace(working)


def refine_mail(mail: dict) -> tuple[dict, str | None]:
    refined = dict(mail)

    body = mail.get("body")
    if body is not None:
        refined_body = refine_text(str(body))
        if not refined_body:
            return mail, "body is empty after refinement"
        refined["body"] = refined_body

    title = mail.get("title")
    if title is not None and _HTML_TAG_PATTERN.search(str(title)):
        refined_title = refine_text(str(title))
        if not refined_title:
            return mail, "title is empty after refinement"
        refined["title"] = refined_title

    return refined, None


def refine_mail_raw(raw: dict) -> tuple[dict, str | None]:
    mail = raw.get("mail")
    if not isinstance(mail, dict):
        return raw, "mail payload must be a dict"

    refined_mail, error = refine_mail(mail)
    if error:
        return raw, error

    return {**raw, "mail": refined_mail}, None
