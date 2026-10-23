"""Opt-in single-submit passive group replies for trusted host callers.

This is not an authorization substitute: callers bind the current session/group,
ACL, freshness and quota. The adapter additionally requires an explicit group
allowlist, the receiving Bot identity and an already usable transport/token.
An unknown result is not permission to retry; even cancellation after submission
cannot retract the request. API acceptance does not prove human delivery.
"""
import math
import re
import time
import unicodedata
from dataclasses import dataclass
from typing import Literal

from plugins.platforms.qqbot.constants import API_BASE
from plugins.platforms.qqbot.utils import build_user_agent


@dataclass(frozen=True)
class StrictSendResult:
    status: Literal["accepted", "rejected", "unknown"]
    message_id: str = ""
    reason: str = ""


def _valid_time(value) -> bool:
    # Bound before isfinite: converting an arbitrary integer can overflow.
    return type(value) in (int, float) and 0 <= value < 1e12 and math.isfinite(value)


def _valid_message_id(value) -> bool:
    return (isinstance(value, str) and 0 < len(value) <= 1024
            and not any(ch.isspace() or unicodedata.category(ch) in {"Cc", "Cs"} for ch in value))


def _input_error(group_id, message_id, content) -> str:
    if not isinstance(group_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", group_id):
        return "invalid_group_id"
    if not _valid_message_id(message_id):
        return "invalid_message_id"
    if (not isinstance(content, str) or not 0 < len(content) <= 2000 or not content.strip()
            or any(unicodedata.category(ch) in {"Cc", "Cs"} and ch not in "\n\t" for ch in content)):
        return "invalid_content"
    return ""


def _classify_response(response) -> StrictSendResult:
    if 400 <= response.status_code < 500 and response.status_code != 408:
        return StrictSendResult("rejected", reason="http_rejected")
    if not 200 <= response.status_code < 300:
        return StrictSendResult("unknown", reason="http_unknown")
    try:
        body = response.json()
    except (ValueError, RecursionError):
        return StrictSendResult("unknown", reason="invalid_response")
    if not isinstance(body, dict):
        return StrictSendResult("unknown", reason="invalid_response")
    if any(key in body and (type(body[key]) not in (int, float) or body[key] != 0)
           for key in ("error", "code")):
        return StrictSendResult("rejected", reason="application_error")
    if not _valid_message_id(body.get("id")):
        return StrictSendResult("unknown", reason="missing_message_id")
    return StrictSendResult("accepted", message_id=body["id"])


async def send_group_reply_once(adapter, *, group_id, message_id, content,
                                message_time, expected_app_id) -> StrictSendResult:
    if adapter.config.extra.get("strict_group_replies") is not True:
        return StrictSendResult("rejected", reason="disabled")
    if not isinstance(expected_app_id, str) or not expected_app_id or expected_app_id != adapter._app_id:
        return StrictSendResult("rejected", reason="app_mismatch")
    if not adapter.is_connected or adapter._http_client is None:
        return StrictSendResult("rejected", reason="offline")
    if error := _input_error(group_id, message_id, content):
        return StrictSendResult("rejected", reason=error)
    if adapter._group_policy != "allowlist" or group_id not in adapter._group_allow_from:
        return StrictSendResult("rejected", reason="group_not_allowed")
    now = time.time()
    if not _valid_time(now) or not _valid_time(message_time) or not 0 <= now - message_time < 285:
        return StrictSendResult("rejected", reason="invalid_message_time")
    if (not isinstance(adapter._access_token, str) or not adapter._access_token
            or not _valid_time(adapter._token_expires_at)
            or adapter._token_expires_at - now <= 60):
        return StrictSendResult("rejected", reason="token_unavailable")
    try:
        response = await adapter._http_client.request(
            "POST", API_BASE + "/v2/groups/" + group_id + "/messages",
            json={"msg_type": 0, "content": content, "msg_id": message_id, "msg_seq": 1},
            headers={"Authorization": "QQBot " + adapter._access_token,
                     "Content-Type": "application/json", "User-Agent": build_user_agent()},
            timeout=10.0, follow_redirects=False,
        )
    except Exception:
        # Never echo transport/token details, retry, or swallow CancelledError.
        return StrictSendResult("unknown", reason="transport_error")
    return _classify_response(response)
