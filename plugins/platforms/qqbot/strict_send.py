"""Opt-in single-submit passive group replies for trusted host callers.

This is not an authorization substitute: callers bind the current session/group,
ACL, freshness and quota. The adapter additionally requires an explicit group
allowlist, the receiving Bot identity and an already usable transport/token.
An unknown result is not permission to retry; even cancellation after submission
cannot retract the request. API acceptance does not prove human delivery.
"""
import logging
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


_NO_RESPONSE_BODY = object()


def _upload_response_shape(result, body):
    """Fixed-key type/size metadata and bounded numeric error codes, not payloads."""
    def shape(value):
        if value is _NO_RESPONSE_BODY:
            return "missing"
        if type(value) is str:
            return f"string:{min(len(value), 16385)}"
        return {type(None): "null", bool: "boolean", int: "integer", float: "number",
                dict: "object", list: "array"}.get(type(value), "other")

    def field(container, name):
        return container.get(name, _NO_RESPONSE_BODY) if type(container) is dict else _NO_RESPONSE_BODY

    reason = result.reason
    reason = reason if type(reason) is str and reason in {
        "invalid_upload_response", "unexpected_upload_message", "upload_application_error", "invalid_file_info",
    } else "other"
    data = field(body, "data")
    info = field(body, "file_info")
    bounded_info = type(info) is str and len(info) <= 16384
    details = {
        "validation": reason, "body_kind": shape(body).split(":", 1)[0],
        "id_shape": shape(field(body, "id")), "message_id_shape": shape(field(body, "message_id")),
        "data_kind": shape(data).split(":", 1)[0],
        "data_id_shape": shape(field(data, "id")), "data_message_id_shape": shape(field(data, "message_id")),
        "file_info_shape": shape(info),
        "file_info_blank": str(not info.strip()).lower() if bounded_info else "none",
        "file_info_controls": str(any(unicodedata.category(c) in {"Cc", "Cs"} for c in info)).lower()
        if bounded_info else "none",
    }
    for name in ("code", "error", "err_code"):
        value = field(body, name)
        details[name + "_shape"] = shape(value)
        details[name + "_value"] = value if type(value) is int and -(2 ** 31) <= value < 2 ** 31 else "none"
    return details


def _diagnose_result(result, *, kind, stage, http_status=None, error=None, response_body=_NO_RESPONSE_BODY):
    """Best-effort allowlisted metadata, never payloads, IDs or exception text."""
    try:
        kind = kind if type(kind) is str and kind in {"image", "audio", "text"} else "unknown"
        stage = stage if type(stage) is str and stage in {
            "gate", "recheck", "upload_request", "upload_response", "submit_request", "submit_response",
        } else "unknown"
        status = result.status if type(result.status) is str and result.status in {
            "accepted", "rejected", "unknown",
        } else "unknown"
        code = http_status if type(http_status) is int and 100 <= http_status <= 599 else "none"
        exception_type = "none"
        if error is not None:
            name = type(error).__name__[:64]
            exception_type = name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", name) else "Exception"
        template = "qq_strict_send kind=%s stage=%s status=%s http_status=%s exception_type=%s"
        values = (kind, stage, status, code, exception_type)
        if stage == "upload_response" and status != "accepted" and response_body is not _NO_RESPONSE_BODY:
            details = _upload_response_shape(result, response_body)
            template += "".join(" " + name + "=%s" for name in details)
            values += tuple(details.values())
        logging.getLogger("gateway.platforms.qqbot").log(
            logging.INFO if status == "accepted" else logging.WARNING,
            template, *values,
        )
    except Exception:
        # A failing handler must not change an already decided outcome or retry.
        # Cancellation and BaseException tripwires still propagate.
        pass
    return result


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
    def finish(result, *, stage="gate", http_status=None, error=None):
        return _diagnose_result(result, kind="text", stage=stage,
                                http_status=http_status, error=error)

    if adapter.config.extra.get("strict_group_replies") is not True:
        return finish(StrictSendResult("rejected", reason="disabled"))
    if not isinstance(expected_app_id, str) or not expected_app_id or expected_app_id != adapter._app_id:
        return finish(StrictSendResult("rejected", reason="app_mismatch"))
    if not adapter.is_connected or adapter._http_client is None:
        return finish(StrictSendResult("rejected", reason="offline"))
    if error := _input_error(group_id, message_id, content):
        return finish(StrictSendResult("rejected", reason=error))
    if adapter._group_policy != "allowlist" or group_id not in adapter._group_allow_from:
        return finish(StrictSendResult("rejected", reason="group_not_allowed"))
    now = time.time()
    if not _valid_time(now) or not _valid_time(message_time) or not 0 <= now - message_time < 285:
        return finish(StrictSendResult("rejected", reason="invalid_message_time"))
    if (not isinstance(adapter._access_token, str) or not adapter._access_token
            or not _valid_time(adapter._token_expires_at)
            or adapter._token_expires_at - now <= 60):
        return finish(StrictSendResult("rejected", reason="token_unavailable"))
    try:
        response = await adapter._http_client.request(
            "POST", API_BASE + "/v2/groups/" + group_id + "/messages",
            json={"msg_type": 0, "content": content, "msg_id": message_id, "msg_seq": 1},
            headers={"Authorization": "QQBot " + adapter._access_token,
                     "Content-Type": "application/json", "User-Agent": build_user_agent()},
            timeout=10.0, follow_redirects=False,
        )
    except Exception as error:
        # Never echo transport/token details, retry, or swallow CancelledError.
        return finish(StrictSendResult("unknown", reason="transport_error"),
                      stage="submit_request", error=error)
    return finish(_classify_response(response), stage="submit_response", http_status=response.status_code)
