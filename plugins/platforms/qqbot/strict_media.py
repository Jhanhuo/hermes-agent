"""Single-upload, single-submit passive image replies for trusted callers.

The caller owns shared reservation/quota accounting. Upload is never a message
receipt, and unknown is never permission to retry or use ordinary media sends.
"""
import asyncio
import base64
import time
import unicodedata

from plugins.platforms.qqbot.constants import API_BASE
from plugins.platforms.qqbot.strict_send import (
    StrictSendResult, _classify_response, _input_error, _valid_time,
)
from plugins.platforms.qqbot.utils import build_user_agent


def _valid_image(data, mime) -> bool:
    # Full decode/resource validation belongs to the approved plugin catalog.
    # This boundary never accepts a URL/path, mutable buffer or arbitrary file.
    if type(data) is not bytes or not 4 <= len(data) <= 2 * 1024 * 1024:
        return False
    if type(mime) is not str:
        return False
    return ((mime == "image/png" and len(data) >= 24 and data.startswith(b"\x89PNG\r\n\x1a\n"))
            or (mime == "image/jpeg" and data.startswith(b"\xff\xd8\xff")))


def _gate(adapter, group_id, message_time, expected_app_id, now) -> str:
    if (adapter.config.extra.get("strict_group_replies") is not True
            or adapter.config.extra.get("strict_group_media_replies") is not True):
        return "disabled"
    if not isinstance(expected_app_id, str) or not expected_app_id or expected_app_id != adapter._app_id:
        return "app_mismatch"
    if not adapter.is_connected or adapter._http_client is None or adapter._http_client.is_closed:
        return "offline"
    if adapter._group_policy != "allowlist" or group_id not in adapter._group_allow_from:
        return "group_not_allowed"
    if not _valid_time(now) or not _valid_time(message_time) or not 0 <= now - message_time < 285:
        return "invalid_message_time"
    if (not isinstance(adapter._access_token, str) or not adapter._access_token
            or not _valid_time(adapter._token_expires_at)
            or adapter._token_expires_at - now <= 60):
        return "token_unavailable"
    return ""


async def send_group_image_reply_once(adapter, *, group_id, message_id, image_data: bytes,
                                      mime_type: str, message_time, expected_app_id,
                                      recheck) -> StrictSendResult:
    if error := _input_error(group_id, message_id, "image"):
        return StrictSendResult("rejected", reason=error)
    if not _valid_image(image_data, mime_type):
        return StrictSendResult("rejected", reason="invalid_image")
    now = time.time()
    if error := _gate(adapter, group_id, message_time, expected_app_id, now):
        return StrictSendResult("rejected", reason=error)
    client, token = adapter._http_client, adapter._access_token
    headers = {"Authorization": "QQBot " + token,
               "Content-Type": "application/json", "User-Agent": build_user_agent()}
    loop = asyncio.get_running_loop()
    budget = min(20.0, 285 - (now - message_time))
    deadline = loop.time() + budget
    last_wall = now

    def checkpoint():
        nonlocal last_wall, deadline
        current = time.time()
        if not _valid_time(current) or current < last_wall:
            return "clock_rollback"
        last_wall = current
        if error := _gate(adapter, group_id, message_time, expected_app_id, current):
            return error
        if adapter._http_client is not client or adapter._access_token != token:
            return "transport_changed"
        # A forward wall-clock jump may shorten, but never extend, the original
        # anchor's remaining monotonic budget, including the next policy await.
        deadline = min(deadline, loop.time() + 285 - (current - message_time))
        if loop.time() >= deadline:
            raise TimeoutError
        total.reschedule(deadline)
        return ""

    try:
        # HTTPX timeouts bound phases, not elapsed time: share one total deadline.
        async with asyncio.timeout(budget) as total:
            if await recheck() is not True:
                return StrictSendResult("rejected", reason="recheck_denied")
            encoded = base64.b64encode(image_data).decode("ascii")
            if error := checkpoint():
                return StrictSendResult("rejected", reason=error)
            upload = await client.request(
                "POST", API_BASE + "/v2/groups/" + group_id + "/files",
                json={"file_type": 1, "file_data": encoded, "srv_send_msg": False},
                headers=headers, timeout=min(10.0, deadline - loop.time()),
                follow_redirects=False, auth=None)
            if not 200 <= upload.status_code < 300:
                return _classify_response(upload)
            body = upload.json()
            if not isinstance(body, dict):
                return StrictSendResult("unknown", reason="invalid_upload_response")
            # Upload must not also send a message, even with srv_send_msg=False.
            if (any(key in body for key in ("id", "message_id"))
                    or (isinstance(body.get("data"), dict)
                        and any(key in body["data"] for key in ("id", "message_id")))):
                return StrictSendResult("unknown", reason="unexpected_upload_message")
            if any(key in body and (type(body[key]) not in (int, float) or body[key] != 0)
                   for key in ("code", "error")):
                return StrictSendResult("rejected", reason="upload_application_error")
            file_info = body.get("file_info")
            if (not isinstance(file_info, str) or not 0 < len(file_info) <= 16384
                    or not file_info.strip()
                    or any(unicodedata.category(ch) in {"Cc", "Cs"} for ch in file_info)):
                return StrictSendResult("unknown", reason="invalid_file_info")
            if error := checkpoint():
                return StrictSendResult("rejected", reason=error)
            if await recheck() is not True:
                return StrictSendResult("rejected", reason="recheck_denied")
            if error := checkpoint():
                return StrictSendResult("rejected", reason=error)
            response = await client.request(
                "POST", API_BASE + "/v2/groups/" + group_id + "/messages",
                json={"msg_type": 7, "media": {"file_info": file_info},
                      "msg_id": message_id, "msg_seq": 1},
                headers=headers, timeout=min(10.0, deadline - loop.time()),
                follow_redirects=False, auth=None)
            if loop.time() >= deadline:
                raise TimeoutError
            return _classify_response(response)
    except Exception:
        # Never expose response bodies, upload handles, token or exception details.
        # CancelledError and the test-only BaseException tripwire propagate.
        return StrictSendResult("unknown", reason="transport_error")
