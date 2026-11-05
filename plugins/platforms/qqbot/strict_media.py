"""Single-upload, single-submit passive image/MP3 replies for trusted callers.

The caller owns shared reservation/quota accounting. Upload is never a message
receipt, and unknown is never permission to retry or use ordinary media sends.
"""
import asyncio
import base64
import time
import unicodedata

from plugins.platforms.qqbot.constants import API_BASE
from plugins.platforms.qqbot.strict_send import (
    StrictSendResult, _classify_response, _diagnose_result, _input_error, _valid_time, _NO_RESPONSE_BODY,
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


def _valid_audio(data, mime) -> bool:
    """Bounded MP3 header/frame check, not a decoder or playback guarantee.

    QQ's group-files schema lists silk,mp3,wav for file_type=3:
    https://bot.q.qq.com/wiki/develop/api-v2/autogen/api/v2_groups_group_openid_files.post.html
    The normal adapter uploads voice unchanged; this strict path accepts only
    MP3 and never invokes conversion, external tools or lazy dependencies.
    """
    if type(data) is not bytes or not 4 < len(data) <= 2 * 1024 * 1024:
        return False
    if type(mime) is not str or mime != "audio/mpeg":
        return False
    offset = 0
    if data.startswith(b"ID3"):
        if len(data) < 10 or data[3] not in (2, 3, 4) or any(b & 0x80 for b in data[6:10]):
            return False
        offset = 10 + sum(b << shift for b, shift in zip(data[6:10], (21, 14, 7, 0)))
        if data[3] == 4 and data[5] & 0x10:
            offset += 10  # ID3v2.4 footer is excluded from the tag size.
    if offset + 4 > len(data):
        return False
    a, b, c, _ = data[offset:offset + 4]
    version, layer, bitrate, rate = (b >> 3) & 3, (b >> 1) & 3, c >> 4, (c >> 2) & 3
    if a != 0xff or b & 0xe0 != 0xe0 or version == 1 or layer != 1 or bitrate in (0, 15) or rate == 3:
        return False
    rates = (44100, 48000, 32000)
    kbps = ((0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320)
            if version == 3 else (0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160))
    sample_rate = rates[rate] // {3: 1, 2: 2, 0: 4}[version]
    frame_size = (144000 if version == 3 else 72000) * kbps[bitrate] // sample_rate + ((c >> 1) & 1)
    return offset + frame_size <= len(data)


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
        return _diagnose_result(StrictSendResult("rejected", reason=error), kind="image", stage="gate")
    if not _valid_image(image_data, mime_type):
        return _diagnose_result(StrictSendResult("rejected", reason="invalid_image"), kind="image", stage="gate")
    return await _send_group_media_reply_once(
        adapter, group_id=group_id, message_id=message_id, data=image_data,
        file_type=1, message_time=message_time, expected_app_id=expected_app_id, recheck=recheck)


async def send_group_audio_reply_once(adapter, *, group_id, message_id, audio_data: bytes,
                                      mime_type: str, message_time, expected_app_id,
                                      recheck) -> StrictSendResult:
    if error := _input_error(group_id, message_id, "audio"):
        return _diagnose_result(StrictSendResult("rejected", reason=error), kind="audio", stage="gate")
    if not _valid_audio(audio_data, mime_type):
        return _diagnose_result(StrictSendResult("rejected", reason="invalid_audio"), kind="audio", stage="gate")
    return await _send_group_media_reply_once(
        adapter, group_id=group_id, message_id=message_id, data=audio_data,
        file_type=3, file_name="reply.mp3", message_time=message_time,
        expected_app_id=expected_app_id, recheck=recheck)


async def _send_group_media_reply_once(adapter, *, group_id, message_id, data, file_type,
                                      message_time, expected_app_id, recheck, file_name=None):
    stage, http_status = "gate", None
    response_body = _NO_RESPONSE_BODY

    def finish(result, error=None):
        return _diagnose_result(result, kind="audio" if file_type == 3 else "image",
                                stage=stage, http_status=http_status, error=error, response_body=response_body)

    now = time.time()
    if error := _gate(adapter, group_id, message_time, expected_app_id, now):
        return finish(StrictSendResult("rejected", reason=error))
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
            stage = "recheck"
            if await recheck() is not True:
                return finish(StrictSendResult("rejected", reason="recheck_denied"))
            encoded = base64.b64encode(data).decode("ascii")
            upload_body = {"file_type": file_type, "file_data": encoded, "srv_send_msg": False}
            if file_name is not None:
                upload_body["file_name"] = file_name
            if error := checkpoint():
                return finish(StrictSendResult("rejected", reason=error))
            stage = "upload_request"
            upload = await client.request(
                "POST", API_BASE + "/v2/groups/" + group_id + "/files",
                json=upload_body,
                headers=headers, timeout=min(10.0, deadline - loop.time()),
                follow_redirects=False, auth=None)
            stage, http_status = "upload_response", upload.status_code
            if not 200 <= upload.status_code < 300:
                return finish(_classify_response(upload))
            body = upload.json()
            response_body = body
            if not isinstance(body, dict):
                return finish(StrictSendResult("unknown", reason="invalid_upload_response"))
            # Upload must not also send a message, even with srv_send_msg=False.
            # QQ may include an exact empty top-level id for upload-only receipts.
            # Do not generalize this to other falsy values or nested/message_id fields.
            if (("id" in body and not (type(body["id"]) is str and body["id"] == ""))
                    or "message_id" in body
                    or (isinstance(body.get("data"), dict)
                        and any(key in body["data"] for key in ("id", "message_id")))):
                return finish(StrictSendResult("unknown", reason="unexpected_upload_message"))
            if any(key in body and (type(body[key]) not in (int, float) or body[key] != 0)
                   for key in ("code", "error")):
                return finish(StrictSendResult("rejected", reason="upload_application_error"))
            file_info = body.get("file_info")
            if (not isinstance(file_info, str) or not 0 < len(file_info) <= 16384
                    or not file_info.strip()
                    or any(unicodedata.category(ch) in {"Cc", "Cs"} for ch in file_info)):
                return finish(StrictSendResult("unknown", reason="invalid_file_info"))
            # The upload's HTTP status must not describe a later check/submit.
            stage, http_status = "recheck", None
            if error := checkpoint():
                return finish(StrictSendResult("rejected", reason=error))
            if await recheck() is not True:
                return finish(StrictSendResult("rejected", reason="recheck_denied"))
            if error := checkpoint():
                return finish(StrictSendResult("rejected", reason=error))
            stage = "submit_request"
            response = await client.request(
                "POST", API_BASE + "/v2/groups/" + group_id + "/messages",
                json={"msg_type": 7, "content": " ", "media": {"file_info": file_info},
                      "msg_id": message_id, "msg_seq": 1},
                headers=headers, timeout=min(10.0, deadline - loop.time()),
                follow_redirects=False, auth=None)
            stage, http_status = "submit_response", response.status_code
            if loop.time() >= deadline:
                raise TimeoutError
            return finish(_classify_response(response))
    except Exception as error:
        # Never expose response bodies, upload handles, token or exception details.
        # CancelledError and the test-only BaseException tripwire propagate.
        return finish(StrictSendResult("unknown", reason="transport_error"), error)
