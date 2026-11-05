"""Allowlisted strict-send completion diagnostics on guarded real HTTPX paths."""
import asyncio
import json
import logging

import httpx
import pytest

from tests.gateway.test_qqbot_strict_media import harness as media_harness
from tests.gateway.test_qqbot_strict_send import harness as text_harness
from tests.gateway.qq_offline_guard import OfflineNetworkAttempt

LOGGER = "gateway.platforms.qqbot"
SECRET = "diagnostic-sensitive-sentinel"


def test_upload_response_shape_distinguishes_empty_id_from_invalid_file_info(media, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    media.state.upload = httpx.Response(200, json={"file_info": SECRET + "\n", "id": ""})
    result = media.diagnostic_send()
    assert (result.status, result.reason) == ("unknown", "invalid_file_info")
    assert len(media.requests) == 1
    records = [record for record in caplog.records if record.name == LOGGER]
    assert len(records) == 1
    fields = dict(part.split("=", 1) for part in records[0].getMessage().split()[1:])
    assert fields == {
        "kind": media.kind, "stage": "upload_response", "status": "unknown",
        "http_status": "200", "exception_type": "none",
        "validation": "invalid_file_info", "body_kind": "object",
        "id_shape": "string:0", "message_id_shape": "missing", "data_kind": "missing",
        "data_id_shape": "missing", "data_message_id_shape": "missing",
        "file_info_shape": f"string:{len(SECRET) + 1}", "file_info_blank": "false",
        "file_info_controls": "true", "code_shape": "missing", "code_value": "none",
        "error_shape": "missing", "error_value": "none",
        "err_code_shape": "missing", "err_code_value": "none",
    }
    assert SECRET not in records[0].getMessage()


@pytest.mark.parametrize(("body", "status", "reason", "expected"), [
    ([SECRET], "unknown", "invalid_upload_response", {"body_kind": "array"}),
    (None, "unknown", "invalid_upload_response", {"body_kind": "null"}),
    (SECRET, "unknown", "invalid_upload_response", {"body_kind": "string"}),
    (17, "unknown", "invalid_upload_response", {"body_kind": "integer"}),
    ({}, "unknown", "invalid_file_info", {"file_info_shape": "missing"}),
    ({"file_info": None}, "unknown", "invalid_file_info", {"file_info_shape": "null"}),
    ({"file_info": False}, "unknown", "invalid_file_info", {"file_info_shape": "boolean"}),
    ({"file_info": []}, "unknown", "invalid_file_info", {"file_info_shape": "array"}),
    ({"file_info": ""}, "unknown", "invalid_file_info", {"file_info_shape": "string:0", "file_info_blank": "true"}),
    ({"file_info": " "}, "unknown", "invalid_file_info", {"file_info_shape": "string:1", "file_info_blank": "true"}),
    ({"file_info": SECRET + "\n"}, "unknown", "invalid_file_info", {"file_info_controls": "true"}),
    ({"file_info": "\ud800"}, "unknown", "invalid_file_info", {"file_info_controls": "true"}),
    ({"file_info": "x" * 20000}, "unknown", "invalid_file_info",
     {"file_info_shape": "string:16385", "file_info_blank": "none", "file_info_controls": "none"}),
    ({"file_info": SECRET, "id": SECRET}, "unknown", "unexpected_upload_message",
     {"id_shape": f"string:{len(SECRET)}"}),
    ({"file_info": SECRET, "message_id": None}, "unknown", "unexpected_upload_message",
     {"message_id_shape": "null"}),
    ({"file_info": SECRET, "data": {"message_id": SECRET}}, "unknown", "unexpected_upload_message",
     {"data_kind": "object", "data_message_id_shape": f"string:{len(SECRET)}"}),
    ({"data": {"id": ""}}, "unknown", "unexpected_upload_message", {"data_id_shape": "string:0"}),
    ({"file_info": SECRET, "code": 19}, "rejected", "upload_application_error", {"code_value": "19"}),
    ({"file_info": SECRET, "code": True}, "rejected", "upload_application_error",
     {"code_shape": "boolean", "code_value": "none"}),
    ({"error": SECRET}, "rejected", "upload_application_error", {"error_value": "none"}),
    ({"err_code": 304083}, "unknown", "invalid_file_info", {"err_code_value": "304083"}),
    ({SECRET: SECRET}, "unknown", "invalid_file_info", {"file_info_shape": "missing"}),
])
def test_upload_response_shape_covers_each_validation_branch(media, caplog, body, status, reason, expected):
    caplog.set_level(logging.INFO, logger=LOGGER)
    media.state.upload = httpx.Response(200, content=json.dumps(body, ensure_ascii=True).encode())
    result = media.diagnostic_send()
    assert (result.status, result.reason) == (status, reason)
    assert len(media.requests) == 1
    records = [record for record in caplog.records if record.name == LOGGER]
    assert len(records) == 1
    message = records[0].getMessage()
    fields = dict(part.split("=", 1) for part in message.split()[1:])
    assert set(fields) == {
        "kind", "stage", "status", "http_status", "exception_type", "validation", "body_kind",
        "id_shape", "message_id_shape", "data_kind", "data_id_shape", "data_message_id_shape",
        "file_info_shape", "file_info_blank", "file_info_controls", "code_shape", "code_value",
        "error_shape", "error_value", "err_code_shape", "err_code_value",
    }
    assert fields["kind"] == media.kind and fields["stage"] == "upload_response"
    assert fields["validation"] == reason and fields["http_status"] == "200"
    for name, value in expected.items():
        assert fields[name] == value
    assert SECRET not in message and "Group-A" not in message and "incoming-id" not in message
    assert len(message) < 1024
    assert records[0].exc_info is None and records[0].stack_info is None
    assert all(type(value) in (str, int) for value in records[0].args)


@pytest.mark.parametrize("value", [True, 1.0, SECRET, 2 ** 31, -(2 ** 31) - 1, {}, []])
def test_upload_response_shape_never_logs_noninteger_or_out_of_range_error_values(media, caplog, value):
    caplog.set_level(logging.INFO, logger=LOGGER)
    media.state.upload = httpx.Response(200, json={"code": value, "error": value, "err_code": value})
    assert media.diagnostic_send().status == "rejected"
    records = [record for record in caplog.records if record.name == LOGGER]
    assert len(records) == 1 and len(media.requests) == 1
    fields = dict(part.split("=", 1) for part in records[0].getMessage().split()[1:])
    assert all(fields[name + "_value"] == "none" for name in ("code", "error", "err_code"))
    assert SECRET not in records[0].getMessage()


def test_upload_response_shape_failure_never_changes_decided_outcome(media, caplog, monkeypatch):
    from plugins.platforms.qqbot import strict_send
    caplog.set_level(logging.INFO, logger=LOGGER)

    def broken(*args):
        raise RuntimeError(SECRET)

    monkeypatch.setattr(strict_send, "_upload_response_shape", broken)
    media.state.upload = httpx.Response(200, json={"message_id": "", "file_info": SECRET})
    result = media.diagnostic_send()
    assert (result.status, result.reason) == ("unknown", "unexpected_upload_message")
    assert len(media.requests) == 1
    assert [r for r in caplog.records if r.name == LOGGER] == []


def test_upload_response_shape_does_not_format_opaque_objects_or_unapproved_reason(caplog):
    from plugins.platforms.qqbot.strict_send import StrictSendResult, _diagnose_result
    caplog.set_level(logging.INFO, logger=LOGGER)

    class Opaque:
        def __str__(self):
            raise AssertionError("object value must not be formatted")

        __repr__ = __str__

    result = StrictSendResult("unknown", reason=SECRET)
    body = {"id": Opaque(), "file_info": Opaque(), "err_code": Opaque(), SECRET: SECRET}
    assert _diagnose_result(result, kind="audio", stage="upload_response", http_status=200,
                            response_body=body) is result
    records = [r for r in caplog.records if r.name == LOGGER]
    assert len(records) == 1
    fields = dict(part.split("=", 1) for part in records[0].getMessage().split()[1:])
    assert fields["validation"] == "other" and fields["id_shape"] == "other"
    assert fields["file_info_shape"] == "other" and fields["err_code_value"] == "none"
    assert SECRET not in records[0].getMessage()


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("HERMES_DISABLE_LAZY_INSTALLS", "1")
    monkeypatch.setenv("HERMES_ENABLE_PROJECT_PLUGINS", "0")


@pytest.fixture(params=["image", "audio"])
def media(request, media_harness):
    h = media_harness
    kind = request.param

    def send(**changes):
        values = h.kwargs()
        if kind == "audio":
            values.pop("image_data")
            values.update(audio_data=b"\xff\xfb\x90\x00" + b"\x00" * 413, mime_type="audio/mpeg")
        values.update(changes)
        sender = getattr(h.adapter, f"send_group_{kind}_reply_once")
        return h.loop.run_until_complete(sender(**values))

    h.diagnostic_send, h.kind = send, kind
    return h


def assert_diagnostic(caplog, *, kind, stage, status, http_status="none", exception_type="none"):
    records = [record for record in caplog.records if record.name == LOGGER]
    assert len(records) == 1
    record = records[0]
    message, separator, _structure = record.getMessage().partition(" validation=")
    assert message == (
        f"qq_strict_send kind={kind} stage={stage} status={status} "
        f"http_status={http_status} exception_type={exception_type}")
    if separator:
        assert stage == "upload_response" and status != "accepted"
    assert record.levelno == (logging.INFO if status == "accepted" else logging.WARNING)
    assert record.exc_info is None and record.stack_info is None
    assert all(type(value) in (str, int) for value in record.args)
    assert SECRET not in record.getMessage()
    for value in ("Group-A", "incoming-id", "sent-id", "offline-token", "opaque-file-secret"):
        assert value not in record.getMessage()


@pytest.mark.parametrize(("case", "stage", "status", "code", "exception", "count"), [
    ("upload_error", "upload_request", "unknown", "none", "ReadTimeout", 1),
    ("upload_http", "upload_response", "rejected", 403, "none", 1),
    ("upload_json", "upload_response", "unknown", 200, "JSONDecodeError", 1),
    ("upload_body", "upload_response", "unknown", 200, "none", 1),
    ("final_error", "submit_request", "unknown", "none", "WriteError", 2),
    ("final_http", "submit_response", "unknown", 503, "none", 2),
    ("accepted", "submit_response", "accepted", 201, "none", 2),
    ("recheck_first", "recheck", "rejected", "none", "none", 0),
    ("recheck_second", "recheck", "rejected", "none", "none", 1),
    ("gate", "gate", "rejected", "none", "none", 0),
])
def test_media_completion_has_only_stage_status_and_exception_metadata(
        media, caplog, case, stage, status, code, exception, count):
    caplog.set_level(logging.INFO, logger=LOGGER)
    media.adapter._access_token = SECRET
    media.state.upload = httpx.Response(200, json={"file_info": SECRET})
    media.state.final = httpx.Response(201, json={"id": SECRET})
    private_request = httpx.Request("POST", f"https://offline.invalid/{SECRET}",
                                    headers={"Authorization": SECRET})
    if case == "upload_error":
        media.state.upload = httpx.ReadTimeout(SECRET, request=private_request)
    elif case == "upload_http":
        media.state.upload = httpx.Response(403, text=SECRET, headers={"Location": str(private_request.url)})
    elif case == "upload_json":
        media.state.upload = httpx.Response(200, text=SECRET)
    elif case == "upload_body":
        media.state.upload = httpx.Response(200, json=[SECRET])
    elif case == "final_error":
        media.state.final = httpx.WriteError(SECRET, request=private_request)
    elif case == "final_http":
        media.state.final = httpx.Response(503, text=SECRET, headers={"Location": str(private_request.url)})
    elif case == "gate":
        media.adapter.config.extra["strict_group_media_replies"] = False
    checks = 0

    async def recheck():
        nonlocal checks
        checks += 1
        return not (case == "recheck_first" or case == "recheck_second" and checks == 2)

    result = media.diagnostic_send(recheck=recheck)
    assert result.status == status
    assert result.message_id == (SECRET if status == "accepted" else "")
    assert len(media.requests) == count
    assert_diagnostic(caplog, kind=media.kind, stage=stage, status=status,
                      http_status=code, exception_type=exception)


@pytest.mark.parametrize("invalid", ["anchor", "media"])
def test_media_input_rejections_have_one_gate_diagnostic(media, caplog, invalid):
    caplog.set_level(logging.INFO, logger=LOGGER)
    changes = {"message_id": ""} if invalid == "anchor" else {"mime_type": SECRET}
    assert media.diagnostic_send(**changes).status == "rejected"
    assert media.requests == []
    assert_diagnostic(caplog, kind=media.kind, stage="gate", status="rejected")


@pytest.mark.parametrize(("case", "status", "code", "exception"), [
    ("error", "unknown", "none", "ReadTimeout"),
    ("http", "rejected", 403, "none"),
    ("server_http", "unknown", 503, "none"),
    ("redirect", "unknown", 307, "none"),
    ("json", "unknown", 200, "none"),
    ("body", "unknown", 200, "none"),
    ("application", "rejected", 200, "none"),
    ("accepted", "accepted", 201, "none"),
])
def test_text_completion_has_only_stage_status_and_exception_metadata(
        text_harness, caplog, case, status, code, exception):
    h = text_harness
    caplog.set_level(logging.INFO, logger=LOGGER)
    h.adapter.config.extra["strict_group_replies"] = True
    h.adapter._access_token = SECRET
    response = httpx.Response(201, json={"id": SECRET})
    if case == "error":
        response = httpx.ReadTimeout(SECRET, request=httpx.Request(
            "POST", f"https://offline.invalid/{SECRET}", headers={"Authorization": SECRET}))
    elif case in {"http", "server_http", "redirect"}:
        response = httpx.Response(code, text=SECRET, headers={"Location": f"https://offline.invalid/{SECRET}"})
    elif case == "json":
        response = httpx.Response(200, text=SECRET)
    elif case == "body":
        response = httpx.Response(200, json=[SECRET])
    elif case == "application":
        response = httpx.Response(200, json={"id": SECRET, "error": SECRET})
    h.response.value = response
    result = h.send(content=SECRET)
    assert result.status == status
    assert result.message_id == (SECRET if status == "accepted" else "")
    assert len(h.requests) == 1
    assert_diagnostic(caplog, kind="text", stage="submit_request" if case == "error" else "submit_response",
                      status=status, http_status=code, exception_type=exception)


@pytest.mark.parametrize("gate", ["disabled", "app", "offline", "input", "allowlist", "time", "token"])
def test_text_gate_diagnostic_never_logs_input(text_harness, caplog, gate):
    h = text_harness
    caplog.set_level(logging.INFO, logger=LOGGER)
    h.adapter.config.extra["strict_group_replies"] = gate != "disabled"
    kwargs = {"content": SECRET}
    if gate == "app":
        kwargs["expected_app_id"] = SECRET
    elif gate == "offline":
        h.adapter._running = False
    elif gate == "input":
        kwargs["message_id"] = ""
    elif gate == "allowlist":
        h.adapter._group_allow_from = []
    elif gate == "time":
        kwargs["message_time"] = 0
    elif gate == "token":
        h.adapter._access_token = ""
    assert h.send(**kwargs).status == "rejected"
    assert h.requests == []
    assert_diagnostic(caplog, kind="text", stage="gate", status="rejected")


@pytest.fixture(params=["image", "audio", "text"])
def sender(request):
    kind = request.param
    h = request.getfixturevalue("text_harness" if kind == "text" else "media_harness")
    h.adapter.config.extra["strict_group_replies"] = True

    def send():
        if kind == "text":
            return h.send(content=SECRET)
        values = h.kwargs()
        if kind == "audio":
            values.pop("image_data")
            values.update(audio_data=b"\xff\xfb\x90\x00" + b"\x00" * 413, mime_type="audio/mpeg")
        return h.loop.run_until_complete(getattr(h.adapter, f"send_group_{kind}_reply_once")(**values))

    def final(value):
        if kind == "text":
            h.response.value = value
        else:
            h.state.final = value

    h.diagnostic_send, h.set_final, h.kind = send, final, kind
    return h


@pytest.mark.parametrize("outcome", ["accepted", "rejected", "unknown", "gate"])
def test_broken_logging_preserves_result_without_retry(sender, outcome):
    h = sender
    if outcome == "rejected":
        h.set_final(httpx.Response(403, text=SECRET))
    elif outcome == "unknown":
        h.set_final(httpx.ReadTimeout(SECRET))
    elif outcome == "gate":
        h.adapter.config.extra["strict_group_replies"] = False
    records = []

    class BrokenHandler(logging.Handler):
        def emit(self, record):
            records.append(record)
            raise OSError(SECRET)

    logger = logging.getLogger(LOGGER)
    handler = BrokenHandler()
    old_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        result = h.diagnostic_send()
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)
        handler.close()
    assert (result.status, result.message_id, result.reason) == {
        "accepted": ("accepted", "sent-id", ""),
        "rejected": ("rejected", "", "http_rejected"),
        "unknown": ("unknown", "", "transport_error"),
        "gate": ("rejected", "", "disabled"),
    }[outcome]
    assert len(records) == 1
    assert len(h.requests) == (0 if outcome == "gate" else 1 if h.kind == "text" else 2)


@pytest.mark.parametrize(("name", "expected"), [
    ("SafeError", "SafeError"), ("E" * 100, "E" * 64),
    ("Bad\nName", "Exception"), ("Bad/Name", "Exception"), ("错误", "Exception"),
])
def test_exception_type_is_bounded_and_never_formats_exception(sender, caplog, name, expected):
    caplog.set_level(logging.INFO, logger=LOGGER)

    def forbidden(self):
        raise OfflineNetworkAttempt("exception formatting is forbidden")

    error_type = type(name, (Exception,), {"__str__": forbidden, "__repr__": forbidden})
    sender.set_final(error_type(SECRET))
    assert sender.diagnostic_send().status == "unknown"
    assert len(sender.requests) == (1 if sender.kind == "text" else 2)
    assert_diagnostic(caplog, kind=sender.kind, stage="submit_request", status="unknown",
                      exception_type=expected)


@pytest.mark.parametrize("error_type", [asyncio.CancelledError, OfflineNetworkAttempt])
def test_non_exception_control_flow_propagates_without_completion_log(sender, caplog, error_type):
    caplog.set_level(logging.INFO, logger=LOGGER)
    error = error_type(SECRET)
    sender.set_final(error)
    with pytest.raises(error_type) as raised:
        sender.diagnostic_send()
    assert raised.value is error
    assert len(sender.requests) == (1 if sender.kind == "text" else 2)
    assert not [record for record in caplog.records if record.name == LOGGER]


@pytest.mark.parametrize("code", [True, 99, 600, 200.0, SECRET, None])
def test_diagnostic_boundary_rejects_non_http_status_metadata(caplog, code):
    from plugins.platforms.qqbot.strict_send import StrictSendResult, _diagnose_result

    caplog.set_level(logging.INFO, logger=LOGGER)
    result = StrictSendResult("unknown", message_id=SECRET, reason=SECRET)
    assert _diagnose_result(result, kind="text", stage="submit_response", http_status=code) is result
    assert_diagnostic(caplog, kind="text", stage="submit_response", status="unknown")


def test_diagnostic_boundary_allowlists_categories_and_omits_reason(caplog):
    from plugins.platforms.qqbot.strict_send import StrictSendResult, _diagnose_result

    caplog.set_level(logging.INFO, logger=LOGGER)
    result = StrictSendResult(SECRET, message_id=SECRET, reason=SECRET)
    assert _diagnose_result(result, kind=SECRET, stage=SECRET) is result
    assert_diagnostic(caplog, kind="unknown", stage="unknown", status="unknown")


@pytest.mark.parametrize("at_check", [1, 2])
def test_recheck_exception_has_no_stale_upload_status(media, caplog, at_check):
    caplog.set_level(logging.INFO, logger=LOGGER)
    checks = 0

    async def recheck():
        nonlocal checks
        checks += 1
        if checks == at_check:
            raise ValueError(SECRET)
        return True

    result = media.diagnostic_send(recheck=recheck)
    assert (result.status, result.reason) == ("unknown", "transport_error")
    assert len(media.requests) == at_check - 1
    assert_diagnostic(caplog, kind=media.kind, stage="recheck", status="unknown", exception_type="ValueError")


def test_response_hook_failure_does_not_claim_unreturned_http_status(sender, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)

    async def hook(response):
        if response.request.url.path.endswith("/messages"):
            raise ValueError(SECRET)

    sender.client.event_hooks["response"].append(hook)
    sender.set_final(httpx.Response(403, text=SECRET))
    assert sender.diagnostic_send().status == "unknown"
    assert len(sender.requests) == (1 if sender.kind == "text" else 2)
    assert_diagnostic(caplog, kind=sender.kind, stage="submit_request", status="unknown",
                      exception_type="ValueError")


@pytest.mark.parametrize("error_type", [asyncio.CancelledError, OfflineNetworkAttempt])
def test_logging_control_flow_is_not_swallowed_or_repeated(sender, monkeypatch, error_type):
    calls = []
    error = error_type(SECRET)

    def fail(*args, **kwargs):
        calls.append(1)
        raise error

    monkeypatch.setattr(logging.getLogger(LOGGER), "log", fail)
    with pytest.raises(error_type) as raised:
        sender.diagnostic_send()
    assert raised.value is error
    assert calls == [1]
    assert len(sender.requests) == (1 if sender.kind == "text" else 2)
