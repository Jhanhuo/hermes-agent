"""Single-submit QQ passive replies, with real HTTPX and no live sockets."""
import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest

from gateway.config import PlatformConfig
from plugins.platforms.qqbot.adapter import QQAdapter
from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt


@pytest.fixture
def harness(monkeypatch, request):
    # Windows creates a socketpair for its loop; block application I/O only after.
    loop = asyncio.new_event_loop()
    request.addfinalizer(loop.close)
    request.addfinalizer(monkeypatch.undo)
    requests = []
    monkeypatch.setattr(time, "time", lambda: 1_900_000_000.0)

    def forbidden(*args, **kwargs):
        pytest.fail("strict replies must not connect, authenticate, format, or retry")

    response = SimpleNamespace(value=httpx.Response(200, json={"id": "sent-id"}))

    async def transport(request):
        requests.append(request)
        if isinstance(response.value, BaseException):
            raise response.value
        if callable(response.value):
            return await response.value(request)
        return response.value

    fake_transport = httpx.MockTransport(transport)
    isolation = OfflineGuard()
    isolation.__enter__()
    request.addfinalizer(lambda: isolation.__exit__(None, None, None))
    client = httpx.AsyncClient(transport=fake_transport, follow_redirects=True, trust_env=False)
    request.addfinalizer(lambda: loop.run_until_complete(client.aclose()))
    isolation.allow_client(client, fake_transport)
    adapter = QQAdapter(PlatformConfig(extra={
        "app_id": "offline-app", "client_secret": "offline-secret",
        "group_policy": "allowlist", "group_allow_from": ["Group-A"],
    }))
    adapter._running = True
    adapter._ws = SimpleNamespace(closed=False)
    adapter._http_client = client
    adapter._access_token = "offline-token"
    adapter._token_expires_at = time.time() + 3600
    for name in ("connect", "send", "_ensure_connected", "_api_request", "_auth_headers",
                 "_ensure_token", "format_message", "_next_msg_seq"):
        monkeypatch.setattr(adapter, name, forbidden, raising=False)
    monkeypatch.setattr(httpx.AsyncClient, "__init__", forbidden)

    def send(**changes):
        kwargs = dict(group_id="Group-A", message_id="incoming-id", content="原文\n\ttext",
                      message_time=1_899_999_999.0, expected_app_id="offline-app")
        kwargs.update(changes)
        return loop.run_until_complete(adapter.send_group_reply_once(**kwargs))

    yield SimpleNamespace(adapter=adapter, client=client, requests=requests, send=send,
                          response=response, loop=loop)


def test_guard_violation_is_not_silently_converted_to_unknown(harness):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.response.value = OfflineNetworkAttempt("inert fixture violation")
    with pytest.raises(OfflineNetworkAttempt):
        harness.send()
    assert len(harness.requests) == 1


def test_default_off_rejects_without_http(harness):
    result = harness.send()
    assert (result.status, result.message_id, result.reason) == ("rejected", "", "disabled")
    assert harness.requests == []


def test_opt_in_posts_one_exact_anchored_payload_on_existing_client(harness):
    from plugins.platforms.qqbot.constants import API_BASE
    from plugins.platforms.qqbot.utils import build_user_agent

    harness.adapter.config.extra["strict_group_replies"] = True
    content = " **原文**\n\t" + "x" * 1990
    result = harness.send(content=content)
    assert (result.status, result.message_id, result.reason) == ("accepted", "sent-id", "")
    assert len(harness.requests) == 1
    request = harness.requests[0]
    assert request.method == "POST"
    assert str(request.url) == API_BASE + "/v2/groups/Group-A/messages"
    assert json.loads(request.content) == {
        "msg_type": 0, "content": content, "msg_id": "incoming-id", "msg_seq": 1,
    }
    assert request.headers["Authorization"] == "QQBot offline-token"
    assert request.headers["Content-Type"] == "application/json"
    assert request.headers["User-Agent"] == build_user_agent()
    assert request.extensions["timeout"] == dict(connect=10.0, read=10.0, write=10.0, pool=10.0)
    assert harness.adapter._http_client is harness.client
    assert not harness.client.is_closed


@pytest.mark.parametrize("value", [False, None, "true", "false", 1, 0, [], {}])
def test_only_literal_true_enables_send(harness, value):
    harness.adapter.config.extra["strict_group_replies"] = value
    assert harness.send().reason == "disabled"
    assert harness.requests == []


@pytest.mark.parametrize(("attribute", "value", "reason"), [
    ("_app_id", "other-bot", "app_mismatch"),
    ("_app_id", " offline-app", "app_mismatch"),
    ("_running", False, "offline"),
    ("_ws", None, "offline"),
    ("_ws", SimpleNamespace(closed=True), "offline"),
    ("_http_client", None, "offline"),
    ("_access_token", None, "token_unavailable"),
    ("_access_token", "", "token_unavailable"),
    ("_token_expires_at", 1_899_999_999, "token_unavailable"),
    ("_token_expires_at", 1_900_000_060, "token_unavailable"),
    ("_token_expires_at", float("nan"), "token_unavailable"),
    ("_token_expires_at", float("inf"), "token_unavailable"),
    ("_token_expires_at", "1900009999", "token_unavailable"),
    ("_token_expires_at", 10**1000, "token_unavailable"),
    ("_group_policy", "disabled", "group_not_allowed"),
    ("_group_policy", "open", "group_not_allowed"),
    ("_group_policy", "pairing", "group_not_allowed"),
    ("_group_allow_from", ["Other"], "group_not_allowed"),
    ("_group_allow_from", ["*"], "group_not_allowed"),
])
def test_current_transport_identity_policy_and_token_fail_closed(harness, attribute, value, reason):
    harness.adapter.config.extra["strict_group_replies"] = True
    setattr(harness.adapter, attribute, value)
    result = harness.send()
    assert (result.status, result.message_id, result.reason) == ("rejected", "", reason)
    assert harness.requests == []


@pytest.mark.parametrize("expected", [None, "", "offline-app ", 123])
def test_expected_app_id_is_exact_text(harness, expected):
    harness.adapter.config.extra["strict_group_replies"] = True
    assert harness.send(expected_app_id=expected).reason == "app_mismatch"
    assert harness.requests == []


@pytest.mark.parametrize("value", [
    "", None, 123, " Group-A", "Group-A ", "Group/A", "Group\\A", "Group%2fA",
    "Group.A", "Group:A", "群", "A" * 129, "A\n", "A\x00", "A?x", "A#x",
])
def test_group_id_must_be_an_exact_safe_url_segment(harness, value):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.adapter._group_allow_from = [value]  # A matching ACL cannot make a URL segment safe.
    result = harness.send(group_id=value)
    assert (result.status, result.reason) == ("rejected", "invalid_group_id")
    assert harness.requests == []


@pytest.mark.parametrize("value", [
    "", None, 123, " incoming", "incoming ", "a b", "a\n", "a\t", "a\u00a0",
    "a\x00", "a\x7f", "a\x85", "a\ud800", "x" * 1025,
])
def test_reply_anchor_rejects_malformed_text_without_repair(harness, value):
    harness.adapter.config.extra["strict_group_replies"] = True
    result = harness.send(message_id=value)
    assert (result.status, result.reason) == ("rejected", "invalid_message_id")
    assert harness.requests == []


@pytest.mark.parametrize("value", [
    "", " \n\t", None, 123, "x" * 2001, "a\x00", "a\r", "a\x7f", "a\x85", "a\udfff",
])
def test_text_rejects_empty_oversize_controls_and_surrogates(harness, value):
    harness.adapter.config.extra["strict_group_replies"] = True
    result = harness.send(content=value)
    assert (result.status, result.reason) == ("rejected", "invalid_content")
    assert harness.requests == []


def test_maximum_valid_identifiers_and_text_are_preserved(harness):
    harness.adapter.config.extra["strict_group_replies"] = True
    group_id = "A_a-0" + "z" * 123
    harness.adapter._group_allow_from = [group_id]
    message_id = "回" * 1024
    content = " 文\n\t" + "本" * 1995 + " "
    assert harness.send(group_id=group_id, message_id=message_id, content=content).status == "accepted"
    assert len(harness.requests) == 1
    assert json.loads(harness.requests[0].content) == {
        "msg_type": 0, "content": content, "msg_id": message_id, "msg_seq": 1,
    }


@pytest.mark.parametrize("message_time", [
    1_900_000_001, 1_899_999_715, 0, -1, float("nan"), float("inf"), float("-inf"),
    1e12, pytest.param(10**1000, id="huge-int"), "1900000000", None, True,
])
def test_platform_time_must_be_bounded_finite_and_younger_than_285_seconds(harness, message_time):
    harness.adapter.config.extra["strict_group_replies"] = True
    result = harness.send(message_time=message_time)
    assert (result.status, result.reason) == ("rejected", "invalid_message_time")
    assert harness.requests == []


@pytest.mark.parametrize("now", [
    -1, float("nan"), float("inf"), float("-inf"), 1e12,
    pytest.param(10**1000, id="huge-int"), "1900000000", None, True,
])
def test_local_clock_must_be_bounded_and_finite(harness, monkeypatch, now):
    from plugins.platforms.qqbot import strict_send

    harness.adapter.config.extra["strict_group_replies"] = True
    monkeypatch.setattr(strict_send.time, "time", lambda: now)
    result = harness.send()
    assert (result.status, result.reason) == ("rejected", "invalid_message_time")
    assert harness.requests == []


@pytest.mark.parametrize("age", [0, 284.999])
def test_passive_window_boundaries_still_allow_a_single_submission(harness, age):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.adapter._token_expires_at = 1_900_000_060.001
    assert harness.send(message_time=1_900_000_000 - age).status == "accepted"
    assert len(harness.requests) == 1


@pytest.mark.parametrize(("http_status", "status"), [
    (200, "accepted"), (201, "accepted"), (202, "accepted"), (299, "accepted"),
    (301, "unknown"), (302, "unknown"), (307, "unknown"), (308, "unknown"),
    (400, "rejected"), (401, "rejected"), (403, "rejected"), (404, "rejected"),
    (408, "unknown"), (409, "rejected"), (429, "rejected"), (499, "rejected"),
    (500, "unknown"), (502, "unknown"), (503, "unknown"), (504, "unknown"),
])
def test_http_classification_never_retries_or_follows_redirects(harness, http_status, status):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.response.value = httpx.Response(
        http_status, json={"id": "server-id"}, headers={"Location": "https://redirect.invalid/secret"},
    )
    result = harness.send()
    assert result.status == status
    assert result.message_id == ("server-id" if status == "accepted" else "")
    assert len(harness.requests) == 1


@pytest.mark.parametrize("body", [
    b"not-json-secret", b"", b"null", b"[]", b'"text"', b"{}",
    b'{"id":null}', b'{"id":123}', b'{"id":true}', b'{"id":""}',
    b'{"id":" id"}', b'{"id":"id "}', b'{"id":"a\\n"}',
    b'{"id":"a\\u007f"}', b'{"id":"a\\ud800"}',
])
def test_2xx_without_valid_real_message_id_is_unknown(harness, body, caplog):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.response.value = httpx.Response(200, content=body)
    result = harness.send()
    assert result.status == "unknown" and result.message_id == ""
    assert len(harness.requests) == 1
    assert "not-json-secret" not in repr(result) + caplog.text


@pytest.mark.parametrize("field", ["code", "error"])
@pytest.mark.parametrize("value", [1, 40003, "raw-error-secret", {"code": 23}, True, None])
def test_application_error_cannot_be_accepted_even_with_message_id(harness, field, value, caplog):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.response.value = httpx.Response(200, json={"id": "server-id", field: value})
    result = harness.send()
    assert (result.status, result.message_id, result.reason) == ("rejected", "", "application_error")
    assert len(harness.requests) == 1
    assert "raw-error-secret" not in repr(result) + caplog.text


def test_explicit_zero_application_codes_with_real_id_are_accepted(harness):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.response.value = httpx.Response(200, json={"id": "真實-id", "code": 0, "error": 0})
    result = harness.send()
    assert (result.status, result.message_id) == ("accepted", "真實-id")
    assert len(harness.requests) == 1


@pytest.mark.parametrize("error_type", [
    httpx.ConnectError, httpx.ReadError, httpx.WriteError, httpx.RemoteProtocolError,
    httpx.ConnectTimeout, httpx.ReadTimeout, httpx.WriteTimeout, httpx.PoolTimeout,
    OSError, RuntimeError,
])
def test_transport_exceptions_are_unknown_without_retry_or_raw_errors(harness, error_type, caplog):
    harness.adapter.config.extra["strict_group_replies"] = True
    harness.response.value = error_type("transport-secret")
    result = harness.send()
    assert (result.status, result.message_id, result.reason) == ("unknown", "", "transport_error")
    assert len(harness.requests) == 1
    assert "transport-secret" not in repr(result) + caplog.text


def test_cancellation_during_http_propagates_without_second_submission(harness):
    harness.adapter.config.extra["strict_group_replies"] = True

    async def scenario():
        started = asyncio.Event()
        blocked = asyncio.Event()

        async def wait_for_cancel(request):
            started.set()
            await blocked.wait()
            pytest.fail("cancelled submission must not complete")

        harness.response.value = wait_for_cancel
        task = asyncio.create_task(harness.adapter.send_group_reply_once(
            group_id="Group-A", message_id="incoming-id", content="cancel me",
            message_time=1_900_000_000, expected_app_id="offline-app",
        ))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()

    harness.loop.run_until_complete(scenario())
    assert len(harness.requests) == 1


def test_cancellation_before_start_sends_nothing(harness):
    harness.adapter.config.extra["strict_group_replies"] = True
    task = harness.loop.create_task(harness.adapter.send_group_reply_once(
        group_id="Group-A", message_id="incoming-id", content="cancel me",
        message_time=1_900_000_000, expected_app_id="offline-app",
    ))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        harness.loop.run_until_complete(task)
    assert harness.requests == []


def test_invocation_rechecks_current_bot_identity_before_http(harness):
    harness.adapter.config.extra["strict_group_replies"] = True
    pending = harness.adapter.send_group_reply_once(
        group_id="Group-A", message_id="incoming-id", content="old identity",
        message_time=1_900_000_000, expected_app_id="offline-app",
    )
    harness.adapter._app_id = "replacement-bot"
    result = harness.loop.run_until_complete(pending)
    assert result.status == "rejected" and result.reason == "app_mismatch"
    assert harness.requests == []


def test_existing_response_guards_remain_active(harness):
    harness.adapter.config.extra["strict_group_replies"] = True
    guarded = []

    async def guard(response):
        guarded.append(response.status_code)
        raise ValueError("guard-secret")

    harness.client.event_hooks["response"].append(guard)
    result = harness.send()
    assert result.status == "unknown"
    assert guarded == [200]
    assert len(harness.requests) == 1
    assert harness.client.event_hooks["response"] == [guard]
