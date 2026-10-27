"""Strict single-image QQ replies on the real adapter, with no live sockets."""
import asyncio
import base64
import io
import json
import time
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from PIL import Image

from gateway.config import PlatformConfig
from plugins.platforms.qqbot.adapter import QQAdapter
from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt

NOW = 1_900_000_000.0


@pytest.fixture
def harness(monkeypatch, request):
    # Windows loop creation uses a socketpair; create it before the tripwires.
    loop = asyncio.new_event_loop()
    request.addfinalizer(loop.close)
    request.addfinalizer(monkeypatch.undo)
    clock = SimpleNamespace(value=NOW)
    monkeypatch.setattr(time, "time", lambda: clock.value)
    requests = []
    state = SimpleNamespace(upload=httpx.Response(200, json={"file_info": "opaque-file-secret"}),
                            final=httpx.Response(200, json={"id": "sent-id"}), checks=0)

    async def handler(req):
        requests.append(req)
        value = state.upload if req.url.path.endswith("/files") else state.final
        if isinstance(value, BaseException):
            raise value
        if callable(value):
            return await value(req)
        return value

    transport = httpx.MockTransport(handler)
    guard = OfflineGuard()
    guard.__enter__()
    request.addfinalizer(lambda: guard.__exit__(None, None, None))
    client = httpx.AsyncClient(transport=transport, trust_env=False, follow_redirects=True)
    request.addfinalizer(lambda: loop.run_until_complete(client.aclose()))
    guard.allow_client(client, transport)
    adapter = QQAdapter(PlatformConfig(extra={
        "app_id": "offline-app", "client_secret": "offline-secret",
        "strict_group_replies": True, "strict_group_media_replies": True,
        "group_policy": "allowlist", "group_allow_from": ["Group-A"],
    }))
    adapter._running = True
    adapter._ws = SimpleNamespace(closed=False)
    adapter._http_client = client
    adapter._access_token = "offline-token"
    adapter._token_expires_at = NOW + 3600

    def forbidden(*args, **kwargs):
        pytest.fail("strict media must not reconnect, refresh, retry, or use generic sends")

    for name in ("connect", "send", "send_image", "send_group_reply_once", "_ensure_connected",
                 "_api_request", "_auth_headers", "_ensure_token", "format_message",
                 "_next_msg_seq", "_upload_media", "_upload_local_file", "_send_media"):
        monkeypatch.setattr(adapter, name, forbidden, raising=False)
    monkeypatch.setattr(httpx.AsyncClient, "__init__", forbidden)

    async def recheck():
        state.checks += 1
        return True

    images = {}
    for fmt, mime in (("PNG", "image/png"), ("JPEG", "image/jpeg")):
        out = io.BytesIO()
        Image.new("RGB", (2, 2), "red").save(out, format=fmt)
        images[mime] = out.getvalue()

    def kwargs(**changes):
        values = dict(group_id="Group-A", message_id="incoming-id", image_data=images["image/png"],
                      mime_type="image/png", message_time=NOW - 1, expected_app_id="offline-app",
                      recheck=recheck)
        values.update(changes)
        return values

    def send(**changes):
        return loop.run_until_complete(adapter.send_group_image_reply_once(**kwargs(**changes)))

    yield SimpleNamespace(adapter=adapter, client=client, requests=requests, state=state,
                          loop=loop, clock=clock, send=send, kwargs=kwargs, images=images)


@pytest.mark.parametrize("mime", ["image/png", "image/jpeg"])
def test_one_upload_then_one_anchored_image_on_existing_client(harness, mime):
    from plugins.platforms.qqbot.constants import API_BASE
    from plugins.platforms.qqbot.utils import build_user_agent

    image = harness.images[mime]
    result = harness.send(image_data=image, mime_type=mime)
    assert (result.status, result.message_id, result.reason) == ("accepted", "sent-id", "")
    assert harness.state.checks == 2
    assert len(harness.requests) == 2
    upload, final = harness.requests
    assert [str(req.url) for req in harness.requests] == [
        API_BASE + "/v2/groups/Group-A/files", API_BASE + "/v2/groups/Group-A/messages"]
    assert json.loads(upload.content) == {
        "file_type": 1, "file_data": base64.b64encode(image).decode("ascii"), "srv_send_msg": False}
    assert json.loads(final.content) == {
        "msg_type": 7, "media": {"file_info": "opaque-file-secret"},
        "msg_id": "incoming-id", "msg_seq": 1}
    for req in harness.requests:
        assert req.method == "POST"
        assert req.headers["Authorization"] == "QQBot offline-token"
        assert req.headers["Content-Type"] == "application/json"
        assert req.headers["User-Agent"] == build_user_agent()
        assert all(0 < value <= 10 for value in req.extensions["timeout"].values())
    assert harness.adapter._http_client is harness.client
    assert not harness.client.is_closed


@pytest.mark.parametrize("field", ["strict_group_replies", "strict_group_media_replies"])
@pytest.mark.parametrize("value", [False, None, "true", 1, 0, [], {}])
def test_both_flags_require_literal_true(harness, field, value):
    harness.adapter.config.extra[field] = value
    assert harness.send().status == "rejected"
    assert harness.requests == []


@pytest.mark.parametrize("field", ["strict_group_replies", "strict_group_media_replies"])
def test_missing_flag_is_disabled(harness, field):
    del harness.adapter.config.extra[field]
    assert harness.send().status == "rejected"
    assert harness.requests == []


@pytest.mark.parametrize(("data", "mime"), [
    ("https://asset.invalid/a.png", "image/png"), ("H:/private/a.png", "image/png"),
    (b"", "image/png"), (None, "image/png"), (bytearray(b"png"), "image/png"),
    (memoryview(b"png"), "image/png"), (b"not an image", "image/png"),
    (b"\x89PNG\r\n\x1a\n", "image/png"), (b"\xff\xd8\xff", "image/jpeg"),
    (b"GIF89a" + b"x" * 50, "image/png"),
])
def test_invalid_bytes_url_and_path_are_not_uploaded(harness, data, mime):
    result = harness.send(image_data=data, mime_type=mime)
    assert (result.status, result.reason) == ("rejected", "invalid_image")
    assert harness.requests == []


@pytest.mark.parametrize("mime", [None, "image/gif", "image/PNG", " image/png", "image/png; charset=x", 1])
def test_mime_is_exact_png_or_jpeg(harness, mime):
    assert harness.send(mime_type=mime).reason == "invalid_image"
    assert harness.requests == []


@pytest.mark.parametrize("mime", ["image/png", "image/jpeg"])
def test_magic_must_match_mime(harness, mime):
    other = "image/png" if mime == "image/jpeg" else "image/jpeg"
    assert harness.send(image_data=harness.images[other], mime_type=mime).reason == "invalid_image"
    assert harness.requests == []


def test_oversize_rejects_before_base64_or_http(harness, monkeypatch):
    image = harness.images["image/png"] + b"x" * (2 * 1024 * 1024)
    monkeypatch.setattr(base64, "b64encode", lambda *args: pytest.fail("must bound before encoding"))
    assert harness.send(image_data=image).reason == "invalid_image"
    assert harness.requests == []


def test_exact_two_mib_is_a_bounded_upload(harness):
    image = harness.images["image/png"]
    image += b"x" * (2 * 1024 * 1024 - len(image))
    assert harness.send(image_data=image).status == "accepted"
    assert len(harness.requests) == 2


@pytest.mark.parametrize(("attribute", "value"), [
    ("_app_id", "other"), ("_running", False), ("_ws", None),
    ("_ws", SimpleNamespace(closed=True)), ("_http_client", None),
    ("_access_token", None), ("_access_token", ""),
    ("_token_expires_at", NOW + 60), ("_token_expires_at", float("nan")),
    ("_token_expires_at", float("inf")), ("_token_expires_at", 10**1000),
    ("_token_expires_at", "future"), ("_group_policy", "open"),
    ("_group_allow_from", ["*"]), ("_group_allow_from", ["Other"]),
])
def test_identity_connection_policy_and_cached_token_gate_upload(harness, attribute, value):
    setattr(harness.adapter, attribute, value)
    assert harness.send().status == "rejected"
    assert harness.requests == []


@pytest.mark.parametrize("changes", [
    {"expected_app_id": None}, {"expected_app_id": ""}, {"expected_app_id": "offline-app "},
    {"group_id": "Group/A"}, {"group_id": "Group%2fA"}, {"group_id": " Group-A"},
    {"group_id": None}, {"group_id": "A" * 129}, {"message_id": ""},
    {"message_id": "anchor\n"}, {"message_id": "a\x85"}, {"message_id": "a\ud800"},
    {"message_time": NOW - 285}, {"message_time": NOW + 1},
    {"message_time": float("nan")}, {"message_time": float("inf")},
    {"message_time": 10**1000}, {"message_time": True}, {"message_time": None},
])
def test_untrusted_identity_anchor_and_timestamp_reject_without_upload(harness, changes):
    assert harness.send(**changes).status == "rejected"
    assert harness.requests == []


@pytest.mark.parametrize("now", [float("nan"), float("inf"), -1, 10**1000, True, None])
def test_invalid_clock_never_uploads(harness, now):
    harness.clock.value = now
    assert harness.send().status == "rejected"
    assert harness.requests == []


@pytest.mark.parametrize(("body", "status"), [
    ({}, "unknown"), ({"file_info": None}, "unknown"), ({"file_info": 23}, "unknown"),
    ({"file_info": ""}, "unknown"), ({"file_info": " "}, "unknown"),
    ({"file_info": "x" * 16385}, "unknown"), ({"file_info": "a\x00"}, "unknown"),
    ({"file_info": "a\x7f"}, "unknown"), ({"file_info": "a\x85"}, "unknown"),
    ({"file_info": "a\ud800"}, "unknown"), ([], "unknown"), (None, "unknown"),
    ({"data": {"file_info": "nested"}}, "unknown"),
    ({"file_info": "opaque", "id": "auto-sent"}, "unknown"),
    ({"file_info": "opaque", "id": None}, "unknown"),
    ({"file_info": "opaque", "message_id": "auto-sent"}, "unknown"),
    ({"file_info": "opaque", "data": {"id": "auto-sent"}}, "unknown"),
    ({"file_info": "opaque", "code": 1}, "rejected"),
    ({"file_info": "opaque", "error": True}, "rejected"),
    ({"file_info": "opaque", "code": "0"}, "rejected"),
])
def test_untrusted_upload_receipt_never_submits_message(harness, body, status, caplog):
    # ensure_ascii preserves malformed Unicode for parser validation.
    harness.state.upload = httpx.Response(200, content=json.dumps(body).encode("ascii"))
    result = harness.send()
    assert result.status == status and result.message_id == ""
    assert len(harness.requests) == 1
    assert "opaque-file-secret" not in repr(result) + caplog.text


def test_non_json_upload_never_submits_or_logs_response(harness, caplog):
    harness.state.upload = httpx.Response(200, content=b"response-secret-not-json")
    result = harness.send()
    assert result.status == "unknown"
    assert len(harness.requests) == 1
    assert "response-secret" not in repr(result) + caplog.text


@pytest.mark.parametrize("phase", ["upload", "final"])
@pytest.mark.parametrize(("code", "status"), [
    (301, "unknown"), (302, "unknown"), (307, "unknown"), (308, "unknown"),
    (400, "rejected"), (401, "rejected"), (403, "rejected"), (408, "unknown"),
    (429, "rejected"), (500, "unknown"), (503, "unknown"),
])
def test_http_errors_and_redirects_do_not_retry(harness, phase, code, status):
    setattr(harness.state, phase, httpx.Response(
        code, json={"id": "not-accepted", "file_info": "opaque"},
        headers={"Location": "https://redirect.invalid/secret"}))
    result = harness.send()
    assert result.status == status and result.message_id == ""
    assert len(harness.requests) == (1 if phase == "upload" else 2)


@pytest.mark.parametrize("phase", ["upload", "final"])
@pytest.mark.parametrize("error", [httpx.ReadTimeout, httpx.WriteError, RuntimeError])
def test_request_errors_are_unknown_and_never_logged_or_retried(harness, phase, error, caplog):
    setattr(harness.state, phase, error("error-secret"))
    result = harness.send()
    assert result.status == "unknown" and result.message_id == ""
    assert len(harness.requests) == (1 if phase == "upload" else 2)
    assert "error-secret" not in repr(result) + caplog.text


@pytest.mark.parametrize(("body", "status"), [
    ({}, "unknown"), ({"id": None}, "unknown"), ({"id": "a\n"}, "unknown"),
    ({"id": "a\ud800"}, "unknown"), ({"id": "receipt", "code": 1}, "rejected"),
    ({"id": "receipt", "code": 0, "error": 0}, "accepted"),
])
def test_final_receipt_uses_existing_strict_classifier(harness, body, status):
    harness.state.final = httpx.Response(200, content=json.dumps(body).encode("ascii"))
    result = harness.send()
    assert result.status == status
    assert result.message_id == ("receipt" if status == "accepted" else "")
    assert len(harness.requests) == 2


def test_maximum_valid_file_info_is_opaque_and_preserved(harness):
    file_info = "材" * 16384
    harness.state.upload = httpx.Response(201, json={"file_info": file_info, "code": 0})
    assert harness.send().status == "accepted"
    assert json.loads(harness.requests[1].content)["media"] == {"file_info": file_info}


@pytest.mark.parametrize("phase", ["upload", "final"])
def test_network_tripwire_propagates_through_real_adapter(harness, phase):
    setattr(harness.state, phase, OfflineNetworkAttempt("inert violation"))
    with pytest.raises(OfflineNetworkAttempt):
        harness.send()
    assert len(harness.requests) == (1 if phase == "upload" else 2)


@pytest.mark.parametrize("at_check", [1, 2])
@pytest.mark.parametrize("value", [False, None, 1, "true", [], {}])
def test_recheck_only_literal_true_permits_each_request(harness, at_check, value):
    calls = []

    async def recheck():
        calls.append(len(harness.requests))
        return value if len(calls) == at_check else True

    result = harness.send(recheck=recheck)
    assert result.status == "rejected"
    assert calls == ([0] if at_check == 1 else [0, 1])
    assert len(harness.requests) == at_check - 1


@pytest.mark.parametrize("at_check", [1, 2])
def test_recheck_exception_never_submits_or_leaks(harness, at_check, caplog):
    count = 0

    async def recheck():
        nonlocal count
        count += 1
        if count == at_check:
            raise ValueError("policy-secret")
        return True

    result = harness.send(recheck=recheck)
    assert result.status == "unknown"
    assert len(harness.requests) == at_check - 1
    assert "policy-secret" not in repr(result) + caplog.text


@pytest.mark.parametrize("at_check", [1, 2])
@pytest.mark.parametrize("mutation", [
    "media_disabled", "text_disabled", "app_changed", "offline", "closed_client",
    "client_changed", "allowlist_changed", "policy_changed", "token_changed", "token_stale",
    "expired", "rollback", "invalid_clock",
])
def test_every_await_boundary_revalidates_transport_and_original_window(harness, at_check, mutation):
    count = 0

    async def recheck():
        nonlocal count
        count += 1
        if count == at_check:
            await mutate(harness, mutation)
        return True

    result = harness.send(recheck=recheck)
    assert result.status == "rejected"
    assert len(harness.requests) == at_check - 1


async def mutate(harness, mutation):
    adapter = harness.adapter
    if mutation == "media_disabled":
        adapter.config.extra["strict_group_media_replies"] = False
    elif mutation == "text_disabled":
        adapter.config.extra["strict_group_replies"] = False
    elif mutation == "app_changed":
        adapter._app_id = "replacement"
    elif mutation == "offline":
        adapter._running = False
    elif mutation == "closed_client":
        await harness.client.aclose()
    elif mutation == "client_changed":
        adapter._http_client = SimpleNamespace(is_closed=False)
    elif mutation == "allowlist_changed":
        adapter._group_allow_from = []
    elif mutation == "policy_changed":
        adapter._group_policy = "open"
    elif mutation == "token_changed":
        adapter._access_token = "replacement-token"
    elif mutation == "token_stale":
        adapter._token_expires_at = NOW + 60
    elif mutation == "expired":
        harness.clock.value = NOW + 284
    elif mutation == "rollback":
        harness.clock.value = NOW - 0.25
    elif mutation == "invalid_clock":
        harness.clock.value = float("nan")
    else:
        raise AssertionError(mutation)


@pytest.mark.parametrize("mutation", [
    "media_disabled", "text_disabled", "app_changed", "offline", "client_changed",
    "allowlist_changed", "policy_changed", "token_changed", "token_stale", "expired", "rollback",
])
def test_upload_latency_cannot_preserve_revoked_permission(harness, mutation):
    async def upload(req):
        await mutate(harness, mutation)
        return httpx.Response(200, json={"file_info": "opaque"})

    harness.state.upload = upload
    result = harness.send()
    assert result.status == "rejected"
    assert len(harness.requests) == 1


def test_rollback_cannot_be_hidden_by_later_callback_clock_recovery(harness):
    async def upload(req):
        harness.clock.value = NOW - 0.25
        return httpx.Response(200, json={"file_info": "opaque"})

    async def recheck():
        harness.clock.value = NOW
        return True

    harness.state.upload = upload
    assert harness.send(recheck=recheck).status == "rejected"
    assert len(harness.requests) == 1


@pytest.mark.parametrize("phase", ["upload", "final", "first_check", "second_check"])
def test_overall_deadline_includes_both_requests_and_policy_waits(harness, phase):
    async def scenario():
        blocked = asyncio.Event()
        checks = 0

        async def hold(req):
            await blocked.wait()
            raise AssertionError("deadline must terminate the wait")

        async def recheck():
            nonlocal checks
            checks += 1
            if phase == ("first_check" if checks == 1 else "second_check"):
                await blocked.wait()
            return True

        if phase in {"upload", "final"}:
            setattr(harness.state, phase, hold)
        # Outer timeout is only a test-harness safety fence for RED, not the implementation.
        result = await asyncio.wait_for(harness.adapter.send_group_image_reply_once(
            **harness.kwargs(message_time=NOW - 284.97, recheck=recheck)), timeout=0.4)
        assert result.status == "unknown"

    harness.loop.run_until_complete(scenario())
    assert len(harness.requests) == {"upload": 1, "final": 2, "first_check": 0, "second_check": 1}[phase]


@pytest.mark.parametrize("phase", ["upload", "final", "first_check", "second_check"])
def test_cancellation_propagates_without_retry_or_fallback(harness, phase):
    async def scenario():
        started, blocked = asyncio.Event(), asyncio.Event()
        checks = 0

        async def hold(req):
            started.set()
            await blocked.wait()

        async def recheck():
            nonlocal checks
            checks += 1
            if phase == ("first_check" if checks == 1 else "second_check"):
                started.set()
                await blocked.wait()
            return True

        if phase in {"upload", "final"}:
            setattr(harness.state, phase, hold)
        task = asyncio.create_task(harness.adapter.send_group_image_reply_once(**harness.kwargs(recheck=recheck)))
        await asyncio.wait_for(started.wait(), timeout=0.4)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()

    harness.loop.run_until_complete(scenario())
    assert len(harness.requests) == {"upload": 1, "final": 2, "first_check": 0, "second_check": 1}[phase]


@pytest.fixture
def ticket_harness(harness, monkeypatch):
    from gateway import owned_group_service as service

    active = ContextVar("test_authorization_home", default=None)
    home = Path("offline-transport-home")
    runtime = Path("different-offline-runtime-home")
    state = SimpleNamespace(allowed=True, checks=0)

    @contextmanager
    def scope(target):
        assert target in (home, runtime)
        token = active.set(target)
        try:
            yield
        finally:
            active.reset(token)

    monkeypatch.setattr(service, "_scope", scope)
    ticket = service.OwnedGroupTicket(
        bot_id="offline-app", runtime_id="runtime", transport_profile="receiving-profile",
        group_id="Group-A", user_id="user", session_id="session", message_id="incoming-id",
        message_time=NOW - 1, text="original", addressed=True, runtime_home=runtime,
        ledger_path=home / "plugin-data/owned-group-outbound/state.db")

    def authorized(candidate):
        assert candidate is ticket
        assert active.get() == home
        state.checks += 1
        return state.allowed

    binding = SimpleNamespace(adapter=harness.adapter, authorization_home=home, authorized=authorized)
    object.__setattr__(ticket, "_binding", binding)
    return SimpleNamespace(ticket=ticket, state=state, active=active, home=home, binding=binding)


def test_ticket_image_is_target_bound_scoped_and_not_serialized(harness, ticket_harness):
    t = ticket_harness
    before = t.ticket.as_record()
    seen = []

    async def recheck():
        # Transport authorization and routed plugin policy have different homes.
        assert t.active.get() == t.ticket.runtime_home
        seen.append(len(harness.requests))
        return True

    result = harness.loop.run_until_complete(t.ticket.submit_image(
        harness.images["image/png"], "image/png", recheck=recheck))
    assert (result.status, result.message_id) == ("accepted", "sent-id")
    assert seen == [0, 1]
    assert t.state.checks >= 5  # entry + authorization before/after both policy awaits
    assert t.active.get() is None
    assert t.ticket.as_record() == before
    assert not any("recheck" in key or "image" in key for key in before)
    final = json.loads(harness.requests[1].content)
    assert final["msg_id"] == t.ticket.message_id and final["msg_seq"] == 1


@pytest.mark.parametrize("phase", ["entry", "upload", "first_check", "second_check"])
def test_ticket_revocation_never_crosses_next_request(harness, ticket_harness, phase):
    t = ticket_harness
    checks = 0
    if phase == "entry":
        t.state.allowed = False

    async def upload(req):
        t.state.allowed = False
        return httpx.Response(200, json={"file_info": "opaque"})

    async def recheck():
        nonlocal checks
        checks += 1
        if phase == ("first_check" if checks == 1 else "second_check"):
            t.state.allowed = False
        return True

    if phase == "upload":
        harness.state.upload = upload
    result = harness.loop.run_until_complete(t.ticket.submit_image(
        harness.images["image/png"], "image/png", recheck=recheck))
    assert result.status == "rejected"
    assert len(harness.requests) == (1 if phase in {"upload", "second_check"} else 0)
    assert t.active.get() is None


def test_unbound_ticket_image_rejects_without_http(harness, ticket_harness):
    ticket = ticket_harness.ticket
    object.__setattr__(ticket, "_binding", None)
    result = harness.loop.run_until_complete(ticket.submit_image(
        harness.images["image/png"], "image/png", recheck=harness.kwargs()["recheck"]))
    assert result.status == "rejected"
    assert harness.requests == []


def test_ticket_has_no_arbitrary_target_parameters(harness, ticket_harness):
    with pytest.raises(TypeError):
        ticket_harness.ticket.submit_image(
            harness.images["image/png"], "image/png", group_id="Other",
            recheck=harness.kwargs()["recheck"])
    assert harness.requests == []


def test_client_auth_cannot_replay_either_post(harness):
    class ReplayAuth(httpx.Auth):
        def auth_flow(self, request):
            yield request
            yield request

    harness.client.auth = ReplayAuth()
    assert harness.send().status == "accepted"
    assert len(harness.requests) == 2


def test_receipt_returning_after_total_deadline_is_unknown(harness):
    async def final(req):
        # Deliberately block the loop: a timeout callback cannot run until control
        # returns. The sender must still reject a late receipt as indeterminate.
        time.sleep(0.06)  # noqa: ASYNC251 -- exercise a blocked-loop deadline
        return httpx.Response(200, json={"id": "late-receipt"})

    harness.state.final = final
    result = harness.send(message_time=NOW - 284.97)
    assert result.status == "unknown" and result.message_id == ""
    assert len(harness.requests) == 2


def test_characterization_total_budget_is_single_and_capped_at_twenty_seconds(harness, monkeypatch):
    from plugins.platforms.qqbot import strict_media

    original = asyncio.timeout
    budgets = []

    def timeout(delay):
        budgets.append(delay)
        return original(delay)

    monkeypatch.setattr(strict_media.asyncio, "timeout", timeout)
    assert harness.send().status == "accepted"
    assert budgets == [20.0]


@pytest.mark.parametrize("phase", ["upload", "final"])
def test_characterization_existing_response_hooks_are_not_bypassed(harness, phase):
    guarded = []

    async def guard(response):
        guarded.append(response.request.url.path)
        if response.request.url.path.endswith("/files" if phase == "upload" else "/messages"):
            raise ValueError("guard-secret")

    harness.client.event_hooks["response"].append(guard)
    assert harness.send().status == "unknown"
    assert len(harness.requests) == (1 if phase == "upload" else 2)
    assert len(guarded) == len(harness.requests)
    assert harness.client.event_hooks["response"] == [guard]


def test_characterization_cancellation_before_start_performs_no_request(harness):
    task = harness.loop.create_task(harness.adapter.send_group_image_reply_once(**harness.kwargs()))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        harness.loop.run_until_complete(task)
    assert harness.requests == []


@pytest.mark.parametrize("replacement", [None, lambda: True])
def test_characterization_non_async_recheck_never_uploads(harness, replacement):
    assert harness.send(recheck=replacement).status != "accepted"
    assert harness.requests == []


@pytest.mark.parametrize("phase", ["first_check", "second_check"])
def test_characterization_ticket_restores_scope_when_cancelled(harness, ticket_harness, phase):
    async def scenario():
        started, blocked = asyncio.Event(), asyncio.Event()
        calls = 0

        async def recheck():
            nonlocal calls
            calls += 1
            if phase == ("first_check" if calls == 1 else "second_check"):
                started.set()
                await blocked.wait()
            return True

        task = asyncio.create_task(ticket_harness.ticket.submit_image(
            harness.images["image/png"], "image/png", recheck=recheck))
        await asyncio.wait_for(started.wait(), timeout=0.4)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert ticket_harness.active.get() is None

    harness.loop.run_until_complete(scenario())
    assert len(harness.requests) == (0 if phase == "first_check" else 1)


@pytest.mark.parametrize("wait_at", ["second_check", "final"])
def test_forward_clock_jump_shortens_remaining_anchor_deadline(harness, wait_at):
    async def scenario():
        blocked = asyncio.Event()
        checks = 0

        async def upload(req):
            harness.clock.value = NOW + 283.97
            return httpx.Response(200, json={"file_info": "opaque"})

        async def final(req):
            await blocked.wait()

        async def recheck():
            nonlocal checks
            checks += 1
            if checks == 2 and wait_at == "second_check":
                await blocked.wait()
            return True

        harness.state.upload, harness.state.final = upload, final
        result = await asyncio.wait_for(harness.adapter.send_group_image_reply_once(
            **harness.kwargs(recheck=recheck)), timeout=0.4)
        assert result.status == "unknown"

    harness.loop.run_until_complete(scenario())
    assert len(harness.requests) == (1 if wait_at == "second_check" else 2)
