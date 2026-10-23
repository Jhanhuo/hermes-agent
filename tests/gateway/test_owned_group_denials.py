"""Denials and compatibility at the real admitted owned-group boundary."""
import dataclasses
import time
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.platforms.event import MessageType
from tests.gateway.test_owned_group_service import rig


def capture(rig):
    values = []
    async def callback(operation, ticket=None, host=None):
        if operation != "shutdown":
            values.append((operation, ticket, host))
    registration = rig.register(callback)
    return values, registration


@pytest.mark.parametrize("raw", [None, "", "not-time", 1900000000000, True, "nan", "inf",
    "2030-03-17", "2030-03-17T17:46:39", " 2030-03-17T17:46:39Z", "2030-03-17T17:46:39Z ",
    "2030-02-30T17:46:39Z", "2030-03-17T17:46:41Z", "2030-03-17T17:41:55Z", "1" * 1000])
def test_bad_platform_time_silences_without_adapter_now_fallback(rig, raw):
    seen, _ = capture(rig)
    event = rig.event(raw_message={"timestamp": raw, "group_openid": "Group-A"})
    assert rig.loop.run_until_complete(rig.runner._handle_message(event)) is None
    assert seen == rig.requests == []


@pytest.mark.parametrize("kind", ["missing", "multiple", "sync", "raises"])
def test_consumer_unavailable_or_raises_never_native_fallback(rig, kind):
    async def callback(operation, ticket=None, host=None):
        if kind == "raises":
            raise RuntimeError("offline failure")
        pytest.fail("ambiguous consumer was called")
    if kind in {"multiple", "raises"}:
        rig.register(callback)
    if kind == "multiple":
        rig.register(callback)
    if kind == "sync":
        rig.register(lambda *a, **k: pytest.fail("sync consumer was called"))
    assert rig.loop.run_until_complete(rig.runner._handle_message(rig.event())) is None
    assert rig.requests == []


@pytest.mark.parametrize("mode", ["slash", "rewritten-slash", "dm", "guild", "disabled", "literal-one", "unselected"])
def test_compatibility_keeps_native_path(rig, mode):
    seen, _ = capture(rig)
    event = rig.event()
    rig.runner._hm_estop_gate = lambda *args: "native compatibility"
    if mode in {"slash", "rewritten-slash"}:
        event.text = "/unknown-command"
        if mode == "rewritten-slash":
            rig.runner._hm_pre_gateway_dispatch_hook = AsyncMock(side_effect=lambda e, s: dataclasses.replace(e, text="rewritten text"))
    elif mode == "dm":
        event.source.chat_type = "dm"
    elif mode == "guild":
        rig.adapter._chat_type_map["Group-A"] = "guild"
    elif mode == "unselected":
        rig.adapter.config.extra["owned_group_allow_from"] = ["Other"]
    else:
        rig.adapter.config.extra["owned_group_replies"] = 1 if mode == "literal-one" else False
    assert rig.loop.run_until_complete(rig.runner._handle_message(event)) == "native compatibility"
    assert seen == rig.requests == []


def test_selected_admission_exception_is_silent(rig):
    seen, _ = capture(rig)
    def broken(source):
        raise RuntimeError("offline ACL unavailable")
    rig.runner._is_user_authorized = broken
    assert rig.loop.run_until_complete(rig.runner._handle_message(rig.event())) is None
    assert seen == rig.requests == []


def test_selected_original_text_rewritten_to_slash_still_cannot_escape(rig):
    seen, _ = capture(rig)
    rig.runner._hm_pre_gateway_dispatch_hook = AsyncMock(side_effect=lambda e, s: dataclasses.replace(e, text="/help"))
    assert rig.loop.run_until_complete(rig.runner._handle_message(rig.event())) is None
    assert seen == rig.requests == []


@pytest.mark.parametrize("change", ["strict", "group-policy", "acl", "drain", "external-drain", "bot", "removed", "duplicate-hook", "ticket-change"])
def test_submit_rechecks_live_authorization_and_fixed_binding(rig, change):
    seen, _ = capture(rig)
    rig.loop.run_until_complete(rig.runner._handle_message(rig.event()))
    _, ticket, _host = seen[0]
    if change == "strict":
        rig.adapter.config.extra["strict_group_replies"] = False
    elif change == "group-policy":
        rig.adapter._group_allow_from = []
    elif change == "acl":
        rig.runner._is_user_authorized = lambda source: False
    elif change == "drain":
        rig.runner._draining = True
    elif change == "external-drain":
        rig.runner._external_drain_active = True
    elif change == "bot":
        rig.adapter._app_id = "different-bot"
    elif change == "removed":
        rig.runner.adapters.clear()
    elif change == "duplicate-hook":
        capture(rig)
    else:
        ticket = dataclasses.replace(ticket, message_time=time.time(), message_id="different")
    assert not ticket.authorized()
    assert rig.loop.run_until_complete(ticket.submit("no" )).status == "rejected"
    assert rig.requests == []


@pytest.mark.parametrize(("field", "value"), [("message_id", "bad\n"), ("text", "bad\x00"),
    ("text", "x" * 2001), ("user_id", "Member\n")])
def test_malformed_ticket_inputs_cannot_call_consumer(rig, field, value):
    seen, _ = capture(rig)
    event = rig.event()
    if field == "user_id":
        event.source.user_id = value
        rig.runner._is_user_authorized = lambda source: True
    else:
        setattr(event, field, value)
    assert rig.loop.run_until_complete(rig.runner._handle_message(event)) is None
    assert seen == rig.requests == []


@pytest.mark.parametrize("mutation", ["opt-out", "other-source", "lost-provenance", "wrong-raw-group", "wrong-event-kind"])
def test_selected_ownership_latch_and_received_provenance(rig, mutation):
    from gateway.session import SessionSource
    seen, _ = capture(rig)
    event = rig.event()
    if mutation == "opt-out":
        async def rewrite(e, s):
            rig.adapter.config.extra["owned_group_replies"] = False
            return e
        rig.runner._hm_pre_gateway_dispatch_hook = rewrite
    elif mutation == "other-source":
        async def rewrite(e, s):
            source = rig.adapter.build_source(chat_id="Other", user_id="Member", chat_type="group")
            return dataclasses.replace(e, source=source)
        rig.runner._hm_pre_gateway_dispatch_hook = rewrite
    elif mutation == "lost-provenance":
        from gateway.owned_group_service import selected
        assert selected(rig.runner, event.source)
        async def rewrite(e, s):
            source = SessionSource(platform=Platform.QQBOT, chat_id="Group-A", user_id="Member", chat_type="group")
            rig.runner._canonicalize(source)
            assert rig.runner._intake_adapter_for(source) is None
            assert not selected(rig.runner, source)
            return dataclasses.replace(e, source=source)
        rig.runner._hm_pre_gateway_dispatch_hook = rewrite
    elif mutation == "wrong-raw-group":
        event.raw_message["group_openid"] = "Different"
    else:
        event.metadata["qq_event_type"] = "AT_MESSAGE_CREATE"
    assert rig.loop.run_until_complete(rig.runner._handle_message(event)) is None
    assert seen == rig.requests == []


def test_synthetic_source_is_not_selected_and_native_codec_denies_before_llm(request, monkeypatch):
    from gateway.owned_group_service import selected
    from gateway.session import SessionSource
    from gateway.session_envelope import prepare_native, snapshot_native
    from gateway.session_identity import identity_of
    from hermes_state_runtime import RuntimeStoreError

    case = request.getfixturevalue("rig")
    runner = case.runner
    seen, _ = capture(case)
    event = case.event()
    event.source = SessionSource(platform=Platform.QQBOT, chat_id="Group-A", user_id="Member", chat_type="group")
    runner._canonicalize(event.source)
    assert identity_of(event.source).transport_inferred
    assert runner._intake_adapter_for(event.source) is None
    assert not selected(runner, event.source)
    # Valid QQ metadata is descriptive only: it cannot mint receiver provenance.
    with pytest.raises(RuntimeStoreError) as error:
        snapshot_native(runner, event)
    assert error.value.field == "authorization_home_without_provenance"
    del event.source._authorization_profile_home
    with pytest.raises(RuntimeStoreError) as error:
        snapshot_native(runner, event)
    assert error.value.field == "multiplex_without_provenance"

    # Exercise real authorization and the admitted handler down to the codec, not
    # the old native-gate sentinel (which runs before durable admission).
    del runner._is_user_authorized
    monkeypatch.setattr("agent.secret_scope._MULTIPLEX_ACTIVE", True)
    for manager in case.managers.values():
        manager._discovered = True
    (case.home / ".env").write_text("QQ_ALLOWED_USERS=Member\n", encoding="utf-8")
    runner.pairing_store, runner.pairing_stores = None, {}
    runner._hm_estop_gate = lambda *args: None
    runner._hm_pending_reply_intercepts = AsyncMock(return_value=None)
    llm = runner._handle_message_with_agent = AsyncMock(side_effect=AssertionError("unproven native turn"))
    codec_calls = []

    async def native_admission(event, *args):
        codec_calls.append(event)
        await prepare_native(runner, event)
        pytest.fail("unproven event passed native codec")

    runner._hm_authority_admission = native_admission
    with pytest.raises(RuntimeStoreError) as error:
        case.loop.run_until_complete(runner._handle_message(event))
    assert error.value.reason == "invalid_params"
    assert error.value.field == "multiplex_without_provenance"
    assert codec_calls == [event]
    llm.assert_not_awaited()
    assert not selected(runner, event.source)
    assert seen == case.requests == []
