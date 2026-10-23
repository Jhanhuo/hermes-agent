"""Actual runner registration, observation and busy callback entry seams."""
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import GatewayConfig
from gateway.platforms.event import MessageEvent
from hermes_constants import get_hermes_home
from tests.gateway.test_owned_group_service import rig


def test_observe_through_authorized_runner_is_separate_from_turn(rig):
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation != "shutdown":
            seen.append((operation, ticket, host, get_hermes_home()))
    rig.register(consumer)
    event = rig.event()
    envelope = {"platform": "qqbot", "event_type": "group_message", "payload": {
        "schema_version": "qq.group_message.v1", "chat_id": "Group-A", "chat_type": "group",
        "user_id": "Member", "message_id": "incoming-id", "text": "ordinary text",
        "is_mention": False, "is_bot": False, "message_timestamp": event.raw_message["timestamp"],
        "qq_event_type": "GROUP_MESSAGE_CREATE"}}
    handler = rig.runner._make_default_profile_platform_event_handler()
    rig.loop.run_until_complete(handler(envelope, event.source))
    assert len(seen) == 1
    operation, ticket, _host, home = seen[0]
    assert operation == "observe" and not ticket.addressed and home == rig.runtime
    assert ticket.text == "ordinary text" and ticket.message_time == 1_899_999_999.0
    assert rig.requests == []


def test_initialized_secondary_ticks_at_runner_boot_without_message(rig, monkeypatch):
    seen = asyncio.Event()
    scopes = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "tick":
            scopes.append((get_hermes_home(), host.runtime_id))
            seen.set()
    rig.register(consumer)
    runner = rig.runner
    monkeypatch.setattr("hermes_cli.env_loader.hydrate_profile_secret_sources", lambda *a, **k: None)
    monkeypatch.setattr("gateway.run_startup.recover_left_core_in", lambda *a, **k: None)
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr("gateway.config.load_gateway_config", lambda: GatewayConfig())
    monkeypatch.setattr("hermes_cli.plugins.discover_plugins", lambda *a, **k: None)
    runner._subscribe_plugin_rewire = lambda *a: None
    runner._register_config_hooks = lambda *a, **k: None
    runner._snapshot_profile_busy_modes = lambda *a: None
    runner._spawn_supervised = lambda *a, **k: None
    runner._spawn_reconnect_watcher = lambda: None
    runner._failed_platforms = {}
    runner._scale_to_zero_should_arm = lambda: False
    runner._log_scale_to_zero_not_armed_reason = lambda: None
    async def exercise():
        await runner._load_secondary_profile_config("runtime", rig.runtime)
        runner._start_spawn_background_watchers()
        await asyncio.wait_for(seen.wait(), 2)
        assert await runner._owned_group_service.shutdown(timeout=2)
    rig.loop.run_until_complete(exercise())
    assert scopes == [(rig.runtime, "runtime")]
    assert rig.requests == []


def test_runner_teardown_closes_owned_callbacks_before_notifications(rig):
    from gateway.owned_group_service import service_for
    operations = []
    async def consumer(operation, ticket=None, host=None):
        operations.append(operation)
    rig.register(consumer)
    service = service_for(rig.runner)
    service.register_runtime("runtime", rig.runtime)
    runner = rig.runner
    runner._restart_requested = False
    runner._clear_plugin_message_injector = lambda: None
    runner._mark_api_runs_shutdown_requested = lambda: None
    runner._stop_hosted_room_worker = None
    runner._stop_systemd_watchdog = None
    runner._cancel_secondary_profile_reconnect_tasks = AsyncMock()
    async def notify():
        assert service.closed
        assert operations == ["shutdown"]
    runner._notify_active_sessions_of_shutdown = notify
    ctx = SimpleNamespace(started_at=0, elapsed=lambda: 0)
    rig.loop.run_until_complete(runner._stop_begin_teardown(ctx))
    assert service.closed


@pytest.mark.parametrize("busy", [False, True])
def test_real_qq_entry_owned_text_never_queues_steers_or_autodelivers(rig, monkeypatch, busy):
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "turn":
            seen.append(ticket)
            assert (await ticket.submit("one controlled reply")).status == "accepted"
            return "forbidden native final"
    rig.register(consumer)
    runner, adapter = rig.runner, rig.adapter
    runner.session_store = None
    runner._busy_text_mode = "steer"
    native_busy = AsyncMock(return_value=True)
    runner._wire_adapter_handlers(adapter, busy_session_handler=native_busy)
    ordinary_send = AsyncMock(side_effect=AssertionError("ordinary send forbidden"))
    monkeypatch.setattr(adapter, "send", ordinary_send)
    monkeypatch.setattr(adapter, "_release_turn_marker", AsyncMock())
    if busy:
        key = adapter._event_session_key(rig.event())
        adapter._active_sessions[key] = asyncio.Event()
    async def exercise():
        await adapter._on_message("GROUP_AT_MESSAGE_CREATE", {
            "group_openid": "Group-A", "id": "incoming-id", "content": "hello",
            "author": {"member_openid": "Member"}, "timestamp": "2030-03-17T17:46:39+00:00"})
        if adapter._background_tasks:
            await asyncio.gather(*list(adapter._background_tasks))
    rig.loop.run_until_complete(exercise())
    assert len(seen) == len(rig.requests) == 1
    native_busy.assert_not_awaited()
    ordinary_send.assert_not_awaited()
    assert adapter._pending_messages == {}


@pytest.mark.parametrize("raw_time", ["2030-03-17T17:46:39Z", None, "malformed"])
def test_real_full_observation_uses_original_platform_anchor_only(rig, raw_time):
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation != "shutdown":
            seen.append((operation, ticket))
    rig.register(consumer)
    runner, adapter = rig.runner, rig.adapter
    runner.session_store = None
    runner._busy_text_mode = "steer"
    runner._wire_adapter_handlers(adapter)
    adapter.observe_group_messages, adapter.require_mention = True, True
    rig.loop.run_until_complete(adapter._on_message("GROUP_MESSAGE_CREATE", {
        "group_openid": "Group-A", "id": "ordinary-id", "content": "ordinary",
        "author": {"member_openid": "Member"}, "timestamp": raw_time}))
    assert [operation for operation, _ in seen] == (["observe"] if raw_time and raw_time != "malformed" else [])
    if seen:
        assert seen[0][1].message_id == "ordinary-id" and not seen[0][1].addressed
    assert not adapter._background_tasks and not adapter._pending_messages
    assert rig.requests == []


def test_secondary_transport_routes_runtime_and_restores_only_same_bot(rig, monkeypatch):
    from gateway.config import Platform, PlatformConfig
    from plugins.platforms.qqbot.adapter import QQAdapter
    from gateway.profile_routing import parse_profile_routes
    from hermes_cli.plugins import PluginManager
    transport_home = rig.home / "profiles" / "transport"
    transport_home.mkdir()
    (transport_home / "config.yaml").write_text("{}", encoding="utf-8")
    homes = {"default": rig.home, "runtime": rig.runtime, "transport": transport_home}
    monkeypatch.setattr("hermes_cli.profiles.get_profile_dir", lambda name: homes[name])
    monkeypatch.setattr("hermes_cli.profiles.profiles_to_serve", lambda *a, **k: list(homes.items()))
    rig.managers[transport_home] = PluginManager()
    adapter = QQAdapter(PlatformConfig(enabled=True, extra=dict(rig.adapter.config.extra, app_id="secondary-app")))
    adapter.gateway_runner = rig.runner
    adapter.set_owner_profile("transport")
    adapter._running, adapter._ws = True, SimpleNamespace(closed=False)
    adapter._http_client = rig.adapter._http_client
    adapter._access_token, adapter._token_expires_at = "secondary-token", rig.adapter._token_expires_at
    rig.runner._profile_adapters["transport"] = {Platform.QQBOT: adapter}
    rig.runner.config.profile_routes = parse_profile_routes([
        {"platform": "qqbot", "chat_id": "Group-A", "profile": "runtime", "bot_profile": "transport"}])
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "turn":
            seen.append((ticket, host, get_hermes_home()))
            assert (await ticket.submit("secondary reply")).status == "accepted"
    rig.register(consumer)
    event = rig.event()
    event.source = adapter.build_source(chat_id="Group-A", user_id="Member", chat_type="group")
    handler = rig.runner._make_profile_message_handler("transport")
    assert rig.loop.run_until_complete(handler(event)) is None
    ticket, host, scope = seen[0]
    assert scope == rig.runtime and ticket.transport_profile == "transport"
    assert ticket.ledger_path == transport_home / "plugin-data/owned-group-outbound/state.db"
    assert all(home == transport_home for home in rig.auth)
    assert rig.requests[0].headers["Authorization"] == "QQBot secondary-token"
    assert host.restore(ticket.as_record()).authorized()
    rig.runner._profile_adapters["transport"].clear()
    assert host.restore(ticket.as_record()) is None
    assert not ticket.authorized()


def test_real_transport_acl_is_not_runtime_acl(rig, monkeypatch):
    monkeypatch.setattr("agent.secret_scope._MULTIPLEX_ACTIVE", True)
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "turn":
            seen.append(ticket)
    rig.register(consumer)
    del rig.runner._is_user_authorized
    rig.runner.pairing_store, rig.runner.pairing_stores = None, {}
    # Synthetic secrets only; the runtime has an opposing policy.
    (rig.home / ".env").write_text("QQ_ALLOWED_USERS=Member\n", encoding="utf-8")
    (rig.runtime / ".env").write_text("QQ_ALLOWED_USERS=SomeoneElse\n", encoding="utf-8")
    rig.loop.run_until_complete(rig.runner._handle_message(rig.event()))
    assert len(seen) == 1 and seen[0].authorized()
    (rig.home / ".env").write_text("QQ_ALLOWED_USERS=SomeoneElse\n", encoding="utf-8")
    assert not seen[0].authorized()
    assert rig.loop.run_until_complete(seen[0].submit("denied")).status == "rejected"
    assert rig.requests == []
