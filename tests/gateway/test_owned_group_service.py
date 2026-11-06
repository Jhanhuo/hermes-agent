"""Owned QQ intake uses canonical transport identity, never the native agent."""
import asyncio
import dataclasses
import json
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from plugins.platforms.qqbot.adapter import QQAdapter
from gateway.platforms.event import MessageEvent
from gateway.profile_routing import parse_profile_routes
from gateway.run import GatewayRunner, _profile_runtime_scope
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from hermes_constants import get_hermes_home
from tests.gateway.qq_offline_guard import OfflineGuard


@pytest.fixture
def rig(tmp_path, monkeypatch):
    loop = asyncio.new_event_loop()  # Windows socketpair before network tripwires.
    home = tmp_path / "home"
    runtime = home / "profiles" / "runtime"
    runtime.mkdir(parents=True)
    for path in (home, runtime):
        (path / "config.yaml").write_text("{}", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(time, "time", lambda: 1_900_000_000.0)
    monkeypatch.setattr("hermes_cli.profiles.profiles_to_serve", lambda *a, **kw: [("default", home), ("runtime", runtime)])
    monkeypatch.setattr("hermes_cli.profiles.get_profile_dir", lambda name: home if name == "default" else runtime)
    monkeypatch.setattr("hermes_cli.profiles.profile_exists", lambda name: True)
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(multiplex_profiles=True)
    runner.config.profile_routes = parse_profile_routes([
        {"name": "group", "platform": "qqbot", "chat_id": "Group-A", "profile": "runtime"}])
    runner._primary_profile_name = "default"
    runner._profile_adapters = {"runtime": {}}
    runner._running, runner._draining = True, False
    runner._external_drain_active = False
    runner._background_tasks = set()
    runner._shutdown_event = asyncio.Event()
    runner._scale_to_zero_note_real_inbound = lambda: None
    runner._admit_bot_message = lambda source: True
    auth = []
    runner._is_user_authorized = lambda source: auth.append(get_hermes_home()) or source.user_id == "Member"
    runner._hm_pre_gateway_dispatch_hook = AsyncMock(side_effect=lambda event, source: event)
    runner._hm_estop_gate = lambda *a: pytest.fail("owned message fell through to native pipeline")
    requests = []
    transport = httpx.MockTransport(lambda request: requests.append(request) or httpx.Response(200, json={"id": "receipt"}))
    with OfflineGuard() as guard:
        client = httpx.AsyncClient(transport=transport, trust_env=False)
        guard.allow_client(client, transport)
        adapter = QQAdapter(PlatformConfig(enabled=True, extra={
            "app_id": "offline-app", "client_secret": "offline-secret",
            "group_policy": "allowlist", "group_allow_from": ["Group-A"],
            "owned_group_replies": True, "owned_group_allow_from": ["Group-A"],
            "strict_group_replies": True,
        }))
        adapter.gateway_runner = runner
        adapter._running, adapter._ws = True, SimpleNamespace(closed=False)
        adapter._http_client, adapter._access_token = client, "offline-token"
        adapter._token_expires_at = time.time() + 3600
        runner.adapters = {Platform.QQBOT: adapter}
        managers = {home: PluginManager(), runtime: PluginManager()}
        monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager", lambda: managers[Path(get_hermes_home())])
        def register(callback, path=runtime):
            return PluginContext(PluginManifest(name="owned-test", source="user"), managers[path]).register_hook("gateway_owned_group", callback)
        def event(text="hello", **changes):
            source = adapter.build_source(chat_id="Group-A", user_id="Member", chat_type="group")
            values = dict(text=text, source=source, message_id="incoming-id", reply_expected=True,
                          raw_message={"timestamp": "2030-03-17T17:46:39+00:00", "group_openid": "Group-A"},
                          metadata={"qq_event_type": "GROUP_AT_MESSAGE_CREATE", "is_mention": True})
            values.update(changes)
            return MessageEvent(**values)
        obj = SimpleNamespace(loop=loop, runner=runner, adapter=adapter, event=event, home=home,
                              runtime=runtime, requests=requests, auth=auth, register=register, managers=managers,
                              offline_guard=guard)
        yield obj
        service = getattr(runner, "_owned_group_service", None)
        if service is not None:
            async def cleanup():
                await service.shutdown(timeout=2)
                await asyncio.gather(*list(service.tasks), return_exceptions=True)
            loop.run_until_complete(cleanup())
        loop.run_until_complete(client.aclose())
    loop.close()


def test_selected_missing_consumer_is_silent_not_native(rig):
    assert rig.loop.run_until_complete(rig.runner._handle_message(rig.event())) is None
    assert rig.requests == []


def test_turn_ticket_is_scoped_frozen_and_sends_exact_receiving_anchor(rig):
    seen = []
    async def callback(operation, ticket=None, host=None):
        seen.append((operation, ticket, host, get_hermes_home()))
        assert ticket.authorized()
        assert (await ticket.submit("owned reply")).status == "accepted"
        return "never auto-deliver this"
    rig.register(callback)
    assert rig.loop.run_until_complete(rig.runner._handle_message(rig.event())) is None
    assert len(seen) == 1
    operation, ticket, host, scope = seen[0]
    assert operation == "turn" and scope == rig.runtime
    assert ticket.runtime_home == host.runtime_home == rig.runtime
    assert ticket.runtime_id == host.runtime_id == "runtime"
    assert ticket.transport_profile == "default" and ticket.bot_id == "offline-app"
    assert ticket.message_time == 1_899_999_999.0
    assert ticket.ledger_path == rig.home / "plugin-data/owned-group-outbound/state.db"
    assert ticket.session_id == rig.runner._session_key_for_source(rig.event().source)
    assert all(path == rig.home for path in rig.auth)
    with pytest.raises(dataclasses.FrozenInstanceError):
        ticket.group_id = "Other"
    record = ticket.as_record()
    assert json.loads(json.dumps(record)) == record
    assert "offline-token" not in json.dumps(record)
    assert len(rig.requests) == 1
    assert json.loads(rig.requests[0].content) == {
        "msg_type": 0, "content": "owned reply", "msg_id": "incoming-id", "msg_seq": 1}


def test_restore_revalidates_current_route_acl_and_original_time(rig, monkeypatch):
    from gateway.owned_group_service import OwnedGroupService
    captured = []
    async def callback(operation, ticket=None, host=None):
        captured.append((ticket, host))
    rig.register(callback)
    rig.loop.run_until_complete(rig.runner._handle_message(rig.event()))
    ticket, old_host = captured[0]
    record = ticket.as_record()
    rig.loop.run_until_complete(rig.runner._owned_group_service.shutdown())
    service = OwnedGroupService(rig.runner)
    host = service.context("runtime", rig.runtime)
    restored = host.restore(json.loads(json.dumps(record)))
    assert restored is not None and restored.authorized()
    assert restored.message_time == ticket.message_time
    assert not old_host.alive()
    rig.runner._is_user_authorized = lambda source: False
    assert host.restore(record) is None
    assert not restored.authorized()
    rig.runner._is_user_authorized = lambda source: True
    rig.runner.config.profile_routes = []
    assert host.restore(record) is None
    rig.runner.config.profile_routes = parse_profile_routes([
        {"platform": "qqbot", "chat_id": "Group-A", "profile": "runtime"}])
    monkeypatch.setattr(time, "time", lambda: 1_900_000_300.0)
    assert host.restore(record) is None
    assert rig.requests == []


def test_restore_allowlisted_record_derives_paths_but_rejects_explicit_wrong_paths(rig):
    captured = []
    async def callback(operation, ticket=None, host=None):
        if operation == "turn":
            captured.append((ticket, host))
    rig.register(callback)
    rig.loop.run_until_complete(rig.runner._handle_message(rig.event()))
    ticket, host = captured[0]
    persisted = {key: value for key, value in ticket.as_record().items()
                 if key not in {"runtime_home", "ledger_path"}}
    restored = host.restore(json.loads(json.dumps(persisted)))
    assert restored is not None and restored.authorized()
    assert restored.runtime_home == rig.runtime
    assert restored.ledger_path == rig.home / "plugin-data/owned-group-outbound/state.db"
    assert restored.message_time == ticket.message_time
    for key in ("runtime_home", "ledger_path"):
        assert host.restore(dict(persisted, **{key: str(rig.home / "wrong")})) is None
    for key in ("bot_id", "transport_profile", "runtime_id", "group_id", "user_id", "session_id"):
        assert host.restore(dict(persisted, **{key: "wrong"})) is None
    assert rig.loop.run_until_complete(restored.submit("restored allowlisted record")).status == "accepted"


def test_dispose_and_reregister_same_callback_does_not_revive_ticket(rig):
    captured = []
    async def callback(operation, ticket=None, host=None):
        captured.append((ticket, host))
    registration = rig.register(callback)
    rig.loop.run_until_complete(rig.runner._handle_message(rig.event()))
    ticket, host = captured[0]
    registration.dispose()
    rig.register(callback)
    assert not host.alive()
    assert not ticket.authorized()
    assert rig.loop.run_until_complete(ticket.submit("forbidden")).status == "rejected"
    assert rig.requests == []
