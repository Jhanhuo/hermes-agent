"""Primary shared-bot busy admission must use the idle handler's runtime scope."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from hermes_cli.plugins import PluginContext, PluginManifest
from hermes_constants import get_hermes_home
from tests.gateway.test_owned_group_service import rig


@pytest.mark.parametrize("busy", [False, True], ids=["idle", "busy"])
@pytest.mark.parametrize("policy", ["skip", "rewrite", "rewrite-command"])
def test_primary_shared_bot_admission_uses_runtime_hooks(rig, monkeypatch, busy, policy):
    runner, adapter = rig.runner, rig.adapter
    # The shared rig's admission mock masks scope bugs. Exercise the real
    # lifecycle -> per-home PluginManager -> registered hook dispatch instead.
    del runner._hm_pre_gateway_dispatch_hook
    hooks, turns = [], []

    async def transport_hook(event, **kwargs):
        hooks.append(("transport", get_hermes_home(), event.text))
        return {"action": "rewrite", "text": "wrong transport rewrite"}

    async def runtime_hook(event, **kwargs):
        hooks.append(("runtime", get_hermes_home(), event.text))
        if policy == "skip":
            return {"action": "skip", "reason": "runtime policy"}
        return {"action": "rewrite", "text": "/help" if policy == "rewrite-command" else "runtime rewrite"}

    for home, callback in ((rig.home, transport_hook), (rig.runtime, runtime_hook)):
        manager = rig.managers[home]
        # Only these explicitly registered offline hooks; never discover plugins.
        manager._discovered = True
        PluginContext(PluginManifest(name="scope-policy", source="user"), manager).register_hook(
            "pre_gateway_dispatch", callback)

    async def consumer(operation, ticket=None, host=None):
        if operation == "turn":
            turns.append((ticket.text, get_hermes_home(), ticket.transport_profile, ticket.runtime_id))
            return "forbidden automatic reply"

    rig.register(consumer)
    runner.session_store = None
    runner._busy_text_mode = "steer"
    native_busy = AsyncMock(return_value=True)
    runner._wire_adapter_handlers(adapter, busy_session_handler=native_busy)
    ordinary_send = AsyncMock(side_effect=AssertionError("ordinary send forbidden"))
    monkeypatch.setattr(adapter, "send", ordinary_send)
    monkeypatch.setattr(adapter, "_release_turn_marker", AsyncMock())
    interrupt = asyncio.Event()
    if busy:
        adapter._active_sessions[adapter._event_session_key(rig.event())] = interrupt

    async def exercise():
        assert get_hermes_home() == rig.home
        await adapter._on_message("GROUP_AT_MESSAGE_CREATE", {
            "group_openid": "Group-A", "id": "incoming-id", "content": "hello",
            "author": {"member_openid": "Member"}, "timestamp": "2030-03-17T17:46:39+00:00"})
        if adapter._background_tasks:
            await asyncio.gather(*list(adapter._background_tasks))
        assert get_hermes_home() == rig.home

    rig.loop.run_until_complete(exercise())
    expected = [("runtime rewrite", rig.runtime, "default", "runtime")] if policy == "rewrite" else []
    assert turns == expected
    assert hooks == [("runtime", rig.runtime, "hello")]
    assert get_hermes_home() == rig.home
    native_busy.assert_not_awaited()
    ordinary_send.assert_not_awaited()
    assert not interrupt.is_set()
    assert adapter._pending_messages == {}
    assert rig.requests == []


def test_selected_busy_message_handler_failure_never_reopens_native_path(rig, monkeypatch):
    """A supplied handler can fail before admission establishes its own latch."""
    runner, adapter = rig.runner, rig.adapter
    runner.session_store = None
    runner._busy_text_mode = "steer"
    message_handler = AsyncMock(side_effect=RuntimeError("offline scope failure"))
    native_busy = AsyncMock(return_value=False)
    runner._wire_adapter_handlers(
        adapter, message_handler=message_handler, busy_session_handler=native_busy)
    ordinary_send = AsyncMock(side_effect=AssertionError("ordinary send forbidden"))
    monkeypatch.setattr(adapter, "send", ordinary_send)
    event = rig.event()
    interrupt = asyncio.Event()
    adapter._active_sessions[adapter._event_session_key(event)] = interrupt

    rig.loop.run_until_complete(adapter.handle_message(event))

    message_handler.assert_awaited_once_with(event)
    native_busy.assert_not_awaited()
    ordinary_send.assert_not_awaited()
    assert event._gateway_accepted is True
    assert not interrupt.is_set()
    assert adapter._pending_messages == {}
    assert not adapter._background_tasks
    assert rig.requests == []
