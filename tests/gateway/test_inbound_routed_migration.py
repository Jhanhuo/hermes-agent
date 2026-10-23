"""Routed inbound policy composes with the extracted plugin and local-reference APIs."""
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.plugin_messages import bound_route
from gateway.run import _profile_runtime_scope
from gateway.session import SessionEntry
from gateway.session_cli_turn import cli_turn, cli_turn_scope
from gateway.session_context import get_session_env
from hermes_constants import get_hermes_home
from tests.gateway.test_plugin_slash_routed_scope import command_event, runner_for


@pytest.mark.asyncio
@pytest.mark.parametrize("handler_kind", ["sync", "async", "awaitable"])
@pytest.mark.parametrize("existing_session", [False, True])
async def test_routed_plugin_keeps_context_keywords_and_live_session(
    tmp_path, monkeypatch, handler_kind, existing_session,
):
    transport, runtime = tmp_path / "transport", tmp_path / "runtime"
    transport.mkdir()
    runtime.mkdir()
    runner = runner_for(runtime)
    event = command_event(transport, runtime)
    key = runner._session_key_for_source(event.source)
    session_id = "existing-session" if existing_session else ""
    if existing_session:
        now = datetime.now()
        entry = SessionEntry(session_key=key, session_id=session_id, created_at=now,
                             updated_at=now, platform=Platform.QQBOT, origin=event.source)
        runner.session_store = object()
        runner._async_session_store = SimpleNamespace(
            _store=runner.session_store, lookup_by_session_key=AsyncMock(return_value=entry))
    calls = []

    def record(args, *, source, session_id, session_key, platform):
        calls.append((args, source, session_id, session_key, platform))
        assert get_hermes_home() == runtime
        assert bound_route() is event.source
        assert get_session_env("HERMES_SESSION_ID") == session_id
        assert get_session_env("HERMES_SESSION_KEY") == session_key
        return "handled"

    async def asynchronous(args, **context):
        return record(args, **context)

    def awaitable(args, **context):
        return asynchronous(args, **context)

    handler = {"sync": record, "async": asynchronous, "awaitable": awaitable}[handler_kind]

    def lookup(name):
        return handler if get_hermes_home() == runtime and name == "probe" else None

    monkeypatch.setattr("hermes_cli.plugins.get_plugin_command_handler", lookup)
    with _profile_runtime_scope(transport):
        assert await runner._hm_dispatch_quick_and_plugin_commands(event, event.source, "probe") == (
            True, "handled", "probe")
        assert get_hermes_home() == transport
        assert bound_route() is None
        assert get_session_env("HERMES_SESSION_KEY") == ""
    assert calls == [("value", event.source, session_id, key, "qqbot")]


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_routed_local_api_references_obey_gate_and_keep_workspace(
    tmp_path, monkeypatch, enabled,
):
    transport, runtime, workspace = (tmp_path / name for name in ("transport", "runtime", "workspace"))
    for home in (transport, runtime, workspace):
        home.mkdir()
    (transport / "config.yaml").write_text("context_references:\n  enabled: true\n", encoding="utf-8")
    (runtime / "config.yaml").write_text(
        f"context_references:\n  enabled: {str(enabled).lower()}\n", encoding="utf-8")
    (workspace / "note.txt").write_text("SESSION-WORKSPACE-CONTENT", encoding="utf-8")
    (transport / "note.txt").write_text("TRANSPORT-DECOY", encoding="utf-8")
    monkeypatch.setenv("TERMINAL_CWD", str(transport))
    runner = runner_for(runtime)
    event = command_event(transport, runtime)
    event.source.platform = Platform.LOCAL
    event.text = "read @file:note.txt"
    api_text = f"[voice] {event.text}"
    model = AsyncMock(return_value=128_000)
    monkeypatch.setattr(runner, "_inbound_model_context_length", model)
    monkeypatch.setattr("gateway.session_policy.policy_for_source",
                        lambda *_args: SimpleNamespace(cwd=str(workspace)))
    with _profile_runtime_scope(transport), cli_turn_scope(event.message_id, {"api_text": api_text}):
        prepared = await runner._prepare_inbound_message_text(
            event=event, source=event.source, history=[], session_key="local-key")
        model_text = cli_turn(event.message_id)["api_text"]
        assert get_hermes_home() == transport
    if enabled:
        assert "SESSION-WORKSPACE-CONTENT" in prepared
        assert "SESSION-WORKSPACE-CONTENT" in model_text
        assert "TRANSPORT-DECOY" not in prepared + model_text
        assert prepared.startswith(event.text) and model_text.startswith(api_text)
        assert "[voice]" not in prepared
        model.assert_awaited_once()
    else:
        assert prepared == event.text and model_text == api_text
        model.assert_not_awaited()
