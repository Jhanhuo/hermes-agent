"""A runtime-only observer must not be selected from the receiving profile."""
import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from gateway.config import Platform
from gateway.run import GatewayRunner, _profile_runtime_scope
from gateway.session import SessionSource
from gateway.session_identity import RoutingIdentity
from hermes_constants import get_hermes_home
from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest


def test_secondary_observer_selects_runtime_after_transport_authorization(tmp_path):
    transport, runtime = tmp_path / "transport", tmp_path / "runtime"
    for home in (transport, runtime):
        home.mkdir()
        (home / "config.yaml").write_text("{}", encoding="utf-8")
    runner = object.__new__(GatewayRunner)
    runner.config = SimpleNamespace(multiplex_profiles=True)
    runner._routed_profile_home = lambda _name: transport
    runner._resolve_profile_home_for_source = lambda _source: runtime
    runner._canonicalize = lambda source, **kwargs: source._identity
    auth = []
    runner._is_user_authorized = lambda source: auth.append(get_hermes_home()) or source.user_id == "Member"
    source = SessionSource(platform=Platform.QQBOT, chat_type="group", chat_id="Group-A", user_id="Member")
    source._identity = RoutingIdentity(transport_profile="transport", runtime_profile="runtime",
                                       authorization_home=transport, runtime_home=runtime)
    managers = {home: PluginManager() for home in (transport, runtime)}
    seen = []
    ctx = PluginContext(PluginManifest(name="runtime-only", source="user"), managers[runtime])
    ctx.register_hook("gateway_platform_event", lambda **event: seen.append((get_hermes_home(), event)))
    handler = runner._make_profile_platform_event_handler("transport")
    with patch("hermes_cli.plugins.get_plugin_manager", side_effect=lambda: managers[Path(get_hermes_home())]):
        with _profile_runtime_scope(transport):
            async def exercise():
                await handler({"platform": "qqbot", "event_type": "group_message", "payload": {}}, source)
                await runner._qq_observer_queue.flush()
            asyncio.run(exercise())
            assert get_hermes_home() == transport
    assert auth == [transport]
    assert len(seen) == 1 and seen[0][0] == runtime


def test_rejected_route_never_publishes_to_ambient_observer():
    runner = object.__new__(GatewayRunner)
    runner.config = SimpleNamespace(multiplex_profiles=True)
    runner._is_user_authorized_for_source = lambda _source: True
    source = SessionSource(platform=Platform.QQBOT, chat_type="group", chat_id="Group-A", user_id="Member")
    source.profile_route_rejected = True
    with patch("hermes_cli.lifecycle.has_hook", return_value=True), patch("hermes_cli.lifecycle.invoke_hook") as invoke:
        asyncio.run(runner._handle_gateway_platform_event({}, source))
    invoke.assert_not_called()
