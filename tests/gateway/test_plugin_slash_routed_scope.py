"""Plugin commands, like table commands, belong to the routed runtime, not the receiving bot."""
import pytest

from gateway.config import GatewayConfig, Platform
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner, _profile_runtime_scope
from gateway.session import SessionSource
from gateway.session_context import get_session_env
from gateway.session_identity import RoutingIdentity
from hermes_constants import get_hermes_home


def runner_for(home):
    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(multiplex_profiles=True)
    runner._draining = False
    runner._hm_quick_commands = dict
    runner._resolve_profile_home_for_source = lambda _source: home
    runner._session_key_for_source = lambda _source: "agent:beta:qqbot:group:g"
    runner._get_executor = lambda: None
    return runner


def command_event(transport_home, runtime_home):
    source = SessionSource(platform=Platform.QQBOT, chat_id="g", user_id="u", chat_type="group", profile="beta")
    source._identity = RoutingIdentity(
        transport_profile="default", runtime_profile="beta",
        authorization_home=transport_home, runtime_home=runtime_home,
    )
    return MessageEvent(text="/probe value", source=source, message_id="m")


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_plugin_lookup_and_execution_use_routed_runtime(tmp_path, monkeypatch, asynchronous):
    launch, routed = tmp_path / "launch", tmp_path / "beta"
    launch.mkdir()
    routed.mkdir()
    seen = {}

    def record(args):
        seen["execution_home"] = get_hermes_home()
        seen["chat_id"] = get_session_env("HERMES_SESSION_CHAT_ID")
        seen["session_key"] = get_session_env("HERMES_SESSION_KEY")
        return f"handled {args}"

    async def async_handler(args):
        return record(args)

    def lookup(name):
        seen["lookup_home"] = get_hermes_home()
        if get_hermes_home() == routed and name == "probe":
            return async_handler if asynchronous else record
        return None

    from hermes_cli import plugins
    monkeypatch.setattr(plugins, "get_plugin_command_handler", lookup)
    event = command_event(launch, routed)
    runner = runner_for(routed)
    with _profile_runtime_scope(launch):
        handled, reply, command = await runner._hm_dispatch_quick_and_plugin_commands(event, event.source, "probe")
        assert get_hermes_home() == launch
    assert handled and reply == "handled value" and command == "probe"
    assert seen["lookup_home"] == routed
    assert seen["execution_home"] == routed
    assert seen["chat_id"] == "g"
    assert seen["session_key"] == "agent:beta:qqbot:group:g"
    assert get_session_env("HERMES_SESSION_KEY") == ""


@pytest.mark.asyncio
async def test_failing_plugin_restores_transport_scope_and_clears_session(tmp_path, monkeypatch):
    launch, routed = tmp_path / "launch", tmp_path / "beta"
    launch.mkdir()
    routed.mkdir()

    async def failing(_args):
        assert get_hermes_home() == routed
        raise RuntimeError("handler failed")

    from hermes_cli import plugins
    monkeypatch.setattr(plugins, "get_plugin_command_handler", lambda _name: failing)
    event = command_event(launch, routed)
    with _profile_runtime_scope(launch):
        handled, reply, _ = await runner_for(routed)._hm_dispatch_quick_and_plugin_commands(event, event.source, "probe")
        assert get_hermes_home() == launch
    assert not handled and reply is None
    assert get_session_env("HERMES_SESSION_KEY") == ""
