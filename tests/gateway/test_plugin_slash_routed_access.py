"""Runtime-only plugin commands keep the receiving bot's real slash policy."""
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner, _profile_runtime_scope
from gateway.session import SessionSource
from gateway.session_context import clear_session_vars, get_session_env
from gateway.session_identity import RoutingIdentity
from hermes_cli import plugins
from hermes_cli.commands import is_gateway_known_command
from hermes_constants import get_hermes_home


@pytest.fixture
def routed_command(tmp_path, monkeypatch):
    # Keep real per-home managers and registration/lookup, without loading installed plugins.
    monkeypatch.setattr(plugins, "_plugin_manager", None)
    monkeypatch.setattr(plugins, "_plugin_managers_by_home", {})
    homes = {name: tmp_path / name for name in ("launch", "alpha", "beta")}
    for home in homes.values():
        home.mkdir()
        with _profile_runtime_scope(home):
            plugins.get_plugin_manager()._discovered = True
    clear_session_vars([])

    def make(*, transport_profile, user_id="user", allowed=(), asynchronous=False,
             typed="runtime_probe", runtime_extra=None):
        seen = {"lookups": [], "executions": []}

        def handler(args):
            seen["executions"].append((
                get_hermes_home(), args, get_session_env("HERMES_SESSION_CHAT_ID"),
                get_session_env("HERMES_SESSION_KEY"), get_session_env("HERMES_SESSION_PROFILE"),
            ))
            return f"handled {args}"

        async def async_handler(args):
            return handler(args)

        with _profile_runtime_scope(homes["beta"]):
            context = plugins.PluginContext(
                plugins.PluginManifest(name="routed-access-probe"), plugins.get_plugin_manager(),
            )
            context.register_command("runtime-probe", async_handler if asynchronous else handler)

        real_lookup = plugins.get_plugin_command_handler

        def lookup(name):
            seen["lookups"].append((get_hermes_home(), name))
            return real_lookup(name)

        monkeypatch.setattr(plugins, "get_plugin_command_handler", lookup)

        def config(extra):
            return GatewayConfig(multiplex_profiles=True, platforms={
                Platform.QQBOT: PlatformConfig(enabled=True, extra=extra),
            })

        transport_config = config({
            # The user is a DM admin, but not a group admin: scope selection matters too.
            "allow_admin_from": ["admin", "user"],
            "group_allow_admin_from": ["admin"],
            "group_user_allowed_commands": list(allowed),
        })
        runner = object.__new__(GatewayRunner)
        runner.config = transport_config if transport_profile == "launch" else config({})
        runner._primary_profile_name = "launch"
        runner._profile_configs = {
            "alpha": transport_config,
            "beta": config(runtime_extra or {}),
        }
        runner._draining = False
        runner._hm_quick_commands = dict
        runner._resolve_profile_home_for_source = lambda _source: homes["beta"]
        runner._session_key_for_source = lambda _source: "agent:beta:qqbot:group:g"
        runner._get_executor = lambda: None
        source = SessionSource(
            platform=Platform.QQBOT, chat_id="g", user_id=user_id, chat_type="group", profile="beta",
        )
        source._identity = RoutingIdentity(
            transport_profile=transport_profile, runtime_profile="beta",
            authorization_home=homes[transport_profile], runtime_home=homes["beta"],
        )
        event = MessageEvent(text=f"/{typed} value", source=source, message_id="m")
        return SimpleNamespace(
            runner=runner, event=event, seen=seen, transport=homes[transport_profile],
            runtime=homes["beta"],
        )

    yield make
    clear_session_vars([])


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_profile", ["launch", "alpha"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("typed", ["runtime-probe", "runtime_probe"])
async def test_idle_runtime_only_plugin_denied_by_transport_group_policy(
    routed_command, transport_profile, asynchronous, typed,
):
    case = routed_command(
        transport_profile=transport_profile, asynchronous=asynchronous, typed=typed,
    )
    with _profile_runtime_scope(case.transport):
        # The real resolver cannot see this command in the receiving bot's registry.
        assert not is_gateway_known_command("runtime-probe")
        handled, reply = await case.runner._hm_dispatch_idle_commands(
            case.event, case.event.source, "agent:beta:qqbot:group:g",
        )
        assert get_hermes_home() == case.transport
        assert get_session_env("HERMES_SESSION_KEY") == ""
        assert get_session_env("HERMES_SESSION_CHAT_ID") == ""
    assert case.seen["lookups"] == [(case.runtime, "runtime-probe")]
    assert case.seen["executions"] == []
    assert handled and "⛔" in reply and "/runtime-probe" in reply


@pytest.mark.asyncio
@pytest.mark.parametrize("transport_profile", ["launch", "alpha"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("user_id,allowed", [("admin", ()), ("user", ("runtime-probe",))])
async def test_idle_runtime_only_plugin_allowed_by_transport_not_runtime_policy(
    routed_command, transport_profile, asynchronous, user_id, allowed,
):
    case = routed_command(
        transport_profile=transport_profile, user_id=user_id, allowed=allowed,
        asynchronous=asynchronous,
        # Opposing runtime policy must not deny a transport admin or explicitly allowed user.
        runtime_extra={"group_allow_admin_from": ["runtime-admin"]},
    )
    with _profile_runtime_scope(case.transport):
        assert not is_gateway_known_command("runtime-probe")
        handled, reply = await case.runner._hm_dispatch_idle_commands(
            case.event, case.event.source, "agent:beta:qqbot:group:g",
        )
        assert get_hermes_home() == case.transport
        assert get_session_env("HERMES_SESSION_KEY") == ""
        assert get_session_env("HERMES_SESSION_CHAT_ID") == ""
        assert get_session_env("HERMES_SESSION_PROFILE") == ""
    assert handled and reply == "handled value"
    assert case.seen["lookups"] == [(case.runtime, "runtime-probe")]
    assert case.seen["executions"] == [(
        case.runtime, "value", "g", "agent:beta:qqbot:group:g", "beta",
    )]
