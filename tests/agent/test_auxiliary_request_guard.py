"""Controlled authorization through real auxiliary orchestration; external I/O only is fake.

These are offline seam contracts, not real model or SDK acceptance.
"""
import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio

from agent import auxiliary_client as aux
from agent.auxiliary_request_guard import AuxiliaryRequestDenied, auxiliary_request_scope
from tests.gateway.qq_offline_guard import OfflineGuard


@pytest_asyncio.fixture(autouse=True)
async def offline(request):
    # The Windows loop must exist before socket tripwires are installed.
    if getattr(request, 'param', True) is False:
        yield
        return
    with OfflineGuard() as guard:
        yield guard


def ok():
    return SimpleNamespace(model='fixture', choices=[SimpleNamespace(
        message=SimpleNamespace(role='assistant', content='ok', tool_calls=None), finish_reason='stop')])


def client(create, name='primary'):
    return SimpleNamespace(base_url=f'https://{name}.invalid/v1', api_key='offline-fixture',
                           chat=SimpleNamespace(completions=SimpleNamespace(create=create)))


class ApiFailure(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status_code = status


def route(monkeypatch, primary):
    # Replace the external client factory, not admission, plan, relays or recovery.
    monkeypatch.setattr(aux, 'resolve_vision_provider_client',
                        lambda **kw: ('custom', primary, 'fixture'))
    monkeypatch.setattr(aux, '_recoverable_pool_provider', lambda *a, **kw: None)


async def invoke():
    return await aux.async_call_llm(task='vision', provider='custom', model='fixture',
        messages=[{'role': 'user', 'content': [
            {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,cGl4ZWxz'}}]}],
        temperature=0.2, max_tokens=64, reasoning_config={'enabled': False})


@pytest.mark.asyncio
@pytest.mark.parametrize('recovery', ['temperature', 'max_tokens', 'reasoning', 'auth', 'fallback'])
async def test_revocation_stops_recovery_physical_request(monkeypatch, recovery):
    allowed, calls, selected = True, [], []
    errors = {
        'temperature': ApiFailure(400, "Unsupported parameter: 'temperature' is not supported"),
        'max_tokens': ApiFailure(400, "Unsupported parameter: 'max_tokens' is not supported"),
        'reasoning': ApiFailure(400, "Unsupported parameter: 'reasoning_effort' is not supported"),
        'auth': ApiFailure(401, 'Unauthorized'),
        'fallback': ApiFailure(429, 'Rate limit exceeded'),
    }
    async def current():
        return allowed
    async def create(**kwargs):
        nonlocal allowed
        calls.append(('primary', allowed))
        if len(calls) == 1:
            allowed = False
            raise errors[recovery]
        return ok()
    async def fallback_create(**kwargs):
        calls.append(('fallback', allowed))
        return ok()
    primary, fallback = client(create), client(fallback_create, 'fallback')
    route(monkeypatch, primary)
    monkeypatch.setattr(aux, '_auth_refresh_provider_for_route', lambda *a, **kw: 'openai-codex')
    monkeypatch.setattr(aux, '_refresh_provider_credentials', lambda *a, **kw: selected.append('auth') or True)
    def choose(*a, **kw):
        selected.append('fallback')
        return fallback, 'fixture', 'fallback_chain[0](custom)'
    monkeypatch.setattr(aux, '_try_configured_fallback_chain', choose)
    monkeypatch.setattr(aux, '_to_async_client', lambda c, m, **kw: (c, m))
    with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
        await invoke()
    assert calls == [('primary', True)]
    if recovery in ('auth', 'fallback'):
        assert recovery in selected  # the real recovery path, not only the primary gate


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['parameter', 'auth'])
async def test_revocation_inside_fallback_stops_its_recovery(monkeypatch, failure):
    allowed, calls = True, []
    async def current():
        return allowed
    async def primary_create(**kwargs):
        calls.append('primary')
        raise ApiFailure(429, 'Rate limit exceeded')
    async def fallback_create(**kwargs):
        nonlocal allowed
        calls.append('fallback')
        if calls.count('fallback') > 1:
            return ok()
        allowed = False
        if failure == 'parameter':
            raise ApiFailure(400, "Unsupported parameter: 'temperature' is not supported")
        raise ApiFailure(401, 'Unauthorized')
    primary, fallback = client(primary_create), client(fallback_create, 'fallback')
    route(monkeypatch, primary)
    monkeypatch.setattr(aux, '_try_configured_fallback_chain',
                        lambda *a, **kw: (fallback, 'fixture', 'fallback_chain[0](custom)'))
    monkeypatch.setattr(aux, '_to_async_client', lambda c, m, **kw: (c, m))
    monkeypatch.setattr(aux, '_refresh_provider_credentials', lambda *a, **kw: True)
    monkeypatch.setattr(aux, '_get_cached_client', lambda *a, **kw: (fallback, 'fixture'))
    monkeypatch.setattr(aux, '_auth_refresh_provider_for_route', lambda *a, **kw: 'openai-codex')
    with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
        await invoke()
    assert calls == ['primary', 'fallback']


@pytest.mark.asyncio
async def test_gate_runs_after_real_auxiliary_observer(monkeypatch):
    from hermes_cli import plugins as plugins_mod
    from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
    allowed, calls, observed = True, [], []
    manager = PluginManager()
    monkeypatch.setattr(plugins_mod, '_plugin_manager', manager)
    monkeypatch.setattr(plugins_mod, '_plugin_managers_by_home', {})
    ctx = PluginContext(PluginManifest(name='gate-observer', source='user'), manager)
    def observer(**kw):
        nonlocal allowed
        observed.append(kw['aux_task'])
        allowed = False
    ctx.register_hook('pre_auxiliary_call', observer)
    async def current():
        return allowed
    async def create(**kwargs):
        calls.append(allowed)
        return ok()
    route(monkeypatch, client(create))
    with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
        await invoke()
    assert observed == ['vision']
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize('offline', [False], indirect=True)
async def test_gate_runs_after_real_relay_callback_wait(monkeypatch, tmp_path):
    from agent import relay_runtime
    relay_runtime._reset_for_tests()
    lease = relay_runtime.SESSION_COORDINATOR.acquire_conversation(
        profile_key=relay_runtime.current_profile_key(), session_id='guard', platform='cli')
    turn = relay_runtime.SESSION_COORDINATOR.begin_turn(lease, turn_id='guard-turn', task_id='guard-task')
    consumer = 'test.guard'
    lease.host.retain_managed_execution(consumer)
    entered, release, allowed, calls = asyncio.Event(), asyncio.Event(), True, []
    original = lease.host.relay.llm.execute
    async def execute(*args, **kwargs):
        # Retain actual Relay execution, injecting only an admission wait in its external seam.
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)
    monkeypatch.setattr(lease.host.relay.llm, 'execute', execute)
    async def current():
        return allowed
    async def create(**kwargs):
        calls.append(allowed)
        return ok()
    route(monkeypatch, client(create))
    async def guarded():
        with auxiliary_request_scope(current):
            return await invoke()
    try:
        with OfflineGuard():
            pending = asyncio.create_task(guarded())
            try:
                await asyncio.wait_for(entered.wait(), 5)
                allowed = False
                release.set()
                with pytest.raises(AuxiliaryRequestDenied):
                    await asyncio.wait_for(pending, 5)
                assert calls == []
            finally:
                release.set()
                if not pending.done():
                    pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
    finally:
        lease.host.release_managed_execution(consumer)
        relay_runtime.SESSION_COORDINATOR.end_turn(turn, outcome='cancelled')
        relay_runtime.SESSION_COORDINATOR.release_conversation(lease)
        relay_runtime._reset_for_tests()


@pytest.mark.asyncio
async def test_guarded_profiles_and_uncontrolled_call_are_isolated(monkeypatch, tmp_path):
    from agent.secret_scope import get_secret, reset_secret_scope, set_secret_scope
    from hermes_constants import get_hermes_home, reset_hermes_home_override, set_hermes_home_override
    homes = {name: tmp_path / name for name in ('A', 'B', 'plain')}
    calls, checked = [], []
    a_entered, release_a = asyncio.Event(), asyncio.Event()
    async def create(**kwargs):
        calls.append((get_hermes_home(), get_secret('GATE_FIXTURE_SECRET')))
        assert 'recheck' not in kwargs and 'guard' not in kwargs
        return ok()
    route(monkeypatch, client(create))
    async def run(name, allowed=None):
        homes[name].mkdir()
        home_token = set_hermes_home_override(homes[name])
        secret_token = set_secret_scope({'GATE_FIXTURE_SECRET': name})
        try:
            async def current():
                assert get_hermes_home() == homes[name]
                assert get_secret('GATE_FIXTURE_SECRET') == name
                checked.append(name)
                if name == 'A':
                    a_entered.set()
                    await release_a.wait()
                return allowed
            if allowed is None:
                return await invoke()
            with auxiliary_request_scope(current):
                return await invoke()
        finally:
            reset_secret_scope(secret_token)
            reset_hermes_home_override(home_token)
    a = asyncio.create_task(run('A', False))
    try:
        await asyncio.wait_for(a_entered.wait(), 5)
        await asyncio.gather(run('B', True), run('plain'))
        release_a.set()
        with pytest.raises(AuxiliaryRequestDenied):
            await a
        assert set(calls) == {(homes['B'], 'B'), (homes['plain'], 'plain')}
        assert set(checked) == {'A', 'B'}
        # No scope left in this parent task; ordinary auxiliary behavior still works.
        await invoke()
        assert len(calls) == 3
    finally:
        release_a.set()
        await asyncio.gather(a, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize('decision', [False, None, 1, 'yes', RuntimeError('sensitive-sentinel')])
async def test_gate_denial_or_exception_is_terminal_and_sanitized(monkeypatch, decision):
    calls, recoveries = [], []
    async def create(**kw):
        calls.append(True)
        return ok()
    route(monkeypatch, client(create))
    monkeypatch.setattr(aux, '_start_recovery_ladder', lambda *a, **kw: recoveries.append(True))
    async def current():
        if isinstance(decision, Exception):
            raise decision
        return decision
    with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied) as failure:
        await invoke()
    assert str(failure.value) == 'Controlled auxiliary request denied'
    assert calls == recoveries == []
    await invoke()
    assert calls == [True]  # guard reset after exception


@pytest.mark.asyncio
async def test_real_cancelled_error_is_not_converted_to_gate_failure(monkeypatch):
    async def current():
        raise asyncio.CancelledError()
    with auxiliary_request_scope(current), pytest.raises(asyncio.CancelledError):
        await invoke()
