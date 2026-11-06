"""Worker/inner-retry gates, including the installed SDK's real Codex stream adapter.

All provider transport is offline. This does not attest live model behavior.
"""
import asyncio
from contextvars import ContextVar
import json
import threading

import httpx
import pytest
import pytest_asyncio

from agent import auxiliary_client as aux
from agent.auxiliary_request_guard import AuxiliaryRequestDenied, auxiliary_request_scope
from tests.agent.test_auxiliary_request_guard import ApiFailure, client, ok
from tests.gateway.qq_offline_guard import OfflineGuard


@pytest_asyncio.fixture(autouse=True)
async def offline():
    with OfflineGuard():
        yield


@pytest.mark.asyncio
@pytest.mark.parametrize('seam', ['completion', 'stream'])
async def test_sync_relay_checks_on_owner_loop_before_callback(seam):
    owner_loop, owner_thread, calls, checked = asyncio.get_running_loop(), threading.get_ident(), [], []
    profile = ContextVar('fixture_profile', default='unbound')
    token = profile.set('owner-A')
    async def current():
        assert asyncio.get_running_loop() is owner_loop
        assert threading.get_ident() == owner_thread
        assert profile.get() == 'owner-A'
        checked.append(True)
        return False
    def create(**kwargs):
        calls.append(kwargs)
        return ok()
    c = client(create)
    fn = aux._relay_sync_completion if seam == 'completion' else aux._relay_sync_stream
    try:
        with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
            await asyncio.wait_for(asyncio.to_thread(fn, c, {'model': 'fixture', 'messages': []}), 5)
        assert checked and calls == []
    finally:
        profile.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize('wire', ['sync', 'async'])
async def test_inner_stream_negotiation_retry_rechecks(wire):
    allowed, calls = True, []
    async def current():
        return allowed
    def send(kwargs):
        nonlocal allowed
        calls.append((kwargs.get('stream', False), allowed))
        if len(calls) == 1:
            allowed = False
            raise ApiFailure(400, 'Stream option unavailable')
        return ok()
    async def acreate(**kwargs):
        return send(kwargs)
    def create(**kwargs):
        return send(kwargs)
    c = client(create if wire == 'sync' else acreate)
    def sync():
        with aux.aux_progress_hook(lambda: None):
            return aux._relay_sync_completion(c, {'model': 'fixture', 'messages': []})
    with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
        if wire == 'sync':
            await asyncio.wait_for(asyncio.to_thread(sync), 5)
        else:
            with aux.aux_progress_hook(lambda: None):
                await aux._relay_async_completion(c, {'model': 'fixture', 'messages': []})
    assert calls == [(True, True)]


@pytest.mark.asyncio
async def test_sync_affordable_cap_retry_rechecks():
    allowed, calls = True, []
    async def current():
        return allowed
    def create(**kwargs):
        nonlocal allowed
        calls.append(allowed)
        if len(calls) == 1:
            allowed = False
            raise ApiFailure(402, 'Insufficient credits; can only afford 1000 tokens')
        return ok()
    with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
        await asyncio.wait_for(asyncio.to_thread(aux._relay_sync_completion,
            client(create), {'model': 'fixture', 'messages': [], 'max_tokens': 4096}), 5)
    assert calls == [True]


def codex_sdk(monkeypatch, handler):
    """Existing native factory + installed SDK; only HTTP I/O is MockTransport."""
    monkeypatch.setattr(aux, '_openai_http_client_kwargs', lambda *a, **kw: {})
    http = httpx.Client(transport=httpx.MockTransport(handler), trust_env=False)
    sdk = aux._create_openai_client(api_key='offline-not-a-credential',
        base_url='https://chatgpt.com/backend-api/codex', http_client=http)
    assert sdk.max_retries == 0
    return sdk, aux.AsyncCodexAuxiliaryClient(aux.CodexAuxiliaryClient(sdk, 'fixture'))


def sse_ok():
    final = {'id': 'offline-response', 'object': 'response', 'status': 'completed',
             'output': [{'type': 'message', 'id': 'offline-message', 'role': 'assistant',
                         'status': 'completed', 'phase': 'final_answer',
                         'content': [{'type': 'output_text', 'text': 'ok', 'annotations': []}]}],
             'usage': {'input_tokens': 1, 'output_tokens': 1, 'total_tokens': 2}}
    events = [{'type': 'response.output_item.done', 'output_index': 0, 'item': final['output'][0]},
              {'type': 'response.completed', 'response': final}]
    return httpx.Response(200, headers={'content-type': 'text/event-stream'},
        text=''.join('data: ' + json.dumps(event) + '\n\n' for event in events))


@pytest.mark.asyncio
@pytest.mark.parametrize('stop', ['revoked', 'cancelled'])
async def test_codex_worker_cannot_submit_after_build_wait(monkeypatch, stop):
    loop = asyncio.get_running_loop()
    entered, exited, release = asyncio.Event(), asyncio.Event(), threading.Event()
    allowed, posts, checks = True, [], []
    def handler(request):
        posts.append(request.method)
        return sse_ok()
    sdk, wrapped = codex_sdk(monkeypatch, handler)
    original = aux._CodexCompletionsAdapter._build_responses_kwargs
    def build(self, kwargs):
        value = original(self, kwargs)
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return value
    monkeypatch.setattr(aux._CodexCompletionsAdapter, '_build_responses_kwargs', build)
    original_create = aux._CodexCompletionsAdapter.create
    def create(self, **kwargs):
        try:
            return original_create(self, **kwargs)
        finally:
            loop.call_soon_threadsafe(exited.set)
    monkeypatch.setattr(aux._CodexCompletionsAdapter, 'create', create)
    async def current():
        assert asyncio.get_running_loop() is loop
        checks.append(allowed)
        return allowed
    async def owner():
        with auxiliary_request_scope(current):
            return await wrapped.chat.completions.create(model='fixture', messages=[])
    pending = asyncio.create_task(owner())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if stop == 'cancelled':
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
        else:
            allowed = False
        release.set()
        if stop == 'revoked':
            with pytest.raises(AuxiliaryRequestDenied):
                await asyncio.wait_for(pending, 5)
        await asyncio.wait_for(exited.wait(), 5)
        assert posts == []
        if stop == 'revoked':
            assert checks and checks[-1] is False
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        await asyncio.wait_for(exited.wait(), 5)
        sdk.close()
        assert sdk.is_closed()


@pytest.mark.asyncio
async def test_real_codex_sdk_transient_retry_is_gated(monkeypatch):
    allowed, posts, checks = True, [], []
    async def current():
        checks.append(allowed)
        return allowed
    def handler(request):
        nonlocal allowed
        posts.append((request.method, request.url.path))
        assert b'controlled-pixels' in request.content
        assert b'recheck' not in request.content and b'guard' not in request.content
        allowed = False
        return httpx.Response(500, json={'error': {'message': 'offline transient fixture'}})
    sdk, wrapped = codex_sdk(monkeypatch, handler)
    monkeypatch.setattr(aux, 'resolve_vision_provider_client',
                        lambda **kw: ('openai-codex', wrapped, 'fixture'))
    try:
        with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
            await aux.async_call_llm(task='vision', messages=[{'role': 'user', 'content': [
                {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,controlled-pixels'}}]}])
        assert len(posts) == 1 and posts[0][0] == 'POST'
        assert checks[-1] is False
    finally:
        sdk.close()
        assert sdk.is_closed()


@pytest.mark.asyncio
async def test_real_codex_sdk_allowed_request_parses_stream(monkeypatch):
    posts, checks = [], []
    def handler(request):
        posts.append((request.method, request.url.path))
        return sse_ok()
    sdk, wrapped = codex_sdk(monkeypatch, handler)
    async def current():
        checks.append(True)
        return True
    try:
        with auxiliary_request_scope(current):
            result = await wrapped.chat.completions.create(model='fixture', messages=[])
        assert result.choices[0].message.content == 'ok'
        assert len(posts) == 1 and posts[0][0] == 'POST'
        assert len(checks) >= 2  # worker start and physical Responses create
    finally:
        sdk.close()
        assert sdk.is_closed()


@pytest.mark.asyncio
async def test_closed_scope_blocks_copied_worker_without_affecting_default():
    loop = asyncio.get_running_loop()
    entered, release = asyncio.Event(), threading.Event()
    calls = []
    async def current():
        return True
    def create(**kwargs):
        calls.append(True)
        return ok()
    c = client(create)
    def worker():
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return aux._relay_sync_completion(c, {'messages': []})
    with auxiliary_request_scope(current):
        pending = asyncio.create_task(asyncio.to_thread(worker))
        await asyncio.wait_for(entered.wait(), 5)
    release.set()
    with pytest.raises(AuxiliaryRequestDenied):
        await asyncio.wait_for(pending, 5)
    assert calls == []
    await asyncio.to_thread(aux._relay_sync_completion, c, {'messages': []})
    assert calls == [True]


@pytest.mark.asyncio
async def test_sync_seam_on_owner_loop_fails_closed_without_waiting():
    calls = []
    async def current():
        return True
    def create(**kwargs):
        calls.append(True)
        return ok()
    with auxiliary_request_scope(current), pytest.raises(AuxiliaryRequestDenied):
        aux._relay_sync_completion(client(create), {'messages': []})
    assert calls == []


@pytest.mark.asyncio
async def test_cancelled_owner_releases_worker_waiting_for_async_gate():
    loop = asyncio.get_running_loop()
    checking, finished = asyncio.Event(), asyncio.Event()
    calls = []
    async def current():
        checking.set()
        await asyncio.Event().wait()
        return True
    def create(**kwargs):
        calls.append(True)
        return ok()
    def worker():
        try:
            return aux._relay_sync_completion(client(create), {'messages': []})
        finally:
            loop.call_soon_threadsafe(finished.set)
    async def owner():
        with auxiliary_request_scope(current):
            await asyncio.to_thread(worker)
    pending = asyncio.create_task(owner())
    await asyncio.wait_for(checking.wait(), 5)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    await asyncio.wait_for(finished.wait(), 5)
    assert calls == []


@pytest.mark.asyncio
async def test_worker_gate_preserves_genuine_cancelled_error():
    calls = []
    async def current():
        raise asyncio.CancelledError()
    def create(**kwargs):
        calls.append(True)
        return ok()
    with auxiliary_request_scope(current), pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.to_thread(aux._relay_sync_completion,
            client(create), {'messages': []}), 5)
    assert calls == []
