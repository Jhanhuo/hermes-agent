"""Outstanding controlled-vision request gate contract at the existing auxiliary seam.

No provider/SDK call: tests exercise actual auxiliary admission/retry orchestration
with only the logical backend implementation or physical external attempt substituted.
"""
import asyncio
import base64
import io
import json
from types import SimpleNamespace

import httpx
from PIL import Image


def data_url():
    stream = io.BytesIO()
    Image.new('RGB', (2, 2), 'blue').save(stream, format='PNG')
    return 'data:image/png;base64,' + base64.b64encode(stream.getvalue()).decode('ascii')


def response():
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content='fixture image'))])


def test_controlled_rechecks_after_existing_auxiliary_admission_wait(monkeypatch):
    import tools.vision_tools as vision
    from agent import auxiliary_client as aux
    allowed, calls = True, []
    class Admission:
        async def acquire(self):
            nonlocal allowed
            allowed = False
        def release(self):
            pass
    async def current():
        return allowed
    async def backend(**kwargs):
        assert kwargs['messages'][0]['content'][1]['image_url']['url'].startswith('data:image/png;base64,')
        calls.append(allowed)
        return response()
    monkeypatch.setattr(vision, 'async_call_llm', aux.async_call_llm)
    monkeypatch.setattr(aux, '_acquire_async_aux_semaphore', lambda task: Admission())
    monkeypatch.setattr(aux, '_async_call_llm_impl', backend)
    result = json.loads(asyncio.run(vision.vision_analyze_tool(
        data_url(), 'describe', controlled_data_url=True, recheck=current)))
    assert calls == [], f'request started after admission wait with authorization={calls}'
    assert result['success'] is False


def test_controlled_rechecks_before_existing_auxiliary_transient_retry(monkeypatch):
    import tools.vision_tools as vision
    from agent import auxiliary_client as aux
    allowed, calls = True, []
    async def current():
        return allowed
    async def physical_attempt(client, kwargs, **metadata):
        nonlocal allowed
        assert kwargs['messages'][0]['content'][1]['image_url']['url'].startswith('data:image/png;base64,')
        calls.append(allowed)
        if len(calls) == 1:
            allowed = False
            raise httpx.ConnectError('offline connection reset fixture')
        return response()
    request = SimpleNamespace(client=object(), kwargs={}, request_provider='openai',
                              base_info='', resolved_base_url='', resolved_api_mode=None)
    monkeypatch.setattr(vision, 'async_call_llm', aux.async_call_llm)
    monkeypatch.setattr(aux, '_acquire_async_aux_semaphore', lambda task: None)
    def plan(*args, **kwargs):
        request.kwargs = {'messages': kwargs['messages']}
        return request, {}, {}
    monkeypatch.setattr(aux, '_plan_aux_call', plan)
    monkeypatch.setattr(aux, '_relay_async_completion', physical_attempt)
    monkeypatch.setattr(aux, '_validate_llm_response', lambda result, *args, **kwargs: result)
    result = json.loads(asyncio.run(vision.vision_analyze_tool(
        data_url(), 'describe', controlled_data_url=True, recheck=current)))
    assert calls == [True], f'physical retry authorization={calls}'
    assert result['success'] is False
