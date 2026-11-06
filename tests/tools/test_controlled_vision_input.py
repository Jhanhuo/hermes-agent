"""Explicit controlled data URLs use native auxiliary routing without persistent inputs."""
import asyncio
import base64
import io
import json
from types import SimpleNamespace

import pytest
from PIL import Image


@pytest.fixture
def pixels():
    stream = io.BytesIO()
    Image.new('RGB', (3, 2), 'purple').save(stream, format='PNG')
    data = stream.getvalue()
    return data, 'data:image/png;base64,' + base64.b64encode(data).decode('ascii')


def response(content):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])


def test_controlled_url_routes_existing_vision_task_without_disk_or_debug(tmp_path, monkeypatch, pixels):
    import tools.vision_tools as vision
    from tests.gateway.qq_offline_guard import OfflineGuard
    calls = []
    async def current():
        return True
    async def caller(**kwargs):
        calls.append(kwargs)
        assert kwargs['task'] == 'vision'
        assert kwargs['messages'][0]['content'][1]['image_url']['url'] == pixels[1]
        return response('fixture image')
    monkeypatch.setattr(vision, 'async_call_llm', caller)
    loop = asyncio.new_event_loop()  # Windows self-pipe must precede network tripwires.
    try:
        with OfflineGuard():
            result = json.loads(loop.run_until_complete(vision.vision_analyze_tool(
                pixels[1], 'describe', controlled_data_url=True, recheck=current)))
    finally:
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()
    assert result == {'success': True, 'analysis': 'fixture image'}
    assert len(calls) == 1
    assert not list(tmp_path.rglob('temp_image_*'))


@pytest.mark.parametrize('source', ['https://example.invalid/private', '/private/image.png',
                                   'data:image/png;base64,bad!', 'data:text/plain;base64,YQ=='])
def test_controlled_mode_rejects_non_bounded_image_data_without_request(monkeypatch, source):
    import tools.vision_tools as vision
    async def current():
        return True
    async def forbidden(**kwargs):
        pytest.fail('unsafe source reached inference')
    monkeypatch.setattr(vision, 'async_call_llm', forbidden)
    result = json.loads(asyncio.run(vision.vision_analyze_tool(
        source, 'describe', controlled_data_url=True, recheck=current)))
    assert result['success'] is False
    assert source not in result.get('error', '')


def test_controlled_mode_fails_closed_without_recheck(monkeypatch, pixels):
    import tools.vision_tools as vision
    async def forbidden(**kwargs):
        pytest.fail('missing gate reached inference')
    monkeypatch.setattr(vision, 'async_call_llm', forbidden)
    result = json.loads(asyncio.run(vision.vision_analyze_tool(
        pixels[1], 'describe', controlled_data_url=True)))
    assert result['success'] is False


def test_controlled_empty_retry_rechecks_each_attempt(monkeypatch, pixels):
    import tools.vision_tools as vision
    allowed, calls = True, []
    async def current():
        return allowed
    async def caller(**kwargs):
        nonlocal allowed
        calls.append(kwargs)
        allowed = False
        return response('')
    monkeypatch.setattr(vision, 'async_call_llm', caller)
    result = json.loads(asyncio.run(vision.vision_analyze_tool(
        pixels[1], 'describe', controlled_data_url=True, recheck=current)))
    assert result['success'] is False
    assert len(calls) == 1


def test_controlled_provider_exception_does_not_persist_sensitive_data(monkeypatch, pixels, caplog):
    import tools.vision_tools as vision
    async def current():
        return True
    async def caller(**kwargs):
        raise ValueError(pixels[1] + ' private-provider-token')
    monkeypatch.setattr(vision, 'async_call_llm', caller)
    result = asyncio.run(vision.vision_analyze_tool(
        pixels[1], 'describe', controlled_data_url=True, recheck=current))
    assert json.loads(result)['success'] is False
    assert pixels[1] not in result + caplog.text
    assert 'private-provider-token' not in result + caplog.text
