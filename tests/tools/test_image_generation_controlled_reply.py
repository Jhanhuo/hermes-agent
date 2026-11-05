"""Controlled replies exercise the native Codex provider with offline HTTP only."""
import base64
import importlib
import inspect
import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from agent import secret_scope
from hermes_constants import get_hermes_home, reset_hermes_home_override, set_hermes_home_override
from tests.gateway.qq_offline_guard import OfflineGuard
from tools import image_generation_tool as image_tool

native = importlib.import_module("plugins.image_gen.openai-codex")
MODEL = native.DEFAULT_MODEL
RAW = b"fixture image bytes: decoding belongs to the controlled caller"


@contextmanager
def profile(home, section=None):
    home.mkdir(parents=True, exist_ok=True)
    section = section if section is not None else {"provider": "openai-codex", "model": MODEL}
    (home / "config.yaml").write_text(json.dumps({"image_gen": section}), encoding="utf-8")
    ht = set_hermes_home_override(home)
    st = secret_scope.set_secret_scope({"TEST_CODEX_TOKEN": home.name}, profile_home=str(home))
    mt = secret_scope.set_multiplex_context(True)
    try:
        yield home
    finally:
        secret_scope.reset_multiplex_context(mt)
        secret_scope.reset_secret_scope(st)
        reset_hermes_home_override(ht)


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "launch"))
    monkeypatch.setenv("HERMES_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("HERMES_DISABLE_LAZY_INSTALLS", "1")
    import pm
    traps = [Mock(side_effect=AssertionError("implicit install"))]
    monkeypatch.setattr(pm, "ensure_import", traps[0])
    for name in ("_dispatch_to_plugin_provider", "_maybe_route_managed_model",
                 "_postprocess_image_generate_result", "image_generate_tool", "_get_plugin_provider"):
        trap = Mock(side_effect=AssertionError("legacy image dispatch"))
        monkeypatch.setattr(image_tool, name, trap)
        traps.append(trap)
    with OfflineGuard() as guard:
        yield guard
    for trap in traps:
        trap.assert_not_called()


@pytest.fixture
def backend(monkeypatch, isolated):
    from agent import auxiliary_client
    calls, clients, owners = [], [], []
    state = {"status": 200, "payload": {"data": [{"b64_json": base64.b64encode(RAW).decode()}]},
             "error": None, "on_request": None}

    def credential():
        owners.append(get_hermes_home())
        return secret_scope.get_secret("TEST_CODEX_TOKEN"), "https://codex.example/backend-api/codex"

    resolver = Mock(side_effect=credential)
    monkeypatch.setattr(auxiliary_client, "_resolve_codex_credential_and_base", resolver)

    def respond(req):
        calls.append(req)
        if state["on_request"]:
            state["on_request"]()
        if state["error"]:
            raise state["error"]
        return httpx.Response(state["status"], json=state["payload"],
                              headers={"Location": "https://other.example/", "Retry-After": "0"})

    client_class = httpx.Client

    def client(**kwargs):
        transport = httpx.MockTransport(respond)
        instance = client_class(**{**kwargs, "transport": transport, "trust_env": False})
        isolated.allow_client(instance, transport)
        clients.append(instance)
        return instance

    monkeypatch.setattr(httpx, "Client", client)
    yield SimpleNamespace(calls=calls, clients=clients, owners=owners, state=state, resolver=resolver)
    assert all(client.is_closed for client in clients)


def generate(**args):
    return json.loads(image_tool._handle_image_generate(
        {"prompt": "a small happy fox", "aspect_ratio": "square", "upscale": False, **args}, controlled=True))


def test_codex_native_single_post_and_scoped_cache(tmp_path, monkeypatch, backend):
    monkeypatch.setenv("OPENAI_IMAGE_MODEL", "gpt-image-2-high")
    with profile(tmp_path / "a") as home:
        result = generate()
    assert result["success"] is True, result
    assert result["provider"] == "openai-codex"
    assert result["requested_model"] == MODEL
    assert "model" not in result  # The backend does not attest to the requested model.
    path = Path(result["image"])
    assert path.is_absolute() and path.is_relative_to(home / "cache" / "generated" / "images")
    assert path.read_bytes() == RAW
    assert backend.owners == [home]
    assert len(backend.calls) == 1
    request = backend.calls[0]
    assert request.headers["Authorization"] == "Bearer a"
    assert str(request.url) == "https://codex.example/backend-api/codex/images/generations"
    assert json.loads(request.content) == {
        "prompt": "a small happy fox", "model": native.API_MODEL, "n": 1,
        "quality": native.GPT_IMAGE_2_TIERS[MODEL]["quality"], "size": "1024x1024", "background": "opaque"}
    parameter = inspect.signature(image_tool._handle_image_generate).parameters["controlled"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is False


@pytest.mark.parametrize('args', [
    {'provider': 'fal'}, {'model': MODEL}, {'image_url': 'https://offline.invalid/a'},
    {'reference_image_urls': []}, {'num_images': 2}, {'upscale': True}, {'upscale': 0},
    {'aspect_ratio': 'portrait'}, {'prompt': ''}, {'prompt': ' '}, {'prompt': 7},
    {'prompt': 'x' * 2001}, {'prompt': '\ud800'}, {'prompt': 'x\x00'},
])
def test_controlled_arguments_fail_before_credentials(tmp_path, backend, args):
    with profile(tmp_path / 'a'):
        result = generate(**args)
    assert result['success'] is False
    assert result['error_type'] == 'invalid_arguments'
    backend.resolver.assert_not_called()
    assert backend.calls == []


@pytest.mark.parametrize('owner', [None, 'other'])
def test_scope_owner_must_match_runtime(tmp_path, backend, owner):
    with profile(tmp_path / 'a'):
        token = secret_scope.set_secret_scope({}, profile_home=str(tmp_path / owner) if owner else None)
        try:
            result = generate()
        finally:
            secret_scope.reset_secret_scope(token)
    assert result['error_type'] == 'runtime_scope_required'
    backend.resolver.assert_not_called()
    assert backend.calls == []


def test_conflicting_nested_selection_is_rejected(tmp_path, backend):
    with profile(tmp_path / 'a', {'provider': 'openai-codex', 'model': MODEL,
                                 'openai-codex': {'model': 'gpt-image-2-high'}}):
        result = generate()
    assert result['error_type'] == 'selection_conflict'
    backend.resolver.assert_not_called()


@pytest.mark.parametrize('payload', [
    {'data': [{'b64_json': base64.b64encode(RAW).decode()}] * 2},
    {'data': [{'b64_json': 'not!base64'}]},
    {'data': [{'b64_json': base64.b64encode(b'x' * (2 * 1024 * 1024 + 1)).decode()}]},
    {'data': [{'b64_json': base64.b64encode(RAW).decode()}], 'padding': 'x' * (3 * 1024 * 1024)},
])
def test_output_rejected_without_cache_write(tmp_path, backend, payload):
    backend.state['payload'] = payload
    with profile(tmp_path / 'a') as home:
        result = generate()
    assert result['success'] is False
    assert len(backend.calls) == 1
    assert not list(home.rglob('*.png'))


@pytest.mark.parametrize('status', [302, 307, 401, 403, 429, 500])
def test_http_failure_never_redirects_retries_or_leaks(tmp_path, backend, status):
    backend.state['status'] = status
    backend.state['payload']['error'] = 'private-provider-diagnostic'
    with profile(tmp_path / 'a'):
        result = generate()
    assert result['success'] is False
    assert len(backend.calls) == 1
    assert 'private-provider-diagnostic' not in json.dumps(result)


def test_read_timeout_is_not_retried(tmp_path, backend):
    backend.state['error'] = httpx.ReadTimeout('private-timeout-message')
    with profile(tmp_path / 'a'):
        result = generate()
    assert result['success'] is False
    assert len(backend.calls) == 1
    assert 'private-timeout-message' not in json.dumps(result)
