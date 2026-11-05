"""Controlled native TTS uses real handlers with offline SDK/network seams."""
import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.gateway.qq_offline_guard import OfflineGuard
from tools import tts_tool as tts


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_DISABLE_LAZY_INSTALLS", "1")
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    import pm
    monkeypatch.setattr(pm, "ensure_import", forbidden)
    # Windows proactor creates a local socket pair; create it before tripwires.
    loop = asyncio.new_event_loop()
    monkeypatch.setattr(asyncio.events, "new_event_loop", lambda: loop)
    with OfflineGuard():
        yield
    if not loop.is_closed():
        loop.close()


def forbidden(*args, **kwargs):
    pytest.fail("unexpected install, fallback, rewrite, or network operation")


def configure(monkeypatch, config):
    import hermes_cli.config
    monkeypatch.setattr(hermes_cli.config, "load_config", lambda: {"tts": config})


def fake_edge(monkeypatch, *, mutate=None, failure=None, audio=b"ID3" + b"\0" * 20):
    calls = []

    class Communicate:
        def __init__(self, text, **kwargs):
            calls.append((text, kwargs))
            if mutate:
                mutate()

        async def _Communicate__stream(self):
            if failure:
                raise failure
            yield {"type": "audio", "data": audio}

        async def save(self, path):
            # Keep this fake synchronous: worker-scope tests reject executor hops.
            with Path(path).open("wb") as output:  # noqa: ASYNC230
                async for chunk in self._Communicate__stream():
                    output.write(chunk["data"])

    monkeypatch.setitem(sys.modules, "edge_tts", SimpleNamespace(Communicate=Communicate, __version__="7.3.1"))
    return calls


def invoke(tmp_path, text="你好，这是最终回复。", **kwargs):
    return json.loads(tts.text_to_speech_tool(
        text, output_path=str(tmp_path / "reply.mp3"), controlled=True, **kwargs))


def test_default_edge_is_single_file_without_lazy_installer(monkeypatch, tmp_path):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch)
    result = invoke(tmp_path)
    assert result["success"] is True
    assert result["file_paths"] == [result["file_path"]] == [str(tmp_path / "reply.mp3")]
    assert result["chunk_count"] == result["delivery_file_count"] == 1
    assert result["provider"] == "edge"
    assert len(calls) == 1
    assert calls[0][0] == "你好，这是最终回复。"


@pytest.mark.parametrize("text", ["", " ", "a" * 601, "<think>hidden</think>"])
def test_requires_short_nonempty_spoken_text(monkeypatch, tmp_path, text):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch)
    result = invoke(tmp_path, text)
    assert result["success"] is False
    assert calls == []
    assert not (tmp_path / "reply.mp3").exists()


def test_provider_exception_is_redacted_without_retry(monkeypatch, tmp_path, caplog):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch, failure=RuntimeError("secret-token https://private/path"))
    result = invoke(tmp_path)
    assert result == {"success": False, "error": "controlled_tts_generation_failed"}
    assert "secret-token" not in caplog.text
    assert len(calls) == 1


def fake_openai(monkeypatch):
    from openai import DefaultHttpxClient
    calls = []

    class OpenAI:
        def __init__(self, **kwargs):
            calls.append(("client", kwargs))
            self.audio = SimpleNamespace(speech=self)
            self.http_client = kwargs.get("http_client")

        def create(self, **kwargs):
            calls.append(("speech", kwargs))
            return self

        def stream_to_file(self, path):
            Path(path).write_bytes(b"ID3" + b"\0" * 20)

        def close(self):
            calls.append(("close", {}))
            if self.http_client is not None:
                self.http_client.close()

    monkeypatch.setitem(sys.modules, "openai", SimpleNamespace(OpenAI=OpenAI, DefaultHttpxClient=DefaultHttpxClient))
    return calls


@pytest.mark.parametrize("provider", ["openai", "nous"])
def test_openai_route_is_frozen_and_sdk_retries_disabled(monkeypatch, tmp_path, provider):
    import hermes_cli.config
    from tools import tts_tool_openai as backend
    config = {"provider": provider, "openai": {"api_key": "fake-direct", "voice": "alloy"}}
    loads = []

    def load():
        loads.append(1)
        assert len(loads) == 1, "provider config was read again after selection"
        return {"tts": config}

    monkeypatch.setattr(hermes_cli.config, "load_config", load)
    monkeypatch.setattr(backend, "read_selection", forbidden)
    monkeypatch.setattr(backend, "_managed_openai_audio_route", lambda: ("fake-nous", "https://nous.invalid/v1", True))
    calls = fake_openai(monkeypatch)
    result = invoke(tmp_path)
    assert result["success"] is True
    assert result["provider"] == "openai"
    assert result["selected_provider"] == provider
    client = calls[0][1]
    assert client["max_retries"] == 0
    assert client["timeout"] == 60
    assert client["api_key"] == ("fake-nous" if provider == "nous" else "fake-direct")
    assert calls[1][1]["response_format"] == "mp3"
    assert [event for event, _ in calls] == ["client", "speech", "close"]


@pytest.mark.parametrize("config", [
    {"provider": "edge", "use_gateway": True},
    {"provider": "edge", "backend": "openai"},
    {"provider": "edge", "cloud_provider": "nous"},
    {"provider": ""}, {"provider": 7}, [],
])
def test_ambiguous_or_invalid_config_never_synthesizes(monkeypatch, tmp_path, config):
    configure(monkeypatch, config)
    calls = fake_edge(monkeypatch)
    assert invoke(tmp_path)["success"] is False
    assert calls == []


def test_failed_config_read_is_not_default_edge(monkeypatch, tmp_path):
    import hermes_cli.config
    from hermes_cli.config_read_errors import FailedConfigRead
    monkeypatch.setattr(hermes_cli.config, "load_config", lambda: FailedConfigRead(error=ValueError("bad yaml")))
    calls = fake_edge(monkeypatch)
    assert invoke(tmp_path) == {"success": False, "error": "controlled_tts_config_invalid"}
    assert calls == []


@pytest.mark.parametrize("provider", [
    "typo", "neutts", "piper", "kittentts", "custom-command", "third-party",
    "gemini", "elevenlabs", "mistral",
])
def test_unaudited_provider_does_not_fall_back(monkeypatch, tmp_path, provider):
    configure(monkeypatch, {"provider": provider, "providers": {provider: {"type": "command", "command": "anything"}}})
    monkeypatch.setattr(tts, "_dispatch_to_plugin_provider", forbidden)
    monkeypatch.setattr(tts, "_select_builtin_engine", forbidden)
    calls = fake_edge(monkeypatch)
    assert invoke(tmp_path) == {"success": False, "error": "controlled_tts_unsupported_provider"}
    assert calls == []


def test_provider_cap_rejects_instead_of_chunking(monkeypatch, tmp_path):
    configure(monkeypatch, {"edge": {"max_text_length": 4}})
    calls = fake_edge(monkeypatch)
    assert invoke(tmp_path)["success"] is False
    assert calls == []


@pytest.mark.parametrize("suffix", [".wav", ".ogg", ""])
def test_only_mp3_output_is_allowed(monkeypatch, tmp_path, suffix):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch)
    result = json.loads(tts.text_to_speech_tool("hello", str(tmp_path / ("reply" + suffix)), controlled=True))
    assert result == {"success": False, "error": "controlled_tts_output_invalid"}
    assert calls == []


def test_existing_output_is_never_overwritten(monkeypatch, tmp_path):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch)
    path = tmp_path / "reply.mp3"
    path.write_bytes(b"existing audio")
    assert invoke(tmp_path)["success"] is False
    assert path.read_bytes() == b"existing audio"
    assert calls == []


@pytest.mark.parametrize("audio", [b"", b"RIFF" + b"\0" * 4 + b"WAVE", b"ID3" + b"x" * (2 * 1024 * 1024)], ids=["empty", "wav", "oversized"])
def test_invalid_or_oversized_audio_is_not_returned(monkeypatch, tmp_path, audio):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch, audio=audio)
    assert invoke(tmp_path) == {"success": False, "error": "controlled_tts_audio_invalid"}
    assert len(calls) == 1
    assert not (tmp_path / "reply.mp3").exists()


@pytest.mark.parametrize("override", [{"provider": "edge"}, {"voice_id": "anything"}, {"find_voices": {}}])
def test_controlled_entry_cannot_select_or_discover_voices(monkeypatch, tmp_path, override):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch)
    assert invoke(tmp_path, **override)["success"] is False
    assert calls == []


@pytest.mark.parametrize("openai_config", [{"model": "unpriced-model"}, {"base_url": "https://untrusted.invalid/v1"}])
def test_nous_never_substitutes_model_or_forwards_managed_token(monkeypatch, tmp_path, openai_config):
    from tools import tts_tool_openai as backend
    configure(monkeypatch, {"provider": "nous", "openai": openai_config})
    monkeypatch.setattr(backend, "_managed_openai_audio_route", lambda: ("fake-nous", "https://nous.invalid/v1", True))
    calls = fake_openai(monkeypatch)
    assert invoke(tmp_path)["success"] is False
    assert calls == []


@pytest.mark.parametrize("provider,module", [("edge", "edge_tts"), ("nous", "openai")])
def test_missing_sdk_blocks_before_auth_or_fallback(monkeypatch, tmp_path, provider, module):
    from tools import tts_tool_openai as backend
    configure(monkeypatch, {"provider": provider})
    monkeypatch.setitem(sys.modules, module, None)
    monkeypatch.setattr(backend, "_managed_openai_audio_route", forbidden)
    monkeypatch.setattr(tts, "_check_neutts_available", forbidden)
    assert invoke(tmp_path) == {"success": False, "error": "controlled_tts_dependency_missing"}


def test_openai_uses_configured_voice_speed_and_instructions(monkeypatch, tmp_path):
    configure(monkeypatch, {"provider": "openai", "openai": {"api_key": "fake", "voice": "nova"}})
    calls = fake_openai(monkeypatch)
    assert invoke(tmp_path, speed=1.2, instructions="Quiet voice")["success"] is True
    assert calls[1][1]["voice"] == "nova"
    assert calls[1][1]["speed"] == 1.2
    assert calls[1][1]["instructions"] == "Quiet voice"


@pytest.mark.parametrize("provider", ["minimax", "xai"])
def test_http_backends_reuse_native_handler_without_auxiliary_rewrite(monkeypatch, tmp_path, provider):
    from tools import tts_tool_providers as backends
    import tools.xai_http
    import requests
    configure(monkeypatch, {"provider": provider, "xai": {"auto_speech_tags": True, "speech_tags": True}})
    monkeypatch.setattr(backends, "_rewrite_with_auxiliary_model", forbidden)
    monkeypatch.setattr(tts, "_resolve_provider_key", lambda *args: "fake-key")
    monkeypatch.setattr(tools.xai_http, "resolve_xai_http_credentials", lambda **kw: {"api_key": "fake-key"})
    calls = []
    audio = b"ID3" + b"\0" * 20

    class Response:
        status_code = 200

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            if provider == "xai":
                yield audio
            else:
                yield json.dumps({"base_resp": {"status_code": 0}, "data": {"audio": audio.hex()}}).encode()

        def close(self):
            pass

    def post(url, **kwargs):
        calls.append((url, kwargs))
        return Response()

    monkeypatch.setattr(requests, "post", post)
    assert invoke(tmp_path)["success"] is True
    assert len(calls) == 1
    assert calls[0][1]["json"]["text"] == "你好，这是最终回复。"
    assert calls[0][1]["timeout"] == 60


def test_pinned_deepinfra_reuses_openai_without_catalog_discovery(monkeypatch, tmp_path):
    from hermes_cli import models
    configure(monkeypatch, {"provider": "deepinfra", "deepinfra": {"model": "pinned-tts", "voice": "speaker"}})
    monkeypatch.setattr(models, "deepinfra_model_ids", forbidden)
    monkeypatch.setattr(tts, "_resolve_provider_key", lambda *args: "fake-deepinfra")
    calls = fake_openai(monkeypatch)
    assert invoke(tmp_path)["success"] is True
    assert calls[0][1]["max_retries"] == 0
    assert calls[1][1]["model"] == "pinned-tts"
    assert calls[1][1]["voice"] == "speaker"


def test_edge_keeps_worker_scope_and_frozen_nested_config(monkeypatch, tmp_path):
    import contextvars
    import os
    import threading
    import concurrent.futures
    marker = contextvars.ContextVar("controlled_tts_test_owner", default="unset")
    token = marker.set("runtime-a")
    config = {"provider": "edge", "edge": {"voice": "zh-CN-XiaoxiaoNeural", "speed": 1.1}}
    configure(monkeypatch, config)
    native_output = tts._resolve_output_base

    def resolve(*args, **kwargs):
        config["provider"] = "openai"
        config["edge"]["voice"] = "wrong-runtime"
        return native_output(*args, **kwargs)

    monkeypatch.setattr(tts, "_resolve_output_base", resolve)
    monkeypatch.setattr(tts, "_run_edge_tts", forbidden)
    monkeypatch.setattr(concurrent.futures, "ThreadPoolExecutor", forbidden)
    caller = threading.get_ident()
    observed = []
    calls = fake_edge(monkeypatch, mutate=lambda: observed.append((threading.get_ident(), marker.get())))
    before_env = dict(os.environ)
    try:
        assert invoke(tmp_path)["success"] is True
        assert observed == [(caller, "runtime-a")]
        assert calls[0][1] == {"voice": "zh-CN-XiaoxiaoNeural", "rate": "+10%"}
        assert dict(os.environ) == before_env
        assert marker.get() == "runtime-a"
    finally:
        marker.reset(token)


@pytest.mark.parametrize("provider", ["openai", "deepinfra"])
def test_missing_credentials_reports_configuration_error_without_fallback(monkeypatch, tmp_path, provider):
    from tools import tts_tool_openai as backend
    configure(monkeypatch, {"provider": provider})
    monkeypatch.setattr(backend, "resolve_openai_audio_api_key", lambda: "")
    monkeypatch.setattr(tts, "_resolve_provider_key", lambda *args: "")
    monkeypatch.setattr(backend, "_managed_openai_audio_route", forbidden)
    calls = fake_openai(monkeypatch)
    assert invoke(tmp_path) == {"success": False, "error": "controlled_tts_config_invalid"}
    assert calls == []


def test_deepinfra_requires_pinned_model(monkeypatch, tmp_path):
    from hermes_cli import models
    configure(monkeypatch, {"provider": "deepinfra"})
    monkeypatch.setattr(tts, "_resolve_provider_key", lambda *args: "fake")
    monkeypatch.setattr(models, "deepinfra_model_ids", forbidden)
    calls = fake_openai(monkeypatch)
    assert invoke(tmp_path) == {"success": False, "error": "controlled_tts_config_invalid"}
    assert calls == []


@pytest.mark.parametrize('version', ['7.2.7', '7.3.1'])
def test_edge_native_403_recovery_cannot_retry_or_mutate_shared_drm(monkeypatch, tmp_path, version):
    import aiohttp
    configure(monkeypatch, {})
    attempts = []
    recovery = []

    class Communicate:
        def __init__(self, *args, **kwargs):
            pass

        async def _Communicate__stream(self):
            attempts.append(1)
            raise aiohttp.ClientResponseError(SimpleNamespace(real_url="https://secret.invalid"), (), status=403)
            yield  # async-generator protocol, like edge-tts' native transport

        async def save(self, path):
            try:
                async for _ in self._Communicate__stream():
                    pass
            except aiohttp.ClientResponseError:
                # The SDK normally mutates DRM clock state and retries here.
                recovery.append(1)
                async for _ in self._Communicate__stream():
                    pass

    monkeypatch.setitem(sys.modules, "edge_tts", SimpleNamespace(Communicate=Communicate, __version__=version))
    result = invoke(tmp_path)
    assert result == {"success": False, "error": "controlled_tts_generation_failed"}
    assert attempts == [1]
    assert recovery == []


def test_unknown_edge_sdk_version_is_not_assumed_retry_safe(monkeypatch, tmp_path):
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch)
    sys.modules["edge_tts"].__version__ = "unknown-future-sdk"
    assert invoke(tmp_path) == {"success": False, "error": "controlled_tts_config_invalid"}
    assert calls == []


def test_real_config_reads_remain_profile_scoped(monkeypatch, tmp_path):
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    calls = fake_openai(monkeypatch)
    for name in ("runtime-a", "runtime-b"):
        home = tmp_path / name
        home.mkdir()
        (home / "config.yaml").write_text(json.dumps({"tts": {
            "provider": "openai", "openai": {"api_key": "fake-" + name, "voice": name},
        }}), encoding="utf-8")
        token = set_hermes_home_override(home)
        try:
            result = invoke(home)
        finally:
            reset_hermes_home_override(token)
        assert result["success"] is True
        assert result["file_paths"] == [str(home / "reply.mp3")]
        assert "media_tag" not in result
    clients = [params for event, params in calls if event == "client"]
    speeches = [params for event, params in calls if event == "speech"]
    assert [client["api_key"] for client in clients] == ["fake-runtime-a", "fake-runtime-b"]
    assert [speech["voice"] for speech in speeches] == ["runtime-a", "runtime-b"]


def test_event_loop_caller_is_rejected_without_unawaited_coroutine(monkeypatch, tmp_path):
    import warnings
    configure(monkeypatch, {})
    calls = fake_edge(monkeypatch)

    async def call():
        return invoke(tmp_path)

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        result = asyncio.run(call())
    assert result["success"] is False
    assert calls == []
    assert not [warning for warning in captured if issubclass(warning.category, RuntimeWarning)]
