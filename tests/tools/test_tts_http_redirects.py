"""QQ-TTS-001: real SDK/session redirects, mocked only at the network boundary.

Reuse the independent review's HTTPTransport/HTTPAdapter seams and OfflineGuard.
No speech API, Session.send, redirect resolver, or httpx.Client.send is replaced.
Synthetic ID3 bytes test transport policy only, not decoding or playback.
"""
import io
import json
from pathlib import Path

import httpx
import openai
import pytest
import requests

from tests.tools.test_tts_controlled_reply import configure, invoke, isolated
from tools import tts_tool as tts


@pytest.fixture(params=["openai", "nous", "deepinfra", "minimax", "xai", "minimax-legacy"])
def http_backend(request, monkeypatch):
    from tools import tts_tool_openai as backend
    import tools.xai_http

    route = request.param
    provider = "minimax" if route == "minimax-legacy" else route
    config = {"provider": provider, "openai": {"api_key": "offline-only"},
              "deepinfra": {"model": "pinned-tts"}}
    if route == "minimax-legacy":
        config["minimax"] = {"base_url": "https://minimax.invalid/text_to_speech"}
    configure(monkeypatch, config)
    monkeypatch.setattr(tts, "_resolve_provider_key", lambda *args: "offline-only")
    monkeypatch.setattr(backend, "_managed_openai_audio_route",
                        lambda: ("offline-only", "https://nous.invalid/v1", True))
    monkeypatch.setattr(backend, "read_selection", lambda *args: provider)
    monkeypatch.setattr(tools.xai_http, "resolve_xai_http_credentials",
                        lambda **kwargs: {"api_key": "offline-only"})
    # Ordinary helpers keep their lazy-import seam, but receive the REAL SDK class.
    monkeypatch.setattr(tts, "_import_openai_client", lambda: openai.OpenAI)
    return route, provider, config


@pytest.fixture
def upstream(monkeypatch, http_backend):
    route, _, _ = http_backend
    calls = []
    audio = b"ID3" + b"\0" * 20
    payload = (json.dumps({"base_resp": {"status_code": 0},
                           "data": {"audio": audio.hex()}}).encode()
               if route == "minimax" else audio)
    content_type = "application/json" if route == "minimax" else "audio/mpeg"
    reply = {"status": 200, "location": None}

    def httpx_send(self, request):
        calls.append((request.method, str(request.url), request.read()))
        headers = {"Content-Type": content_type}
        if len(calls) == 1 and reply["location"]:
            headers["Location"] = reply["location"]
        return httpx.Response(reply["status"] if len(calls) == 1 else 200,
                              headers=headers, content=payload, request=request)

    def requests_send(self, request, **kwargs):
        calls.append((request.method, request.url, request.body))
        response = requests.Response()
        response.request, response.url = request, request.url
        response.status_code = reply["status"] if len(calls) == 1 else 200
        response.headers["Content-Type"] = content_type
        if len(calls) == 1 and reply["location"]:
            response.headers["Location"] = reply["location"]
        response.raw = io.BytesIO(payload)
        return response

    # Same lowest-level no-network seams as the review probe. OfflineGuard stays
    # active (including DNS/socket tripwires); proxy mounts also hit these seams.
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", httpx_send)
    monkeypatch.setattr(requests.adapters.HTTPAdapter, "send", requests_send)
    return reply, calls, audio


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("location", ["/redirected", "https://other.invalid/redirected"])
def test_controlled_rejects_redirect_without_second_request(tmp_path, upstream, status, location):
    reply, calls, _ = upstream
    reply.update(status=status, location=location)
    result = invoke(tmp_path)
    print({"status": status, "result": result, "calls": calls})
    assert len(calls) == 1, "controlled synthesis followed the redirect"
    assert calls[0][0] == "POST"
    assert result == {"success": False, "error": "controlled_tts_generation_failed"}
    assert not (tmp_path / "reply.mp3").exists()


@pytest.mark.parametrize("status", [300, 304, 399])
def test_controlled_rejects_3xx_even_with_audio_and_no_location(tmp_path, upstream, status):
    reply, calls, _ = upstream
    reply["status"] = status
    result = invoke(tmp_path)
    assert len(calls) == 1
    assert calls[0][0] == "POST"
    assert result == {"success": False, "error": "controlled_tts_generation_failed"}
    assert not (tmp_path / "reply.mp3").exists()


def test_controlled_accepts_direct_200(tmp_path, upstream):
    _, calls, audio = upstream
    result = invoke(tmp_path)
    assert result["success"] is True
    assert len(calls) == 1
    assert calls[0][0] == "POST"
    assert Path(result["file_path"]).read_bytes() == audio


@pytest.mark.parametrize("status", [307, 308])
def test_ordinary_native_helper_keeps_default_redirects(tmp_path, http_backend, upstream, status):
    _, provider, config = http_backend
    reply, calls, audio = upstream
    reply.update(status=status, location="/redirected")
    generate = {"openai": tts._generate_openai_tts, "nous": tts._generate_openai_tts,
                "deepinfra": tts._generate_deepinfra_tts, "minimax": tts._generate_minimax_tts,
                "xai": tts._generate_xai_tts}[provider]
    path = tmp_path / "ordinary.mp3"
    assert generate("ordinary speech", str(path), config) == str(path)
    assert path.read_bytes() == audio
    assert [call[0] for call in calls] == ["POST", "POST"]
    assert calls[0][2] == calls[1][2]
    assert calls[1][1].endswith("/redirected")
