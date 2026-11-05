"""Existing voice input reaches owned admission without replacing ASR/STT."""
from pathlib import Path

import httpx
import pytest

from hermes_constants import get_hermes_home
from tests.gateway.test_owned_group_service import rig


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("HERMES_DISABLE_LAZY_INSTALLS", "1")
    monkeypatch.setenv("HERMES_ENABLE_PROJECT_PLUGINS", "0")


@pytest.mark.parametrize("mode", ["qq_asr", "configured_stt"])
@pytest.mark.parametrize("denial", [None, "unauthorized", "stale", "unaddressed"])
def test_voice_input_owned_ticket_preserves_identity_time_and_admission(rig, monkeypatch, mode, denial):
    tickets, calls, downloads = [], [], []
    adapter = rig.adapter

    def forbidden(*args, **kwargs):
        pytest.fail("voice regression attempted real conversion, provider, refresh or delivery")

    for name in ("_ensure_token", "_auth_headers", "_convert_audio_to_wav_file", "send", "send_voice"):
        monkeypatch.setattr(adapter, name, forbidden)
    monkeypatch.setattr(adapter, "handle_message", rig.runner._handle_message)
    monkeypatch.setattr("tools.url_safety.is_safe_url", lambda url: url == "https://offline.invalid/pre.wav")

    async def fake_stt(path):
        calls.append(path)
        assert Path(path).read_bytes() == b"offline-wave-fixture"
        return "configured transcript"

    async def download(req):
        downloads.append(req)
        assert req.method == "GET" and str(req.url) == "https://offline.invalid/pre.wav"
        return httpx.Response(200, content=b"offline-wave-fixture")

    # Change only the handler of the already approved proxy-free MockTransport.
    adapter._http_client._transport.handler = download
    monkeypatch.setattr(adapter, "_call_stt", forbidden if mode == "qq_asr" else fake_stt)

    async def callback(operation, ticket=None, host=None):
        if operation == "turn":
            assert get_hermes_home() == rig.runtime
            assert ticket.authorized()
            tickets.append(ticket)

    rig.register(callback)
    if denial == "unauthorized":
        rig.runner._is_user_authorized = lambda source: False
    attachment = {"content_type": "audio/silk", "url": "https://offline.invalid/original.silk",
                  "voice_wav_url": "https://offline.invalid/pre.wav"}
    if mode == "qq_asr":
        attachment["asr_refer_text"] = "QQ transcript"
    payload = {"id": "voice-original", "group_openid": "Group-A",
               "author": {"member_openid": "Member"}, "content": "",
               "timestamp": "2030-03-17T17:46:39+00:00", "attachments": [attachment]}
    if denial == "stale":
        payload["timestamp"] = "2030-03-17T17:00:00+00:00"
    event_type = "GROUP_MESSAGE_CREATE" if denial == "unaddressed" else "GROUP_AT_MESSAGE_CREATE"
    rig.loop.run_until_complete(adapter._on_message(event_type, payload))
    if denial:
        assert tickets == []
    else:
        ticket, = tickets
        assert ticket.text == "[Voice] " + ("QQ transcript" if mode == "qq_asr" else "configured transcript")
        assert (ticket.bot_id, ticket.group_id, ticket.user_id, ticket.message_id) == (
            "offline-app", "Group-A", "Member", "voice-original")
        assert ticket.message_time == 1_899_999_999.0
        assert ticket.runtime_home == rig.runtime and ticket.transport_profile == "default"
        assert ticket.addressed is True
    if mode == "qq_asr":
        assert downloads == calls == []
    elif denial != "unaddressed":
        assert len(downloads) == len(calls) == 1
        assert not Path(calls[0]).exists()
    assert rig.requests == []
