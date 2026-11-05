"""Owned strict MP3 delivery; registered fake HTTP only, never synthesize/send audio."""
import asyncio
import base64
import json

import httpx

import pytest

from tests.gateway.test_qqbot_strict_media import (
    harness as media_harness, ticket_harness,
    test_both_flags_require_literal_true,
    test_missing_flag_is_disabled,
    test_identity_connection_policy_and_cached_token_gate_upload,
    test_untrusted_identity_anchor_and_timestamp_reject_without_upload,
    test_invalid_clock_never_uploads,
    test_untrusted_upload_receipt_never_submits_message,
    test_non_json_upload_never_submits_or_logs_response,
    test_http_errors_and_redirects_do_not_retry,
    test_request_errors_are_unknown_and_never_logged_or_retried,
    test_final_receipt_uses_existing_strict_classifier,
    test_network_tripwire_propagates_through_real_adapter,
    test_recheck_only_literal_true_permits_each_request,
    test_recheck_exception_never_submits_or_leaks,
    test_every_await_boundary_revalidates_transport_and_original_window,
    test_upload_latency_cannot_preserve_revoked_permission,
    test_rollback_cannot_be_hidden_by_later_callback_clock_recovery,
    test_client_auth_cannot_replay_either_post,
    test_characterization_total_budget_is_single_and_capped_at_twenty_seconds,
    test_characterization_existing_response_hooks_are_not_bypassed,
    test_characterization_non_async_recheck_never_uploads,
)


@pytest.fixture
def harness(media_harness, monkeypatch):
    """Exercise the same strict transport invariants with audio, not images."""
    original = media_harness.kwargs

    def kwargs(**changes):
        values = original()
        values.pop("image_data")
        values.update(audio_data=MP3, mime_type="audio/mpeg")
        values.update(changes)
        return values

    def send(**changes):
        return media_harness.loop.run_until_complete(
            media_harness.adapter.send_group_audio_reply_once(**kwargs(**changes)))

    def forbidden(*args, **kwargs):
        pytest.fail("strict audio cannot use ordinary voice/conversion/image delivery")

    for name in ("send_voice", "send_document", "send_group_image_reply_once", "_convert_audio_to_wav"):
        monkeypatch.setattr(media_harness.adapter, name, forbidden)
    media_harness.kwargs, media_harness.send = kwargs, send
    return media_harness

# Synthetic MPEG-1 layer III frame-shaped bytes; not a playback acceptance fixture.
MP3 = b"\xff\xfb\x90\x00" + b"\x00" * 413


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_RUNTIME_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("HERMES_DISABLE_LAZY_INSTALLS", "1")
    monkeypatch.setenv("HERMES_ENABLE_PROJECT_PLUGINS", "0")


def test_owned_audio_uses_original_target_and_two_scoped_checks(harness, ticket_harness, monkeypatch):
    t = ticket_harness
    before = t.ticket.as_record()
    seen = []

    def forbidden(*args, **kwargs):
        pytest.fail("strict audio cannot use ordinary voice/conversion/image delivery")

    for name in ("send_voice", "send_document", "send_group_image_reply_once", "_convert_audio_to_wav"):
        monkeypatch.setattr(harness.adapter, name, forbidden)

    async def recheck():
        assert t.active.get() == t.ticket.runtime_home
        seen.append(len(harness.requests))
        return True

    result = harness.loop.run_until_complete(t.ticket.submit_audio(MP3, "audio/mpeg", recheck=recheck))
    assert (result.status, result.message_id) == ("accepted", "sent-id")
    assert seen == [0, 1]
    assert t.state.checks >= 5
    assert t.active.get() is None
    assert t.ticket.as_record() == before
    upload, final = harness.requests
    assert upload.url.path == "/v2/groups/Group-A/files"
    assert json.loads(upload.content) == {
        "file_type": 3, "file_data": base64.b64encode(MP3).decode("ascii"),
        "file_name": "reply.mp3", "srv_send_msg": False,
    }
    assert final.url.path == "/v2/groups/Group-A/messages"
    assert json.loads(final.content) == {
        "msg_type": 7, "content": " ", "media": {"file_info": "opaque-file-secret"},
        "msg_id": "incoming-id", "msg_seq": 1,
    }
    assert all(req.headers["Authorization"] == "QQBot offline-token" for req in harness.requests)


@pytest.mark.parametrize(("data", "mime"), [
    (b"", "audio/mpeg"), (None, "audio/mpeg"), (bytearray(MP3), "audio/mpeg"),
    (memoryview(MP3), "audio/mpeg"), ("https://offline.invalid/voice.mp3", "audio/mpeg"),
    ("H:/private/voice.mp3", "audio/mpeg"), (MP3, "audio/mp3"), (MP3, "audio/wav"),
    (MP3, None), (MP3, "audio/mpeg; charset=x"), (b"not audio", "audio/mpeg"),
    (b"ID3", "audio/mpeg"), (b"ID3\x04\x00\x00\x7f\x7f\x7f\x7f" + MP3, "audio/mpeg"),
    (b"ID3\x04\x00\x00\x80\x00\x00\x00" + MP3, "audio/mpeg"),
    (b"\xff\xfb\x90\x00", "audio/mpeg"),
    (b"\xff\xff\x90\x00" + MP3, "audio/mpeg"),
    (b"\xff\xfb\xfc\x00" + MP3, "audio/mpeg"),
    (MP3 + b"x" * (2 * 1024 * 1024), "audio/mpeg"),
], ids=lambda value: f"bytes-{len(value)}" if isinstance(value, bytes) else None)
def test_invalid_audio_rejects_before_encoding_or_http(harness, data, mime, monkeypatch):
    def forbidden(*args):
        pytest.fail("invalid audio reached encoding")

    monkeypatch.setattr(base64, "b64encode", forbidden)
    values = harness.kwargs()
    values.update(audio_data=data, mime_type=mime)
    result = harness.loop.run_until_complete(harness.adapter.send_group_audio_reply_once(**values))
    assert (result.status, result.reason) == ("rejected", "invalid_audio")
    assert harness.requests == []


def test_id3_tagged_mp3_is_uploaded_unchanged(harness):
    data = b"ID3\x04\x00\x00\x00\x00\x00\x04" + b"tag!" + MP3
    values = harness.kwargs()
    values.update(audio_data=data, mime_type="audio/mpeg")
    result = harness.loop.run_until_complete(harness.adapter.send_group_audio_reply_once(**values))
    assert result.status == "accepted"
    assert base64.b64decode(json.loads(harness.requests[0].content)["file_data"]) == data


@pytest.mark.parametrize("phase", ["entry", "upload", "first_check", "second_check"])
@pytest.mark.parametrize("mutation", ["revoked", "binding", "adapter"])
def test_ticket_audio_cannot_cross_revocation_or_retarget(harness, ticket_harness, phase, mutation):
    t = ticket_harness
    checks = 0

    def revoke():
        if mutation == "revoked":
            t.state.allowed = False
        elif mutation == "binding":
            object.__setattr__(t.ticket, "_binding", None)
        else:
            t.binding.adapter = None

    async def upload(req):
        revoke()
        return httpx.Response(200, json={"file_info": "opaque"})

    async def recheck():
        nonlocal checks
        checks += 1
        if phase == ("first_check" if checks == 1 else "second_check"):
            revoke()
        return True

    if phase == "entry":
        if mutation == "adapter":
            # Real binding.authorized rejects a disconnected/replaced adapter.
            t.state.allowed = False
        else:
            revoke()
    elif phase == "upload":
        harness.state.upload = upload
    result = harness.loop.run_until_complete(t.ticket.submit_audio(MP3, "audio/mpeg", recheck=recheck))
    assert result.status == "rejected"
    assert len(harness.requests) == (1 if phase in {"upload", "second_check"} else 0)
    assert t.active.get() is None


@pytest.mark.parametrize("phase", ["upload", "final", "first_check", "second_check"])
@pytest.mark.parametrize("cancel", [False, True])
def test_audio_waits_share_deadline_and_cancellation_never_falls_back(harness, phase, cancel):
    async def scenario():
        entered, blocked = asyncio.Event(), asyncio.Event()
        checks = 0

        async def hold(req):
            entered.set()
            await blocked.wait()

        async def recheck():
            nonlocal checks
            checks += 1
            if phase == ("first_check" if checks == 1 else "second_check"):
                entered.set()
                await blocked.wait()
            return True

        if phase in {"upload", "final"}:
            setattr(harness.state, phase, hold)
        values = harness.kwargs(recheck=recheck)
        if not cancel:
            values["message_time"] -= 283.97
        task = asyncio.create_task(harness.adapter.send_group_audio_reply_once(**values))
        try:
            if cancel:
                await asyncio.wait_for(entered.wait(), 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                assert (await asyncio.wait_for(task, 2)).status == "unknown"
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    harness.loop.run_until_complete(scenario())
    assert len(harness.requests) == {"upload": 1, "final": 2, "first_check": 0, "second_check": 1}[phase]


def test_audio_ticket_accepts_no_retarget_parameters(harness, ticket_harness):
    with pytest.raises(TypeError):
        ticket_harness.ticket.submit_audio(MP3, "audio/mpeg", group_id="Other", recheck=None)
    assert harness.requests == []
