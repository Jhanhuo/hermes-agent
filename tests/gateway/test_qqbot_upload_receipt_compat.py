"""Observed upload receipt compatibility, without live traffic or retries."""
import json

import httpx
import pytest

from tests.gateway.test_qqbot_strict_diagnostics import isolated_runtime, media
from tests.gateway.test_qqbot_strict_media import harness as media_harness


def test_empty_top_level_id_is_only_an_upload_placeholder(media):
    # Synthetic handle matching the observed shape, not the actual server handle.
    handle = "x" * 640
    media.state.upload = httpx.Response(200, json={"id": "", "file_info": handle})
    result = media.diagnostic_send()
    assert (result.status, result.message_id, result.reason) == ("accepted", "sent-id", "")
    assert len(media.requests) == 2 and media.state.checks == 2
    upload, final = media.requests
    assert upload.url.path == "/v2/groups/Group-A/files"
    assert json.loads(upload.content)["srv_send_msg"] is False
    assert final.url.path == "/v2/groups/Group-A/messages"
    body = json.loads(final.content)
    assert body["msg_id"] == "incoming-id" and body["msg_seq"] == 1
    assert body["msg_type"] == 7 and body["media"] == {"file_info": handle}


def test_media_submit_supplies_the_documented_space_content(media):
    assert media.diagnostic_send().status == "accepted"
    assert len(media.requests) == 2
    assert json.loads(media.requests[1].content).get("content") == " "


@pytest.mark.parametrize("value", [None, False, 0, [], {}, " ", "\t", "sent-id", "\x00"])
def test_other_top_level_id_values_are_still_ambiguous(media, value):
    media.state.upload = httpx.Response(200, json={"id": value, "file_info": "opaque"})
    result = media.diagnostic_send()
    assert (result.status, result.reason) == ("unknown", "unexpected_upload_message")
    assert len(media.requests) == 1


@pytest.mark.parametrize("extra", [
    {"message_id": ""}, {"message_id": None}, {"message_id": "sent-id"},
    {"data": {"id": ""}}, {"data": {"id": None}}, {"data": {"id": "sent-id"}},
    {"data": {"message_id": ""}}, {"data": {"message_id": "sent-id"}},
])
def test_empty_top_level_id_never_overrides_other_message_indicators(media, extra):
    media.state.upload = httpx.Response(200, json={"id": "", "file_info": "opaque", **extra})
    result = media.diagnostic_send()
    assert (result.status, result.reason) == ("unknown", "unexpected_upload_message")
    assert len(media.requests) == 1


@pytest.mark.parametrize("value", [None, False, [], {}, "", " ", "x\n", "x" * 16385])
def test_empty_id_still_requires_a_valid_upload_handle(media, value):
    media.state.upload = httpx.Response(200, json={"id": "", "file_info": value})
    result = media.diagnostic_send()
    assert (result.status, result.reason) == ("unknown", "invalid_file_info")
    assert len(media.requests) == 1


@pytest.mark.parametrize("error", [{"code": 1}, {"code": True}, {"code": "0"}, {"error": 1}])
def test_empty_id_does_not_suppress_application_errors(media, error):
    media.state.upload = httpx.Response(200, json={"id": "", "file_info": "opaque", **error})
    result = media.diagnostic_send()
    assert (result.status, result.reason) == ("rejected", "upload_application_error")
    assert len(media.requests) == 1


@pytest.mark.parametrize("gate", ["recheck", "allowlist", "token", "deadline", "owner", "flag"])
def test_empty_id_upload_still_rechecks_before_final_send(media, gate):
    async def upload(_request):
        if gate == "allowlist":
            media.adapter._group_allow_from.clear()
        elif gate == "token":
            media.adapter._token_expires_at = media.clock.value - 1
        elif gate == "deadline":
            media.clock.value += 3600
        elif gate == "owner":
            media.adapter._app_id = "different-owner"
        elif gate == "flag":
            media.adapter.config.extra["strict_group_media_replies"] = False
        return httpx.Response(200, json={"id": "", "file_info": "opaque"})

    checks = 0

    async def recheck():
        nonlocal checks
        checks += 1
        return not (gate == "recheck" and checks == 2)

    media.state.upload = upload
    result = media.diagnostic_send(recheck=recheck)
    assert result.status == "rejected"
    assert len(media.requests) == 1


@pytest.mark.parametrize("final", [
    httpx.Response(200, json={"id": ""}), httpx.Response(200, json={"id": None}),
    httpx.Response(503), httpx.ReadTimeout("offline-only"),
])
def test_empty_upload_id_never_accepts_an_unknown_final_result_or_retries(media, final):
    media.state.upload = httpx.Response(200, json={"id": "", "file_info": "opaque"})
    media.state.final = final
    result = media.diagnostic_send()
    assert result.status == "unknown"
    assert len(media.requests) == 2
    assert [request.url.path for request in media.requests] == [
        "/v2/groups/Group-A/files", "/v2/groups/Group-A/messages"]
