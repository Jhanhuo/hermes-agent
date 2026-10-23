"""Real QQ ingress keeps typed group context through native admission, never trust."""
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import SendResult
from gateway.run import GatewayRunner
from gateway.session_authority import initialize_session_authority
from gateway.session_envelope import restore_native, snapshot_native
from gateway.session_results import execution_result
from plugins.platforms.qqbot.adapter import QQAdapter
from tests.gateway.qq_offline_guard import OfflineGuard
from tests.waiting import async_wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize("ownership", ["disabled", "other-group"])
@pytest.mark.parametrize("event_type", ["GROUP_AT_MESSAGE_CREATE", "GROUP_MESSAGE_CREATE"])
async def test_real_non_owned_qq_group_reaches_native_authority(tmp_path, monkeypatch, ownership, event_type):
    monkeypatch.setenv("QQ_ALLOWED_USERS", "Member")
    with OfflineGuard():
        runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
        adapter = QQAdapter(PlatformConfig(enabled=True, extra={
            "app_id": "offline-app", "client_secret": "offline-secret",
            "group_policy": "allowlist", "group_allow_from": ["Group-A"],
            "owned_group_replies": ownership != "disabled",
            "owned_group_allow_from": ["Group-A" if ownership == "disabled" else "Other"],
        }))
        adapter._bot_user_id = "self"
        adapter.gateway_runner, runner.adapters = runner, {Platform.QQBOT: adapter}
        authority = await initialize_session_authority(runner, profile_id="default", instance_id="qq")
        runner._wire_adapter_handlers(adapter)
        sent, turns, snapshots = [], [], []
        admission = runner._hm_authority_admission

        async def admit(event, *args):
            from gateway.session_ingress import executing_admission
            if executing_admission.get():
                return await admission(event, *args)
            payload = snapshot_native(runner, event)
            restored = restore_native(payload, runner)
            assert restored.metadata == event.metadata
            assert restored.reply_expected is True
            assert runner._intake_adapter_for(restored.source) is adapter
            assert "provenance" in payload["native_text_v1"]
            snapshots.append(payload)
            return await admission(event, *args)

        async def send(chat_id, content, **kwargs):
            sent.append(content)
            return SendResult(success=True, message_id="reply")

        async def turn(event, *args, **kwargs):
            turns.append((event.text, event.metadata, event.reply_expected))
            execution_result.get()["result"] = {"final_response": "ok", "messages": []}
            return "ok"

        monkeypatch.setattr(runner, "_hm_authority_admission", admit)
        monkeypatch.setattr(runner, "_handle_message_with_agent", turn)
        monkeypatch.setattr(adapter, "send", send)
        monkeypatch.setattr(adapter, "_release_turn_marker", AsyncMock())
        try:
            await adapter._on_message(event_type, {
                "group_openid": "Group-A", "id": "incoming-id", "content": "hello",
                "author": {"member_openid": "Member"}, "mentions": [{"id": "self"}],
                "timestamp": "2030-03-17T17:46:39+00:00",
            })
            await async_wait_until(lambda: bool(sent) and not adapter._background_tasks,
                                   timeout=20, what="native QQ reply")
            assert sent == ["ok"]
            assert turns == [("hello", {"qq_event_type": event_type, "is_mention": True}, True)]
            assert snapshots
        finally:
            authority.db.close()


@pytest.fixture
def codec_event(tmp_path, monkeypatch):
    from gateway.platforms.event import MessageEvent
    monkeypatch.setenv("QQ_ALLOWED_USERS", "Member")
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = QQAdapter(PlatformConfig(enabled=True, extra={"app_id": "offline-app"}))
    adapter.gateway_runner, runner.adapters = runner, {Platform.QQBOT: adapter}
    event = MessageEvent(text="hello", message_id="m1", source=adapter.build_source(
        chat_id="Group-A", user_id="Member", chat_type="group"),
        metadata={"qq_event_type": "GROUP_MESSAGE_CREATE", "is_mention": False})
    return runner, event


@pytest.mark.parametrize("mention", [False, True])
@pytest.mark.parametrize("kind", ["GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"])
def test_qq_native_metadata_round_trip(codec_event, kind, mention):
    runner, event = codec_event
    event.metadata = {"qq_event_type": kind, "is_mention": mention}
    with OfflineGuard():
        payload = snapshot_native(runner, event)
        event.metadata["is_mention"] = not mention
        restored = restore_native(payload)
        assert restored.metadata == {"qq_event_type": kind, "is_mention": mention}


@pytest.mark.parametrize("metadata", [
    {"qq_event_type": "GROUP_MESSAGE_CREATE"}, {"is_mention": True},
    {"qq_event_type": "AT_MESSAGE_CREATE", "is_mention": True},
    {"qq_event_type": "C2C_MESSAGE_CREATE", "is_mention": True},
    {"qq_event_type": ["GROUP_MESSAGE_CREATE"], "is_mention": True},
    *({"qq_event_type": "GROUP_MESSAGE_CREATE", "is_mention": value}
      for value in (0, 1, "true", None, [], {})),
    *({"qq_event_type": "GROUP_MESSAGE_CREATE", "is_mention": True, key: True}
      for key in ("unknown", "internal", "role_authorized", "_authorization_profile_home",
                  "gateway_session_id", "provenance", "whatsapp_from_owner")),
    ["qq_event_type", "is_mention"], [], False, 0, "", None,
])
def test_qq_native_metadata_refuses_wrong_types_and_extra_fields(codec_event, metadata):
    from hermes_state_runtime import RuntimeStoreError
    runner, event = codec_event
    with OfflineGuard(), pytest.raises(RuntimeStoreError) as error:
        snapshot_native(runner, replace(event, metadata=metadata))
    assert error.value.reason == "invalid_params" and error.value.field == "metadata"


@pytest.mark.parametrize("chat_type", ["group", "dm"])
def test_qq_without_group_metadata_keeps_legacy_native_shape(codec_event, chat_type):
    runner, event = codec_event
    event = replace(event, source=replace(event.source, chat_type=chat_type), metadata={})
    with OfflineGuard():
        payload = snapshot_native(runner, event)
        assert "metadata" not in payload["native_text_v1"]["event"]
        assert restore_native(payload).metadata == {}


@pytest.mark.parametrize("platform,chat_type", [(Platform.WHATSAPP, "group"),
    (Platform.TELEGRAM, "group"), (Platform.QQBOT, "dm")])
def test_qq_metadata_cannot_be_used_by_another_platform_or_dm(codec_event, platform, chat_type):
    from hermes_state_runtime import RuntimeStoreError
    runner, event = codec_event
    source = replace(event.source, platform=platform, chat_type=chat_type)
    with OfflineGuard(), pytest.raises(RuntimeStoreError) as error:
        snapshot_native(runner, replace(event, source=source))
    assert error.value.reason == "invalid_params" and error.value.field == "metadata"
