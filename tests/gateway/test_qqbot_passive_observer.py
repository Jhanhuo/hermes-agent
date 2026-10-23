"""QQ passive text observation never enters the message/media/busy lanes."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.qqbot.adapter import QQAdapter


def payload(**changes):
    value = {"id": "m1", "group_openid": "Group-A", "author": {"member_openid": "Member"},
             "content": "ordinary @file:private.txt", "attachments": [{"url": "https://invalid.invalid"}]}
    value.update(changes)
    return value


@pytest.mark.asyncio
async def test_opt_in_observes_passive_text_before_gate_without_media_or_dispatch():
    adapter = QQAdapter(PlatformConfig(extra={"require_mention": True, "observe_group_messages": True,
                                              "group_policy": "allowlist", "group_allow_from": ["Group-A"]}))
    observer = AsyncMock()
    adapter.set_platform_event_handler(observer)
    adapter._process_attachments = AsyncMock(side_effect=AssertionError("no media in passive lane"))
    adapter._message_handler = AsyncMock()
    await adapter._on_message("GROUP_MESSAGE_CREATE", payload())
    observer.assert_awaited_once()
    event, source = observer.call_args.args
    assert event["platform"] == "qqbot"
    assert event["event_type"] == "group_message"
    assert event["payload"]["text"] == "ordinary @file:private.txt"
    assert event["payload"]["message_id"] == "m1"
    assert event["payload"]["is_mention"] is False
    assert "attachments" not in event["payload"]
    assert source.chat_id == "Group-A" and source.user_id == "Member"
    adapter._process_attachments.assert_not_awaited()
    adapter._message_handler.assert_not_awaited()
    assert not adapter._dedup.contains("m1")
    assert adapter._last_msg_id == {} and adapter._pending_messages == {}


@pytest.mark.parametrize("value", ["false", 0, 1, None, [], {}])
def test_observer_flag_requires_literal_boolean(value):
    with pytest.raises(ValueError, match="observe_group_messages.*boolean"):
        QQAdapter(PlatformConfig(extra={"observe_group_messages": value}))


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [
    {"group_openid": "Other"}, {"group_openid": 123}, {"group_openid": " Group-A"},
    {"id": None}, {"id": ""}, {"id": "x" * 257}, {"author": {}}, {"author": {"member_openid": 1}},
    {"author": {"member_openid": "Member "}}, {"content": None}, {"content": "  "},
])
async def test_observation_rejects_invalid_identity_empty_text_and_group_acl(changes):
    adapter = QQAdapter(PlatformConfig(extra={"require_mention": True, "observe_group_messages": True,
                                              "group_policy": "allowlist", "group_allow_from": ["Group-A"]}))
    observer = AsyncMock()
    adapter.set_platform_event_handler(observer)
    await adapter._on_message("GROUP_MESSAGE_CREATE", payload(**changes))
    observer.assert_not_awaited()


@pytest.mark.asyncio
async def test_passive_observer_failure_and_long_text_never_affect_turn_lane():
    adapter = QQAdapter(PlatformConfig(extra={"require_mention": True, "observe_group_messages": True,
                                              "group_policy": "open"}))
    observer = AsyncMock(side_effect=RuntimeError("offline observer failure"))
    adapter.set_platform_event_handler(observer)
    await adapter._on_message("GROUP_MESSAGE_CREATE", payload(content="x" * 3000))
    assert len(observer.call_args.args[0]["payload"]["text"]) == 2000
    assert not adapter._dedup.contains("m1")
    assert adapter._last_msg_id == {}


@pytest.mark.asyncio
async def test_ordinary_full_then_at_still_dispatches_one_addressed_turn():
    adapter = QQAdapter(PlatformConfig(extra={"require_mention": True, "observe_group_messages": True,
                                              "group_policy": "open"}))
    observer = AsyncMock()
    adapter.set_platform_event_handler(observer)
    received = []
    async def receive(event):
        received.append(event)
    adapter.handle_message = receive
    for kind in ("GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"):
        await adapter._on_message(kind, payload(content="hello", attachments=[]))
    assert len(received) == 1 and received[0].reply_expected is True
    assert [c.args[0]["payload"]["is_mention"] for c in observer.call_args_list] == [False, True, True]


@pytest.mark.asyncio
async def test_default_off_does_not_publish_passive_messages():
    adapter = QQAdapter(PlatformConfig(extra={"require_mention": True, "group_policy": "open"}))
    observer = AsyncMock()
    adapter.set_platform_event_handler(observer)
    await adapter._on_message("GROUP_MESSAGE_CREATE", payload())
    observer.assert_not_awaited()
