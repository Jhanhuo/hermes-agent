"""Offline QQ group-event contracts through the real inbound pipeline."""

import asyncio
from copy import deepcopy

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.qqbot.adapter import QQAdapter


@pytest.fixture
def admitted_intake(monkeypatch, request):
    """Keep QQ/Base admission real; stop before runner/model/delivery I/O."""
    adapter = QQAdapter(PlatformConfig(enabled=True, extra={
        "app_id": "offline-app", "client_secret": "offline-secret",
        "group_policy": "open", "require_mention": True,
        **getattr(request, "param", {}),
    }))
    events = []

    async def receive(event):
        events.append(event)

    async def process(event, session_key):
        await receive(event)

    adapter.set_message_handler(receive)
    monkeypatch.setattr(adapter, "_process_message_background", process)
    return adapter, events


async def drain_intake(adapter):
    if adapter._background_tasks:
        await asyncio.gather(*list(adapter._background_tasks))


@pytest.fixture
def intake(monkeypatch, request):
    monkeypatch.setenv("GATEWAY_ALLOW_ALL_USERS", "true")
    adapter = QQAdapter(PlatformConfig(enabled=True, extra={
        "app_id": "offline-app", "client_secret": "offline-secret",
        "group_policy": "open", **getattr(request, "param", {}),
    }))
    events = []

    async def receive(event):
        events.append(event)

    # Stop at the transport/runner boundary: no gateway, model, or QQ network.
    monkeypatch.setattr(adapter, "handle_message", receive)
    return adapter, events


def group_payload(**overrides):
    payload = {
        "id": "message-001", "group_openid": "000GROUP-openid",
        "author": {"member_openid": "000MEMBER-openid"},
        "content": "@other-user hello everyone",
        "timestamp": "2026-10-21T12:00:00+08:00",
    }
    payload.update(overrides)
    return payload


@pytest.mark.asyncio
async def test_full_group_message_preserves_other_user_mention(intake):
    adapter, events = intake
    payload = group_payload()

    await adapter._on_message("GROUP_MESSAGE_CREATE", payload)

    assert len(events) == 1
    event = events[0]
    assert event.text == "@other-user hello everyone"
    assert event.source.chat_id == "000GROUP-openid"
    assert event.source.user_id == "000MEMBER-openid"
    assert event.source.chat_type == "group"
    assert event.raw_message is payload
    assert adapter._guess_chat_type(event.source.chat_id) == "group"


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type,expected_text,is_mention", [
    ("GROUP_AT_MESSAGE_CREATE", "hello everyone", True),
    ("GROUP_MESSAGE_CREATE", "@other-user hello everyone", False),
])
async def test_group_addressing_is_event_specific(intake, event_type, expected_text, is_mention):
    adapter, events = intake
    # Another bot in mentions is not proof that THIS bot was addressed.
    payload = group_payload(mentions=[{"id": "someone-else", "bot": True}])

    await adapter._on_message(event_type, payload)

    event = events[0]
    assert event.text == expected_text
    assert event.metadata["qq_event_type"] == event_type
    assert event.metadata["is_mention"] is is_mention
    assert event.reply_expected is is_mention
    assert "qq_event_type" not in payload


@pytest.mark.asyncio
@pytest.mark.parametrize("mention,expected", [("bot-user", True), ("another-bot", False)])
async def test_full_group_mentions_match_ready_bot_identity(intake, mention, expected):
    adapter, events = intake
    adapter._dispatch_payload({"op": 0, "t": "READY", "s": 1,
        "d": {"session_id": "session", "user": {"id": "bot-user"}}})
    payload = group_payload(mentions=[{"id": mention, "bot": True}])
    await adapter._on_message("GROUP_MESSAGE_CREATE", payload)
    assert events[0].reply_expected is expected
    assert events[0].metadata["is_mention"] is expected
    assert events[0].text == "@other-user hello everyone"


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"])
async def test_bot_author_reaches_existing_bot_admission_as_bot(intake, event_type):
    adapter, events = intake
    payload = group_payload(author={"member_openid": "other-bot-openid", "bot": True})

    await adapter._on_message(event_type, payload)

    assert len(events) == 1  # Other bots are not blanket-dropped by the adapter.
    assert events[0].source.is_bot is True
    assert events[0].source.user_id == "other-bot-openid"


@pytest.mark.asyncio
@pytest.mark.parametrize("author_fields,member_fields,expected_name", [
    ({"username": "Alice"}, {}, "Alice"),
    ({"nickname": "小明"}, {}, "小明"),
    ({"username": "Alice"}, {"nick": "群昵称"}, "群昵称"),
])
async def test_group_display_name_is_preserved(intake, author_fields, member_fields, expected_name):
    adapter, events = intake
    payload = group_payload(
        author={"member_openid": "000MEMBER-openid", **author_fields}, member=member_fields,
    )

    await adapter._on_message("GROUP_MESSAGE_CREATE", payload)

    assert events[0].source.user_name == expected_name
    assert events[0].source.user_id == "000MEMBER-openid"
    assert events[0].source.is_bot is False


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"])
async def test_websocket_dispatch_uses_real_pipeline_and_deduplicates(intake, monkeypatch, event_type):
    adapter, events = intake
    tasks = []
    create_task = asyncio.create_task

    def schedule(coro):
        task = create_task(coro)
        tasks.append(task)
        return task

    monkeypatch.setattr("plugins.platforms.qqbot.adapter.asyncio.create_task", schedule)
    payload = group_payload()
    adapter._dispatch_payload({"op": 0, "t": event_type, "s": 1, "d": payload})
    assert tasks  # Unsupported dispatch must not silently succeed.
    await asyncio.gather(*tasks)
    await adapter._on_message("GROUP_MESSAGE_CREATE", payload)
    await adapter._on_message("GROUP_AT_MESSAGE_CREATE", payload)
    assert len(events) == 1
    assert events[0].metadata["qq_event_type"] == event_type


@pytest.mark.asyncio
@pytest.mark.parametrize("intake,allowed", [
    ({"group_policy": "disabled"}, False),
    ({"group_policy": "allowlist", "group_allow_from": ["another-group"]}, False),
    ({"group_policy": "allowlist", "group_allow_from": ["000GROUP-openid"]}, True),
], indirect=["intake"])
@pytest.mark.parametrize("event_type", ["GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"])
async def test_full_and_at_group_events_share_group_acl(intake, allowed, event_type):
    adapter, events = intake

    await adapter._on_message(event_type, group_payload())

    assert bool(events) is allowed


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [None, [], {}, group_payload(id=""),
    group_payload(group_openid=""), group_payload(content="   ")])
async def test_invalid_or_empty_full_group_events_are_not_forwarded(intake, payload):
    adapter, events = intake

    await adapter._on_message("GROUP_MESSAGE_CREATE", payload)

    assert events == []


@pytest.mark.asyncio
async def test_full_group_media_quote_and_raw_extensions_survive(intake, monkeypatch, tmp_path):
    adapter, events = intake
    image = tmp_path / "offline.png"
    image.write_bytes(b"offline-image")

    async def download(url, content_type, filename):
        assert url == "https://cdn.example/image.png"
        return str(image)

    monkeypatch.setattr(adapter, "_download_and_cache", download)
    payload = group_payload(content="", message_type=103,
        attachments=[
            {"content_type": "image/png", "url": "https://cdn.example/image.png"},
            {"content_type": "audio/silk", "url": "https://cdn.example/voice.silk",
             "asr_refer_text": "offline transcript"},
        ],
        msg_elements=[{"content": "quoted text"}],
        message_reference={"message_id": "000-reference-id"},
        ext={"future_field": {"opaque": "kept"}},
    )
    original = deepcopy(payload)

    await adapter._on_message("GROUP_MESSAGE_CREATE", payload)

    event = events[0]
    assert "[Quoted message]:\nquoted text" in event.text
    assert "[Voice] offline transcript" in event.text
    assert event.media_urls == [str(image)]
    assert event.media_types == ["image/png"]
    assert event.raw_message is payload
    assert event.raw_message == original
    assert event.reply_expected is False


@pytest.mark.asyncio
@pytest.mark.parametrize("full_first", [True, False])
@pytest.mark.parametrize("ready_id,mentions", [
    (None, []),
    ("bot-user", []),
    ("bot-user", [{"id": "another-bot", "bot": True}]),
    ("bot-user", [{"id": "bot-user"}]),
])
async def test_mention_only_dual_delivery_executes_one_addressed_message(
    admitted_intake, full_first, ready_id, mentions,
):
    adapter, events = admitted_intake
    if ready_id:
        adapter._dispatch_payload({"op": 0, "t": "READY", "s": 1,
            "d": {"session_id": "session", "user": {"id": ready_id}}})
    full = group_payload(content="hello", mentions=mentions)
    at = group_payload(content="hello")
    pair = [("GROUP_MESSAGE_CREATE", full), ("GROUP_AT_MESSAGE_CREATE", at)]
    if not full_first:
        pair.reverse()
    for event_type, payload in pair * 2:
        await adapter._on_message(event_type, payload)
        await drain_intake(adapter)
    assert len(events) == 1
    assert events[0].reply_expected is True
    assert events[0].metadata["is_mention"] is True
    full_wins = full_first and ready_id and mentions == [{"id": ready_id}]
    assert events[0].metadata["qq_event_type"] == (
        "GROUP_MESSAGE_CREATE" if full_wins else "GROUP_AT_MESSAGE_CREATE")
    assert events[0].raw_message is (full if full_wins else at)
    assert adapter._last_msg_id == {full["group_openid"]: full["id"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted_intake", [{"require_mention": True}, {"require_mention": False}],
                         indirect=True)
@pytest.mark.parametrize("full_first", [True, False])
@pytest.mark.parametrize("prefix", ["<@bot-user>", "<@!bot-user>"])
async def test_self_mention_command_survives_either_dual_delivery_order(
    admitted_intake, full_first, prefix,
):
    adapter, events = admitted_intake
    adapter._dispatch_payload({"op": 0, "t": "READY", "s": 1,
        "d": {"session_id": "session", "user": {"id": "bot-user"}}})
    full = group_payload(content=f"{prefix} /qqsilent off", mentions=[{"id": "bot-user"}])
    at = group_payload(content=f"{prefix} /qqsilent off")
    original = deepcopy(full)
    pair = [("GROUP_MESSAGE_CREATE", full), ("GROUP_AT_MESSAGE_CREATE", at)]
    if not full_first:
        pair.reverse()
    for event_type, payload in pair * 2:
        await adapter._on_message(event_type, payload)
        await drain_intake(adapter)
    assert len(events) == 1
    assert events[0].text == "/qqsilent off"
    assert events[0].get_command() == "qqsilent"
    assert events[0].get_command_args() == "off"
    assert full == original


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", ["<@!another-bot>", "<@bot-user-extra>", "@other-user"])
async def test_full_self_addressing_does_not_strip_someone_elses_prefix(admitted_intake, prefix):
    adapter, events = admitted_intake
    adapter._bot_user_id = "bot-user"
    text = f"{prefix} /qqsilent off"
    await adapter._on_message("GROUP_MESSAGE_CREATE",
        group_payload(content=text, mentions=[{"id": "bot-user"}]))
    await drain_intake(adapter)
    assert len(events) == 1
    assert events[0].text == text
    assert events[0].get_command() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["GUILD_MESSAGE_CREATE", "GUILD_AT_MESSAGE_CREATE"])
async def test_qq_group_mention_gate_does_not_regress_existing_guild_intake(admitted_intake, event_type):
    adapter, events = admitted_intake
    payload = {
        "id": "guild-message", "guild_id": "guild", "channel_id": "channel",
        "author": {"id": "guild-member"}, "content": "guild hello",
    }
    await adapter._on_message(event_type, payload)
    await drain_intake(adapter)
    assert len(events) == 1
    assert events[0].source.chat_type == "group"
    assert adapter._guess_chat_type("channel") == "guild"
    assert events[0].text == "guild hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted_intake", [{"require_mention": False}], indirect=True)
@pytest.mark.parametrize("full_first", [True, False])
async def test_full_intake_keeps_first_copy_without_late_addressing_upgrade(admitted_intake, full_first):
    adapter, events = admitted_intake
    full = group_payload(content="hello", mentions=[])
    at = group_payload(content="hello")
    pair = [("GROUP_MESSAGE_CREATE", full), ("GROUP_AT_MESSAGE_CREATE", at)]
    if not full_first:
        pair.reverse()
    for event_type, payload in pair:
        await adapter._on_message(event_type, payload)
        # The first turn may have already consumed addressing before AT arrives.
        await drain_intake(adapter)
    assert len(events) == 1
    assert events[0].metadata["qq_event_type"] == pair[0][0]
    assert events[0].reply_expected is (not full_first)
    assert events[0].metadata["is_mention"] is (not full_first)
    assert events[0].raw_message is pair[0][1]


@pytest.mark.asyncio
async def test_unaddressed_full_does_not_process_attachments_or_reserve_dedup(admitted_intake, monkeypatch):
    adapter, events = admitted_intake

    async def must_not_process(_attachments):
        pytest.fail("unaddressed full must be dropped before media/STT")

    monkeypatch.setattr(adapter, "_process_attachments", must_not_process)
    payload = group_payload(attachments=[{"content_type": "audio/silk", "url": "offline"}])
    await adapter._on_message("GROUP_MESSAGE_CREATE", payload)
    assert events == []
    assert not adapter._dedup.contains(payload["id"])
    assert adapter._last_msg_id == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted_intake", [{"require_mention": True}, {"require_mention": False}],
                         indirect=True)
@pytest.mark.parametrize("full_first", [True, False])
async def test_dual_delivery_dedup_is_reserved_before_media_await(admitted_intake, monkeypatch, full_first):
    adapter, events = admitted_intake
    adapter._bot_user_id = "bot-user"
    entered, release = asyncio.Event(), asyncio.Event()
    process_attachments = adapter._process_attachments

    async def paused(attachments):
        entered.set()
        await release.wait()
        return await process_attachments(attachments)

    monkeypatch.setattr(adapter, "_process_attachments", paused)
    payload = group_payload(content="hello", mentions=[{"id": "bot-user"}])
    pair = ["GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"]
    if not full_first:
        pair.reverse()
    first = asyncio.create_task(adapter._on_message(pair[0], payload))
    try:
        await entered.wait()
        await adapter._on_message(pair[1], payload)
        assert events == []
    finally:
        release.set()
        await first
    await drain_intake(adapter)
    assert len(events) == 1
    assert events[0].reply_expected is True
    assert events[0].metadata["qq_event_type"] == pair[0]
