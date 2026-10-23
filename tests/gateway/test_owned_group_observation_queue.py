"""Owned observation cannot block receive behind a model turn or SQLite wait."""
import asyncio

from tests.gateway.test_owned_group_service import rig


def envelope(message_id):
    return {"platform": "qqbot", "event_type": "group_message", "payload": {
        "schema_version": "qq.group_message.v1", "chat_id": "Group-A", "chat_type": "group",
        "user_id": "Member", "message_id": message_id, "text": "ordinary text",
        "is_mention": False, "is_bot": False, "message_timestamp": "2030-03-17T17:46:39Z",
        "qq_event_type": "GROUP_MESSAGE_CREATE"}}


def test_observation_receive_and_persistence_do_not_wait_for_long_turn(rig):
    entered, release, observed = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def consumer(operation, ticket=None, host=None):
        if operation == "turn":
            entered.set()
            await release.wait()  # fake model boundary
        if operation == "observe":
            observed.set()
    rig.register(consumer)
    rig.runner.session_store = None
    rig.runner._busy_text_mode = "steer"
    rig.runner._wire_adapter_handlers(rig.adapter)
    rig.adapter.observe_group_messages, rig.adapter.require_mention = True, True
    async def exercise():
        turn = asyncio.create_task(rig.runner._handle_message(rig.event()))
        await asyncio.wait_for(entered.wait(), 2)
        try:
            await asyncio.wait_for(rig.adapter._on_message("GROUP_MESSAGE_CREATE", {
                "group_openid": "Group-A", "id": "passive", "content": "ordinary text",
                "author": {"member_openid": "Member"}, "timestamp": "2030-03-17T17:46:39Z"}), 2)
            await asyncio.wait_for(observed.wait(), 2)
            assert not turn.done()
        finally:
            release.set()
            await asyncio.wait_for(turn, 2)
    rig.loop.run_until_complete(exercise())
    assert rig.requests == []


def test_observation_queue_is_bounded_fifo_drop_newest_including_active_item(rig):
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "observe":
            seen.append(ticket.message_id)
            entered.set()
            await release.wait()  # fake DB write wait
    rig.register(consumer)
    submitted = [f"message-{index}" for index in range(41)]
    async def exercise():
        source = rig.event().source
        try:
            await asyncio.wait_for(rig.runner._handle_gateway_platform_event(envelope(submitted[0]), source), 2)
            await asyncio.wait_for(entered.wait(), 2)
            for message_id in submitted[1:]:
                await asyncio.wait_for(rig.runner._handle_gateway_platform_event(envelope(message_id), source), 2)
            service = rig.runner._owned_group_service
            assert service.observation_pending == 32
            assert service.observation_dropped == len(submitted) - 32
            release.set()
            await asyncio.wait_for(service.flush_observations(), 2)
            assert service.observation_pending == 0
        finally:
            release.set()
    rig.loop.run_until_complete(exercise())
    assert seen == submitted[:32]
    assert rig.requests == []


def test_shutdown_rejects_new_observation_drops_queue_and_cancels_active_wait(rig):
    entered, cancelled = asyncio.Event(), asyncio.Event()
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "observe":
            seen.append(ticket.message_id)
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
    rig.register(consumer)
    async def exercise():
        source = rig.event().source
        await rig.runner._handle_gateway_platform_event(envelope("active"), source)
        await asyncio.wait_for(entered.wait(), 2)
        await rig.runner._handle_gateway_platform_event(envelope("queued"), source)
        service = rig.runner._owned_group_service
        assert not await service.shutdown(timeout=0)
        await asyncio.wait_for(cancelled.wait(), 2)
        await rig.runner._handle_gateway_platform_event(envelope("after-close"), source)
        await asyncio.wait_for(service.flush_observations(), 2)
        assert service.observation_pending == 0
        await asyncio.gather(*list(service.tasks), return_exceptions=True)
    rig.loop.run_until_complete(exercise())
    assert seen == ["active"] and rig.requests == []
