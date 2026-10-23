"""Bounded off-loop observers preserve order and task-local runtime identity."""
import asyncio
from contextvars import ContextVar
from threading import Event

import pytest


@pytest.mark.asyncio
async def test_slow_observer_does_not_block_loop_and_queue_has_finite_drop_newest_policy():
    from gateway.passive_observer_queue import ObserverQueue
    entered, release = Event(), Event()
    finished = []
    queue = ObserverQueue(capacity=2)
    def slow():
        entered.set()
        if not release.wait(5):
            raise AssertionError("test did not release blocked observer")
        finished.append("first")
    try:
        assert queue.submit(slow)
        assert await asyncio.to_thread(entered.wait, 2)
        assert queue.submit(lambda: finished.append("second"))
        assert queue.submit(lambda: finished.append("third"))
        assert not queue.submit(lambda: finished.append("dropped"))
        # A scheduled heartbeat runs while the observer thread is still blocked.
        heartbeat = asyncio.Event()
        asyncio.get_running_loop().call_soon(heartbeat.set)
        await asyncio.wait_for(heartbeat.wait(), timeout=2)
        assert finished == [] and not release.is_set()
    finally:
        release.set()
        await queue.flush()
    assert finished == ["first", "second", "third"]
    assert queue.dropped == 1


@pytest.mark.asyncio
async def test_each_item_preserves_its_context_and_failures_do_not_stop_later_items():
    from gateway.passive_observer_queue import ObserverQueue
    home = ContextVar("fixture_home", default="launch")
    queue = ObserverQueue(capacity=4)
    seen = []
    for value in ("runtime-A", "runtime-B"):
        token = home.set(value)
        try:
            queue.submit(lambda: seen.append(home.get()))
        finally:
            home.reset(token)
    def broken():
        raise ValueError("offline fixture")
    queue.submit(broken)
    queue.submit(lambda: seen.append(home.get()))
    await queue.flush()
    assert seen == ["runtime-A", "runtime-B", "launch"]
    assert home.get() == "launch"


@pytest.mark.asyncio
async def test_shutdown_cancellation_drops_pending_work_and_closes_intake_before_worker_start():
    from gateway.passive_observer_queue import ObserverQueue
    tasks, seen = set(), []
    queue = ObserverQueue(tasks=tasks)
    queue.submit(lambda: seen.append("must not run"))
    task = next(iter(tasks))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert queue.closed
    assert not queue.submit(lambda: seen.append("must not restart"))
    assert seen == []


@pytest.mark.asyncio
async def test_qq_runner_enqueues_observer_without_delaying_message_admission():
    from types import SimpleNamespace
    from unittest.mock import patch
    from gateway.config import Platform
    from gateway.run import GatewayRunner
    from gateway.session import SessionSource
    entered, release, finished = Event(), Event(), Event()
    runner = object.__new__(GatewayRunner)
    runner.config = SimpleNamespace(multiplex_profiles=False)
    runner._is_user_authorized_for_source = lambda source: True
    runner._background_tasks = set()
    source = SessionSource(platform=Platform.QQBOT, chat_type="group", chat_id="Group-A", user_id="Member")
    def slow(*args, **kwargs):
        entered.set()
        release.wait(2)
        finished.set()
    try:
        with patch("hermes_cli.lifecycle.has_hook", return_value=True), patch("hermes_cli.lifecycle.invoke_hook", slow):
            await runner._handle_gateway_platform_event(
                {"platform": "qqbot", "event_type": "group_message", "payload": {}}, source)
            assert await asyncio.to_thread(entered.wait, 2)
            heartbeat = asyncio.Event()
            asyncio.get_running_loop().call_soon(heartbeat.set)
            await asyncio.wait_for(heartbeat.wait(), 2)
            assert not finished.is_set(), "slow observer ran inline before normal message admission"
    finally:
        release.set()
        queue = getattr(runner, "_qq_observer_queue", None)
        if queue is not None:
            await queue.flush()
