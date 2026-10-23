"""Runner-owned callback tasks and per-runtime serial tick lifecycle.

Cancellation only cancels coroutine waits; it cannot kill a plugin's worker
thread or undo an in-flight platform request. A timed-out drain reports False.
"""
import asyncio
import logging
from contextvars import Context
from pathlib import Path

from agent.async_utils import consume_detached_task_result

logger = logging.getLogger(__name__)


class OwnedGroupLifecycle:
    def __init__(self, runner):
        self.runner = runner
        self.closed = False
        self.started = False
        self.tasks = set()
        self.tickers = {}
        self.runtimes = {}
        self.locks = {}
        self.stop_event = asyncio.Event()
        from gateway.owned_group_observation import ObservationInbox
        self.observations = ObservationInbox(self)

    @property
    def observation_pending(self):
        return self.observations.pending

    @property
    def observation_dropped(self):
        return self.observations.dropped

    async def flush_observations(self):
        await self.observations.flush()

    def register_runtime(self, runtime_id, runtime_home):
        if self.closed:
            return
        home = Path(runtime_home)
        self.runtimes[runtime_id] = home
        self.locks.setdefault(runtime_id, asyncio.Lock())
        if self.started:
            self._start_ticker(runtime_id)

    def _track(self, coroutine, *, ticker=False):
        task = Context().run(lambda: asyncio.create_task(coroutine))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        task.add_done_callback(consume_detached_task_result)
        if ticker:
            task._hermes_supervised_watcher = True
        background = getattr(self.runner, "_background_tasks", None)
        if isinstance(background, set):
            background.add(task)
            task.add_done_callback(background.discard)
        return task

    async def invoke(self, host, operation, ticket=None):
        self.register_runtime(host.runtime_id, host.runtime_home)
        async def run():
            from gateway.owned_group_service import _scope
            async with self.locks[host.runtime_id]:
                if operation != "shutdown" and (not host.alive()
                        or (ticket is not None and not ticket.authorized())):
                    return
                with _scope(host.runtime_home):
                    await host._callback(operation, ticket=ticket, host=host)
        # Retain callback ownership even if the adapter's waiter is cancelled.
        await asyncio.shield(self._track(run()))

    def start(self):
        if self.closed or self.started:
            return
        self.started = True
        for runtime_id in self.runtimes:
            self._start_ticker(runtime_id)

    def _start_ticker(self, runtime_id):
        previous = self.tickers.get(runtime_id)
        if previous is None or previous.done():
            self.tickers[runtime_id] = self._track(self._ticker(runtime_id), ticker=True)

    async def _ticker(self, runtime_id):
        while not self.closed:
            try:
                host = self.context(runtime_id, self.runtimes[runtime_id])
                if host is not None and host.alive():
                    await self.invoke(host, "tick")
            except Exception:
                logger.warning("Owned group tick failed; no delivery fallback", exc_info=True)
            if not self.closed:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=1.0)
                except TimeoutError:
                    pass

    async def shutdown(self, timeout=5.0):
        if not self.closed:
            self.closed = True  # Close submit/restore before waiting on any callback.
            self.observations.close()
            self.stop_event.set()
            for runtime_id, home in self.runtimes.items():
                try:
                    host = self.context(runtime_id, home)
                    if host is not None:
                        self._track(self.invoke(host, "shutdown"))
                except Exception:
                    logger.warning("Owned group shutdown lookup failed; draining retained work", exc_info=True)
        current = asyncio.current_task()
        pending = {task for task in self.tasks if task is not current and not task.done()}
        if not pending:
            return True
        _, unfinished = await asyncio.wait(pending, timeout=timeout)
        # invoke(shutdown) may have spawned its managed child after the snapshot.
        unfinished |= {task for task in self.tasks if task is not current and not task.done()}
        if unfinished:
            for task in unfinished:
                task.cancel()
            logger.warning("Owned group drain timed out; coroutine cancellation does not stop worker threads")
            return False
        return True
