"""Best-effort FIFO for passive QQ observers, never a message-turn scheduler."""
from __future__ import annotations

import asyncio
from collections import deque
from contextvars import copy_context
import logging

logger = logging.getLogger(__name__)


class ObserverQueue:
    """One off-loop callback at a time, bounded pending work, drop newest on overflow.

    Tasks inherit the submitting profile's context per item, not the first
    worker's profile. No deadline pretends to terminate Python threads: a hung
    callback occupies the sole worker until it finishes; overflow is discarded.
    Shutdown cancellation closes intake and drops queued work. The already
    running thread cannot be forcibly stopped and may complete its current write.
    """
    def __init__(self, *, capacity=32, tasks=None):
        if type(capacity) is not int or capacity < 1:
            raise ValueError("capacity must be a positive integer")
        self.capacity = capacity
        self._pending = deque()
        self._task = None
        self._tasks = tasks
        self.closed = False
        self.dropped = 0

    def submit(self, callback):
        if self.closed or len(self._pending) >= self.capacity:
            self.dropped += 1
            if self.dropped == 1:
                logger.warning("Passive observer queue full or closed; dropping observations (not message turns)")
            return False
        self._pending.append((copy_context(), callback))
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._drain(), name="qq-passive-observers")
            self._task.add_done_callback(self._completed)
            if self._tasks is not None:
                self._tasks.add(self._task)
                self._task.add_done_callback(self._tasks.discard)
        return True

    def _completed(self, task):
        if task.cancelled():
            self.closed = True
            self.dropped += len(self._pending)
            self._pending.clear()

    async def _drain(self):
        try:
            while self._pending:
                context, callback = self._pending.popleft()
                try:
                    await asyncio.to_thread(context.run, callback)
                except Exception:
                    logger.warning("Passive observer callback failed; observation discarded", exc_info=True)
        except asyncio.CancelledError:
            self.closed = True
            self.dropped += len(self._pending)
            self._pending.clear()
            raise

    async def flush(self):
        """Drain accepted work for verification/graceful callers; never used on intake."""
        if self._task is not None:
            await asyncio.shield(self._task)
