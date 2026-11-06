"""Opt-in, context-local authorization for controlled auxiliary requests.

Not a provider argument: the owning async caller binds this scope around the
existing auxiliary path. Ordinary callers have no guard and retain their behavior.
"""
from __future__ import annotations

import asyncio
from concurrent.futures import CancelledError, TimeoutError
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
import threading


class AuxiliaryRequestDenied(BaseException):
    """Stop the entire recovery chain, not a recoverable provider failure."""

    def __init__(self):
        super().__init__("Controlled auxiliary request denied")


class _RequestGuard:
    def __init__(self, recheck):
        self.recheck = recheck
        self.owner = asyncio.current_task()
        self.loop = asyncio.get_running_loop()
        self.thread = threading.get_ident()
        self.context = copy_context()
        self.active = True

    def ensure_active(self):
        if not self.active or self.owner.done() or self.owner.cancelling():
            raise AuxiliaryRequestDenied()

    async def check(self):
        if asyncio.get_running_loop() is not self.loop:
            await asyncio.wrap_future(self.submit())
            return
        self.ensure_active()
        try:
            allowed = await self.recheck()
        except Exception:
            raise AuxiliaryRequestDenied() from None
        self.ensure_active()
        if allowed is not True:
            raise AuxiliaryRequestDenied()

    def submit(self):
        self.ensure_active()
        if self.loop.is_closed() or not self.loop.is_running():
            raise AuxiliaryRequestDenied()
        pending = self.check()
        try:
            # Recheck on the owning profile/loop, not the worker's incidental context.
            return self.context.copy().run(asyncio.run_coroutine_threadsafe, pending, self.loop)
        except RuntimeError:
            pending.close()
            raise AuxiliaryRequestDenied() from None

    def check_sync(self):
        self.ensure_active()
        # Never synchronously wait on the loop that must execute the callback.
        if threading.get_ident() == self.thread:
            raise AuxiliaryRequestDenied()
        pending = self.submit()
        try:
            while True:
                try:
                    pending.result(timeout=0.05)
                    self.ensure_active()
                    return
                except TimeoutError:
                    self.ensure_active()
                    if not self.loop.is_running():
                        raise AuxiliaryRequestDenied()
        except CancelledError:
            self.ensure_active()
            raise asyncio.CancelledError() from None
        finally:
            pending.cancel()


_CURRENT: ContextVar[_RequestGuard | None] = ContextVar("auxiliary_request_guard", default=None)


@contextmanager
def auxiliary_request_scope(recheck):
    """Bind authorization to one owner's lifetime; inherited tasks cannot outlive it."""
    guard = _RequestGuard(recheck)
    token = _CURRENT.set(guard)
    try:
        yield
    finally:
        guard.active = False
        _CURRENT.reset(token)


async def check_auxiliary_request():
    guard = _CURRENT.get()
    if guard is not None:
        await guard.check()


def check_auxiliary_request_sync():
    guard = _CURRENT.get()
    if guard is not None:
        guard.check_sync()
