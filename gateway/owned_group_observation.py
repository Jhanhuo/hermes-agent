"""Bounded best-effort owned observation, independent of model-turn locks.

Capacity includes the active write. Overflow drops newest, shutdown drops items
not yet started. Cancellation cannot stop a plugin's in-flight database thread.
"""
import asyncio
import logging

logger = logging.getLogger(__name__)


async def dispatch_platform_event(runner, event, source):
    """Authorize observer intake, then dispatch within the routed runtime scope."""
    from gateway.run_shutdown import _log_suppressed
    # Observer failures must never break the adapter's update loop.
    with _log_suppressed(logging.DEBUG, "gateway_platform_event hook dispatch failed", exc_info=True):
        from hermes_cli.lifecycle import has_hook, invoke_hook
        if (getattr(source, "profile_route_rejected", False) is True
                or not runner._is_user_authorized_for_source(source)):
            return
        # Authorization follows the receiver; subscriptions belong to the runtime.
        from gateway.run_inbound_consumer import _routed_scope
        async with _routed_scope(runner, source):
            from gateway.owned_group_service import observe
            await observe(runner, event, source)
            if has_hook("gateway_platform_event"):
                if event.get("platform") == "qqbot" and event.get("event_type") == "group_message":
                    if getattr(runner, "_draining", False):
                        return
                    from gateway.passive_observer_queue import ObserverQueue
                    queue = getattr(runner, "_qq_observer_queue", None)
                    if queue is None:
                        queue = runner._qq_observer_queue = ObserverQueue(
                            tasks=getattr(runner, "_background_tasks", None))
                    queue.submit(lambda: invoke_hook("gateway_platform_event", **event))
                else:
                    invoke_hook("gateway_platform_event", **event)


class ObservationInbox:
    def __init__(self, owner, capacity=32):
        self.owner = owner
        self.capacity = capacity
        self.queues = {}
        self.counts = {}
        self.workers = {}
        self.dropped = 0

    @property
    def pending(self):
        return sum(self.counts.values())

    def submit(self, host, ticket):
        if self.owner.closed or not host.alive():
            return False
        key = host.runtime_id
        if self.counts.get(key, 0) >= self.capacity:
            self.dropped += 1
            return False
        self.owner.register_runtime(key, host.runtime_home)
        queue = self.queues.setdefault(key, asyncio.Queue(maxsize=self.capacity))
        queue.put_nowait((host, ticket))
        self.counts[key] = self.counts.get(key, 0) + 1
        worker = self.workers.get(key)
        if worker is None or worker.done():
            self.workers[key] = self.owner._track(self._drain(key))
        return True

    async def _drain(self, key):
        from gateway.owned_group_service import _scope
        queue = self.queues[key]
        while not queue.empty():
            host, ticket = queue.get_nowait()
            try:
                if host.alive() and ticket.authorized():
                    # Intentionally no turn/tick lock: this callback only persists.
                    with _scope(host.runtime_home):
                        await host._callback("observe", ticket=ticket, host=host)
            except Exception:
                logger.warning("Owned observation persistence failed; item dropped", exc_info=True)
            finally:
                self.counts[key] -= 1
                queue.task_done()

    async def flush(self):
        await asyncio.gather(*(queue.join() for queue in self.queues.values()))

    def close(self):
        for key, queue in self.queues.items():
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()
                self.counts[key] -= 1
                self.dropped += 1
