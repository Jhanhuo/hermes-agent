"""Runner-owned ticks restore private pending state without new inbound traffic."""
import asyncio
import json

from hermes_constants import get_hermes_home
from tests.gateway.test_owned_group_service import rig


def test_restart_tick_restores_pending_in_runtime_scope_without_inbound(rig):
    from gateway.owned_group_service import OwnedGroupService
    path = rig.runtime / "pending.json"
    completed = asyncio.Event()
    seen = []
    async def consumer(operation, ticket=None, host=None):
        seen.append((operation, get_hermes_home(), host.alive()))
        if operation == "turn":
            record = {key: value for key, value in ticket.as_record().items()
                      if key not in {"runtime_home", "ledger_path"}}
            path.write_text(json.dumps(record), encoding="utf-8")
        if operation == "tick" and path.exists():
            restored = host.restore(json.loads(path.read_text(encoding="utf-8")))
            assert restored.message_time == 1_899_999_999.0
            assert (await restored.submit("recovered")).status == "accepted"
            path.unlink()
            completed.set()
    rig.register(consumer)
    async def exercise():
        await rig.runner._handle_message(rig.event())
        await rig.runner._owned_group_service.shutdown()
        service = OwnedGroupService(rig.runner)
        rig.runner._owned_group_service = service
        service.register_runtime("runtime", rig.runtime)
        service.start()
        await asyncio.wait_for(completed.wait(), 2)
        assert await service.shutdown(timeout=2)
    rig.loop.run_until_complete(exercise())
    assert len(rig.requests) == 1
    assert [s[0] for s in seen] == ["turn", "shutdown", "tick", "shutdown"]
    assert all(s[1] == rig.runtime for s in seen)
    assert not seen[-1][2]


def test_shutdown_cancels_managed_callback_without_claiming_thread_death(rig):
    from gateway.owned_group_service import service_for
    entered, cancelled = asyncio.Event(), asyncio.Event()
    captured = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "turn":
            captured.append(ticket)
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()
    rig.register(consumer)
    async def exercise():
        waiter = asyncio.create_task(rig.runner._handle_message(rig.event()))
        await asyncio.wait_for(entered.wait(), 2)
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        service = service_for(rig.runner)
        assert service.tasks  # caller cancellation did not abandon owned work
        assert not await service.shutdown(timeout=0)
        await asyncio.wait_for(cancelled.wait(), 2)
        assert not captured[0].authorized()
        assert (await captured[0].submit("late")).status == "rejected"
        await asyncio.gather(*list(service.tasks), return_exceptions=True)
    rig.loop.run_until_complete(exercise())
    assert rig.requests == []


def test_removed_runtime_revokes_context_and_stops_new_ticks(rig):
    from gateway.owned_group_service import service_for
    entered = asyncio.Event()
    captured = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "turn":
            captured.append((ticket, host))
        if operation == "tick":
            entered.set()
    rig.register(consumer)
    rig.loop.run_until_complete(rig.runner._handle_message(rig.event()))
    ticket, host = captured[0]
    rig.runner._served_profile_homes = {"default": rig.home}
    assert not host.alive()
    assert not ticket.authorized()
    assert host.restore(ticket.as_record()) is None


def test_ticks_and_turns_are_serial_within_profile(rig):
    from gateway.owned_group_service import service_for
    entered, release, turn_done = asyncio.Event(), asyncio.Event(), asyncio.Event()
    order = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "tick":
            order.append("tick-start")
            entered.set()
            await release.wait()
            order.append("tick-end")
        if operation == "turn":
            order.append("turn")
            turn_done.set()
    rig.register(consumer)
    async def exercise():
        service = service_for(rig.runner)
        service.register_runtime("runtime", rig.runtime)
        service.start()
        await asyncio.wait_for(entered.wait(), 2)
        waiter = asyncio.create_task(rig.runner._handle_message(rig.event()))
        await asyncio.sleep(0)
        assert not turn_done.is_set()
        release.set()
        await asyncio.wait_for(waiter, 2)
        assert await service.shutdown(timeout=2)
    rig.loop.run_until_complete(exercise())
    assert order == ["tick-start", "tick-end", "turn"]


def test_queued_callback_rechecks_acl_before_model_boundary(rig):
    from gateway.owned_group_service import service_for
    entered, release = asyncio.Event(), asyncio.Event()
    turns = []
    async def consumer(operation, ticket=None, host=None):
        if operation == "tick":
            entered.set()
            await release.wait()
        elif operation == "turn":
            turns.append(ticket)
    rig.register(consumer)
    async def exercise():
        service = service_for(rig.runner)
        service.register_runtime("runtime", rig.runtime)
        service.start()
        await asyncio.wait_for(entered.wait(), 2)
        queued = asyncio.create_task(rig.runner._handle_message(rig.event()))
        # Admission runs without async I/O in this fixture; the owned lock is its next wait.
        await asyncio.sleep(0)
        rig.runner._is_user_authorized = lambda source: False
        release.set()
        await asyncio.wait_for(queued, 2)
        await service.shutdown(timeout=2)
    rig.loop.run_until_complete(exercise())
    assert turns == []


def test_shutdown_lookup_failure_still_closes_and_drains(rig, monkeypatch):
    from gateway.owned_group_service import service_for
    async def consumer(operation, ticket=None, host=None):
        return None
    rig.register(consumer)
    service = service_for(rig.runner)
    service.register_runtime("runtime", rig.runtime)
    def broken():
        raise RuntimeError("profile lookup failed")
    monkeypatch.setattr("hermes_cli.plugins.get_plugin_manager", broken)
    assert rig.loop.run_until_complete(service.shutdown(timeout=2))
    assert service.closed and not service.tasks
