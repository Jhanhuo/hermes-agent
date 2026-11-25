"""Offline lifecycle frames through the QQ receiver, without a social plugin."""
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from hermes_constants import reset_hermes_home_override, set_hermes_home_override
from plugins.platforms.qqbot import discovery
from plugins.platforms.qqbot.adapter import QQAdapter


def receiver(home, bot="app-a"):
    token = set_hermes_home_override(home)
    try:
        adapter = QQAdapter(PlatformConfig(enabled=True, extra={
            "app_id": bot, "client_secret": "secret-not-for-store", "group_policy": "disabled",
        }))
        adapter._running = True
        adapter._ws = SimpleNamespace(closed=False)
        adapter._discovery_epoch = discovery.ReceiverEpoch(adapter)
        return adapter
    finally:
        reset_hermes_home_override(token)


def frame(adapter, kind="GROUP_ADD_ROBOT", **fields):
    adapter._dispatch_payload({"op": 0, "t": kind, "d": {
        "group_openid": "000group", "op_member_openid": "000member", "timestamp": int(time.time()),
        "content": "private-not-for-store", **fields,
    }})


def test_receiver_owns_candidate_not_dispatch_context(tmp_path, monkeypatch):

    home = tmp_path / "receiver"
    home.mkdir()
    other = tmp_path / "primary"
    monkeypatch.setenv("HERMES_HOME", str(other))
    adapter = receiver(home)
    assert discovery.pending(home, "app-a") == []
    assert not list(home.iterdir())
    frame(adapter, profile=str(other), app_id="app-b")
    [row] = discovery.pending(home, "app-a")
    assert row["group_openid"] == "000group" and row["op_member_openid"] == "000member"
    assert isinstance(row["revision"], str) and row["revision"]
    assert not discovery.pending(home, "app-b") and not discovery.pending(other, "app-a")
    assert not adapter._last_msg_id and not adapter._group_allow_from
    for file in home.rglob("*"):
        if file.is_file():
            data = file.read_bytes()
            assert b"private-not-for-store" not in data and b"secret-not-for-store" not in data


def test_known_receivers_from_ready_and_membership(tmp_path, monkeypatch):

    # Simulate env-only configuration; no config file or secret needed by the reader.
    monkeypatch.setenv("QQ_APP_ID", "env-only")
    a = receiver(tmp_path, "")
    for data in (None, {}, {"session_id": "", "user": {"id": "qq-user"}},
                 {"session_id": "session", "user": {}}):
        a._dispatch_payload({"op": 0, "t": "READY", "d": data})
    assert discovery.bots(tmp_path) == []
    for _ in range(2):
        a._dispatch_payload({"op": 0, "t": "READY", "d": {
            "session_id": "session", "user": {"id": "qq-user"}}})
    monkeypatch.delenv("QQ_APP_ID")
    assert discovery.bots(tmp_path) == ["env-only"]
    assert discovery.pending(tmp_path, "env-only") == []
    frame(receiver(tmp_path, "b"), "GROUP_DEL_ROBOT")
    frame(receiver(tmp_path, "a"))
    assert discovery.bots(tmp_path) == ["a", "b", "env-only"]
    assert discovery.bots(tmp_path / "elsewhere") == []


def test_replay_remove_rejoin_and_missing_inviter(tmp_path):

    a = receiver(tmp_path)
    start = int(time.time()) - 30
    frame(a, timestamp=start)
    [row] = discovery.pending(tmp_path, "app-a")
    a = receiver(tmp_path)
    frame(a, timestamp=str(start))
    assert discovery.pending(tmp_path, "app-a") == [row]
    frame(a, "GROUP_DEL_ROBOT", timestamp=start + 2)
    for ts in (start, start + 1, start + 2):
        frame(a, timestamp=ts)
    assert not discovery.pending(tmp_path, "app-a")
    frame(a, timestamp=start + 3, op_member_openid=None, author={"id": "not-inviter"})
    frame(a, "GROUP_DEL_ROBOT", timestamp=start + 1)
    [new] = discovery.pending(tmp_path, "app-a")
    assert new["op_member_openid"] is None and new["revision"] != row["revision"]


def test_malformed_events_do_not_register_or_publish_scopes(tmp_path):

    a = receiver(tmp_path)
    for bad in (None, [], {}, {"group_openid": "*", "timestamp": int(time.time())},
                {"group_openid": "x", "timestamp": True}, {"group_openid": "x"},
                {"group_openid": "   "}, {"group_openid": 1}):
        a._dispatch_payload({"op": 0, "t": "GROUP_ADD_ROBOT", "d": bad})
    assert discovery.bots(tmp_path) == [] and discovery.pending(tmp_path, "app-a") == []
    frame(a, op_member_openid="*")
    assert discovery.pending(tmp_path, "app-a")[0]["op_member_openid"] is None


def test_revision_guard_serializes_writers_and_rejects_stale(tmp_path):

    a = receiver(tmp_path)
    ts = int(time.time()) + 1
    frame(a, timestamp=ts)
    [row] = discovery.pending(tmp_path, "app-a")
    key = (tmp_path, "app-a", "000group", row["revision"])
    with discovery.guarded_candidate(*key) as guarded:
        assert guarded == row
        # A separate connection, as used by an independent receiver process,
        # cannot commit either a removal or a revision change in this section.
        db_path, = tmp_path.rglob("*.sqlite3")
        with sqlite3.connect(db_path, timeout=0) as competing:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                competing.execute("BEGIN IMMEDIATE")
    assert not discovery.ignore(tmp_path, "another", *key[2:])
    assert discovery.ignore(*key) and not discovery.ignore(*key)
    frame(a, timestamp=ts)
    assert not discovery.pending(tmp_path, "app-a")
    frame(a, timestamp=ts + 1)
    assert not discovery.ignore(*key)
    with pytest.raises(discovery.StaleCandidate), discovery.guarded_candidate(*key):
        pytest.fail("entered with a replaced revision")
    [new] = discovery.pending(tmp_path, "app-a")
    frame(a, "GROUP_DEL_ROBOT", timestamp=ts + 2)
    with pytest.raises(discovery.StaleCandidate), discovery.guarded_candidate(*key[:3], new["revision"]):
        pytest.fail("entered with a removed candidate")


def test_only_new_winning_revisions_in_this_reader_epoch_count(tmp_path, monkeypatch):
    monkeypatch.setattr(discovery, '_CAP_PER_BOT', 2)
    first = receiver(tmp_path)
    stamp = int(time.time()) + 10
    frame(first, timestamp=stamp)
    [row] = discovery.pending(tmp_path, 'app-a')
    key = (tmp_path, 'app-a', row['group_openid'], row['revision'])
    assert discovery.attested(*key)
    current = receiver(tmp_path)
    for group, ts in [('unrelated', stamp), ('000group', stamp - 1), ('000group', stamp)]:
        frame(current, group_openid=group, timestamp=ts)
        assert not discovery.attested(*key)
    frame(first, timestamp=stamp + 1)  # delayed worker from the retired reader
    assert row in discovery.pending(tmp_path, 'app-a')
    epoch = current._discovery_epoch
    assert epoch is not None
    for i in range(6):
        frame(current, group_openid=f'g{i}', timestamp=stamp + i + 1)
        pending = discovery.pending(tmp_path, 'app-a')
        assert epoch.revisions[f'g{i}'] == pending[0]['revision']
        assert len(epoch.revisions) <= 2
        assert len(pending) <= 2
    final = discovery.pending(tmp_path, 'app-a')[0]
    last = (tmp_path, 'app-a', final['group_openid'], final['revision'])
    with discovery.guarded_candidate(*last):
        # The config caller retains the very lock used for epoch retirement,
        # independent of SQLite's transaction lock.
        assert not epoch.scope.lock.acquire(blocking=False)
    current._running = False
    assert not discovery.attested(*last)


def test_expiry_and_capacity_cannot_revive_stale_candidates(tmp_path, monkeypatch):

    monkeypatch.setattr(discovery, "_CAP_PER_BOT", 2)
    monkeypatch.setattr(discovery, "_TTL", 60)
    now = int(time.time())
    a = receiver(tmp_path)
    other = receiver(tmp_path, "other")
    frame(other, timestamp=now, op_member_openid="other-member")
    frame(a, "GROUP_DEL_ROBOT", group_openid="old", timestamp=now - 5)
    frame(a, group_openid="x", timestamp=now - 4)
    frame(a, group_openid="y", timestamp=now - 3)
    frame(a, group_openid="old", timestamp=now - 6)
    rows = discovery.pending(tmp_path, "app-a")
    assert [r["group_openid"] for r in rows] == ["y", "x"]
    # Deleting the tombstone under capacity pressure must not resurrect the group.
    assert discovery.pending(tmp_path, "other")[0]["op_member_openid"] == "other-member"
    monkeypatch.setattr(discovery.time, "time", lambda: now + 61)
    key = (tmp_path, "app-a", "y", rows[0]["revision"])
    assert discovery.pending(tmp_path, "app-a") == [] and not discovery.ignore(*key)
    with pytest.raises(discovery.StaleCandidate), discovery.guarded_candidate(*key):
        pytest.fail("expired candidate")
    frame(a, timestamp=now - 4)
    assert discovery.pending(tmp_path, "app-a") == []
    assert discovery.bots(tmp_path) == ["app-a", "other"]


@pytest.mark.asyncio
async def test_idle_reader_physically_prunes_at_start_and_periodically(tmp_path, monkeypatch):
    from plugins.platforms.qqbot import adapter as module

    now = int(time.time())
    clock = SimpleNamespace(time=lambda: now)
    monkeypatch.setattr(discovery, 'time', clock)
    monkeypatch.setattr(module, '_DISCOVERY_MAINTENANCE_INTERVAL', 0.02, raising=False)
    a = receiver(tmp_path)
    frame(a, group_openid='offline', timestamp=now - 1)
    frame(a, 'GROUP_DEL_ROBOT', group_openid='idle', timestamp=now)
    frame(a, group_openid='fresh', timestamp=now + 20)
    path, = tmp_path.rglob('*.sqlite3')

    def physical_rows():
        with sqlite3.connect(path) as db:
            return sorted(row[0] for row in db.execute('SELECT group_openid FROM membership'))

    async def until_rows(expected):
        async with asyncio.timeout(2):
            while physical_rows() != expected:
                await asyncio.sleep(0.005)

    now += discovery._TTL - 1
    assert physical_rows() == ['fresh', 'idle', 'offline']  # nothing runs offline
    a._running = True
    a._ws = SimpleNamespace(closed=False, receive=asyncio.Event().wait)
    reader = asyncio.create_task(a._read_events())  # no READY, ACK, or group frames
    try:
        await until_rows(['fresh', 'idle'])
        now += 1
        await until_rows(['fresh'])  # includes the tombstone, exactly at the TTL cutoff
        assert not reader.done()
    finally:
        reader.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reader


def test_unorderable_removal_revokes_rather_than_leaving_inviter_actionable(tmp_path):

    a = receiver(tmp_path)
    ts = int(time.time())
    frame(a, timestamp=ts)
    frame(a, "GROUP_DEL_ROBOT", timestamp=None)
    frame(a, timestamp=ts)
    assert discovery.pending(tmp_path, "app-a") == []


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['filesystem', 'sqlite'])
async def test_ready_store_failure_does_not_reconnect_or_drop_message(tmp_path, monkeypatch, failure):
    from plugins.platforms.qqbot.adapter import aiohttp

    if failure == 'filesystem':
        (tmp_path / 'qq-discovery').touch()
    else:
        (tmp_path / 'qq-discovery').mkdir()
        (tmp_path / 'qq-discovery' / 'membership.sqlite3').write_bytes(b'not SQLite')
    a = receiver(tmp_path)
    a._running = True
    body = {'id': 'ordinary', 'content': 'hello'}
    wire = iter([{'op': 0, 's': 1, 't': 'READY', 'd': {'session_id': 'session', 'user': {'id': 'user'}}},
                 {'op': 0, 's': 2, 't': 'C2C_MESSAGE_CREATE', 'd': body}])

    async def receive():
        payload = next(wire)
        if payload['s'] == 2:
            a._running = False
        return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(payload))

    a._ws = SimpleNamespace(closed=False, receive=receive)
    reconnect = AsyncMock(side_effect=AssertionError('unexpected reconnect'))
    message = AsyncMock()
    monkeypatch.setattr(a, '_reconnect', reconnect)
    monkeypatch.setattr(a, '_on_message', message)
    await a._listen_loop()
    await asyncio.sleep(0)  # run the queued ordinary handler
    assert (a._session_id, a._bot_user_id, a._last_seq) == ('session', 'user', 2)
    message.assert_awaited_once_with('C2C_MESSAGE_CREATE', body)
    reconnect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['GROUP_ADD_ROBOT', 'GROUP_DEL_ROBOT'])
async def test_failed_membership_write_invalidates_saved_preview(tmp_path, kind):
    from plugins.platforms.qqbot.adapter import aiohttp

    a = receiver(tmp_path)
    now = int(time.time())
    frame(a, timestamp=now - 2)
    [old] = discovery.pending(tmp_path, 'app-a')
    key = (tmp_path, 'app-a', '000group', old['revision'])
    path, = tmp_path.rglob('*.sqlite3')
    with sqlite3.connect(path) as db:
        db.execute("CREATE TRIGGER fault BEFORE UPDATE ON membership BEGIN SELECT RAISE(ABORT, 'fault'); END")
    a._running = True
    a._last_seq = 5
    wire = {'op': 0, 's': 6, 't': kind, 'd': {'group_openid': '000group', 'timestamp': now - 1}}
    body = {'id': 'ordinary', 'content': 'hello'}
    frames = iter([wire, {'op': 0, 's': 7, 't': 'C2C_MESSAGE_CREATE', 'd': body}])
    async def receive():
        payload = next(frames)
        if payload['s'] == 7:
            a._running = False
        return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(payload))
    a._ws = SimpleNamespace(closed=False, receive=receive)
    a._on_message = AsyncMock()
    a._reconnect = AsyncMock(side_effect=AssertionError('discovery triggered reconnect'))
    await a._listen_loop()
    await asyncio.sleep(0)
    a._on_message.assert_awaited_once_with('C2C_MESSAGE_CREATE', body)
    a._reconnect.assert_not_awaited()
    assert a._last_seq == 7
    with sqlite3.connect(path) as db:
        db.execute('DROP TRIGGER fault')  # storage recovers; old revision still must not be usable
    assert not discovery.pending(tmp_path, 'app-a')
    with pytest.raises(discovery.StaleCandidate), discovery.guarded_candidate(*key, consume=True):
        pytest.fail('failed membership update left prior revision actionable')
    assert not discovery.ignore(*key)
    frame(receiver(tmp_path), timestamp=now - 2)  # delayed add, not proof of membership
    assert not discovery.pending(tmp_path, 'app-a')


def fresh_process_inbox(home):
    script = """
import json, sys
from pathlib import Path
from plugins.platforms.qqbot import discovery as d
home = Path(sys.argv[1])
rows = d.pending(home, 'app-a')
for row in rows:
    try:
        with d.guarded_candidate(home, 'app-a', row['group_openid'], row['revision'], consume=True):
            raise AssertionError('fresh process authorized config')
    except d.ReceiverUnverified:
        pass
print(json.dumps([d.bots(home), rows]))
"""
    env = {'HOME': str(home), 'USERPROFILE': str(home), 'HERMES_HOME': str(home), 'PYTHONUTF8': '1'}
    # Native Windows needs SystemRoot to initialize asyncio/Winsock; keep credentials absent.
    if 'SYSTEMROOT' in os.environ:
        env['SYSTEMROOT'] = os.environ['SYSTEMROOT']
    result = subprocess.run([sys.executable, '-c', script, str(home)],
                            capture_output=True, text=True, check=True, timeout=30,
                            env=env)
    return json.loads(result.stdout)


def test_simultaneous_fence_and_db_failure_then_recovery(tmp_path, monkeypatch, caplog):
    a = receiver(tmp_path)
    now = int(time.time()) + 5
    frame(a, timestamp=now - 2)
    [row] = discovery.pending(tmp_path, 'app-a')
    key = (tmp_path, 'app-a', row['group_openid'], row['revision'])
    with discovery.guarded_candidate(*key):
        pass  # The exact same revision was actionable before the lost removal.
    db_path, = tmp_path.rglob('*.sqlite3')
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TRIGGER fault BEFORE DELETE ON membership BEGIN SELECT RAISE(ABORT,'fault'); END")
    with monkeypatch.context() as m:
        # Real open(2) ENOENT and a real SQLite abort of the fallback transaction.
        m.setattr(discovery, '_fence', lambda *_: tmp_path / 'absent' / 'fence')
        frame(a, 'GROUP_DEL_ROBOT', timestamp=now)
    assert 'Discovery unavailable (IntegrityError)' in caplog.text  # fallback really aborted
    with sqlite3.connect(db_path) as db:
        db.execute('DROP TRIGGER fault')
    assert discovery.pending(tmp_path, 'app-a') == [row]
    assert fresh_process_inbox(tmp_path) == [['app-a'], [row]]

    def denied():
        with pytest.raises(discovery.ReceiverUnverified), discovery.guarded_candidate(*key, consume=True):
            pytest.fail('recovered disk row authorized config after a lost removal')

    denied()
    frame(a, group_openid='other', timestamp=now)
    denied()
    frame(a, timestamp=now - 3)
    denied()
    replacement = receiver(tmp_path)
    frame(replacement, timestamp=now - 2)
    denied()


def test_unwritable_fence_does_not_leave_a_confirmable_revision(tmp_path, monkeypatch):
    a = receiver(tmp_path)
    frame(a, timestamp=int(time.time()) + 1)
    [row] = discovery.pending(tmp_path, 'app-a')
    assert discovery.attested(tmp_path, 'app-a', '000group', row['revision'])
    with monkeypatch.context() as patch:
        def full(*args):
            raise OSError('cannot allocate fence')
        patch.setattr(discovery.os, 'open', full)
        frame(a, 'GROUP_DEL_ROBOT')
    with pytest.raises(discovery.StaleCandidate), discovery.guarded_candidate(
            tmp_path, 'app-a', '000group', row['revision']):
        pytest.fail('old revision survived fence failure')


@pytest.mark.asyncio
async def test_reader_awaits_durable_lifecycle_off_loop_before_advancing_resume(tmp_path, monkeypatch):
    from plugins.platforms.qqbot.adapter import aiohttp

    a = receiver(tmp_path)
    a._running = True
    a._last_seq = 5
    wire = {"op": 0, "s": 6, "t": "GROUP_DEL_ROBOT",
            "d": {"group_openid": "group", "timestamp": int(time.time())}}
    async def receive():
        a._running = False
        return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(wire))
    a._ws = SimpleNamespace(closed=False, receive=receive)
    threads = []

    def unavailable(*args, **kwargs):
        threads.append(threading.get_ident())
        raise sqlite3.OperationalError("store unavailable")

    monkeypatch.setattr(discovery, "record", unavailable)
    await a._read_events()
    assert a._last_seq == 6
    assert threads and threads[0] != threading.get_ident()


@pytest.mark.parametrize("link_file", [False, True])
def test_store_does_not_follow_another_profiles_discovery_path(tmp_path, link_file):

    owner, other = tmp_path / "owner", tmp_path / "other"
    a = receiver(owner)
    frame(a)
    db_path, = owner.rglob("*.sqlite3")
    other.mkdir()
    if link_file:
        target = other / db_path.relative_to(owner)
        target.parent.mkdir()
        target.symlink_to(db_path)
    else:
        (other / db_path.parent.name).symlink_to(db_path.parent, target_is_directory=True)
    with pytest.raises(OSError):
        discovery.bots(other)
    frame(receiver(other), "GROUP_DEL_ROBOT")
    assert len(discovery.pending(owner, "app-a")) == 1


@pytest.mark.asyncio
async def test_silent_wire_add_is_readable_in_fresh_process_without_credentials(tmp_path, monkeypatch):
    from plugins.platforms.qqbot.adapter import aiohttp

    a = receiver(tmp_path)
    a._running = True
    wire = {"op": 0, "s": 7, "t": "GROUP_ADD_ROBOT", "d": {
        "group_openid": "group-002", "op_member_openid": "member-008", "timestamp": int(time.time())}}

    async def receive():
        a._running = False
        return SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(wire))

    def unexpected(*args):
        pytest.fail("discovery reached a chat or delivery handler")

    for name in ("_on_message", "_on_interaction", "send", "handle_message"):
        monkeypatch.setattr(a, name, unexpected)
    a._ws = SimpleNamespace(closed=False, receive=receive)
    await a._read_events()
    assert a._last_seq == 7
    known, [row] = fresh_process_inbox(tmp_path)
    assert known == ["app-a"] and row == discovery.pending(tmp_path, "app-a")[0]
    assert (row["group_openid"], row["op_member_openid"]) == ("group-002", "member-008")
