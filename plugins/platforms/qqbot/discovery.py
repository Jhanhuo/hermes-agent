"""Host-owned QQ membership discoveries. No config, credential or plugin lookups."""
from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import time
import threading
import weakref
import uuid
from contextlib import closing, contextmanager
from pathlib import Path


_TTL = 30 * 24 * 60 * 60
_CAP_PER_BOT = 1024  # includes ignored and removed rows
_COLUMNS = "group_openid, op_member_openid, revision"


def _path(home: Path) -> Path:
    path = home / "qq-discovery" / "membership.sqlite3"
    if path.parent.is_symlink() or path.is_symlink():
        raise OSError("Refusing a linked QQ discovery store")
    return path


def _exists(path: Path) -> bool:
    # A permission/I/O error is not an empty store (Path.exists can mask it).
    try:
        path.stat()
    except FileNotFoundError:
        return False
    return True


@contextmanager
def _writer(home: Path, *, timeout: float = 5):
    path = _path(home)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with closing(sqlite3.connect(path, timeout=timeout)) as db, db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE IF NOT EXISTS membership (
            bot TEXT NOT NULL, group_openid TEXT NOT NULL, op_member_openid TEXT,
            revision TEXT NOT NULL, state TEXT NOT NULL, event_time INTEGER NOT NULL,
            PRIMARY KEY (bot, group_openid))""")
        db.execute("CREATE TABLE IF NOT EXISTS receivers (bot TEXT PRIMARY KEY NOT NULL, floor INTEGER NOT NULL DEFAULT 0)")
        db.execute("DELETE FROM membership WHERE event_time <= ?", (int(time.time()) - _TTL,))
        yield db


def _read(home, query, args):
    path = _path(home)
    if not _exists(path):
        return []
    with closing(sqlite3.connect(f"{path.absolute().as_uri()}?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(query, args)]


def bots(home: Path) -> list[str]:
    return [row["bot"] for row in _read(home, "SELECT bot FROM receivers ORDER BY bot", ())]


def _fence(home: Path, bot: str) -> Path:
    # Independent of the SQLite file: a DB lock, rollback, or failed commit
    # must not make an old revision actionable in another dashboard process.
    name = hashlib.sha256(bot.encode()).hexdigest()
    return home / f".qq-discovery-uncertain-{name}"


def _fenced(home: Path, bot: str) -> bool:
    try:
        _fence(home, bot).lstat()  # even a dangling link is not a clean fence
    except FileNotFoundError:
        return False
    return True


def register(home: Path, bot: str) -> None:
    if _valid_id(bot):
        with _writer(home, timeout=0) as db:  # READY must not queue behind a dashboard write.
            db.execute("INSERT OR IGNORE INTO receivers(bot) VALUES (?)", (bot,))


def maintain(home: Path) -> None:
    """Commit the profile-wide TTL sweep without registering a bot or creating a store."""
    if _exists(_path(home)):
        with _writer(home):
            pass  # _writer prunes all membership states before yielding.


def pending(home: Path, bot: str) -> list[dict]:
    rows = _read(home, f"SELECT {_COLUMNS} FROM membership WHERE bot=? AND state='pending' "
                 "AND event_time > ? ORDER BY event_time DESC, group_openid", (bot, int(time.time()) - _TTL))
    return [] if _fenced(home, bot) else rows


class StaleCandidate(LookupError):
    """The requested pending revision no longer exists in this receiver scope."""


class ReceiverUnverified(StaleCandidate):
    """A persisted revision is not evidence of current membership."""


class _Scope:
    def __init__(self):
        self.lock = threading.Lock()
        self.current: weakref.ReferenceType[ReceiverEpoch] | None = None  # weak reference; the reader owns the epoch


_scopes = weakref.WeakValueDictionary()
_registry_lock = threading.Lock()


def _scope(home, bot):
    # Both callers supply the resolved, trusted receiver home. Include PID so a
    # fork cannot inherit confirmation authority from its parent's live reader.
    key = (os.getpid(), home, bot)
    with _registry_lock:
        scope = _scopes.get(key)
        if scope is None:
            scope = _scopes[key] = _Scope()
        return scope


class ReceiverEpoch:
    def __init__(self, adapter):
        self.home, self.bot = adapter._discovery_home, adapter._app_id
        self.adapter = weakref.ref(adapter)
        self.socket = adapter._ws
        self.scope = _scope(self.home, self.bot)
        self.revisions = {}
        # Ignore delayed source frames predating this reader. A resumed stream
        # can replay frames across a gap (including accepted clock skew).
        self.floor = int(time.time()) + (300 if adapter._session_id else 0)
        with self.scope.lock:
            previous = self.scope.current and self.scope.current()
            if previous is not None:
                previous.revisions.clear()
            self.scope.current = weakref.ref(self)

    def close(self):
        with self.scope.lock:
            self.revisions.clear()
            if self.scope.current and self.scope.current() is self:
                self.scope.current = None

    def _gap(self):
        self.revisions.clear()
        # A missed/unorderable remove could have any accepted source timestamp.
        self.floor = max(self.floor, int(time.time()) + 300)

    def gap(self):
        with self.scope.lock:
            self._gap()

    def observe(self, kind, data):
        with self.scope.lock:
            if not self.scope.current or self.scope.current() is not self:
                return  # cancelled/offloaded work from a retired reader
            try:
                change = record(self.home, self.bot, kind, data, epoch=self)
            except BaseException:
                self._gap()
                raise
            if change:
                group, revision, stamp, evicted = change
                for key in evicted:
                    self.revisions.pop(key, None)
                self.revisions.pop(group, None)
                if revision and stamp > self.floor and group not in evicted:
                    if len(self.revisions) >= _CAP_PER_BOT:
                        self.revisions.pop(next(iter(self.revisions)))
                    self.revisions[group] = revision


def _witness(scope, group, revision):
    epoch = scope.current and scope.current()
    adapter = epoch and epoch.adapter()
    if (epoch and adapter and adapter.is_connected and adapter._ws is epoch.socket
            and epoch.revisions.get(group) == revision):
        return epoch, adapter
    return None


def attested(home, bot, group, revision):
    scope = _scope(home, bot)
    with scope.lock:
        return _witness(scope, group, revision) is not None


_MATCH = "bot=? AND group_openid=? AND revision=? AND state='pending'"


@contextmanager
def guarded_candidate(home: Path, bot: str, group: str, revision: str, *, consume=False):
    """Serialize a short caller-owned config write with receiver changes and ignore."""
    scope = _scope(home, bot)
    with scope.lock:
        with _guarded_row(home, bot, group, revision, consume) as row:
            witness = _witness(scope, group, revision)
            if witness is None:
                raise ReceiverUnverified
            yield row
            if consume:
                # Apply returned; even a subsequent SQLite commit failure must
                # not leave this approval reusable in the receiving process.
                witness[0].revisions.pop(group, None)


@contextmanager
def _guarded_row(home, bot, group, revision, consume):
    if not _exists(_path(home)):
        raise StaleCandidate
    with _writer(home) as db:
        row = db.execute(f"SELECT {_COLUMNS} FROM membership WHERE {_MATCH}",
                         (bot, group, revision)).fetchone()
        if row is None or _fenced(home, bot):
            raise StaleCandidate
        yield dict(row)
        if consume:
            db.execute(f"UPDATE membership SET state='confirmed', op_member_openid=NULL WHERE {_MATCH}",
                       (bot, group, revision))


def ignore(home: Path, bot: str, group: str, revision: str) -> bool:
    if not _exists(_path(home)):
        return False
    with _writer(home) as db:
        if _fenced(home, bot):
            return False
        return db.execute(f"UPDATE membership SET state='ignored', op_member_openid=NULL WHERE {_MATCH}",
                          (bot, group, revision)).rowcount == 1


_ID = re.compile(r"[A-Za-z0-9_-]{1,256}")


def _valid_id(value) -> bool:
    return isinstance(value, str) and _ID.fullmatch(value) is not None


def record(home: Path, bot: str, kind: str, data: object, *, epoch=None):
    if not _valid_id(bot) or not isinstance(data, dict):
        if epoch is not None:
            epoch._gap()
        return
    group = data.get("group_openid")
    stamp = data.get("timestamp")
    if not _valid_id(group):
        if epoch is not None:
            epoch._gap()
        return
    now = int(time.time())
    stamp = (int(stamp) if isinstance(stamp, (str, int)) and not isinstance(stamp, bool)
             and re.fullmatch(r"[0-9]{1,12}", str(stamp)) else None)
    if stamp is None or stamp > now + 300:
        if kind != "GROUP_DEL_ROBOT":
            if epoch is not None:
                epoch._gap()
            return
        # No usable source time: invalidate even an existing slightly-future add.
        stamp = now + 300
    if stamp <= now - _TTL:
        return
    member = data.get("op_member_openid")
    removed = kind == "GROUP_DEL_ROBOT"
    member = member if _valid_id(member) and not removed else None
    # One live receiver owns this app (platform lock). Arm *before* SQLite:
    # even a process exit between BEGIN and COMMIT leaves a visible fence.
    fence = _fence(home, bot)
    try:
        home.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.close(os.open(fence, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600))
        interrupted = False
    except FileExistsError:
        interrupted = True
    except OSError:
        if epoch is not None:
            epoch._gap()  # before attempting the fallible SQLite fallback
        # The profile directory itself may be full/read-only while its existing
        # SQLite subdirectory remains writable. Invalidate there before returning.
        with _writer(home) as db:
            _discard_uncertain(db, bot)
        raise
    if interrupted and epoch is not None:
        epoch._gap()
    change = _record_fenced(home, bot, group, member, removed, stamp, interrupted)
    fence.unlink()  # publish only after commit
    return change


def _discard_uncertain(db, bot):
    # No backfill guarantee, and an unorderable delete can cover a future add.
    db.execute("DELETE FROM membership WHERE bot=?", (bot,))
    db.execute("""INSERT INTO receivers(bot, floor) VALUES (?, ?) ON CONFLICT(bot)
               DO UPDATE SET floor=max(floor, excluded.floor)""", (bot, int(time.time()) + 300))


def _record_fenced(home, bot, group, member, removed, stamp, interrupted):
    with _writer(home) as db:
        db.execute("INSERT OR IGNORE INTO receivers(bot) VALUES (?)", (bot,))
        if interrupted:
            _discard_uncertain(db, bot)
        floor, = db.execute("SELECT floor FROM receivers WHERE bot=?", (bot,)).fetchone()
        if stamp <= floor and not db.execute(
                "SELECT 1 FROM membership WHERE bot=? AND group_openid=?", (bot, group)).fetchone():
            return  # An evicted tombstone may have covered this older frame.
        # QQ timestamps are seconds. On a tie removal wins; resume duplicates
        # must not rotate a revision or resurrect an ignored candidate.
        revision = uuid.uuid4().hex
        changed = db.execute("""INSERT INTO membership VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(bot, group_openid) DO UPDATE SET
            op_member_openid=excluded.op_member_openid, revision=excluded.revision,
            state=excluded.state, event_time=excluded.event_time
            WHERE excluded.event_time > membership.event_time OR
            (excluded.event_time = membership.event_time AND excluded.state='removed'
             AND membership.state!='removed')""",
            (bot, group, member, revision, "removed" if removed else "pending", stamp)).rowcount
        evicted = db.execute("""SELECT group_openid, event_time FROM membership WHERE bot=?
            ORDER BY event_time DESC, group_openid LIMIT -1 OFFSET ?""", (bot, _CAP_PER_BOT)).fetchall()
        if evicted:
            db.execute("UPDATE receivers SET floor=max(floor, ?) WHERE bot=?", (evicted[0][1], bot))
            db.executemany("DELETE FROM membership WHERE bot=? AND group_openid=?",
                           ((bot, row[0]) for row in evicted))
        if changed:
            return group, None if removed else revision, stamp, {row[0] for row in evicted}
