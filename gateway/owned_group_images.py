"""Process-local capabilities for owned QQ top-level image attachments.

Only this registry holds the untrusted attachment locator; tickets persist opaque
handles, never a path or URL. A durable row cannot recreate lost capabilities.
"""
from __future__ import annotations

import asyncio
import io
import secrets
from dataclasses import dataclass, field
from urllib.parse import urlsplit

MAX_IMAGE_BYTES = 2 * 1024 * 1024
MAX_MESSAGE_BYTES = 4 * 1024 * 1024
MAX_CACHE_BYTES = 32 * 1024 * 1024
MAX_MESSAGES = 200
CDN_HOSTS = frozenset({"multimedia.nt.qq.com.cn", "gchat.qpic.cn", "c2cpicdw.qpic.cn"})


def _valid_url(url):
    if (type(url) is not str or not 1 <= len(url) <= 4096
            or any(ch.isspace() or ord(ch) < 32 or ch == "\\" for ch in url)):
        return False
    try:
        parts = urlsplit(url)
        return (parts.scheme == "https" and parts.hostname in CDN_HOSTS
                and parts.netloc.lower() in {parts.hostname, parts.hostname + ":443"}
                and not parts.fragment)
    except ValueError:
        return False


def _attachments(raw):
    if not isinstance(raw, list) or len(raw) > 32:
        return ()
    images, total = [], 0
    for item in raw:
        if not isinstance(item, dict) or not str(item.get("content_type", "")).startswith("image/"):
            continue
        mime, url = item.get("content_type"), item.get("url")
        size = item.get("size", 0)
        if (mime not in {"image/png", "image/jpeg"} or not _valid_url(url)
                or type(size) is not int or not 0 <= size <= MAX_IMAGE_BYTES):
            return ()
        total += size
        images.append((url, mime))
    return tuple(images) if len(images) <= 4 and total <= MAX_MESSAGE_BYTES else ()


@dataclass
class _Entry:
    adapter: object
    record: dict
    attachments: tuple
    handles: tuple[str, ...]
    content: str
    texts: set[str] = field(default_factory=set)
    cache: tuple | None = None
    released: bool = False


class ImageRegistry:
    def __init__(self):
        self._entries = {}
        self._load_lock = asyncio.Lock()
        self.closed = False

    def prune(self):
        from gateway.owned_group_service import _fresh
        for key, entry in list(self._entries.items()):
            if not _fresh(entry.record["message_time"]):
                del self._entries[key]

    def close(self):
        self.closed = True
        self._entries.clear()

    @staticmethod
    def _key(record):
        return tuple(record[key] for key in ("bot_id", "transport_profile", "group_id", "message_id"))

    def issue(self, adapter, record, attachments, content):
        self.prune()
        if self.closed:
            return ()
        key = self._key(record)
        entry = self._entries.get(key)
        if entry is not None:
            return (entry.handles if not entry.released and entry.adapter is adapter
                    and entry.record == record and entry.attachments == attachments
                    and entry.content == content else ())
        if len(self._entries) >= MAX_MESSAGES:
            return ()
        handles = tuple(secrets.token_hex(32) for _ in attachments)
        self._entries[key] = _Entry(adapter, record, attachments, handles, content)
        return handles

    def release(self, adapter, record):
        if self.matches(adapter, record):
            entry = self._entries[self._key(record)]
            entry.cache = None
            entry.released = True  # A terminal release cannot mint new handles via FULL/AT.

    async def load(self, ticket, *, recheck):
        binding = ticket._binding
        async def checked():
            from gateway.owned_group_service import _scope
            if not ticket.authorized():
                return False
            with _scope(ticket.runtime_home):
                allowed = await recheck()
            return allowed is True and ticket._binding is binding and ticket.authorized()
        try:
            if not await checked():
                return ()
            async with self._load_lock:
                if not await checked():
                    return ()
                entry = self._entries[self._key(ticket.as_record())]
                if entry.cache is None:
                    images, total = [], 0
                    for url, mime in entry.attachments:
                        if not await checked():
                            return ()
                        cached = sum(len(image["data"]) for other in self._entries.values()
                                     for image in (other.cache or ()))
                        limit = min(MAX_IMAGE_BYTES, MAX_MESSAGE_BYTES - total,
                                    MAX_CACHE_BYTES - cached - total)
                        if limit <= 0:
                            return ()
                        data = await _download(binding.adapter, url, mime, limit=limit)
                        if not await checked():
                            return ()
                        total += len(data)
                        images.append({"data": data, "mime_type": mime})
                    entry.cache = tuple(images)
                if not await checked():
                    return ()
                # Never hand mutable registry dictionaries to the consumer.
                return tuple(dict(image) for image in entry.cache)
        except Exception:
            return ()

    def bind_text(self, adapter, record):
        entry = self._entries.get(self._key(record))
        if (entry is not None and entry.adapter is adapter and len(entry.texts) < 2
                and tuple(record.get("images_v1", ())) == entry.handles
                and all(record.get(key) == value for key, value in entry.record.items())):
            # Observation carries the caption; intake may append the existing
            # voice transcript. Only actually issued host renderings can restore.
            entry.texts.add(record["text"])

    def matches(self, adapter, record):
        self.prune()
        entry = self._entries.get(self._key(record))
        return (not self.closed and entry is not None and not entry.released
                and entry.adapter is adapter and record.get("text") in entry.texts
                and tuple(record.get("images_v1", ())) == entry.handles
                and all(record.get(key) == value for key, value in entry.record.items()))


async def _download(adapter, url, mime, *, limit):
    from tools.url_safety import create_ssrf_safe_async_client
    if not _valid_url(url):
        raise ValueError("invalid owned image locator")
    async with create_ssrf_safe_async_client(
            timeout=15.0, follow_redirects=False, trust_env=False) as client:
        headers = dict(adapter._qq_media_headers(), **{"Accept-Encoding": "identity"})
        async with client.stream("GET", url, headers=headers) as response:
            response.raise_for_status()
            if (response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != mime
                    or response.headers.get("content-encoding", "identity").lower() != "identity"):
                raise ValueError("unexpected owned image response type")
            length = response.headers.get("content-length")
            if length is not None and (not length.isascii() or not length.isdecimal()
                                       or int(length) > limit):
                raise ValueError("invalid owned image response length")
            data = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=65536):
                if len(data) + len(chunk) > limit:
                    raise ValueError("owned image exceeds byte limit")
                data.extend(chunk)
    from PIL import Image
    raw = bytes(data)
    with Image.open(io.BytesIO(raw), formats=("PNG", "JPEG")) as image:
        if (Image.MIME.get(image.format) != mime or image.format not in {"PNG", "JPEG"}
                or getattr(image, "is_animated", False) or getattr(image, "n_frames", 1) != 1
                or not 0 < image.width * image.height <= 4_000_000):
            raise ValueError("invalid owned static image")
        image.verify()
    with Image.open(io.BytesIO(raw), formats=("PNG", "JPEG")) as image:
        image.load()
    return raw


def stage(adapter, source, data):
    """Authorize live transport before issuing a capability; never fetch here."""
    from gateway.owned_group_service import _fresh, platform_time, selected, service_for
    runner = getattr(adapter, "gateway_runner", None)
    if runner is None or not selected(runner, source):
        return ()
    message_time = platform_time(data.get("timestamp"))
    attachments = data.get("attachments")
    if not _fresh(message_time) or not isinstance(attachments, list):
        return ()
    from plugins.platforms.qqbot.strict_send import _valid_message_id
    content = data.get("content", "")
    if (source.is_bot or not _valid_message_id(data.get("id"))
            or type(content) is not str or len(content) > 4096):
        return ()
    images = _attachments(attachments)
    if not images:
        return ()
    service = service_for(runner)
    identity = runner._canonicalize(source)
    if identity is None or identity.transport_inferred or identity.adapter() is not adapter:
        return ()
    host = service.context(identity.runtime_profile, identity.runtime_home)
    if host is None:
        return ()
    record = dict(bot_id=adapter._app_id, runtime_id=identity.runtime_profile,
                  transport_profile=identity.transport_profile, runtime_home=str(identity.runtime_home),
                  group_id=source.chat_id, user_id=source.user_id,
                  session_id=runner._session_key_for_source(source), message_id=data.get("id"),
                  message_time=message_time)
    if service.validate(host, adapter, record) is None:
        return ()
    return service.images.issue(adapter, record, images, content)
