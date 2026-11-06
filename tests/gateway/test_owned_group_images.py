"""Owned QQ images stay opaque until a claimed, rechecked load (no live I/O)."""
import io
import json
import time
from unittest.mock import AsyncMock

import httpx
import pytest
from PIL import Image

from gateway.owned_group_service import OwnedGroupService
from tests.gateway import test_owned_group_service as service_tests

rig = service_tests.rig


CDN = "https://multimedia.nt.qq.com.cn/download?appid=1407&file=fixture"
STAMP = "2030-03-17T17:46:39+00:00"


def message(caption="", **changes):
    values = dict(id="picture-original", group_openid="Group-A", author={"member_openid": "Member"},
                  content=caption, timestamp=STAMP,
                  attachments=[{"content_type": "image/png", "url": CDN}])
    values.update(changes)
    return values


@pytest.fixture
def image_rig(rig, monkeypatch):
    seen = []
    async def consumer(operation, ticket=None, host=None):
        if operation in {"turn", "observe"}:
            seen.append((operation, ticket, host))
    rig.register(consumer)
    monkeypatch.setattr(rig.adapter, "handle_message", rig.runner._handle_message)
    rig.adapter.set_platform_event_handler(rig.runner._make_default_profile_platform_event_handler())
    rig.seen = seen
    return rig


def ingest(rig, payload=None, event_type="GROUP_AT_MESSAGE_CREATE"):
    async def run():
        await rig.adapter._on_message(event_type, message() if payload is None else payload)
        service = getattr(rig.runner, "_owned_group_service", None)
        if service is not None:
            await service.flush_observations()
    rig.loop.run_until_complete(run())


@pytest.mark.parametrize("caption", ["", "what is in this image?"])
def test_owned_top_level_images_issue_opaque_handles_without_downloading(image_rig, monkeypatch, caption):
    rig = image_rig
    old_download = AsyncMock(side_effect=AssertionError("eager unbounded download forbidden"))
    monkeypatch.setattr(rig.adapter, "_download_and_cache", old_download)
    ingest(rig, message(caption))
    assert len(rig.seen) == 1
    operation, ticket, _ = rig.seen[0]
    assert operation == "turn" and ticket.text == caption
    assert ticket.authorized() and ticket.addressed
    assert isinstance(ticket.images_v1, tuple) and len(ticket.images_v1) == 1
    handle, = ticket.images_v1
    assert isinstance(handle, str) and len(handle) == 64
    assert set(handle) <= set("0123456789abcdef")
    record = ticket.as_record()
    assert record["images_v1"] == [handle]
    assert json.loads(json.dumps(record)) == record
    assert CDN not in json.dumps(record) and "fixture" not in handle
    old_download.assert_not_awaited()
    assert rig.requests == []


def test_full_observation_issues_same_live_capability_without_fetch(image_rig, monkeypatch):
    rig = image_rig
    rig.adapter.observe_group_messages = rig.adapter.require_mention = True
    envelopes = []
    handler = rig.adapter._platform_event_handler
    async def capture(envelope, source):
        envelopes.append(envelope)
        await handler(envelope, source)
    rig.adapter.set_platform_event_handler(capture)
    old_download = AsyncMock(side_effect=AssertionError("observation downloaded media"))
    monkeypatch.setattr(rig.adapter, "_download_and_cache", old_download)
    ingest(rig, event_type="GROUP_MESSAGE_CREATE")
    assert [op for op, _, _ in rig.seen] == ["observe"]
    observed = rig.seen[0][1]
    assert observed.text == "" and not observed.addressed and observed.images_v1
    payload = envelopes[0]["payload"]
    assert payload["images_v1"] == list(observed.images_v1)
    assert CDN not in json.dumps(envelopes) and "attachments" not in payload
    ingest(rig)
    turned = [ticket for op, ticket, _ in rig.seen if op == "turn"]
    assert len(turned) == 1 and turned[0].images_v1 == observed.images_v1
    assert turned[0].message_time == observed.message_time
    old_download.assert_not_awaited()
    assert rig.requests == []


def test_image_restore_requires_live_handles_but_legacy_text_survives_restart(image_rig):
    rig = image_rig
    ingest(rig)
    _, ticket, host = rig.seen[0]
    record = json.loads(json.dumps(ticket.as_record()))
    restored = host.restore(record)
    assert restored is not None and restored.authorized()
    assert isinstance(restored.images_v1, tuple) and restored.images_v1 == ticket.images_v1
    assert restored.message_time == ticket.message_time
    pathless = {key: value for key, value in record.items() if key not in {"runtime_home", "ledger_path"}}
    restored_pathless = host.restore(pathless)
    assert restored_pathless is not None and restored_pathless.authorized()
    assert restored_pathless.images_v1 == ticket.images_v1
    for key, value in {"images_v1": ["forged-handle"], "message_time": ticket.message_time + 1,
                       "group_id": "Other", "user_id": "Other", "text": "forged caption"}.items():
        assert host.restore(dict(record, **{key: value})) is None
    rig.loop.run_until_complete(rig.runner._handle_message(rig.event()))
    text_ticket = rig.seen[-1][1]
    assert text_ticket.images_v1 == () and "images_v1" not in text_ticket.as_record()
    rig.loop.run_until_complete(rig.runner._owned_group_service.shutdown())
    service = OwnedGroupService(rig.runner)
    new_host = service.context("runtime", rig.runtime)
    assert new_host.restore(record) is None
    assert new_host.restore(text_ticket.as_record()).authorized()
    assert rig.requests == []


@pytest.mark.parametrize("url", ["http://gchat.qpic.cn/a.png", "https://127.0.0.1/a.png",
    "https://example.com/a.png", "https://evil.qq.com/a.png", "https://sub.gchat.qpic.cn/a.png",
    "https://gchat.qpic.cn.evil.invalid/a.png", "https://user:secret@gchat.qpic.cn/a.png",
    "https://gchat.qpic.cn:444/a.png", "//gchat.qpic.cn/a.png", "file:///C:/private.png",
    "https://gchat.qpic.cn/a.png#fragment", "https://gchat.qpic.cn/\\evil", "https://gchat.qpic.cn/a\n.png"])
def test_untrusted_locator_never_becomes_owned_image_handle(image_rig, monkeypatch, url):
    old_download = AsyncMock(side_effect=AssertionError("unsafe locator fetched"))
    monkeypatch.setattr(image_rig.adapter, "_download_and_cache", old_download)
    ingest(image_rig, message(attachments=[{"content_type": "image/png", "url": url}]))
    assert image_rig.seen == []
    old_download.assert_not_awaited()
    assert image_rig.requests == []


@pytest.mark.parametrize("attachments", [
    [{"content_type": "image/png", "url": CDN}] * 5,
    [{"content_type": "image/png", "url": CDN, "size": 2 * 1024 * 1024 + 1}],
    [{"content_type": "image/png", "url": CDN, "size": 2 * 1024 * 1024}] * 3,
])
def test_attachment_intake_bounds_are_fail_closed_before_fetch(image_rig, attachments):
    ingest(image_rig, message(attachments=attachments))
    assert image_rig.seen == [] and image_rig.requests == []


@pytest.mark.parametrize("denial", ["unauthorized", "stale", "future", "bot", "no_strict", "no_hook"])
def test_no_capability_without_live_admission(image_rig, denial):
    rig = image_rig
    payload = message()
    if denial == "unauthorized":
        rig.runner._is_user_authorized = lambda source: False
    elif denial == "stale":
        payload["timestamp"] = "2030-03-17T17:00:00+00:00"
    elif denial == "future":
        payload["timestamp"] = "2030-03-17T17:47:00+00:00"
    elif denial == "bot":
        payload["author"]["bot"] = True
    elif denial == "no_strict":
        rig.adapter.config.extra["strict_group_replies"] = False
    elif denial == "no_hook":
        for registrations in list(rig.managers[rig.runtime]._ownership_ledger.values()):
            for registration in list(registrations):
                registration.dispose()
    ingest(rig, payload)
    assert rig.seen == [] and rig.requests == []


def png_bytes():
    output = io.BytesIO()
    Image.new("RGB", (3, 2), "red").save(output, format="PNG")
    return output.getvalue()


@pytest.fixture
def downloader(image_rig, monkeypatch):
    rig = image_rig
    downloads, options = [], []
    payload = {"data": png_bytes(), "mime": "image/png", "status": 200}
    def handle(request):
        downloads.append(request)
        assert request.method == "GET" and str(request.url) == CDN
        assert request.headers["Authorization"] == "QQBot offline-token"
        body = {"stream": payload["stream"]} if "stream" in payload else {"content": payload["data"]}
        return httpx.Response(payload["status"], **body,
                              headers={"content-type": payload["mime"], **payload.get("headers", {})})
    def factory(**kwargs):
        options.append(kwargs)
        assert kwargs["trust_env"] is False and kwargs["follow_redirects"] is False
        transport = httpx.MockTransport(handle)
        client = httpx.AsyncClient(transport=transport, **kwargs)
        rig.offline_guard.allow_client(client, transport)
        return client
    monkeypatch.setattr("tools.url_safety.create_ssrf_safe_async_client", factory)
    return rig, downloads, options, payload


def test_claimed_load_returns_validated_bytes_and_wait_restore_reuses_cache(downloader):
    rig, downloads, options, _payload = downloader
    ingest(rig)
    _, ticket, host = rig.seen[0]
    checks = []
    async def recheck():
        from hermes_constants import get_hermes_home
        checks.append(get_hermes_home())
        return True
    loaded = rig.loop.run_until_complete(ticket.load_images(recheck=recheck))
    assert isinstance(loaded, tuple) and len(loaded) == 1
    assert set(loaded[0]) == {"data", "mime_type"}
    assert type(loaded[0]["data"]) is bytes and loaded[0]["mime_type"] == "image/png"
    with Image.open(io.BytesIO(loaded[0]["data"])) as image:
        image.load()
        assert image.size == (3, 2)
    assert len(checks) >= 2 and all(home == rig.runtime for home in checks)
    assert len(downloads) == len(options) == 1
    restored = host.restore(json.loads(json.dumps(ticket.as_record())))
    assert rig.loop.run_until_complete(restored.load_images(recheck=recheck)) == loaded
    assert len(downloads) == 1
    ticket.release_images()
    assert host.restore(ticket.as_record()) is None
    assert rig.loop.run_until_complete(ticket.load_images(recheck=recheck)) == ()
    assert rig.requests == []


async def allow():
    return True


def test_non_png_container_is_rejected_before_ico_or_embedded_png_load(downloader, monkeypatch):
    """A declared PNG must not enter ICO's eager, pre-return decoder."""
    import struct
    from PIL import IcoImagePlugin, PngImagePlugin

    rig, downloads, _, payload = downloader
    out = io.BytesIO()
    Image.new("RGB", (2001, 2000)).save(out, format="PNG")
    embedded = out.getvalue()
    # ICO's directory can claim 256x256 while the embedded PNG is over 4MP.
    raw = (struct.pack("<HHH", 0, 1, 1)
           + struct.pack("<BBBBHHII", 0, 0, 0, 0, 1, 32, len(embedded), 22)
           + embedded)
    assert len(raw) < 2 * 1024 * 1024
    payload["data"] = raw  # HTTP and attachment still declare image/png.
    ico_loads, png_loads = [], []
    ico_load = IcoImagePlugin.IcoImageFile.load
    png_load = PngImagePlugin.PngImageFile.load
    def track_ico(image, *args, **kwargs):
        ico_loads.append(True)
        return ico_load(image, *args, **kwargs)
    def track_png(image, *args, **kwargs):
        png_loads.append(image.size)
        return png_load(image, *args, **kwargs)
    monkeypatch.setattr(IcoImagePlugin.IcoImageFile, "load", track_ico)
    monkeypatch.setattr(PngImagePlugin.PngImageFile, "load", track_png)
    ingest(rig)
    assert rig.loop.run_until_complete(rig.seen[0][1].load_images(recheck=allow)) == ()
    assert ico_loads == [], f"disallowed ICO decoder entered; embedded loads={png_loads}"
    assert png_loads == []
    assert len(downloads) == 1 and rig.requests == []


def test_oversized_png_is_rejected_before_pixel_load(downloader, monkeypatch):
    from PIL import PngImagePlugin

    rig, downloads, _, payload = downloader
    out = io.BytesIO()
    Image.new("RGB", (2001, 2000)).save(out, format="PNG")
    payload["data"] = out.getvalue()
    loads = []
    original = PngImagePlugin.PngImageFile.load
    def track(image, *args, **kwargs):
        loads.append(image.size)
        return original(image, *args, **kwargs)
    monkeypatch.setattr(PngImagePlugin.PngImageFile, "load", track)
    ingest(rig)
    assert rig.loop.run_until_complete(rig.seen[0][1].load_images(recheck=allow)) == ()
    assert loads == []
    assert len(downloads) == 1 and rig.requests == []


def test_static_jpeg_load_keeps_verified_complete_image(downloader):
    rig, downloads, _, payload = downloader
    out = io.BytesIO()
    Image.new("RGB", (3, 2), "red").save(out, format="JPEG")
    payload.update(data=out.getvalue(), mime="image/jpeg")
    ingest(rig, message(attachments=[{"content_type": "image/jpeg", "url": CDN}]))
    loaded = rig.loop.run_until_complete(rig.seen[0][1].load_images(recheck=allow))
    assert loaded == ({"data": payload["data"], "mime_type": "image/jpeg"},)
    with Image.open(io.BytesIO(loaded[0]["data"])) as image:
        image.load()
        assert image.format == "JPEG" and image.size == (3, 2)
    assert len(downloads) == 1 and rig.requests == []


@pytest.mark.parametrize("bad", ["content_type", "format", "animated", "truncated", "pixels", "corrupt",
                                  "content_length", "compression", "redirect", "failure"])
def test_load_rejects_unsafe_or_invalid_image_without_delivery(downloader, bad):
    rig, downloads, _, payload = downloader
    if bad == "content_type":
        payload["mime"] = "text/html"
    elif bad == "format":
        out = io.BytesIO()
        Image.new("RGB", (2, 2)).save(out, format="GIF")
        payload["data"] = out.getvalue()
    elif bad == "animated":
        out = io.BytesIO()
        Image.new("RGB", (2, 2), "red").save(out, format="PNG", save_all=True,
            append_images=[Image.new("RGB", (2, 2), "blue")], duration=100, loop=0)
        payload["data"] = out.getvalue()
    elif bad == "truncated":
        payload["data"] = payload["data"][:-12]
    elif bad == "pixels":
        out = io.BytesIO()
        Image.new("RGB", (2001, 2000)).save(out, format="PNG")
        payload["data"] = out.getvalue()
    elif bad == "corrupt":
        payload["data"] = b"not a real image"
    elif bad == "content_length":
        payload["headers"] = {"content-length": str(2 * 1024 * 1024 + 1)}
    elif bad == "compression":
        import gzip
        payload["data"] = gzip.compress(payload["data"])
        payload["headers"] = {"content-encoding": "gzip"}
    elif bad == "redirect":
        payload["status"], payload["headers"] = 302, {"location": "https://evil.invalid/image"}
    elif bad == "failure":
        payload["status"] = 503
    ingest(rig)
    ticket = rig.seen[0][1]
    assert rig.loop.run_until_complete(ticket.load_images(recheck=allow)) == ()
    assert len(downloads) == 1 and rig.requests == []


class Chunks(httpx.AsyncByteStream):
    def __init__(self, chunks):
        self.chunks, self.read, self.closed = chunks, 0, False
    async def __aiter__(self):
        for chunk in self.chunks:
            self.read += 1
            yield chunk
    async def aclose(self):
        self.closed = True


def test_stream_stops_before_unbounded_body_and_always_closes(downloader):
    rig, downloads, _, payload = downloader
    body = Chunks([b"x" * 65536] * 80)
    payload["stream"] = body
    ingest(rig)
    assert rig.loop.run_until_complete(rig.seen[0][1].load_images(recheck=allow)) == ()
    assert body.read <= 33 and body.closed
    assert len(downloads) == 1


def test_message_stream_budget_stops_at_total_limit_not_after_whole_third_image(downloader):
    rig, downloads, _, payload = downloader
    first = png_bytes()
    body = Chunks([first + b"\0" * (65536 - len(first))] + [b"\0" * 65536] * 23)
    payload["stream"] = body
    ingest(rig, message(attachments=[{"content_type": "image/png", "url": CDN}] * 3))
    assert rig.loop.run_until_complete(rig.seen[0][1].load_images(recheck=allow)) == ()
    assert body.read <= 65 and body.closed
    assert len(downloads) == 3


@pytest.mark.parametrize("denial", ["before", "inside_check", "after_fetch", "expired", "changed_transport", "exception"])
def test_load_checks_live_authorization_around_each_await(downloader, monkeypatch, denial):
    rig, downloads, _, _payload = downloader
    ingest(rig)
    ticket = rig.seen[0][1]
    if denial == "before":
        rig.runner._is_user_authorized = lambda source: False
    elif denial == "expired":
        monkeypatch.setattr(time, "time", lambda: ticket.message_time + 285)
    elif denial == "changed_transport":
        from gateway.config import Platform
        rig.runner.adapters[Platform.QQBOT] = object()
    checks = []
    async def recheck():
        checks.append(True)
        if denial == "inside_check":
            rig.runner._is_user_authorized = lambda source: False
        if denial == "exception":
            raise RuntimeError("policy failure")
        if denial == "after_fetch" and downloads:
            rig.runner._is_user_authorized = lambda source: False
        return True
    assert rig.loop.run_until_complete(ticket.load_images(recheck=recheck)) == ()
    assert len(downloads) == (1 if denial == "after_fetch" else 0)
    assert rig.requests == []


def test_message_total_and_registry_bytes_are_bounded(downloader, monkeypatch):
    rig, _downloads, _, payload = downloader
    payload["data"] += b"\0" * (1536 * 1024 - len(payload["data"]))
    ingest(rig, message(attachments=[{"content_type": "image/png", "url": CDN}] * 3))
    assert rig.loop.run_until_complete(rig.seen[0][1].load_images(recheck=allow)) == ()
    monkeypatch.setattr("gateway.owned_group_images.MAX_CACHE_BYTES", 2 * 1024 * 1024)
    ingest(rig, message(id="second-picture"))
    second = rig.seen[-1][1]
    assert rig.loop.run_until_complete(second.load_images(recheck=allow))
    ingest(rig, message(id="third-picture"))
    third = rig.seen[-1][1]
    assert rig.loop.run_until_complete(third.load_images(recheck=allow)) == ()
    assert rig.loop.run_until_complete(second.load_images(recheck=allow))
    second.release_images()
    assert rig.loop.run_until_complete(third.load_images(recheck=allow))
    assert rig.requests == []


def test_registry_capacity_release_expiry_and_shutdown_do_not_reissue_capabilities(image_rig, monkeypatch):
    rig = image_rig
    monkeypatch.setattr("gateway.owned_group_images.MAX_MESSAGES", 2)
    rig.adapter.observe_group_messages = rig.adapter.require_mention = True
    ingest(rig, event_type="GROUP_MESSAGE_CREATE")
    first, host = rig.seen[-1][1:]
    first.release_images()
    ingest(rig, event_type="GROUP_AT_MESSAGE_CREATE")
    assert not any(op == "turn" for op, _, _ in rig.seen)
    ingest(rig, message(id="second"), event_type="GROUP_MESSAGE_CREATE")
    second = rig.seen[-1][1]
    ingest(rig, message(id="overflow"), event_type="GROUP_MESSAGE_CREATE")
    assert [ticket.message_id for _, ticket, _ in rig.seen] == ["picture-original", "second"]
    service = rig.runner._owned_group_service
    monkeypatch.setattr(time, "time", lambda: first.message_time + 285)
    assert not second.authorized() and host.restore(second.as_record()) is None
    assert service.images._entries == {}
    rig.loop.run_until_complete(service.shutdown())
    assert service.images._entries == {}


@pytest.mark.parametrize("consumer_failure", ["missing_hook", "manager_error"])
def test_existing_ticker_prunes_expired_wait_cache_before_consumer_lookup(downloader, monkeypatch, consumer_failure):
    import asyncio
    from hermes_cli import plugins

    rig, downloads, _, payload = downloader
    ingest(rig)
    _, ticket, host = rig.seen[0]
    loaded = rig.loop.run_until_complete(ticket.load_images(recheck=allow))
    assert loaded == ({"data": payload["data"], "mime_type": "image/png"},)
    # Model a retained wait: a same-process queued record restores and reuses bytes.
    restored = host.restore(json.loads(json.dumps(ticket.as_record())))
    assert rig.loop.run_until_complete(restored.load_images(recheck=allow)) == loaded
    assert len(downloads) == 1
    service = rig.runner._owned_group_service
    assert len(service.images._entries) == 1
    assert next(iter(service.images._entries.values())).cache is not None
    if consumer_failure == "missing_hook":
        for registrations in list(rig.managers[rig.runtime]._ownership_ledger.values()):
            for registration in list(registrations):
                registration.dispose()
    lookup_attempted = asyncio.Event()
    original_lookup = plugins.get_plugin_manager
    def lookup():
        lookup_attempted.set()
        if consumer_failure == "manager_error":
            raise RuntimeError("profile manager unavailable")
        return original_lookup()
    monkeypatch.setattr(plugins, "get_plugin_manager", lookup)
    monkeypatch.setattr(time, "time", lambda: ticket.message_time + 285)
    async def tick():
        # Exercise the actual lifecycle loop; do not issue/load/release/shutdown.
        service.start()
        await asyncio.wait_for(lookup_attempted.wait(), timeout=2)
        assert not service.closed and not service.images.closed
        assert service.images._entries == {}
        assert "runtime" in service.tickers and not service.tickers["runtime"].done()
    rig.loop.run_until_complete(tick())
    assert len(downloads) == 1 and rig.requests == []


def test_shutdown_discards_unexpired_cached_bytes(downloader):
    rig, _, _, _ = downloader
    ingest(rig)
    ticket = rig.seen[0][1]
    assert rig.loop.run_until_complete(ticket.load_images(recheck=allow))
    service = rig.runner._owned_group_service
    rig.loop.run_until_complete(service.shutdown())
    assert service.images._entries == {}
    assert rig.loop.run_until_complete(ticket.load_images(recheck=allow)) == ()


def test_full_and_at_share_normalized_caption_and_original_anchor(image_rig):
    rig = image_rig
    rig.adapter.observe_group_messages = rig.adapter.require_mention = True
    rig.adapter._bot_user_id = "self"
    payload = message("<@!self> caption question?", mentions=[{"id": "self"}])
    ingest(rig, payload, event_type="GROUP_MESSAGE_CREATE")
    observed = rig.seen[0][1]
    assert observed.text == "caption question?"
    ingest(rig, payload)
    turns = [ticket for op, ticket, _ in rig.seen if op == "turn"]
    assert len(turns) == 1
    assert turns[0].text == observed.text and turns[0].images_v1 == observed.images_v1
    assert turns[0].message_time == observed.message_time


@pytest.mark.parametrize("changed", ["caption", "url", "anchor", "sender"])
def test_duplicate_full_never_rebinds_media_or_refreshes_anchor(image_rig, changed):
    rig = image_rig
    rig.adapter.observe_group_messages = rig.adapter.require_mention = True
    envelopes = []
    handler = rig.adapter._platform_event_handler
    async def capture(envelope, source):
        envelopes.append(envelope)
        await handler(envelope, source)
    rig.adapter.set_platform_event_handler(capture)
    ingest(rig, message("original caption"), event_type="GROUP_MESSAGE_CREATE")
    original, host = rig.seen[0][1:]
    payload = message("original caption")
    if changed == "caption":
        payload["content"] = "substituted caption"
    elif changed == "url":
        payload["attachments"][0]["url"] = CDN + "-changed"
    elif changed == "anchor":
        payload["timestamp"] = "2030-03-17T17:46:40+00:00"
    elif changed == "sender":
        rig.runner._is_user_authorized = lambda source: True
        payload["author"]["member_openid"] = "Other"
    ingest(rig, payload, event_type="GROUP_MESSAGE_CREATE")
    assert not envelopes[-1]["payload"].get("images_v1")
    assert host.restore(original.as_record()).images_v1 == original.images_v1
    ingest(rig, message("original caption"))
    turns = [ticket for op, ticket, _ in rig.seen if op == "turn"]
    assert len(turns) == 1 and turns[0].images_v1 == original.images_v1
    assert turns[0].message_time == original.message_time
    assert rig.requests == []


def test_image_named_like_audio_never_enters_voice_downloader(image_rig, monkeypatch):
    stt = AsyncMock(side_effect=AssertionError("image confused with voice"))
    monkeypatch.setattr(image_rig.adapter, "_stt_voice_attachment", stt)
    ingest(image_rig, message(attachments=[{"content_type": "image/png", "url": CDN,
                                          "filename": "image.wav"}]))
    assert len(image_rig.seen) == 1 and image_rig.seen[0][1].images_v1
    stt.assert_not_awaited()


@pytest.mark.parametrize("kind", ["link", "quoted", "forwarded", "gif", "webp", "svg", "video", "file"])
def test_only_current_top_level_static_image_attachments_are_inputs(image_rig, monkeypatch, kind):
    payload = message(attachments=[])
    if kind == "link":
        payload["content"] = CDN
    elif kind == "quoted":
        payload.update(message_type=103, msg_elements=[{"attachments": message()["attachments"]}])
    elif kind == "forwarded":
        payload["message_reference"] = {"attachments": message()["attachments"]}
    else:
        mime = {"gif": "image/gif", "webp": "image/webp", "svg": "image/svg+xml",
                "video": "video/mp4", "file": "file"}[kind]
        payload["attachments"] = [{"content_type": mime, "url": CDN}]
    old_download = AsyncMock(side_effect=AssertionError("non-input media downloaded"))
    monkeypatch.setattr(image_rig.adapter, "_download_and_cache", old_download)
    ingest(image_rig, payload)
    assert all(not ticket.images_v1 for _, ticket, _ in image_rig.seen)
    old_download.assert_not_awaited()
    assert image_rig.requests == []


def test_omitted_caption_is_a_pure_image_not_a_missing_message(image_rig):
    rig = image_rig
    rig.adapter.observe_group_messages = rig.adapter.require_mention = True
    payload = message()
    del payload["content"]
    ingest(rig, payload, event_type="GROUP_MESSAGE_CREATE")
    ingest(rig, payload)
    assert any(op == "observe" for op, _, _ in rig.seen)
    assert sum(op == "turn" for op, _, _ in rig.seen) == 1
    assert all(ticket.text == "" and ticket.images_v1 for _, ticket, _ in rig.seen)


@pytest.mark.parametrize("with_image", [False, True])
def test_owned_quote_retains_text_body_and_original_ticket_binding(image_rig, with_image):
    rig = image_rig
    rig.adapter.observe_group_messages = rig.adapter.require_mention = True
    payload = message("current question", message_type=103,
                      msg_elements=[{"content": "referenced sentence"}],
                      attachments=message()["attachments"] if with_image else [])
    if with_image:
        ingest(rig, payload, event_type="GROUP_MESSAGE_CREATE")
    ingest(rig, payload)
    turns = [(ticket, host) for op, ticket, host in rig.seen if op == "turn"]
    assert len(turns) == 1
    ticket, host = turns[0]
    assert ticket.text == "[Quoted message]:\nreferenced sentence\n\ncurrent question"
    assert ticket.message_id == payload["id"] and ticket.message_time == 1_899_999_999.0
    assert ticket.authorized() and bool(ticket.images_v1) is with_image
    restored = host.restore(json.loads(json.dumps(ticket.as_record())))
    assert restored is not None and restored.authorized() and restored.text == ticket.text
    if with_image:
        observed = rig.seen[0][1]
        assert observed.images_v1 == ticket.images_v1
        assert host.restore(observed.as_record()).authorized()
        assert host.restore(dict(ticket.as_record(), text="forged quote rendering")) is None
    assert rig.requests == []


@pytest.mark.parametrize("with_image", [False, True])
def test_owned_quote_attachments_never_download_or_mint_image_handles(image_rig, monkeypatch, with_image):
    rig = image_rig
    quoted = [*message()["attachments"],
              {"content_type": "audio/silk", "url": "https://offline.invalid/quoted.silk",
               "asr_refer_text": "not an admitted transcript"},
              {"content_type": "file", "url": "https://offline.invalid/forward", "filename": "forward"}]
    payload = message("current question", message_type=103,
        attachments=message()["attachments"] if with_image else [],
        msg_elements=[{"content": "referenced sentence", "attachments": quoted,
                       "message_reference": {"content": "do not expand", "attachments": quoted}}],
        message_reference={"attachments": quoted})
    download = AsyncMock(side_effect=AssertionError("quoted attachment downloaded"))
    stt = AsyncMock(side_effect=AssertionError("quoted voice transcribed"))
    monkeypatch.setattr(rig.adapter, "_download_and_cache", download)
    monkeypatch.setattr(rig.adapter, "_stt_voice_attachment", stt)
    ingest(rig, payload)
    ticket = rig.seen[0][1]
    assert ticket.text == "[Quoted message]:\nreferenced sentence\n\ncurrent question"
    assert len(ticket.images_v1) == int(with_image)
    download.assert_not_awaited()
    stt.assert_not_awaited()
    assert rig.requests == []


def test_owned_quote_text_is_bounded_before_ticket_admission(image_rig):
    ingest(image_rig, message("current question", attachments=[], message_type=103,
                             msg_elements=[{"content": "q" * 4000}]))
    ticket = image_rig.seen[0][1]
    assert ticket.text == "[Quoted message]:\n" + "q" * 1024 + "\n\ncurrent question"
    assert ticket.authorized()
    assert image_rig.requests == []


@pytest.mark.parametrize("chat_type", ["group", "dm"])
def test_non_owned_quote_keeps_existing_attachment_processing(image_rig, monkeypatch, chat_type):
    rig = image_rig
    rig.adapter.config.extra["owned_group_replies"] = False
    capture = AsyncMock()
    monkeypatch.setattr(rig.adapter, "handle_message", capture)
    payload = message("current question", attachments=[], message_type=103,
        msg_elements=[{"content": "referenced sentence", "attachments": [
            {"content_type": "audio/silk", "url": "https://offline.invalid/quote.silk",
             "asr_refer_text": "quoted voice remains supported"}]}])
    rig.loop.run_until_complete(rig.adapter._ingest(
        payload, payload["id"], payload["content"], [], payload["timestamp"],
        chat_id="Group-A" if chat_type == "group" else "Member", qq_chat_type=chat_type,
        user_id="Member", chat_type=chat_type))
    capture.assert_awaited_once()
    event = capture.call_args.args[0]
    assert event.text == ("[Quoted message]:\nreferenced sentence\n"
                          "[Voice] quoted voice remains supported\n\ncurrent question")
    assert not event.metadata.get("images_v1") and rig.requests == []


def test_existing_voice_processing_can_add_transcript_to_same_image_capability(image_rig):
    rig = image_rig
    rig.adapter.observe_group_messages = rig.adapter.require_mention = True
    payload = message("caption")
    payload["attachments"].append({"content_type": "audio/silk", "url": "https://offline.invalid/voice",
                                   "asr_refer_text": "spoken words"})
    ingest(rig, payload, event_type="GROUP_MESSAGE_CREATE")
    ingest(rig, payload)
    turns = [(ticket, host) for op, ticket, host in rig.seen if op == "turn"]
    assert len(turns) == 1
    ticket, host = turns[0]
    assert ticket.text == "caption\n\n[Voice] spoken words"
    assert ticket.images_v1 == rig.seen[0][1].images_v1
    assert host.restore(ticket.as_record()).authorized()
    assert host.restore(rig.seen[0][1].as_record()).authorized()
    assert rig.requests == []
