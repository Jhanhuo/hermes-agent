"""Fail-closed ownership boundary for explicitly selected QQ groups."""
from __future__ import annotations

import inspect
import logging
import math
import re
import time
from dataclasses import dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Any

from gateway.config import Platform
from gateway.owned_group_lifecycle import OwnedGroupLifecycle
from gateway.platforms.event import MessageType
from plugins.platforms.qqbot.strict_send import StrictSendResult

logger = logging.getLogger(__name__)
HOOK = "gateway_owned_group"


def _scope(home):
    from gateway.run import _profile_runtime_scope
    return _profile_runtime_scope(home, hydrate_secrets=False)


def platform_time(raw) -> float | None:
    """Strict platform ISO/legacy ms, never the adapter's fallback-to-now clock."""
    if type(raw) is not str or not 1 <= len(raw) <= 64 or raw != raw.strip():
        return None
    try:
        if re.fullmatch(r"[0-9]{13}", raw):
            value = int(raw) / 1000
        else:
            if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|[+-]\d\d:\d\d)", raw):
                return None
            value = datetime.fromisoformat(raw).timestamp()
        return value if math.isfinite(value) and 0 <= value < 1e12 else None
    except (ValueError, OverflowError, OSError):
        return None


def _fresh(value):
    now = time.time()
    return (type(value) in (int, float) and 0 <= value < 1e12
            and math.isfinite(value) and math.isfinite(now) and 0 <= now - value < 285)


@dataclass(frozen=True)
class OwnedGroupTicket:
    bot_id: str
    runtime_id: str
    transport_profile: str
    group_id: str
    user_id: str
    session_id: str
    message_id: str
    message_time: float
    text: str
    addressed: bool
    runtime_home: Path
    ledger_path: Path
    _binding: Any = field(default=None, repr=False, compare=False)

    def as_record(self) -> dict:
        return {f.name: str(value) if isinstance(value := getattr(self, f.name), Path) else value
                for f in fields(self) if not f.name.startswith("_")}

    def authorized(self) -> bool:
        try:
            return self._binding is not None and self._binding.authorized(self)
        except Exception:
            return False

    async def submit(self, text: str) -> StrictSendResult:
        if not self.authorized():
            return StrictSendResult("rejected", reason="owned_authorization_revoked")
        binding = self._binding
        with _scope(binding.authorization_home):
            return await binding.adapter.send_group_reply_once(
                group_id=self.group_id, message_id=self.message_id, content=text,
                message_time=self.message_time, expected_app_id=self.bot_id)

    async def submit_image(self, image_data: bytes, mime_type: str, *, recheck) -> StrictSendResult:
        """Trusted image submission; never accepts an alternate target or anchor.

        The caller owns the shared outbound ledger. The callback is ephemeral,
        uses routed-runtime scope while transport authorization is checked in
        the receiving profile before/after it. It is never serialized.
        """
        binding = self._binding
        if binding is None:
            return StrictSendResult("rejected", reason="owned_authorization_revoked")
        adapter = binding.adapter
        with _scope(binding.authorization_home):
            if not self.authorized():
                return StrictSendResult("rejected", reason="owned_authorization_revoked")

            async def authorized_recheck():
                if (self._binding is not binding or binding.adapter is not adapter
                        or not self.authorized()):
                    return False
                with _scope(self.runtime_home):
                    allowed = await recheck()
                if allowed is not True:
                    return False
                return (self._binding is binding and binding.adapter is adapter
                        and self.authorized())

            return await adapter.send_group_image_reply_once(
                group_id=self.group_id, message_id=self.message_id, image_data=image_data,
                mime_type=mime_type, message_time=self.message_time, expected_app_id=self.bot_id,
                recheck=authorized_recheck)


@dataclass(frozen=True)
class NativeGroupContext:
    runtime_home: Path
    runtime_id: str
    _service: Any = field(repr=False)
    _manager: Any = field(repr=False)
    _callback: Any = field(repr=False)
    _registration: Any = field(repr=False)

    def restore(self, record: dict) -> OwnedGroupTicket | None:
        try:
            return self._service.restore(self, record)
        except Exception:
            return None

    def alive(self) -> bool:
        try:
            if not self._service.alive() or not self._registration.active:
                return False
            served = getattr(self._service.runner, "_served_profile_homes", None)
            if isinstance(served, dict) and served.get(self.runtime_id) != self.runtime_home:
                return False
            from hermes_cli.plugins import get_plugin_manager
            from agent.estop import is_engaged
            with _scope(self.runtime_home):
                if is_engaged():
                    return False
                manager = get_plugin_manager()
                callbacks = manager.iter_hook_callbacks(HOOK)
                return (manager is self._manager and len(callbacks) == 1
                        and callbacks[0] is self._callback)
        except Exception:
            return False


@dataclass
class _Binding:
    host: NativeGroupContext
    adapter: Any
    authorization_home: Path
    record: dict

    def authorized(self, ticket):
        if ticket.as_record() != self.record or not self.host.alive():
            return False
        return self.host._service.validate(self.host, self.adapter, self.record) is not None


class OwnedGroupService(OwnedGroupLifecycle):

    def alive(self):
        return (not self.closed and getattr(self.runner, "_running", False) is True
                and not getattr(self.runner, "_draining", False)
                and not getattr(self.runner, "_external_drain_active", False))

    def context(self, runtime_id, runtime_home):
        from hermes_cli.plugins import get_plugin_manager
        with _scope(runtime_home):
            manager = get_plugin_manager()
            callbacks = manager.iter_hook_callbacks(HOOK)
        registrations = [r for owned in manager._ownership_ledger.values() for r in owned
                         if r.kind == "hook" and r.key == HOOK and r.active]
        if (len(callbacks) != 1 or len(registrations) != 1
                or not inspect.iscoroutinefunction(callbacks[0])):
            return None
        return NativeGroupContext(Path(runtime_home), runtime_id, self, manager, callbacks[0], registrations[0])

    def restore(self, host, record):
        from plugins.platforms.qqbot.strict_send import _input_error, _valid_message_id
        names = {f.name for f in fields(OwnedGroupTicket) if not f.name.startswith("_")}
        required = names - {"runtime_home", "ledger_path"}
        if (type(record) is not dict or not required <= set(record) <= names
                or type(record["addressed"]) is not bool):
            return None
        if (not all(_valid_message_id(record[k]) for k in (
                "bot_id", "runtime_id", "transport_profile", "user_id", "session_id"))
                or _input_error(record["group_id"], record["message_id"], record["text"])):
            return None
        runner = self.runner
        profile = record["transport_profile"]
        # Exact receiving registry, never the shared-satellite delivery fallback.
        adapters = (runner._primary_adapters() if profile == runner._primary_profile_name
                    else runner._profile_adapters_map().get(profile, {}))
        adapter = adapters.get(Platform.QQBOT)
        if adapter is None:
            return None
        identity = self.validate(host, adapter, record)
        if identity is None:
            return None
        values = dict(record, runtime_home=identity.runtime_home,
                      ledger_path=identity.authorization_home / "plugin-data/owned-group-outbound/state.db")
        ticket = OwnedGroupTicket(**values)
        object.__setattr__(ticket, "_binding", _Binding(host, adapter, identity.authorization_home, ticket.as_record()))
        return ticket

    def validate(self, host, adapter, record):
        """Rebuild from live received transport, with no persisted runtime override."""
        if not host.alive() or not _fresh(record["message_time"]):
            return None
        runner = self.runner
        registered, owner = runner._owning_profile(adapter, Platform.QQBOT)
        owner = owner or runner._primary_profile_name
        if not registered or owner != record["transport_profile"] or adapter._app_id != record["bot_id"]:
            return None
        with _scope(host.runtime_home):
            source = adapter.build_source(chat_id=record["group_id"], user_id=record["user_id"], chat_type="group")
            identity = runner._canonicalize(source, transport_profile=owner)
        if (identity is None or identity.runtime_profile != host.runtime_id
                or identity.runtime_home != host.runtime_home
                or identity.transport_profile != owner or runner._intake_adapter_for(source) is not adapter
                or ("runtime_home" in record and str(identity.runtime_home) != record["runtime_home"])
                or record["runtime_id"] != host.runtime_id
                or runner._session_key_for_source(source) != record["session_id"]):
            return None
        from agent.estop import is_engaged
        with _scope(identity.authorization_home):
            if is_engaged():
                return None
        ledger = identity.authorization_home / "plugin-data/owned-group-outbound/state.db"
        if (("ledger_path" in record and str(ledger) != record["ledger_path"])
                or not selected(runner, source)):
            return None
        if (adapter.config.extra.get("strict_group_replies") is not True
                or adapter._group_policy != "allowlist" or source.chat_id not in adapter._group_allow_from
                or not runner._is_user_authorized_for_source(source)):
            return None
        return identity

    def ticket(self, event, host, identity, *, require_addressed=True):
        raw = event.raw_message if isinstance(event.raw_message, dict) else {}
        message_time = platform_time(raw.get("timestamp"))
        if (message_time is None or event.message_type != MessageType.TEXT
                or not isinstance(event.text, str) or not event.text.strip()
                or not isinstance(event.message_id, str) or not event.message_id
                or (require_addressed and event.reply_expected is not True)):
            return None
        from plugins.platforms.qqbot.strict_send import _input_error, _valid_message_id
        if (not _valid_message_id(event.source.user_id)
                or _input_error(event.source.chat_id, event.message_id, event.text)):
            return None
        adapter = self.runner._intake_adapter_for(event.source)
        if identity.transport_inferred or identity.adapter() is not adapter:
            return None
        if require_addressed and (raw.get("group_openid") != event.source.chat_id
                or event.metadata.get("qq_event_type") not in {"GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"}):
            return None
        ticket = OwnedGroupTicket(
            bot_id=adapter._app_id, runtime_id=identity.runtime_profile,
            transport_profile=identity.transport_profile, group_id=event.source.chat_id,
            user_id=event.source.user_id, session_id=self.runner._session_key_for_source(event.source),
            message_id=event.message_id, message_time=message_time, text=event.text,
            addressed=event.reply_expected is True, runtime_home=identity.runtime_home,
            ledger_path=identity.authorization_home / "plugin-data/owned-group-outbound/state.db")
        binding = _Binding(host, adapter, identity.authorization_home, ticket.as_record())
        object.__setattr__(ticket, "_binding", binding)
        return ticket if ticket.authorized() else None

    async def turn(self, event):
        identity = self.runner._canonicalize(event.source)
        if identity is None:
            return
        host = self.context(identity.runtime_profile, identity.runtime_home)
        if host is None:
            return
        ticket = self.ticket(event, host, identity)
        if ticket is not None:
            await self.invoke(host, "turn", ticket)


async def observe(runner, envelope, source):
    """Separate async owned observation; never execute inside the memory worker."""
    if (envelope.get("platform") != "qqbot" or envelope.get("event_type") != "group_message"
            or not selected(runner, source)):
        return
    try:
        from gateway.platforms.event import MessageEvent
        payload = envelope.get("payload", {})
        if (payload.get("schema_version") != "qq.group_message.v1"
                or payload.get("chat_type") != "group" or payload.get("chat_id") != source.chat_id
                or payload.get("user_id") != source.user_id or payload.get("is_bot") is not False
                or payload.get("qq_event_type") not in {"GROUP_MESSAGE_CREATE", "GROUP_AT_MESSAGE_CREATE"}
                or type(payload.get("is_mention")) is not bool):
            return
        event = MessageEvent(text=payload.get("text"), source=source,
                             message_id=payload.get("message_id"), reply_expected=payload["is_mention"],
                             raw_message={"timestamp": payload.get("message_timestamp")})
        if event.is_command():
            return
        service = service_for(runner)
        identity = runner._canonicalize(source)
        if identity is None:
            return
        host = service.context(identity.runtime_profile, identity.runtime_home)
        if host is not None:
            ticket = service.ticket(event, host, identity, require_addressed=False)
            if ticket is not None:
                service.observations.submit(host, ticket)
    except Exception:
        logger.warning("Owned group observation failed; no turn fallback", exc_info=True)


def service_for(runner):
    service = getattr(runner, "_owned_group_service", None)
    if service is None:
        service = runner._owned_group_service = OwnedGroupService(runner)
    return service


def selected(runner, source) -> bool:
    if source.platform != Platform.QQBOT or source.chat_type != "group":
        return False
    adapter = runner._intake_adapter_for(source)
    if adapter is None or adapter._guess_chat_type(source.chat_id) == "guild":
        return False
    extra = adapter.config.extra
    groups = extra.get("owned_group_allow_from")
    return (extra.get("owned_group_replies") is True and type(groups) is list
            and source.chat_id in groups and all(type(g) is str for g in groups))


def wrap_busy_handler(runner, fallback, *, message_handler):
    """Base's active-session path must reach the same admitted ownership gate."""
    async def handle(event, session_key):
        if not event.internal and not event.is_command() and selected(runner, event.source):
            # Base treats a raised busy callback as unhandled unless this is set.
            # Selection owns even an admission/plugin failure: never steer or queue.
            event._gateway_accepted = True
            try:
                # Reuse idle admission's profile scope, not the receiving loop's ambient home.
                await message_handler(event)
            except Exception:
                logger.warning("Owned busy admission failed; consumed without fallback", exc_info=True)
            return True
        return await fallback(event, session_key)
    return handle


async def admit_event(runner, event):
    """Latch ingress selection, admit normally, then consume owned turns before native gates."""
    original_is_command = event.is_command()
    selected_at_ingress = not original_is_command and not event.internal and selected(runner, event.source)
    try:
        admitted = await runner._hm_admit_event(event)
    except Exception:
        if not selected_at_ingress:
            raise
        logger.warning("Owned group admission failed; silently consumed", exc_info=True)
        return None
    if admitted is None:
        return None
    if await consume_turn(runner, admitted[0], original_is_command=original_is_command,
                          selected_at_ingress=selected_at_ingress):
        return None
    return admitted


async def consume_turn(runner, event, *, original_is_command=False, selected_at_ingress=False) -> bool:
    if original_is_command or event.internal:
        return False
    if not selected(runner, event.source):
        return selected_at_ingress
    if event.is_command():
        return True  # Rewriting selected prose into a command cannot escape ownership.
    try:
        await service_for(runner).turn(event)
    except Exception:
        logger.warning("Owned group turn failed; native fallback remains disabled", exc_info=True)
    return True
