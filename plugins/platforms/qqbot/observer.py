"""Opt-in QQ group text observation, separate from model-turn admission."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _exact_id(value) -> bool:
    return (isinstance(value, str) and 1 <= len(value) <= 256
            and not any(ch.isspace() or ord(ch) < 32 for ch in value))


async def observe_group_message(adapter, event_type: str, data: dict) -> None:
    """Publish only bounded text; the gateway callback owns auth and runtime scope.

    Run before turn dedup: a rejected full copy must not hide the later addressed
    copy. Observers may see duplicates and must deduplicate by (chat_id, message_id).
    No callback means no storage, media processing, session or model side effects.
    """
    handler = adapter._platform_event_handler
    if not adapter.observe_group_messages or handler is None:
        return
    author = data.get("author")
    if not isinstance(author, dict):
        return
    group, member, message_id = data.get("group_openid"), author.get("member_openid"), data.get("id")
    text = data.get("content", "")
    if (not all(_exact_id(v) for v in (group, member, message_id))
            or not isinstance(text, str)
            or not adapter._is_group_allowed(group, member)):
        return
    source = adapter.build_source(chat_id=group, user_id=member, chat_type="group",
                                  message_id=message_id, is_bot=author.get("bot") is True)
    if source.profile_route_rejected:
        return
    from gateway.owned_group_images import stage
    images = stage(adapter, source, data)
    if not text.strip() and not images:
        return
    if images:
        text = adapter._group_message_content(event_type, data, text.strip())
    envelope = {"platform": "qqbot", "event_type": "group_message", "payload": {
        "schema_version": "qq.group_message.v1", "chat_id": group, "chat_type": "group",
        "message_id": message_id, "user_id": member, "text": text[:2000],
        # Preserve source time for strict owned replies; never substitute receive time.
        "message_timestamp": data.get("timestamp") if isinstance(data.get("timestamp"), str)
        and len(data["timestamp"]) <= 64 else None,
        "qq_event_type": event_type,
        "is_mention": adapter._group_message_is_mention(event_type, data), "is_bot": source.is_bot,
    }}
    if images:
        envelope["payload"]["images_v1"] = list(images)
    try:
        await handler(envelope, source)
    except Exception:
        logger.warning("QQ group text observer failed; turn admission unchanged", exc_info=True)
