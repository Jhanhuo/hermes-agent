"""Keep original QQ timestamps for owned scheduling; never mint freshness."""
from unittest.mock import AsyncMock

import pytest
from gateway.config import PlatformConfig
from plugins.platforms.qqbot.adapter import QQAdapter
from tests.gateway.qq_offline_guard import OfflineGuard


@pytest.mark.asyncio
@pytest.mark.parametrize("stamp,expected", [
    ("2026-10-22T10:00:00Z", "2026-10-22T10:00:00Z"),
    ("invalid", "invalid"), (None, None), (1234, None), ("x" * 65, None),
])
async def test_observer_keeps_bounded_raw_timestamp_without_fallback(stamp, expected):
    with OfflineGuard():
        adapter = QQAdapter(PlatformConfig(extra={"require_mention": True, "observe_group_messages": True,
                                                 "group_policy": "allowlist", "group_allow_from": ["Group-A"]}))
        observed = AsyncMock()
        adapter.set_platform_event_handler(observed)
        adapter._message_handler = AsyncMock(side_effect=AssertionError("ordinary text cannot enter model lane"))
        data = {"id": "m", "group_openid": "Group-A", "author": {"member_openid": "Member"},
                "content": "Python?", "timestamp": stamp}
        await adapter._on_message("GROUP_MESSAGE_CREATE", data)
        observed.assert_awaited_once()
        payload = observed.call_args.args[0]["payload"]
        assert payload["message_timestamp"] == expected
        assert payload["qq_event_type"] == "GROUP_MESSAGE_CREATE"
        adapter._message_handler.assert_not_awaited()
