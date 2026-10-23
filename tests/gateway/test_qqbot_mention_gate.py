"""Mention-only intake must precede BasePlatformAdapter's idle/busy dispatch."""

import asyncio
import unittest
from typing import Any
from unittest.mock import AsyncMock, patch

from gateway.config import Platform, PlatformConfig
from gateway.platforms.event import MessageEvent
from plugins.platforms.qqbot.adapter import QQAdapter
from gateway.session import SessionSource


class QQMentionGateTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, *, require_mention: Any = True, chat_type: Any = "group", addressed: Any = False,
                       busy: Any = False, mode: Any = None, text: Any = "ordinary text"):
        adapter = QQAdapter(PlatformConfig(enabled=True, extra={
            "app_id": "offline-app", "client_secret": "offline-secret",
            "require_mention": require_mention,
        }))
        event = MessageEvent(text=text, message_id="unaddressed-id", reply_expected=addressed,
                             source=SessionSource(platform=Platform.QQBOT, chat_id="chat",
                                                  user_id="user", chat_type=chat_type))
        handler = AsyncMock(return_value=None)
        adapter._message_handler = handler
        key = adapter._event_session_key(event)
        if busy:
            adapter._active_sessions[key] = asyncio.Event()
        busy_handler = AsyncMock(return_value=True)
        if mode:
            adapter._busy_session_handler = busy_handler
            adapter._busy_text_mode = mode

        async def process(inbound, session_key):
            await handler(inbound)

        # Keep real Base.handle_message and busy routing, stop before delivery/model I/O.
        with patch.object(adapter, "_process_message_background", process):
            await adapter.handle_message(event)
            if adapter._background_tasks:
                await asyncio.gather(*list(adapter._background_tasks))
        return adapter, event, handler, busy_handler

    async def test_unaddressed_group_never_dispatches_queues_or_steers(self):
        for busy, mode in [(False, None), (True, None), (True, "queue"), (True, "steer")]:
            for addressed in (False, None, 1, "true"):
                with self.subTest(busy=busy, mode=mode, addressed=addressed):
                    adapter, event, handler, busy_handler = await self.exercise(
                        busy=busy, mode=mode, addressed=addressed)
                    handler.assert_not_awaited()
                    busy_handler.assert_not_awaited()
                    self.assertEqual(adapter._pending_messages, {})
                    self.assertEqual(adapter._last_msg_id, {})
                    self.assertFalse(event._gateway_accepted)

    async def test_unaddressed_control_command_cannot_bypass_gate(self):
        adapter, _, handler, busy_handler = await self.exercise(busy=True, text="/status")
        handler.assert_not_awaited()
        busy_handler.assert_not_awaited()
        self.assertEqual(adapter._pending_messages, {})

    def test_require_mention_rejects_non_booleans(self):
        for value in ("false", "true", 0, 1, None, [], {}):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "require_mention.*boolean"):
                QQAdapter(PlatformConfig(extra={"require_mention": value}))

    def test_require_mention_defaults_to_full_intake(self):
        self.assertIs(QQAdapter(PlatformConfig()).require_mention, False)

    async def test_addressed_dm_and_opt_out_keep_real_base_paths(self):
        for kwargs in ({"addressed": True}, {"chat_type": "dm"}, {"require_mention": False}):
            for busy, mode in [(False, None), (True, None), (True, "queue"), (True, "steer")]:
                with self.subTest(kwargs=kwargs, busy=busy, mode=mode):
                    adapter, event, handler, busy_handler = await self.exercise(
                        **kwargs, busy=busy, mode=mode)
                    self.assertEqual(adapter._last_msg_id, {"chat": event.message_id})
                    if not busy:
                        handler.assert_awaited_once_with(event)
                    elif mode:
                        busy_handler.assert_awaited_once()
                    else:
                        self.assertIn(event, adapter._pending_messages.values())


if __name__ == "__main__":
    unittest.main()
