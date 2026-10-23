"""Gateway context references are a runtime-profile policy, independent of model tools."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

from agent import context_references as refs
from gateway.config import GatewayConfig, Platform
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner, _profile_runtime_scope
from gateway.session import SessionSource
from hermes_constants import get_hermes_home


class InboundContextReferencePolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = self.enterContext(tempfile.TemporaryDirectory(dir=os.environ["TMPDIR"]))
        self.home = Path(self.tmp)
        self.enterContext(patch.dict(os.environ, {"HERMES_HOME": str(self.home)}))
        self.enterContext(patch.object(Path, "home", return_value=self.home))
        self.runner = object.__new__(GatewayRunner)
        self.runner.config = GatewayConfig()
        self.source = SessionSource(platform=Platform.QQBOT, chat_id="chat", chat_type="dm")
        self.runner._session_key_for_source = lambda source: "agent:main:qqbot:dm:chat"
        self.text = "literal @file:note.txt @folder:src @diff @staged @git:log @url:https://offline.invalid @probe:item"
        self.model = self.enterContext(patch.object(
            self.runner, "_inbound_model_context_length", AsyncMock(return_value=100_000)))
        # An expansion recorder, not a real network/git/file/provider operation.
        self.expand = self.enterContext(patch.object(
            refs, "preprocess_context_references_async", AsyncMock(return_value=refs.ContextReferenceResult(
                message="EXPANDED", original_message=self.text, expanded=True))))
        self.parser = self.enterContext(patch.object(refs, "parse_context_references", wraps=refs.parse_context_references))

    async def prepare(self):
        return await self.runner._prepare_inbound_message_text(
            event=MessageEvent(text=self.text, source=self.source), source=self.source, history=[])

    async def test_disabled_retains_literal_before_model_parser_or_expansion(self):
        (self.home / "config.yaml").write_text("context_references:\n  enabled: false\n", encoding="utf-8")
        for direct in (False, True):
            with self.subTest(direct=direct):
                if direct:
                    result = await self.runner._expand_inbound_context_references(self.source, "key", self.text)
                else:
                    result = await self.prepare()
                self.assertEqual(result, self.text)
                self.model.assert_not_awaited()
                self.expand.assert_not_awaited()
                self.parser.assert_not_called()

    async def test_routed_runtime_opt_out_overrides_enabled_transport(self):
        transport = self.home / "transport"
        runtime = self.home / "runtime"
        transport.mkdir()
        runtime.mkdir()
        (transport / "config.yaml").write_text("context_references:\n  enabled: true\n", encoding="utf-8")
        (runtime / "config.yaml").write_text("context_references:\n  enabled: false\n", encoding="utf-8")
        self.runner.config.multiplex_profiles = True
        self.runner._resolve_profile_home_for_source = lambda source: runtime
        self.source.profile = "runtime"
        with _profile_runtime_scope(transport):
            self.assertEqual(await self.prepare(), self.text)
            self.assertEqual(get_hermes_home(), transport)
        self.model.assert_not_awaited()
        self.expand.assert_not_awaited()

    async def test_absent_setting_preserves_expansion(self):
        self.assertEqual(await self.prepare(), "EXPANDED")
        self.model.assert_awaited_once()
        self.expand.assert_awaited_once()

    async def test_invalid_policy_and_failed_config_reads_never_expand(self):
        from hermes_cli.config_read_errors import FailedConfigRead

        invalid = [
            {"context_references": {"enabled": value}}
            for value in ("false", "true", 0, 1, None, [], {})
        ] + [
            {"context_references": value} for value in (False, True, None, [], "disabled")
        ] + [FailedConfigRead({}, error=OSError("offline unreadable config"))]
        for cfg in invalid:
            with self.subTest(config=cfg), patch("hermes_cli.config.load_config_readonly", return_value=cfg):
                self.assertEqual(await self.prepare(), self.text)
                self.model.assert_not_awaited()
                self.expand.assert_not_awaited()
        with patch("hermes_cli.config.load_config_readonly", side_effect=OSError("offline config failure")):
            self.assertEqual(await self.prepare(), self.text)
            self.expand.assert_not_awaited()

    async def test_failed_profile_binding_cannot_fall_back_to_transport(self):
        with patch.object(self.runner, "_async_profile_scope_for_source", side_effect=RuntimeError("scope unavailable")):
            self.assertEqual(await self.prepare(), self.text)
        self.model.assert_not_awaited()
        self.expand.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
