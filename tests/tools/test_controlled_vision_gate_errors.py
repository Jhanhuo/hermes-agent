"""Controlled native vision converts terminal guard refusal, but preserves cancellation."""
import asyncio
import json

import pytest

from tests.gateway.qq_offline_guard import OfflineGuard
from tests.tools.test_controlled_vision_request_gates import data_url


@pytest.mark.parametrize('cancelled', [False, True])
def test_controlled_native_gate_error_contract(monkeypatch, cancelled, caplog):
    import tools.vision_tools as vision
    from agent import auxiliary_client as aux
    checks = []
    async def current():
        checks.append(True)
        if len(checks) == 1:
            return True
        if cancelled:
            raise asyncio.CancelledError()
        raise RuntimeError('SENSITIVE_GATE_SENTINEL')
    monkeypatch.setattr(vision, 'async_call_llm', aux.async_call_llm)
    monkeypatch.setattr(aux, '_plan_aux_call',
                        lambda *a, **kw: pytest.fail('denied admission must not plan provider I/O'))
    with asyncio.Runner() as runner:
        runner.get_loop()
        with OfflineGuard():
            pending = vision.vision_analyze_tool(data_url(), 'describe',
                controlled_data_url=True, recheck=current)
            if cancelled:
                with pytest.raises(asyncio.CancelledError):
                    runner.run(pending)
            else:
                result = json.loads(runner.run(pending))
                assert result == {'success': False, 'error': 'Controlled image analysis unavailable'}
                assert 'SENSITIVE_GATE_SENTINEL' not in caplog.text
