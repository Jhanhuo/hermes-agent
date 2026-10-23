"""Guard regressions use inert sentinels, never a real networking fallback."""
import asyncio
import socket

import pytest


@pytest.mark.parametrize("name", ["getaddrinfo", "gethostbyname", "gethostbyname_ex", "create_connection"])
def test_dns_and_connection_helpers_are_blocked_before_underlying_calls(monkeypatch, name):
    from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt
    reached = []
    def sentinel(*args, **kwargs):
        reached.append(True)
        return []
    monkeypatch.setattr(socket, name, sentinel)
    with OfflineGuard(), pytest.raises(OfflineNetworkAttempt):
        getattr(socket, name)("offline.invalid", 443)
    assert reached == []


@pytest.mark.parametrize("name", ["connect", "connect_ex", "sendto"])
def test_socket_methods_are_blocked_with_inert_outer_sentinel(monkeypatch, name):
    from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt
    reached = []
    def sentinel(*args, **kwargs):
        reached.append(True)
    monkeypatch.setattr(socket.socket, name, sentinel)
    with OfflineGuard(), pytest.raises(OfflineNetworkAttempt):
        getattr(socket.socket, name)(object(), ("offline.invalid", 443))
    assert reached == []


def test_httpcore_backend_is_blocked_before_any_tcp_backend(monkeypatch):
    from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt
    from httpcore._backends.auto import AutoBackend
    reached = []
    async def sentinel(*args, **kwargs):
        reached.append(True)
    monkeypatch.setattr(AutoBackend, "connect_tcp", sentinel)
    loop = asyncio.new_event_loop()
    try:
        async def exercise():
            await AutoBackend().connect_tcp("offline.invalid", 443)
        with OfflineGuard(), pytest.raises(OfflineNetworkAttempt):
            loop.run_until_complete(exercise())
    finally:
        loop.close()
    assert reached == []


@pytest.mark.platforms("windows")
def test_windows_iocp_is_blocked_before_native_connect(monkeypatch):
    from asyncio.windows_events import IocpProactor
    from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt
    reached = []
    def sentinel(*args, **kwargs):
        reached.append(True)
    monkeypatch.setattr(IocpProactor, "connect", sentinel)
    with OfflineGuard(), pytest.raises(OfflineNetworkAttempt):
        IocpProactor.connect(object(), object(), ("offline.invalid", 443))
    assert reached == []


def test_only_registered_mock_transport_with_proxy_discovery_off_is_allowed():
    import httpx
    from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt
    loop = asyncio.new_event_loop()
    seen = []
    fake = httpx.MockTransport(lambda request: seen.append(request) or httpx.Response(200))
    client = httpx.AsyncClient(transport=fake, trust_env=False)
    rogue = httpx.AsyncClient(transport=fake, trust_env=False)
    proxy_enabled = httpx.AsyncClient(transport=fake, trust_env=True)
    try:
        with OfflineGuard() as guard:
            guard.allow_client(client, fake)
            assert loop.run_until_complete(client.get("https://offline.invalid/")).status_code == 200
            with pytest.raises(OfflineNetworkAttempt):
                loop.run_until_complete(rogue.get("https://offline.invalid/"))
            with pytest.raises(OfflineNetworkAttempt):
                guard.allow_client(proxy_enabled, fake)
        assert len(seen) == 1
    finally:
        for item in (client, rogue, proxy_enabled):
            loop.run_until_complete(item.aclose())
        loop.close()


def test_late_proxy_mount_is_rejected_before_its_inert_handler():
    import httpx
    from httpx._utils import URLPattern
    from tests.gateway.qq_offline_guard import OfflineGuard, OfflineNetworkAttempt
    loop = asyncio.new_event_loop()
    approved_calls, unexpected_calls = [], []
    approved = httpx.MockTransport(lambda req: approved_calls.append(req) or httpx.Response(200))
    # Even a broken guard reaches only this inert mock, not a real proxy.
    unexpected = httpx.MockTransport(lambda req: unexpected_calls.append(req) or httpx.Response(200))
    client = httpx.AsyncClient(transport=approved, trust_env=False)
    try:
        with OfflineGuard() as guard:
            guard.allow_client(client, approved)
            client._mounts = {URLPattern("https://offline.invalid"): unexpected}
            with pytest.raises(OfflineNetworkAttempt):
                loop.run_until_complete(client.get("https://offline.invalid/"))
        assert approved_calls == unexpected_calls == []
    finally:
        loop.run_until_complete(client.aclose())
        loop.close()
