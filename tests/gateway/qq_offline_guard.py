"""Test-only Python network tripwires; not an OS sandbox.

Create the Windows event loop before entering this context. No networking is
performed by the guard itself. Tests must only use explicitly approved fakes.
"""
from contextlib import ExitStack
import socket
import sys
from unittest.mock import patch

from httpcore._backends.auto import AutoBackend
import httpx


class OfflineNetworkAttempt(BaseException):
    """Bypass production `except Exception` so an attempted escape fails the test."""


def _deny(*args, **kwargs):
    raise OfflineNetworkAttempt("network operation forbidden in offline QQ tests")


class OfflineGuard:
    def __enter__(self):
        self._approved = {}
        self._select = httpx.AsyncClient._transport_for_url
        def select(client, url):
            allowed = self._approved.get(id(client))
            selected = self._select(client, url)
            if allowed is None or allowed[0] is not client or selected is not allowed[1]:
                raise OfflineNetworkAttempt("unapproved HTTP transport in offline QQ tests")
            return selected
        with ExitStack() as stack:
            stack.enter_context(patch.object(httpx.AsyncClient, "_transport_for_url", select))
            for name in ("getaddrinfo", "gethostbyname", "gethostbyname_ex", "create_connection"):
                stack.enter_context(patch.object(socket, name, _deny))
            for name in ("connect", "connect_ex", "sendto"):
                stack.enter_context(patch.object(socket.socket, name, _deny))
            stack.enter_context(patch.object(AutoBackend, "connect_tcp", _deny))
            if sys.platform == "win32":
                from asyncio.windows_events import IocpProactor
                stack.enter_context(patch.object(IocpProactor, "connect", _deny))
            self._stack = stack.pop_all()
        return self

    def allow_client(self, client, transport):
        if client.trust_env is not False or type(transport) is not httpx.MockTransport:
            raise OfflineNetworkAttempt("only explicit proxy-free MockTransport is allowed")
        self._approved[id(client)] = (client, transport)
        # Exercise actual mount resolution before any request, then again on every request.
        client._transport_for_url(httpx.URL("https://api.sgroup.qq.com/v2/groups/Group-A/messages"))

    def __exit__(self, *exc):
        try:
            return self._stack.__exit__(*exc)
        finally:
            self._approved.clear()
