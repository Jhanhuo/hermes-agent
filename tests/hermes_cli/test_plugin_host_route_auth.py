"""Regression for #140499: upstream admission survives credential-free hosting."""
import json
import os
from types import SimpleNamespace

import pytest

from hermes_cli.plugin_host import PluginHost
from tests.hermes_cli import test_plugin_secondary_profile_host as secondary

daemon = secondary.daemon

_API = '''
import os
from fastapi import APIRouter, Request
from hermes_cli.web_deps import late
from hermes_cli.plugin_host_http_auth import is_profile_operator

router = APIRouter()

@router.put("/config")
async def config(request: Request):
    late("_require_token")(request)
    return {"pid": os.getpid(), "saved": True, "operator": is_profile_operator(request),
            "credentials": [k for k in request.headers if k in
                {"authorization", "cookie", "x-hermes-session-token", "x-hermes-gateway-ticket"}]}
'''


@pytest.mark.platforms("any")
def test_secondary_guarded_mutation_uses_upstream_admission(daemon, monkeypatch):
    monkeypatch.setattr(PluginHost, "asgi_request", secondary._REAL_ASGI_REQUEST)
    secondary._beta_plugin(daemon)
    api = daemon.beta / "plugins" / secondary.NAME / "dashboard" / "plugin_api.py"
    api.write_text(_API, encoding="utf-8")
    url = f"/api/plugins/{secondary.NAME}/config"
    try:
        response = daemon.client.put(url, headers=daemon.beta_ticket())
        assert response.status_code == 200, response.text
        assert response.json()["saved"] is True
        assert response.json()["operator"] is True
        assert response.json()["pid"] != os.getpid()
        assert response.json()["credentials"] == []
        assert daemon.client.put(url, params={"profile": "beta"}).status_code == 401
        # An on-disk selection alone is not proof this standalone dashboard serves beta.
        monkeypatch.setattr(secondary.web_server.app.state, 'gateway_runner', None, raising=False)
        selected = daemon.client.put(url, params={'profile': 'beta'}, headers=daemon.owner)
        assert selected.status_code == 200  # existing ordinary admission is unchanged
        assert selected.json()['operator'] is False
    finally:
        secondary._shutdown_hosts()


@pytest.mark.platforms("any")
def test_host_admission_does_not_leak_or_come_from_http_input(tmp_path):
    api = tmp_path / "api"
    api.mkdir()
    (api / "plugin_api.py").write_text(_API, encoding="utf-8")
    home = tmp_path / "home"
    home.mkdir()
    host = PluginHost(SimpleNamespace(home_path=home, owner_home=home, _plugins={}))
    args = ("guard-probe", str(api), "plugin_api.py", "PUT", "/config", "authenticated=true",
            [("X-Hosted-Http-Authenticated", "true"), ("X-Hosted-Profile-Operator", "true")],
            b'{"authenticated":true}')
    try:
        assert host.asgi_request(*args, authenticated=True)["status"] == 200
        for marker in (False, None, "true", 1):
            assert host.asgi_request(*args, authenticated=marker)["status"] == 401
        assert host.asgi_request(*args)["status"] == 401
        for marker in (True, False, None, "true", 1):
            response = host.asgi_request(*args, authenticated=True, profile_operator=marker)
            assert json.loads(response["body"])["operator"] is (marker is True)
        assert not json.loads(host.asgi_request(*args, authenticated=True)["body"])["operator"]
        assert host.asgi_request(*args, authenticated=True)["status"] == 200
    finally:
        host.shutdown()


@pytest.mark.platforms("any")
def test_launch_host_preserves_native_admission(daemon, monkeypatch):
    from hermes_cli import web_server
    from hermes_cli.dashboard_auth import middleware

    monkeypatch.setattr(PluginHost, "asgi_request", secondary._REAL_ASGI_REQUEST)
    secondary._install(daemon.launch, "launch-guard")
    secondary._plugins(daemon.launch, enabled=["launch-guard"], isolation="host")
    api = daemon.launch / "plugins" / "launch-guard" / "dashboard" / "plugin_api.py"
    api.write_text(_API, encoding="utf-8")
    try:
        response = daemon.client.put("/api/plugins/launch-guard/config", headers=daemon.owner)
        assert response.status_code == 200, response.text
        assert response.json()["operator"] is True
        assert response.json()["pid"] != os.getpid()

        # Keep the real gate and plugin host; stand in only for the OAuth verifier.
        monkeypatch.setattr(web_server.app.state, "auth_required", True, raising=False)
        monkeypatch.setattr(middleware, "_verify_access_token", lambda *args, **kwargs: object())
        ordinary = daemon.client.put("/api/plugins/launch-guard/config", headers={
            **daemon.owner, "Authorization": "Bearer ordinary", "X-Hosted-Profile-Operator": "true"})
        assert ordinary.status_code == 200, ordinary.text  # ordinary route admission is unchanged
        assert ordinary.json()["operator"] is False
    finally:
        secondary._shutdown_hosts()


def test_admission_matches_normal_guard_and_preserves_unguarded_routes():
    from fastapi import FastAPI, HTTPException
    from starlette.requests import Request
    from hermes_cli.plugin_host_http_auth import admitted_for_sensitive_route
    from hermes_cli import web_server

    app = FastAPI()
    scope = {"type": "http", "method": "PUT", "path": "/config", "headers": [], "app": app}
    assert not admitted_for_sensitive_route(Request(scope))
    assert admitted_for_sensitive_route(Request({**scope, "headers": [
        (web_server._SESSION_HEADER_NAME.lower().encode(), web_server._SESSION_TOKEN.encode())]}))
    app.state.auth_required = True
    assert not admitted_for_sensitive_route(Request(scope))
    assert admitted_for_sensitive_route(Request({**scope, "state": {"session": object()}}))
    assert admitted_for_sensitive_route(Request({**scope, "state": {"native_http_principal": object()}}))
    for marker in (None, False, "true", 1):
        with pytest.raises(HTTPException) as error:
            web_server._require_token(Request({**scope, "state": {"hosted_http_authenticated": marker}}))
        assert error.value.status_code == 401


def test_profile_operator_requires_profile_bound_native_or_non_gated_owner_token():
    from fastapi import FastAPI
    from starlette.requests import Request
    from hermes_cli import plugin_host_http_auth as auth, web_server
    from hermes_constants import get_hermes_home

    app = FastAPI()
    home = get_hermes_home().resolve()
    token = (web_server._SESSION_HEADER_NAME.lower().encode(), web_server._SESSION_TOKEN.encode())

    def request(*headers, **state):
        return Request({"type": "http", "app": app, "headers": list(headers), "state": state})

    assert auth.is_profile_operator(request(token))
    app.state.auth_required = True
    assert not auth.is_profile_operator(request(token, session=object()))
    assert auth.is_profile_operator(request(native_http_principal={"profile_id": str(home)}))
    assert not auth.is_profile_operator(request(native_http_principal={"profile_id": str(home / 'other')}))
    assert not auth.is_profile_operator(request((b'x-hermes-profile-operator', b'true'), session=object()))
    app.state.auth_required = False
    # In a child, forwarded HTTP credentials must not override the private decision.
    for marker in (False, None, 1, 'true'):
        assert not auth.is_profile_operator(request(token, hosted_http_authenticated=True,
                                                    hosted_profile_operator=marker))
    assert not auth.is_profile_operator(request(token, hosted_http_authenticated=True))
    hosted = request(hosted_profile_operator=True)
    assert auth.is_profile_operator(hosted)
    assert not auth.upstream_profile_operator(hosted)
    assert not auth.admitted_for_sensitive_route(hosted)
    from hermes_cli.dashboard_auth.native_http import grant_home_scope
    with grant_home_scope({"profile_id": str(home / "other")}):
        assert not auth.is_profile_operator(hosted)
