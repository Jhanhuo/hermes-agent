"""Carry route-local admission over the private plugin-host channel, without credentials."""
from fastapi import HTTPException


def admitted_for_sensitive_route(request) -> bool:
    """Preserve the normal guard's decision without changing unguarded plugin routes."""
    from hermes_cli.web_server import _require_token
    try:
        _require_token(request)
    except HTTPException as exc:
        if exc.status_code != 401:
            raise
        return False
    return True


def upstream_profile_operator(request) -> bool:
    """Mint proof only on the original dashboard request, in its selected home."""
    state = request.state
    if hasattr(state, "hosted_http_authenticated") or hasattr(state, "hosted_profile_operator"):
        return False  # A hosted decision cannot be delegated to another host.
    native = getattr(state, "native_http_principal", None)
    from hermes_constants import get_hermes_home, get_process_hermes_home
    if native is not None:
        return isinstance(native, dict) and native.get("profile_id") == str(get_hermes_home().resolve())
    from hermes_cli.web_server import _has_valid_session_token
    if getattr(request.app.state, "auth_required", False) or not _has_valid_session_token(request):
        return False

    # A selector establishes routing, not ownership of every home on disk.
    home = get_hermes_home().resolve()
    if home == get_process_hermes_home().resolve():
        return True
    runner = getattr(request.app.state, "gateway_runner", None)
    if runner is None:
        return False
    from gateway.session_authorities import local_bots
    return any(not bot.problem and not bot.retiring and bot.home == home and home.is_dir()
               for bot in local_bots(runner))


def is_profile_operator(request) -> bool:
    """Plugin REST guard for operator actions in the current profile, not general admission."""
    if getattr(request.state, "hosted_profile_operator", None) is True:
        from hermes_constants import get_process_hermes_home, hermes_home_key
        return hermes_home_key() == hermes_home_key(get_process_hermes_home())
    return upstream_profile_operator(request)


def with_hosted_admission(app, authenticated, profile_operator: object = False):
    """Admission belongs to this IPC request, never to the cached plugin app."""
    async def scoped(scope, receive, send):
        state = dict(scope.get("state") or {})
        state["hosted_http_authenticated"] = authenticated is True
        state["hosted_profile_operator"] = profile_operator is True
        await app({**scope, "state": state}, receive, send)
    return scoped
