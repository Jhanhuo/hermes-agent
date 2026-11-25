"""Native Windows case-alias regression for external-secret cache reset (#154609)."""

from pathlib import Path

import pytest

from agent.secret_sources import registry
from agent.secret_sources.base import FetchResult, SecretSource
from hermes_cli import env_loader
from hermes_constants import hermes_home_key, reset_hermes_home_key_cache


class _WindowsHomeKeyProbe(SecretSource):
    name = "winhomeprobe"
    label = "Windows home key probe"
    shape = "bulk"

    def fetch(self, cfg: dict, home_path: Path) -> FetchResult:
        return FetchResult(secrets={"WIN_HOME_KEY_PROBE_TOKEN": "scoped-value"})


@pytest.mark.platforms("windows")
def test_case_variant_reset_evicts_missing_home_snapshot(tmp_path, monkeypatch):
    home = tmp_path / "MixedCase"
    home.mkdir()
    (home / "config.yaml").write_text(
        "secrets:\n  winhomeprobe:\n    enabled: true\n", encoding="utf-8"
    )
    monkeypatch.delenv("WIN_HOME_KEY_PROBE_TOKEN", raising=False)
    registry._reset_registry_for_tests()
    registry.register_source(_WindowsHomeKeyProbe(), replace=True)
    env_loader.reset_secret_source_cache()
    try:
        expected = {"WIN_HOME_KEY_PROBE_TOKEN": "scoped-value"}
        assert env_loader.hydrate_profile_secret_sources(home) == expected
        home.rename(tmp_path / "moved")
        alias = Path(str(home).upper())
        assert hermes_home_key(alias) == hermes_home_key(home)
        env_loader.reset_secret_source_cache(alias)
        assert env_loader.get_secret_source_values(home) == {}
    finally:
        env_loader.reset_secret_source_cache()
        registry._reset_registry_for_tests()
        reset_hermes_home_key_cache()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("cache_name", [
    "_SECRET_SOURCE_VALUES_BY_HOME", "_SECRET_SOURCE_RESTORE_BY_HOME", "_SECRET_SOURCE_WRITES_BY_HOME",
])
def test_cached_source_names_use_canonical_home_key(tmp_path, monkeypatch, cache_name):
    home = tmp_path / "MixedCase"
    home.mkdir()
    names = frozenset({"WIN_HOME_KEY_PROBE_TOKEN"})
    monkeypatch.setattr(env_loader, "_SOURCE_SUPPLIED_NAMES", set())
    monkeypatch.setattr(env_loader, "_SECRET_SOURCES", {})
    for name in ("_SECRET_SOURCE_VALUES_BY_HOME", "_SECRET_SOURCE_RESTORE_BY_HOME",
                 "_SECRET_SOURCE_WRITES_BY_HOME"):
        monkeypatch.setattr(env_loader, name, {})
    monkeypatch.setitem(getattr(env_loader, cache_name), hermes_home_key(home), dict.fromkeys(names))
    assert env_loader.has_cached_secret_source_names(home, names)
    assert env_loader.has_cached_secret_source_names(Path(str(home).upper()), names)
    assert not env_loader.has_cached_secret_source_names(tmp_path / "other", names)
