"""The Claude Code plugin (plugins/lagaam) as `/plugin install` sees it.

The plugin is read straight from git with no build step, so a version bump
that forgets it would keep plugin users on the old server.
"""

import json
import re
import tomllib
from pathlib import Path
from typing import Any

_SERVER = Path(__file__).resolve().parents[1]
_ROOT = _SERVER.parent
_PLUGIN = _ROOT / "plugins" / "lagaam"


def _json(path: Path) -> dict[str, Any]:
    data: dict[str, Any] = json.loads(path.read_text())
    return data


def _manifest() -> dict[str, Any]:
    return _json(_PLUGIN / ".claude-plugin" / "plugin.json")


def _server() -> dict[str, Any]:
    server: dict[str, Any] = _json(_PLUGIN / ".mcp.json")["mcpServers"]["lagaam"]
    return server


def _version() -> str:
    with (_SERVER / "pyproject.toml").open("rb") as f:
        version: str = tomllib.load(f)["project"]["version"]
    return version


def test_plugin_version_matches_the_package_version() -> None:
    assert _manifest()["version"] == _version()


def test_plugin_runs_the_released_package_at_that_version() -> None:
    server = _server()

    assert server["command"] == "uvx"
    assert server["args"] == [f"lagaam@{_version()}"]


def test_marketplace_entry_points_at_the_plugin() -> None:
    marketplace = _json(_ROOT / ".claude-plugin" / "marketplace.json")
    [entry] = marketplace["plugins"]

    assert entry["name"] == _manifest()["name"]
    assert (_ROOT / entry["source"]).resolve() == _PLUGIN


def test_every_env_var_the_plugin_sets_is_one_the_registry_lists() -> None:
    listing = _json(_ROOT / "server.json")
    listed = {v["name"] for v in listing["packages"][0]["environmentVariables"]}

    assert set(_server()["env"]) <= listed


def test_every_user_config_reference_is_declared() -> None:
    referenced = re.findall(r"\$\{user_config\.(\w+)\}", json.dumps(_server()))

    assert referenced
    assert set(referenced) <= set(_manifest()["userConfig"])
