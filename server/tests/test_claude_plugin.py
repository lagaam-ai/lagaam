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


def _registry_env() -> dict[str, dict[str, Any]]:
    listing = _json(_ROOT / "server.json")
    return {v["name"]: v for v in listing["packages"][0]["environmentVariables"]}


def _env_from_options() -> dict[str, str]:
    """Env var -> the userConfig option that is its whole value."""
    options: dict[str, str] = {}
    for name, value in _server()["env"].items():
        match = re.fullmatch(r"\$\{user_config\.(\w+)\}", value)
        if match:
            options[name] = match.group(1)
    return options


def test_plugin_can_run_the_pinot_adapter() -> None:
    wired = set(_env_from_options())

    assert {"LAGAAM_ENGINE", "PINOT_CONTROLLER_URL", "PINOT_BROKER_URL"} <= wired


def test_every_user_config_default_is_the_registry_default() -> None:
    # Accepting the plugin's defaults must start the server a registry user gets.
    options = _manifest()["userConfig"]
    registry = _registry_env()

    for env_var, key in _env_from_options().items():
        option = options[key]
        default = str(option["default"]) if "default" in option else None
        assert default == registry[env_var].get("default"), env_var


def test_engine_field_names_every_registry_choice() -> None:
    # Free text: `options` would stop Claude Code before 2.1.271 loading the plugin.
    engine = _manifest()["userConfig"][_env_from_options()["LAGAAM_ENGINE"]]

    assert "options" not in engine
    for choice in _registry_env()["LAGAAM_ENGINE"]["choices"]:
        assert choice in engine["description"].split(), choice


def test_marketplace_entry_describes_the_plugin_as_its_manifest_does() -> None:
    # /plugin shows the entry's description over plugin.json's.
    [entry] = _json(_ROOT / ".claude-plugin" / "marketplace.json")["plugins"]

    assert entry["description"] == _manifest()["description"]
