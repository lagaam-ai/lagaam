"""The MCP Registry listing (server.json) as the registry and PyPI see it.

A version bump that forgets server.json, or an env var the server never
reads, would publish a listing that lies about how to run lagaam.
"""

import json
import re
import tomllib
from pathlib import Path
from typing import Any

_SERVER = Path(__file__).resolve().parents[1]
_ROOT = _SERVER.parent


def _server_json() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((_ROOT / "server.json").read_text())
    return data


def _project() -> dict[str, Any]:
    with (_SERVER / "pyproject.toml").open("rb") as f:
        project: dict[str, Any] = tomllib.load(f)["project"]
    return project


def test_server_json_versions_match_the_package_version() -> None:
    listing = _server_json()
    version = _project()["version"]

    assert listing["version"] == version
    assert listing["packages"][0]["version"] == version


def test_server_json_package_is_the_pypi_package() -> None:
    package = _server_json()["packages"][0]

    assert package["registryType"] == "pypi"
    assert package["identifier"] == _project()["name"]


def test_pypi_readme_carries_the_registry_ownership_marker() -> None:
    name = _server_json()["name"]
    readme = (_SERVER / "README.md").read_text()

    assert re.search(rf"mcp-name: {re.escape(name)}(\s|-->)", readme)


def test_every_listed_env_var_is_one_the_server_documents() -> None:
    main_source = (_SERVER / "src" / "lagaam" / "__main__.py").read_text()
    names = [v["name"] for v in _server_json()["packages"][0]["environmentVariables"]]

    assert names
    missing = [n for n in names if not re.search(rf"\b{re.escape(n)}\b", main_source)]
    assert missing == []


def test_description_fits_the_registry_limit() -> None:
    assert len(_server_json()["description"]) <= 100
