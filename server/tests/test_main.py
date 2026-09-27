"""The `lagaam` entry point as a PyPI user meets it: `uvx lagaam`.

The console script is the install path, so its name and target are pinned,
and a grant-less start must fail with a line an operator can act on.
"""

import os
import re
import tomllib
from pathlib import Path

import pytest

from lagaam.__main__ import main

_SERVER = Path(__file__).resolve().parents[1]


def test_main_without_a_grant_exits_2_naming_the_env_var(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name in list(os.environ):
        if name.startswith(("LAGAAM_", "TRINO_", "PINOT_")):
            monkeypatch.delenv(name)

    with pytest.raises(SystemExit) as exc:
        main()

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert err.startswith("lagaam: ")
    assert "LAGAAM_ALLOWED_TABLES" in err


def test_pyproject_declares_the_lagaam_console_script() -> None:
    project = tomllib.loads((_SERVER / "pyproject.toml").read_text())["project"]

    assert project["name"] == "lagaam"
    assert project["scripts"]["lagaam"] == "lagaam.__main__:main"


def test_pypi_readme_links_are_all_absolute() -> None:
    text = (_SERVER / "README.md").read_text()
    targets = re.findall(r"\]\(([^)\s]+)", text)

    assert targets
    assert all(t.startswith("https://") for t in targets), targets
