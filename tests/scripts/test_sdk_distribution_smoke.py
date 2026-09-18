"""Executable SDK documentation contract."""

import runpy
from pathlib import Path


def test_readme_quickstarts_execute() -> None:
    """Public sync and async examples must stay executable."""
    smoke_script = Path(__file__).resolve().parents[2] / "scripts" / "sdk_distribution_smoke.py"
    runpy.run_path(str(smoke_script), run_name="__main__")
