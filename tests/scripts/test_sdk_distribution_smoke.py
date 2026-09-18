"""Executable SDK documentation contract."""

from scripts.sdk_distribution_smoke import main


def test_readme_quickstarts_execute() -> None:
    """Public sync and async examples must stay executable."""
    main()
