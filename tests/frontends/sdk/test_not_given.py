"""Tests for the public omitted-argument sentinel."""

from omnigent_client import NOT_GIVEN, NotGiven
from omnigent_client import __all__ as public_exports
from omnigent_client._not_given import NOT_GIVEN as MODULE_NOT_GIVEN


def test_not_given_is_the_public_module_singleton() -> None:
    assert NOT_GIVEN is MODULE_NOT_GIVEN
    assert isinstance(NOT_GIVEN, NotGiven)
    assert {"NOT_GIVEN", "NotGiven"} <= set(public_exports)


def test_not_given_has_stable_falsey_representation() -> None:
    assert not NOT_GIVEN
    assert repr(NOT_GIVEN) == "NOT_GIVEN"
