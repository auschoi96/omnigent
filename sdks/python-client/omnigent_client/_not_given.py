"""Sentinel for distinguishing omitted arguments from explicit ``None``."""

from __future__ import annotations

from typing import Literal


class NotGiven:
    """Type of the public :data:`NOT_GIVEN` request-argument sentinel."""

    def __bool__(self) -> Literal[False]:
        return False

    def __repr__(self) -> str:
        return "NOT_GIVEN"


NOT_GIVEN = NotGiven()

__all__ = ["NOT_GIVEN", "NotGiven"]
