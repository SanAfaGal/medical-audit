"""Small typing helpers for SQLAlchemy results."""

from __future__ import annotations

from typing import Any, cast

from sqlalchemy import CursorResult, Result


def affected_rows(result: Result[Any]) -> int:
    """Rows changed by an UPDATE/DELETE (``Result`` does not declare ``rowcount``; the DML result does)."""
    return cast("CursorResult[Any]", result).rowcount
