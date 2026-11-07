"""Translate the errors services raise into HTTP errors."""

from __future__ import annotations

import functools
from collections.abc import Awaitable, Callable
from typing import ParamSpec, TypeVar

from fastapi import HTTPException

from app.services.errors import ServiceError

P = ParamSpec("P")
R = TypeVar("R")


def http_errors(endpoint: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """Answer a ``ServiceError`` raised inside an endpoint with the HTTP error it describes.

    Put it right under the ``@router.<verb>`` decorator.
    """

    @functools.wraps(endpoint)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        try:
            return await endpoint(*args, **kwargs)
        except ServiceError as exc:
            raise HTTPException(exc.status_code, exc.detail) from exc

    return wrapper
