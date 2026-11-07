"""Stage registry: maps a stage name to its async-generator handler."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable

from app.services.pipeline.context import StageContext

StageHandler = Callable[[StageContext], AsyncGenerator[str, None]]

STAGE_HANDLERS: dict[str, StageHandler] = {}


def stage(name: str) -> Callable[[StageHandler], StageHandler]:
    """Decorator to register a stage handler under ``name``."""

    def decorator(fn: StageHandler) -> StageHandler:
        STAGE_HANDLERS[name] = fn
        return fn

    return decorator
