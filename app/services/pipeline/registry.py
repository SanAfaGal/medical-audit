"""Stage registry: maps a stage name to its async-generator handler."""

from __future__ import annotations

import functools
from collections.abc import AsyncGenerator, Callable
from typing import Literal

from app.services.pipeline.context import StageContext
from app.services.pipeline.logfmt import plog

StageHandler = Callable[[StageContext], AsyncGenerator[str, None]]

STAGE_HANDLERS: dict[str, StageHandler] = {}


def stage(
    name: str,
    *,
    requires: Literal["drive", "stage", "audit"] | None = None,
    missing: Literal["WARN", "ERROR"] = "WARN",
) -> Callable[[StageHandler], StageHandler]:
    """Decorator to register a stage handler under ``name``.

    ``requires`` names the period folder (DRIVE, STAGE or AUDIT) the stage works on: when it does not exist the
    stage logs one ``missing``-level line and ends, so handlers don't each repeat that guard. Use
    ``missing="ERROR"`` when the absence means the stage failed rather than that there is nothing to do.
    """

    def decorator(fn: StageHandler) -> StageHandler:
        handler = fn
        if requires is not None:

            @functools.wraps(fn)
            async def guarded(ctx: StageContext) -> AsyncGenerator[str, None]:
                folder = ctx[f"{requires}_path"]  # type: ignore[literal-required]
                if not folder.is_dir():
                    yield plog(missing, f"Directorio {requires.upper()} no existe: {folder}")
                    return
                async for line in fn(ctx):
                    yield line

            handler = guarded
        STAGE_HANDLERS[name] = handler
        return handler

    return decorator
