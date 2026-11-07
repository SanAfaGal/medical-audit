"""Dispatch: run one stage and turn failures into log lines."""

from __future__ import annotations

from typing import Any
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.institution import Institution
from app.models.period import AuditPeriod
from app.services.pipeline.context import build_context
from app.services.pipeline.registry import STAGE_HANDLERS

import logging

logger = logging.getLogger(__name__)


async def execute(
    stage: str,
    institution: Institution,
    period: AuditPeriod,
    db: AsyncSession,
    extra: dict[str, Any] | None = None,
) -> AsyncGenerator[str, None]:
    """Yield log lines as a pipeline stage executes."""
    ctx = build_context(institution, period, db, extra or {})
    handler_fn = STAGE_HANDLERS.get(stage)

    if handler_fn is None:
        yield f"[ERROR] Etapa desconocida: {stage}"
        return

    try:
        async for line in handler_fn(ctx):
            yield line
    except Exception as exc:
        logger.exception("Pipeline stage %s failed", stage)
        exc_desc = f"{type(exc).__name__}: {exc}" if str(exc).strip() else type(exc).__name__
        yield f"[ERROR] {stage} falló: {exc_desc}"
