"""Helpers for the tests that run pipeline stages."""

from __future__ import annotations

from app.services.pipeline.registry import STAGE_HANDLERS


async def run_stage(name: str, ctx) -> list[str]:
    """Run a registered stage to the end and return all its log lines."""
    return [line async for line in STAGE_HANDLERS[name](ctx)]
