"""Tests for PipelineTaskManager — concurrency and log-streaming behavior."""

from __future__ import annotations

import asyncio

import pytest

from app.services.task_manager import PipelineTaskManager, TaskAlreadyRunningError


class TestStartRaceCondition:
    async def test_second_start_for_same_context_raises_immediately(self):
        tm = PipelineTaskManager()

        async def _fake_run_stage(run, stage, institution_id, period_id, extra):
            await asyncio.sleep(10)

        tm._run_stage = _fake_run_stage  # bypass the real DB-backed pipeline execution

        run1 = await tm.start("SOME_STAGE", 1, 1, {})
        try:
            with pytest.raises(TaskAlreadyRunningError):
                await tm.start("SOME_STAGE", 1, 1, {})
        finally:
            run1._task.cancel()

    async def test_different_context_does_not_raise(self):
        tm = PipelineTaskManager()

        async def _fake_run_stage(run, stage, institution_id, period_id, extra):
            await asyncio.sleep(10)

        tm._run_stage = _fake_run_stage

        run1 = await tm.start("SOME_STAGE", 1, 1, {})
        run2 = await tm.start("SOME_STAGE", 2, 1, {})
        try:
            assert run1.task_id != run2.task_id
        finally:
            run1._task.cancel()
            run2._task.cancel()


class TestStreamFromLogEviction:
    async def test_reconnect_before_log_offset_does_not_crash_and_warns(self):
        tm = PipelineTaskManager()
        run = await tm.start("SOME_STAGE", 1, 1, {})
        run._task.cancel()  # we drive logs manually below, no need for the real stage to run

        # Simulate eviction: 10 lines existed, first 6 were trimmed (log_offset=6)
        run.logs = ["line6", "line7", "line8", "line9"]
        run.log_offset = 6
        run.status = "done"

        results = [item async for item in tm.stream_from(run.task_id, from_index=0)]

        assert any("[WARN]" in line for _, line in results)
        # after the warning, it must resume from log_offset, not crash with a negative index
        assert (6, "line6") in results
        assert (9, "line9") in results
