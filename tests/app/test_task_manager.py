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


class TestHeartbeatAndCancel:
    async def test_stream_emits_heartbeat_when_quiet_then_finishes(self, monkeypatch):
        import app.services.task_manager as task_manager

        monkeypatch.setattr(task_manager, "HEARTBEAT_SECONDS", 0.05)
        tm = PipelineTaskManager()
        run = await tm.start("SOME_STAGE", 1, 1, {})
        run._task.cancel()
        run.status = "running"  # keep the stream open: no new lines will arrive

        async def _finish_later():
            await asyncio.sleep(0.2)
            async with run._condition:
                run.status = "done"
                run._condition.notify_all()

        finisher = asyncio.create_task(_finish_later())
        items = [item async for item in tm.stream_from(run.task_id, from_index=0)]
        await finisher

        assert any(line is None for _, line in items)  # at least one heartbeat

    async def test_cancel_sets_cancel_event(self):
        tm = PipelineTaskManager()

        async def _fake_run_stage(run, stage, institution_id, period_id, extra):
            await asyncio.sleep(10)

        tm._run_stage = _fake_run_stage
        run = await tm.start("SOME_STAGE", 1, 1, {})
        assert not run.cancel_event.is_set()

        assert await tm.cancel(run.task_id) is True
        assert run.cancel_event.is_set()

    async def test_run_stage_updates_last_activity_and_passes_cancel_event(self, monkeypatch):
        import app.services.pipeline as pipeline_runner
        from app import database
        from app.repositories.institution_repo import InstitutionRepo
        from app.repositories.invoice_repo import InvoiceRepo

        seen: dict = {}

        async def _fake_execute(stage, institution, period, db, extra):
            seen["extra"] = extra
            yield "[INFO] hola"

        class _FakeSession:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        async def _get(self, _id):
            return object()

        monkeypatch.setattr(pipeline_runner, "execute", _fake_execute)
        monkeypatch.setattr(database, "AsyncSessionLocal", lambda: _FakeSession())
        monkeypatch.setattr(InstitutionRepo, "__init__", lambda self, db: None)
        monkeypatch.setattr(InvoiceRepo, "__init__", lambda self, db: None)
        monkeypatch.setattr(InstitutionRepo, "get_by_id", _get)
        monkeypatch.setattr(InvoiceRepo, "get_period_by_id", _get)

        tm = PipelineTaskManager()
        run = await tm.start("S", 1, 1, {})
        before = run.last_activity
        await asyncio.sleep(0.05)
        await run._task

        assert run.status == "done"
        assert seen["extra"]["cancel_event"] is run.cancel_event
        assert run.last_activity >= before


class TestSharedContextLock:
    """Background tasks and request-streamed stages (GET /run) share one lock per institution/period."""

    async def test_sync_run_blocks_background_start_until_released(self):
        tm = PipelineTaskManager()
        sync_run = tm.begin_sync("REMOVE_NON_PDF", 1, 1)
        with pytest.raises(TaskAlreadyRunningError) as exc_info:
            await tm.start("OTHER", 1, 1, {})
        assert exc_info.value.run is sync_run

        tm.end_sync(sync_run)
        run = await tm.start("OTHER", 1, 1, {})
        run._task.cancel()

    async def test_background_task_blocks_sync_run(self):
        tm = PipelineTaskManager()

        async def _fake_run_stage(run, stage, institution_id, period_id, extra):
            await asyncio.sleep(10)

        tm._run_stage = _fake_run_stage
        run = await tm.start("LONG", 1, 1, {})
        try:
            with pytest.raises(TaskAlreadyRunningError):
                tm.begin_sync("REVISAR_SOBRANTES", 1, 1)
        finally:
            run._task.cancel()

    def test_two_sync_runs_for_the_same_period_conflict_but_other_periods_do_not(self):
        tm = PipelineTaskManager()
        first = tm.begin_sync("A", 1, 1)
        with pytest.raises(TaskAlreadyRunningError):
            tm.begin_sync("B", 1, 1)
        other = tm.begin_sync("B", 1, 2)  # different period: independent
        tm.end_sync(first)
        tm.end_sync(other)
        tm.end_sync(tm.begin_sync("C", 1, 1))  # lock was released

    def test_sync_runs_are_not_listed_as_reattachable_tasks(self):
        tm = PipelineTaskManager()
        run = tm.begin_sync("A", 1, 1)
        assert tm.get_all_active() == []  # /active only offers background tasks
        assert tm.get_active_for_context(1, 1) is run  # but the lock still sees it
        tm.end_sync(run, "error")
        assert tm.get_active_for_context(1, 1) is None
