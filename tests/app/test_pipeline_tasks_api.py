"""HTTP tests for the task endpoints of /api/pipeline (status, active, stream, cancel) — no DB needed."""

from __future__ import annotations

import asyncio
import json

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.services.task_manager import PipelineTaskManager


@pytest.fixture
def tm() -> PipelineTaskManager:
    return PipelineTaskManager()


@pytest_asyncio.fixture
async def client(tm):
    from app.main import app
    from app.routers.api.pipeline import get_task_manager

    app.dependency_overrides[get_task_manager] = lambda: tm
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
            yield c
    finally:
        app.dependency_overrides.pop(get_task_manager, None)


async def _start_background(tm: PipelineTaskManager, institution_id=1, period_id=1):
    async def _forever(run, stage, institution_id, period_id, extra):
        await asyncio.sleep(30)

    tm._run_stage = _forever  # type: ignore[method-assign]
    return await tm.start("SOME_STAGE", institution_id, period_id, {})


class TestStatusAndActive:
    async def test_status_of_unknown_task_is_404(self, client):
        assert (await client.get("/api/pipeline/status/nope")).status_code == 404

    async def test_status_reports_running_task(self, client, tm):
        run = await _start_background(tm)
        try:
            body = (await client.get(f"/api/pipeline/status/{run.task_id}")).json()
            assert body["status"] == "running"
            assert body["stage"] == "SOME_STAGE"
            assert body["institution_id"] == 1 and body["period_id"] == 1
        finally:
            run._task.cancel()

    async def test_active_filters_by_institution_and_period(self, client, tm):
        a = await _start_background(tm, institution_id=1, period_id=1)
        b = await _start_background(tm, institution_id=2, period_id=7)
        try:
            everything = (await client.get("/api/pipeline/active")).json()
            assert {r["task_id"] for r in everything} == {a.task_id, b.task_id}

            only_b = (await client.get("/api/pipeline/active", params={"institution_id": 2})).json()
            assert [r["task_id"] for r in only_b] == [b.task_id]

            assert (await client.get("/api/pipeline/active", params={"period_id": 99})).json() == []
        finally:
            a._task.cancel()
            b._task.cancel()

    async def test_streamed_stage_holding_the_lock_is_not_listed_as_active(self, client, tm):
        run = tm.begin_sync("SOME_STAGE", 1, 1)
        try:
            assert (await client.get("/api/pipeline/active")).json() == []
        finally:
            tm.end_sync(run)


class TestCancel:
    async def test_cancel_running_task(self, client, tm):
        run = await _start_background(tm)
        resp = await client.delete(f"/api/pipeline/{run.task_id}")

        assert resp.status_code == 200
        assert resp.json() == {"ok": True}
        assert run.cancel_event.is_set()

    async def test_cancel_unknown_task_is_404(self, client):
        assert (await client.delete("/api/pipeline/nope")).status_code == 404


class TestStream:
    async def test_stream_unknown_task_is_404(self, client):
        assert (await client.get("/api/pipeline/stream/nope")).status_code == 404

    async def test_stream_replays_logs_then_done(self, client, tm):
        run = tm.begin_sync("SOME_STAGE", 1, 1)
        run.logs.extend(["[INFO] uno", "[INFO] dos"])
        tm.end_sync(run, "done")

        resp = await client.get(f"/api/pipeline/stream/{run.task_id}")

        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/event-stream")
        events = [json.loads(line[len("data: ") :]) for line in resp.text.splitlines() if line.startswith("data: ")]
        assert [e["msg"] for e in events] == ["[INFO] uno", "[INFO] dos", "[DONE]"]
        assert [e.get("idx") for e in events[:2]] == [0, 1]
        assert events[-1]["status"] == "done"

    async def test_stream_resumes_from_cursor(self, client, tm):
        run = tm.begin_sync("SOME_STAGE", 1, 1)
        run.logs.extend(["[INFO] uno", "[INFO] dos", "[INFO] tres"])
        tm.end_sync(run, "done")

        resp = await client.get(f"/api/pipeline/stream/{run.task_id}", params={"from": 2})

        messages = [
            json.loads(line[len("data: ") :])["msg"] for line in resp.text.splitlines() if line.startswith("data: ")
        ]
        assert messages == ["[INFO] tres", "[DONE]"]
