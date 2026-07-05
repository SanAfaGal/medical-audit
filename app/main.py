"""FastAPI application factory with lifespan and router registration."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager

import structlog
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.exc import IntegrityError

from app.config import settings
from app.database import engine
from app.security import ApiKeyAuthMiddleware

logger = structlog.get_logger()

_SKIP_LOG_PATHS = {"/health", "/health/db", "/static"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    from app.services.task_manager import PipelineTaskManager

    app.state.task_manager = PipelineTaskManager()
    logger.info("startup", host=settings.host, port=settings.port, docs=settings.docs_enabled)
    yield
    await engine.dispose()
    logger.info("shutdown")


app = FastAPI(
    title="Medical Audit v2",
    version="2.0.0",
    lifespan=lifespan,
    # Swagger UI and ReDoc are only available when DOCS_ENABLED=true (dev)
    docs_url="/docs" if settings.docs_enabled else None,
    redoc_url="/redoc" if settings.docs_enabled else None,
    openapi_url="/openapi.json" if settings.docs_enabled else None,
)

app.add_middleware(ApiKeyAuthMiddleware, app_password=settings.app_password)

@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    """Log every request with method, path, status code and latency."""
    if any(request.url.path.startswith(p) for p in _SKIP_LOG_PATHS):
        return await call_next(request)

    t0 = time.perf_counter()
    try:
        response = await call_next(request)
        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        logger.info(
            "request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            latency_ms=latency_ms,
        )
        return response
    except Exception as exc:
        latency_ms = round((time.perf_counter() - t0) * 1000, 1)
        logger.error(
            "request_error",
            method=request.method,
            path=request.url.path,
            latency_ms=latency_ms,
            error=type(exc).__name__,
        )
        raise


@app.get("/health", include_in_schema=False)
async def health() -> JSONResponse:
    """Used by Docker HEALTHCHECK and nginx upstream probes."""
    return JSONResponse({"status": "ok"})


@app.get("/health/db", include_in_schema=False)
async def health_db() -> JSONResponse:
    """Deep health check: verifies the database connection is alive."""
    from sqlalchemy import text
    from app.database import AsyncSessionLocal

    try:
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
        return JSONResponse({"status": "ok", "db": "ok"})
    except Exception as exc:
        logger.warning("health_db_failed", error=str(exc))
        return JSONResponse({"status": "degraded", "db": "unavailable"}, status_code=503)


@app.exception_handler(IntegrityError)
async def integrity_error_handler(request: Request, exc: IntegrityError) -> JSONResponse:
    """Catch-all for FK/unique-constraint violations not handled by a specific router.

    The per-request session is closed by get_db's ``async with`` block regardless
    of how this handler returns, which issues an implicit ROLLBACK — no explicit
    rollback needed here.
    """
    logger.warning("integrity_error", path=request.url.path, error=str(exc.orig) if exc.orig else str(exc))
    return JSONResponse(
        {"detail": "No se puede completar la operación: el recurso está en uso por otros registros."},
        status_code=409,
    )


# --- Routers (post-app para evitar imports circulares) ---
from app.routers.pages import router as pages_router  # noqa: E402
from app.routers.api.institutions import router as institutions_router  # noqa: E402
from app.routers.api.periods import router as periods_router  # noqa: E402
from app.routers.api.invoices import router as invoices_router  # noqa: E402
from app.routers.api.findings import router as findings_router  # noqa: E402
from app.routers.api.pipeline import router as pipeline_router  # noqa: E402
from app.routers.api.settings import router as settings_router  # noqa: E402
from app.routers.api.explorer import router as explorer_router  # noqa: E402

app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.include_router(pages_router)
app.include_router(institutions_router, prefix="/api")
app.include_router(periods_router, prefix="/api")
app.include_router(invoices_router, prefix="/api")
app.include_router(findings_router, prefix="/api")
app.include_router(pipeline_router, prefix="/api")
app.include_router(settings_router, prefix="/api")
app.include_router(explorer_router, prefix="/api")


if __name__ == "__main__":
    uvicorn.run("app.main:app", host=settings.host, port=settings.port, reload=True)
