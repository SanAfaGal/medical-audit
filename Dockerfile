# ── Stage 1: builder (ligero, solo para el target api) ────────────────────────
FROM python:3.11-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# ── Stage 2: api — sin Playwright (~350 MB) ───────────────────────────────────
FROM python:3.11-slim AS api

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
        libgl1 gosu \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --uid 1000 --no-create-home --shell /bin/false appuser

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Venv del builder — ownership fijado aquí (capa cacheada con las deps)
COPY --from=builder /app/.venv /app/.venv
RUN chown -R appuser:appuser /app/.venv

# Entrypoint — rara vez cambia, cacheado antes del código fuente
COPY scripts/docker-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh \
    && chmod +x /usr/local/bin/entrypoint.sh

# Código fuente — COPY --chown evita un RUN chown posterior en cada deploy
COPY --chown=appuser:appuser app         ./app
COPY --chown=appuser:appuser core        ./core
COPY --chown=appuser:appuser alembic     ./alembic
COPY --chown=appuser:appuser alembic.ini ./
COPY --chown=appuser:appuser seeds       ./seeds

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]

# ── Stage 3: full — Ghostscript + Playwright ──────────────────────────────────
FROM python:3.11-slim AS full

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends gosu ghostscript \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /usr/local/bin/uv

WORKDIR /app

# ENVs temprano: PATH activo para RUN posteriores, evita rutas absolutas
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PLAYWRIGHT_BROWSERS_PATH=/app/.playwright

COPY pyproject.toml uv.lock ./

# uv sync: cacheado mientras pyproject.toml/uv.lock no cambien
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# Una sola capa para: crear usuario + instalar Chromium + fijar ownership.
# Se invalida solo cuando cambia uv.lock (nuevas deps).
# En deploys de código queda 100% cacheada → el mayor ahorro de tiempo.
RUN useradd --uid 10001 --no-create-home --shell /bin/false appuser \
    && python -m playwright install chromium --with-deps \
    && rm -rf /app/.playwright/.links \
    && chown -R appuser:appuser /app/.venv /app/.playwright

# Entrypoint — rara vez cambia, cacheado antes del código fuente
COPY scripts/docker-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh \
    && chmod +x /usr/local/bin/entrypoint.sh

# Código fuente — única capa que se invalida en cada deploy.
# COPY --chown elimina el "RUN chown -R /app" que antes escaneaba GB de archivos.
COPY --chown=appuser:appuser app         ./app
COPY --chown=appuser:appuser core        ./core
COPY --chown=appuser:appuser alembic     ./alembic
COPY --chown=appuser:appuser alembic.ini ./
COPY --chown=appuser:appuser seeds       ./seeds

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
