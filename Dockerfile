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
COPY --chown=appuser:appuser alembic     ./alembic
COPY --chown=appuser:appuser alembic.ini ./
COPY --chown=appuser:appuser seeds       ./seeds

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
# 1 solo worker: PipelineTaskManager guarda las tareas en memoria del proceso; con más workers
# el stream/cancelar de una tarea puede caer en otro proceso y dar 404. Las descargas corren
# en hilos, así que un worker atiende varios hospitales en paralelo.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

# ── Stage 3a: pw-browser — descarga Chromium headless shell (en paralelo con apt) ─
# Reutiliza el venv del builder (trae el paquete playwright) y baja solo el shell
# headless + ffmpeg. Sin --with-deps: las libs de sistema las instala el stage full.
# El cache mount evita re-descargar (~170 MB); se copian solo los dirs necesarios
# para no arrastrar navegadores viejos que haya dejado el cache en otras builds.
FROM builder AS pw-browser

RUN --mount=type=cache,target=/pw-cache     PLAYWRIGHT_BROWSERS_PATH=/pw-cache /app/.venv/bin/python -m playwright install --only-shell chromium     && mkdir -p /pw     && cp -a /pw-cache/chromium_headless_shell-* /pw-cache/ffmpeg-* /pw/

# ── Stage 3b: full — Ghostscript + Playwright ─────────────────────────────────
# BuildKit ejecuta en paralelo: apt de este stage, uv sync (builder) y la descarga
# del navegador (pw-browser). Solo se juntan en los COPY --from de abajo.
FROM python:3.11-slim AS full

# Una sola pasada de apt: gosu + Ghostscript + libs de sistema de Chromium headless
# (lista de `playwright install-deps --dry-run chromium`, sin xvfb ni fuentes CJK/emoji,
# innecesarias para descargar). Incluye el usuario para poder usar COPY --chown abajo.
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked     --mount=type=cache,target=/var/lib/apt,sharing=locked     rm -f /etc/apt/apt.conf.d/docker-clean     && apt-get update && apt-get install -y --no-install-recommends         gosu ghostscript         libasound2t64 libatk-bridge2.0-0t64 libatk1.0-0t64 libatspi2.0-0t64 libcairo2         libcups2t64 libdbus-1-3 libdrm2 libgbm1 libglib2.0-0t64 libnspr4 libnss3         libpango-1.0-0 libx11-6 libxcb1 libxcomposite1 libxdamage1 libxext6 libxfixes3         libxkbcommon0 libxrandr2 libfontconfig1 libfreetype6 fonts-liberation     && useradd --uid 10001 --no-create-home --shell /bin/false appuser

COPY --from=ghcr.io/astral-sh/uv:0.11.26 /uv /usr/local/bin/uv

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"     PYTHONUNBUFFERED=1     PYTHONDONTWRITEBYTECODE=1     PLAYWRIGHT_BROWSERS_PATH=/app/.playwright

# COPY --chown fija el dueño al copiar: sin RUN chown -R (que duplicaba ~500 MB de capa)
COPY --from=builder --chown=appuser:appuser /app/.venv /app/.venv
COPY --from=pw-browser --chown=appuser:appuser /pw /app/.playwright

# Entrypoint — rara vez cambia, cacheado antes del código fuente
COPY scripts/docker-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh     && chmod +x /usr/local/bin/entrypoint.sh

# Código fuente — única capa que se invalida en cada deploy.
COPY --chown=appuser:appuser app         ./app
COPY --chown=appuser:appuser alembic     ./alembic
COPY --chown=appuser:appuser alembic.ini ./
COPY --chown=appuser:appuser seeds       ./seeds

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3     CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
# 1 solo worker: PipelineTaskManager guarda las tareas en memoria del proceso; con más workers
# el stream/cancelar de una tarea puede caer en otro proceso y dar 404. Las descargas corren
# en hilos, así que un worker atiende varios hospitales en paralelo.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
