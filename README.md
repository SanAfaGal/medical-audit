# Medical Audit

Aplicación web de producción para automatizar la auditoría de documentos de facturación médica en instituciones de salud colombianas. Ingesta facturas desde SIHOS, valida carpetas físicas de documentos contra las reglas de documentación requerida por tipo de servicio, y lleva el estado de auditoría por periodo de facturación.

---

## Tabla de contenidos

- [Stack tecnológico](#stack-tecnológico)
- [Arquitectura](#arquitectura)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Desarrollo local](#desarrollo-local)
- [Despliegue a producción](#despliegue-a-producción)
- [Documentación adicional](#documentación-adicional)
- [Seguridad](#seguridad)
- [Monitoreo](#monitoreo)

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Lenguaje | Python 3.11+ |
| Framework web | FastAPI (async) |
| Base de datos | PostgreSQL 16 |
| ORM / Migraciones | SQLAlchemy 2.0 (asyncio) + Alembic |
| Frontend | Jinja2 (templates server-side) |
| Servidor de app | Uvicorn (2 workers, nativo en host) |
| Proxy inverso | Nginx 1.27 (rate limiting, gzip, SSE) |
| Contenedores | Docker + Docker Compose |
| PDF | PyMuPDF, pdfplumber, Ghostscript |
| Automatización de browser | Playwright (descarga de facturas SIHOS) |
| Almacenamiento en nube | Google Drive API |
| Procesamiento de datos | pandas, openpyxl |
| Logs | structlog (estructurado) |
| Seguridad | HTTP Basic Auth compartido + cryptography para credenciales almacenadas |
| Gestor de paquetes | uv |
| Testing | pytest + pytest-asyncio |

---

## Arquitectura

```
  Dispositivos LAN ──── :8000 ──► Uvicorn / FastAPI  (nativo en Windows)
                                          │
                          ┌───────────────┼───────────────┐
                          │               │               │
                   sistema de         Google         localhost:5432
                   archivos           Drive API           │
                   Windows                         ┌─────────────┐
                   (DRIVE/STAGE/AUDIT)              │   Docker    │
                                                    │ PostgreSQL  │
                                                    └─────────────┘
```

- **Backend** (FastAPI + Uvicorn) corre de forma nativa en el host Windows. Accede a los archivos de auditoría directamente desde el sistema de archivos local sin overhead de virtualización.
- **Base de datos** (PostgreSQL 16) corre en Docker con el puerto 5432 expuesto a `localhost`. Solo la base de datos está contenedorizada.
- **Logos de instituciones** se almacenan en la base de datos como `BYTEA` y se sirven vía `GET /api/institutions/{id}/logo` — sin volúmenes compartidos.
- El backend escucha en `0.0.0.0:8000`, por lo que cualquier dispositivo en la misma red LAN puede acceder a la app (protegido por HTTP Basic Auth — ver [Seguridad](#seguridad)).

---

## Estructura del proyecto

```
medical-audit-v2/
├── app/
│   ├── main.py                    # Fábrica de FastAPI, lifespan, middleware, registro de routers
│   ├── config.py                  # Settings (pydantic-settings, lee desde .env)
│   ├── database.py                # Session factory async de SQLAlchemy
│   ├── crypto.py                  # Cifrado de credenciales almacenadas (SIHOS, Drive)
│   ├── security.py                # Middleware de HTTP Basic Auth compartido
│   ├── paths.py                   # Conversión de rutas Windows ↔ contenedor + helpers de sandbox
│   ├── models/
│   │   ├── base.py
│   │   ├── institution.py         # Institution, Administrator, Contract, Agreement, Service
│   │   ├── invoice.py             # Invoice
│   │   ├── period.py              # AuditPeriod
│   │   ├── finding.py             # MissingFile (hallazgos de auditoría)
│   │   ├── rules.py               # ServiceType, DocType, FolderStatus, PrefixCorrection, SystemSettings
│   │   └── __init__.py            # Importa todos los modelos para autogenerate de Alembic
│   ├── repositories/              # Capa de acceso a datos
│   │   ├── institution_repo.py
│   │   ├── invoice_repo.py
│   │   ├── finding_repo.py
│   │   └── rules_repo.py
│   ├── routers/
│   │   ├── pages.py               # Rutas Jinja2 (/, /audit, /settings)
│   │   └── api/
│   │       ├── institutions.py    # /api/institutions
│   │       ├── periods.py         # /api/institutions/{id}/periods
│   │       ├── invoices.py        # /api/invoices
│   │       ├── findings.py        # /api/missing-files
│   │       ├── pipeline.py        # /api/pipeline (SSE streaming + task manager)
│   │       ├── settings.py        # /api/settings
│   │       └── explorer.py        # /api/explorer (explorador de archivos)
│   ├── services/
│   │   ├── billing.py             # Ingesta de Excel SIHOS + normalización
│   │   ├── pipeline_runner.py     # Pipeline de 22 etapas (async generator)
│   │   └── task_manager.py        # Gestión de tareas pipeline en background
│   ├── schemas/                   # Modelos Pydantic de request/response
│   ├── static/                    # CSS y assets estáticos
│   └── templates/                 # Templates Jinja2 HTML
├── core/                          # Lógica de dominio y procesamiento de documentos
│   ├── scanner.py                 # Descubrimiento de archivos (glob/regex)
│   ├── reader.py                  # Extracción de texto PDF (PyMuPDF + pdfplumber)
│   ├── processor.py               # Compresión de PDFs con Ghostscript
│   ├── validator.py                # Validación de facturas (CUFE, código de factura)
│   ├── inspector.py                # Validación de estructura de carpetas
│   ├── organizer.py                # Operaciones de mover y renombrar archivos/carpetas
│   ├── standardizer.py             # Normalización de nombres de archivo
│   ├── downloader.py                # Descarga de facturas SIHOS vía Playwright
│   ├── drive.py                    # Sincronización con Google Drive
│   └── ops.py                      # Utilidades de operaciones de archivo
├── alembic/
│   └── versions/                  # Migraciones auto-generadas desde los modelos
├── seeds/
│   └── seed_data.py               # Script de datos iniciales
├── tests/
│   ├── app/
│   ├── core/
│   ├── load/
│   └── conftest.py                # Fixtures y markers de pytest
├── nginx/
│   └── nginx.conf                 # Proxy inverso + rate limiting + SSE
├── docs/
│   ├── DEPLOY.md                  # Guía de despliegue a producción
│   ├── DEVELOPMENT.md             # Comandos, migraciones, testing, variables de entorno
│   ├── API.md                     # Referencia de endpoints
│   └── DOMAIN.md                  # Modelo de dominio + etapas del pipeline
├── backups/                       # Snapshots de tablas de configuración (no en git)
├── docker-compose.yml             # Servicio PostgreSQL 16
├── docker-compose.override.yml    # Dev: Adminer
├── docker-compose.dev.yml         # Dev: backend en Docker con hot-reload (alternativa a serve)
├── docker-compose.prod.yml        # Producción: db + backend + nginx
├── Dockerfile                     # Multi-stage: builder + runtime
├── dev.sh                         # Script de desarrollo local (./dev.sh help)
├── deploy.sh                      # Script de producción (./deploy.sh help)
├── pyproject.toml                 # Dependencias, pytest, ruff, mypy
├── uv.lock                        # Dependencias congeladas (reproducible)
└── .env.example                   # Template de configuración
```

---

## Desarrollo local

### Prerrequisitos

- [Docker](https://docs.docker.com/get-docker/) y Docker Compose v2
- [uv](https://github.com/astral-sh/uv) para el entorno Python local

### Setup local

**1. Clonar y configurar el entorno**

```bash
git clone <repo-url>
cd medical-audit-v2
cp .env.example .env
```

Edita `.env` con tus valores locales. El `SECRET_KEY` se genera así:

```bash
python -c "import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
```

`APP_PASSWORD` no hace falta generarla — es la contraseña de login (usuario fijo `admin`), elegí una memorable.

**2. Iniciar la base de datos**

```bash
./dev.sh db
```

El `docker-compose.override.yml` se aplica automáticamente y levanta también Adminer en `http://localhost:8080`.

**3. Aplicar migraciones**

```bash
./dev.sh migrate
```

**4. Iniciar el backend**

```bash
./dev.sh serve
```

Uvicorn inicia con hot-reload en `0.0.0.0:8000`.

**5. Configurar la carpeta de auditoría**

La ruta base de auditoría se configura desde la interfaz: **Configuración → Sistema**.

**6. URLs de acceso**

| URL | Descripción |
|---|---|
| `http://localhost:8000` | Aplicación principal (pide usuario/contraseña — usuario `admin`, la contraseña es `APP_PASSWORD`) |
| `http://<ip-lan>:8000` | Acceso desde otros dispositivos en la red |
| `http://localhost:8000/docs` | Swagger UI (solo si `DOCS_ENABLED=true`) |
| `http://localhost:8080` | Adminer — UI de base de datos (solo dev) |

### Backend en Docker (alternativa)

Por defecto el backend corre nativo (paso 4 arriba). Si prefieres correrlo también en Docker — para probar el pipeline con Playwright igual que en producción, o si no tienes `uv`/Python instalado — usa en su lugar:

```bash
./dev.sh docker-up
```

Levanta `db` + `backend` (con hot-reload vía bind mount de `app/`, `core/`, `alembic/`) + Adminer, aplica migraciones y siembra datos iniciales. Reutiliza el mismo contenedor y volumen de PostgreSQL que el modo nativo — puedes alternar entre `./dev.sh serve` y `./dev.sh docker-up` sin perder datos.

> Requiere `AUDIT_DATA_ROOT` definido en `.env` (ruta absoluta a tu carpeta de auditoría) — falla con un mensaje claro si falta.

```bash
./dev.sh docker-logs backend   # ver logs / confirmar reload
./dev.sh docker-migrate        # migrar sin uv local
./dev.sh docker-seed           # sembrar datos sin uv local
./dev.sh docker-down           # detener todo
```

> Comandos completos de desarrollo (testing, migraciones, backups) en **[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)**.

---

## Despliegue a producción

Stack separado (`docker-compose.prod.yml`): tres contenedores (db + backend + nginx), aislado del entorno de desarrollo. Todos los comandos usan el script `deploy.sh` — **no uses `dev.sh` para producción, ni `deploy.sh` para desarrollo**.

```bash
git clone <repo-url> && cd medical-audit-v2
cp .env.example .env   # editar con valores de producción
./deploy.sh up          # build + up -d + migraciones + seed, en un solo paso
```

### Dev vs. producción

| | Desarrollo (`dev.sh`) | Producción (`deploy.sh`) |
|---|---|---|
| Compose file | `docker-compose.yml` (+ `override.yml` / `dev-backend.yml`) | `docker-compose.prod.yml` |
| Proyecto Docker | `medical-audit-v2` (derivado del nombre de la carpeta) | `medical-audit-prod` (fijo, `name:` en el compose) |
| Qué corre en contenedores | PostgreSQL + Adminer (+ backend si usas `docker-up`) | db + backend + nginx |
| Backend | Nativo en el host por defecto, hot-reload (o en Docker con `./dev.sh docker-up`) | Contenedorizado |
| Puerto de la app | `8000` (directo a Uvicorn) | `80` (vía Nginx) |
| Puerto DB expuesto al host | `5432` | Ninguno (solo red interna Docker) |
| Volumen DB | `medical-audit-v2_pgdata` | `medical-audit-prod_pgdata` |

Ambos stacks pueden correr al mismo tiempo sin conflicto (nombres de proyecto y puertos distintos).

> Guía completa de despliegue (requisitos de servidor, rollback, backups, firewall, automatización) en **[docs/DEPLOY.md](docs/DEPLOY.md)**.

---

## Documentación adicional

| Documento | Contenido |
|---|---|
| [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) | Variables de entorno, comandos `dev.sh`, migraciones, backups locales, testing |
| [docs/DEPLOY.md](docs/DEPLOY.md) | Runbook de producción: requisitos, rollback, backups, firewall, automatización |
| [docs/MIGRATION.md](docs/MIGRATION.md) | Migrar el stack completo (DB + PDFs) a otra máquina |
| [docs/API.md](docs/API.md) | Referencia completa de endpoints REST |
| [docs/DOMAIN.md](docs/DOMAIN.md) | Modelo de dominio (entidades, estados) y las 22 etapas del pipeline |

---

## Seguridad

- **HTTP Basic Auth en toda la app** — usuario fijo `admin` y una única contraseña compartida (`APP_PASSWORD`) protegen todas las rutas excepto `/health` y `/health/db`, vía `app/security.py`.
- **`.env` nunca se commitea** — está en `.gitignore`. Usar `.env.example` como plantilla.
- **Variables requeridas son obligatorias** — `docker-compose.yml` usa sintaxis `:?` para `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `APP_PASSWORD`. Si alguna falta, Docker Compose aborta con error explícito.
- **Secretos cifrados en reposo** — contraseñas SIHOS y credenciales de Google Drive se almacenan en DB cifradas vía `app/crypto.py`. La clave proviene de `SECRET_KEY`.
- **Logos almacenados en DB** — los logos de instituciones se guardan como `BYTEA` en PostgreSQL y se sirven vía API, sin volúmenes compartidos.
- **Contenedor sin root** — la imagen Docker corre como `appuser`, no como `root`.
- **Sin exposición directa de la DB** — PostgreSQL solo es accesible dentro de la red Docker.
- **Swagger deshabilitado por defecto** — `DOCS_ENABLED=false` en producción evita exponer la documentación de la API.
- **Rate limiting** — Nginx limita las peticiones API a 30 req/s por IP (burst de 20).
- **Historial git limpio** — ninguna credencial ni secreto ha sido commiteado al repositorio.

---

## Monitoreo

### Logs estructurados (structlog)

Todos los logs incluyen campos de contexto (`method`, `path`, `status`, `latency_ms`). Nivel configurable vía `LOG_LEVEL`. Las rutas `/health`, `/health/db` y `/static` están excluidas del logging de requests.

### Health checks

| Endpoint | Descripción |
|---|---|
| `GET /health` | Health básico (siempre 200 si el proceso está corriendo) |
| `GET /health/db` | Health con verificación de conexión a DB (503 si la DB no está disponible) |

> Para el despliegue a producción, ver **[docs/DEPLOY.md](docs/DEPLOY.md)**.
