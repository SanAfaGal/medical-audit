# Guía de desarrollo

> Ver el [README](../README.md) para la visión general del proyecto, el stack y el setup rápido. Ver [docs/DEPLOY.md](DEPLOY.md) para producción.

---

## Variables de entorno

Todas las variables son leídas por `app/config.py` vía pydantic-settings.

| Variable | Requerida | Default | Descripción |
|---|---|---|---|
| `DATABASE_URL` | Sí | — | Cadena de conexión asyncpg. Usar `localhost:5432` porque el backend corre nativamente. |
| `SECRET_KEY` | Sí | — | Clave de 32 bytes en base64 para cifrado de credenciales almacenadas. |
| `APP_PASSWORD` | Sí | — | Contraseña compartida (HTTP Basic Auth, usuario fijo `admin`) que protege toda la app excepto `/health` y `/health/db`. |
| `POSTGRES_USER` | Sí | — | Usuario PostgreSQL (usado por el contenedor Docker). |
| `POSTGRES_PASSWORD` | Sí | — | Contraseña PostgreSQL. |
| `POSTGRES_DB` | Sí | — | Nombre de la base de datos PostgreSQL. |
| `LOG_LEVEL` | No | `INFO` | Nivel de structlog: `DEBUG`, `INFO`, `WARNING`, `ERROR`. |
| `DOCS_ENABLED` | No | `false` | `true` para habilitar `/docs` y `/redoc`. Nunca activar en producción. |

> `.env` está en `.gitignore` y nunca debe commitearse. Usar `.env.example` como punto de partida.

---

## Comandos de desarrollo

Docker Compose aplica `docker-compose.override.yml` automáticamente (puerto de db, Adminer, backend con hot-reload). Las variables `POSTGRES_USER` y `POSTGRES_DB` salen de `.env` (`source .env` en bash).

### Base de datos (Docker)

```bash
docker compose up -d db adminer      # Iniciar PostgreSQL + Adminer (no levanta el backend)
docker compose down                  # Detener
docker compose exec db psql -U "$POSTGRES_USER" "$POSTGRES_DB"   # psql
docker compose down -v               # Destruir TODOS los volúmenes (borra la base de datos)
```

### Backend nativo

```bash
uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload   # Hot-reload en :8000
uv run alembic upgrade head          # Aplicar migraciones pendientes
uv run python seeds/seed_data.py     # Datos iniciales
```

### Backend en Docker (alternativa)

```bash
docker compose up -d --build db adminer backend
docker compose exec -T backend uv run alembic upgrade head
docker compose exec -T backend uv run python seeds/seed_data.py
docker compose logs -f backend
docker compose down
```

### Testing y calidad

```bash
uv run pytest                              # Todos los tests
uv run pytest -k test_invoice -v           # Filtrar por nombre
uv run pytest -m "not db and not slow"     # Solo tests unitarios rápidos
uv run pytest --cov=app                    # Con cobertura
uv run ruff check . && uv run ruff format --check .   # Lint (como CI)
uv run ruff format .                       # Auto-formatear
uv run mypy app/                           # Type checking
curl -s http://localhost:8000/health       # Verificar /health
```

---

## Migraciones

El proyecto usa [Alembic](https://alembic.sqlalchemy.org/) para migraciones de schema.

```bash
# Aplicar todas las migraciones pendientes
uv run alembic upgrade head

# Crear una nueva migración (auto-detecta cambios en los modelos)
uv run alembic revision --autogenerate -m "descripción del cambio"

# Comandos directos de Alembic
uv run alembic current          # Ver revisión actual
uv run alembic history          # Ver historial de migraciones
uv run alembic downgrade -1     # Revertir una migración
```

---

## Backups de configuración (desarrollo local)

Las tablas de configuración (`institutions`, `service_types`, `doc_types`, `folder_statuses`, `prefix_corrections`, `admins`, `contracts`, `services`, `service_type_documents`) pueden llevar tiempo en reconstruirse. Los comandos de backup/restore permiten hacer snapshots sin tocar datos operacionales (`audit_periods`, `invoices`, `missing_files`).

Esta sección es para **desarrollo local** (contra `docker-compose.yml` + `docker-compose.override.yml`). Para producción usa los mismos comandos con `docker compose -f docker-compose.yml` — ver [docs/DEPLOY.md](DEPLOY.md).

```bash
# Crear snapshot
mkdir -p backups
docker compose exec -T db pg_dump -U "$POSTGRES_USER" --data-only -t institutions -t service_types -t doc_types -t folder_statuses -t prefix_corrections -t admins -t contracts -t services -t service_type_documents "$POSTGRES_DB" > backups/configuracion_base_$(date +%Y%m%d_%H%M%S).sql
# → backups/configuracion_base_20260322_120000.sql

# Listar snapshots
ls -lh backups/

# Restaurar snapshot
docker compose exec -T db psql -U "$POSTGRES_USER" "$POSTGRES_DB" < backups/configuracion_base_20260322_120000.sql
```

Los archivos `backups/*.sql` están excluidos de git por defecto.

---

## Testing

### Framework

pytest + pytest-asyncio con cobertura mínima de 60% en `app/`.

### Markers

```python
@pytest.mark.db       # Requiere PostgreSQL activo (lento, puede modificar DB)
@pytest.mark.slow     # Tests de larga duración (archivos grandes)
@pytest.mark.pdf      # Requiere fixtures PDF reales
```

### Comandos

```bash
uv run pytest   # Todos los tests
uv run pytest tests/app/core/test_scanner.py   # Archivo específico
uv run pytest -k "test_validate"   # Tests que coincidan con el patrón
uv run pytest -m "not db and not slow"   # Solo tests unitarios rápidos
uv run pytest --cov=app --cov-report=html   # Con reporte de cobertura HTML

# Type checking
uv run mypy app/
```
