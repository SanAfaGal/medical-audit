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

## Comandos de desarrollo (`dev.sh`)

```bash
./dev.sh help                          # Listar todos los comandos
```

### Base de datos (Docker)

```bash
./dev.sh db                            # Iniciar PostgreSQL
./dev.sh db-down                       # Detener PostgreSQL
./dev.sh psql                          # Conectar vía psql
./dev.sh backup [nombre]               # Snapshot de tablas de config → backups/<nombre>_TIMESTAMP.sql
./dev.sh restore <archivo.sql>         # Restaurar desde snapshot
./dev.sh nuke                          # Destruir todos los volúmenes (pide confirmación)
```

### Backend (nativo)

```bash
./dev.sh start                         # Iniciar base de datos + backend juntos
./dev.sh serve                         # Iniciar uvicorn con hot-reload en 0.0.0.0:8000
./dev.sh migrate                       # Aplicar migraciones Alembic pendientes
./dev.sh migration "describe cambio"   # Generar nueva migración (auto-detecta cambios de schema)
./dev.sh seed                          # Poblar base de datos con datos iniciales
```

### Testing y calidad

```bash
./dev.sh test                          # Ejecutar pytest
./dev.sh test -k test_invoice -v       # Filtrar tests por nombre
./dev.sh test -m "not db and not slow" # Solo tests unitarios rápidos
./dev.sh test --cov=core,app           # Con reporte de cobertura
./dev.sh lint                          # ruff check + format check (seguro para CI)
./dev.sh format                        # Auto-corregir formato con ruff
./dev.sh health                        # Verificar endpoint /health
```

---

## Migraciones

El proyecto usa [Alembic](https://alembic.sqlalchemy.org/) para migraciones de schema.

```bash
# Aplicar todas las migraciones pendientes
./dev.sh migrate

# Crear una nueva migración (auto-detecta cambios en los modelos)
./dev.sh migration "descripción del cambio"

# Comandos directos de Alembic
uv run alembic current          # Ver revisión actual
uv run alembic history          # Ver historial de migraciones
uv run alembic downgrade -1     # Revertir una migración
```

---

## Backups de configuración (desarrollo local)

Las tablas de configuración (`institutions`, `service_types`, `doc_types`, `folder_statuses`, `prefix_corrections`, `admins`, `contracts`, `services`, `service_type_documents`) pueden llevar tiempo en reconstruirse. Los comandos `backup`/`restore` permiten hacer snapshots sin tocar datos operacionales (`audit_periods`, `invoices`, `missing_files`).

Esta sección es para **desarrollo local** (`dev.sh`, contra `docker-compose.yml`). Para producción usa los mismos comandos vía `./deploy.sh backup` / `./deploy.sh restore` — ver [docs/DEPLOY.md](DEPLOY.md).

```bash
# Crear snapshot
./dev.sh backup configuracion_base
# → backups/configuracion_base_20260322_120000.sql

# Listar snapshots
ls -lh backups/

# Restaurar snapshot
./dev.sh restore backups/configuracion_base_20260322_120000.sql
```

Los archivos `backups/*.sql` están excluidos de git por defecto.

---

## Testing

### Framework

pytest + pytest-asyncio con cobertura mínima de 60% en `core/` y `app/`.

### Markers

```python
@pytest.mark.db       # Requiere PostgreSQL activo (lento, puede modificar DB)
@pytest.mark.slow     # Tests de larga duración (archivos grandes)
@pytest.mark.pdf      # Requiere fixtures PDF reales
```

### Comandos

```bash
./dev.sh test                                    # Todos los tests
./dev.sh test tests/core/test_scanner.py         # Archivo específico
./dev.sh test -k "test_validate"                 # Tests que coincidan con el patrón
./dev.sh test -m "not db and not slow"           # Solo tests unitarios rápidos
./dev.sh test --cov=core,app --cov-report=html   # Con reporte de cobertura HTML

# Type checking (sin wrapper en dev.sh)
uv run mypy app/
```
