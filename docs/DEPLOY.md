# Despliegue a producción

Stack: `docker-compose.prod.yml` — tres contenedores orquestados, datos persistentes en volúmenes Docker nombrados, completamente aislado del entorno de desarrollo.

Todos los comandos de este documento tienen un wrapper en `./deploy.sh` (ver `./deploy.sh help`). Este documento usa esos wrappers como camino principal; el comando `docker compose -f docker-compose.prod.yml ...` equivalente se muestra debajo cuando aporta como referencia directa.

> `./deploy.sh` es solo para producción. Para desarrollo local usa `./dev.sh` — ver [docs/DEVELOPMENT.md](DEVELOPMENT.md).

---

## Referencia de comandos

```bash
./deploy.sh help                       # Listar todos los comandos
```

```bash
./deploy.sh up                         # Primer deploy: build + up + migrate + seed
./deploy.sh up-clean                   # Primer deploy SIN cache
./deploy.sh update                     # Actualizar tras git pull: build + up + migrate
./deploy.sh down                       # Detener stack de producción
./deploy.sh ps                         # Estado de los contenedores
./deploy.sh logs [servicio]            # Ver logs en tiempo real
./deploy.sh migrate                    # Aplicar migraciones Alembic
./deploy.sh seed                       # Ejecutar seed de datos iniciales
./deploy.sh psql                       # Conectar vía psql
./deploy.sh health                     # Verificar /health (localhost:80)
./deploy.sh backup [nombre]            # Snapshot de tablas de config → backups/<nombre>_TIMESTAMP.sql
./deploy.sh backup-full                # Backup completo de la base de datos
./deploy.sh restore <archivo.sql>      # Restaurar snapshot de configuración
./deploy.sh restore-full <archivo>     # Restaurar backup completo (detiene el backend)
```

---

## Requisitos del servidor

| Requisito | Mínimo | Recomendado |
|---|---|---|
| OS | Windows 10 / Ubuntu 22.04 | Ubuntu 22.04 LTS |
| RAM | 2 GB | 4 GB |
| CPU | 2 núcleos | 4 núcleos |
| Disco | 20 GB libres | 50 GB libres |
| Docker | Desktop 4.x / Engine 24.x | Engine 24.x |
| Docker Compose | v2.20+ | v2.20+ |

> **En Windows:** Docker Desktop debe estar corriendo antes de ejecutar cualquier comando.

---

## Primer despliegue — paso a paso

### 1. Clonar el repositorio

```bash
git clone <repo-url>
cd medical-audit-v2
```

### 2. Configurar el entorno

```bash
cp .env.example .env
```

Editar `.env` con los valores de producción:

```env
# Base de datos — en producción Docker resuelve "db" internamente, este valor
# solo importa si alguna vez corres el backend fuera del compose
DATABASE_URL=postgresql+asyncpg://audit:PASSWORD_SEGURA@localhost:5432/medical_audit

# Clave de cifrado AES — generar con:
# python -c "import secrets,base64; print(base64.b64encode(secrets.token_bytes(32)).decode())"
SECRET_KEY=REEMPLAZAR_CON_CLAVE_GENERADA

# Credenciales de PostgreSQL (usadas por el contenedor db)
POSTGRES_USER=audit
POSTGRES_PASSWORD=PASSWORD_SEGURA
POSTGRES_DB=medical_audit

# Opcionales
LOG_LEVEL=INFO
DOCS_ENABLED=false
```

> Nunca commitear `.env`. Verificar que está en `.gitignore` antes de continuar.

### 3. Construir, iniciar, migrar y cargar datos iniciales

```bash
./deploy.sh up
```

Equivale a: `build` + `up -d` + `alembic upgrade head` + `seed_data.py`. La primera vez tarda 5-15 minutos: instala el venv de Python y descarga Playwright Chromium (~300 MB).

Progreso esperado durante el build:
```
[+] Building
 => [full 1/7] FROM python:3.11-slim
 => [full 2/7] COPY pyproject.toml uv.lock
 => [full 3/7] RUN uv sync ...
 => [full 4/7] COPY app/ core/ alembic/
 => [full 5/7] RUN playwright install chromium
 => [full 6/7] RUN useradd appuser
 => exporting to image
```

Verifica que los tres contenedores están corriendo:

```bash
./deploy.sh ps
# equivalente: docker compose -f docker-compose.prod.yml ps
```

Salida esperada:
```
NAME                          IMAGE                    STATUS
medical-audit-prod-db         postgres:16-alpine       running (healthy)
medical-audit-prod-backend    medical-audit-prod-...   running (healthy)
medical-audit-prod-nginx      nginx:1.27-alpine        running
```

> Si `medical-audit-prod-db` aparece como `starting` espera unos segundos — el healthcheck de PostgreSQL tarda hasta 50 segundos en pasar.

> Si necesitas reconstruir sin caché (por ejemplo tras un build corrupto), usa `./deploy.sh up-clean`.

### 4. Verificar que la aplicación responde

```bash
# Desde el mismo servidor:
curl http://localhost/health
curl http://localhost/health/db

# Desde otro dispositivo en la red (reemplazar con la IP del servidor):
curl http://192.168.1.X/health
```

Respuesta esperada en ambos: `{"status": "ok"}`

### 5. Configurar la ruta de auditoría

Abrir la aplicación en el navegador → **Configuración → Sistema** → establecer:

```
audit_data_root = /audit_data
```

Esta ruta apunta al volumen Docker `medical-audit-prod_audit_data` donde se almacenan todos los archivos de auditoría (DRIVE, STAGE, AUDIT, exports).

---

## Arquitectura en producción

```
  Dispositivos LAN
        │
        ▼ :80
  ┌─────────────────────┐
  │  medical-audit-prod │  Docker network: medical-audit-prod_default
  │  ─────────────────  │
  │  nginx :80          │──► backend :8000 ──► db :5432
  │  (rate limit, gzip) │
  │  (SSE timeout 4h)   │
  └─────────────────────┘
        │                         │                    │
   puerto 80                 sin puerto           sin puerto
   expuesto                  al host              al host
   al host
```

- **nginx** es el único punto de entrada. Gestiona rate limiting (30 req/s), gzip, y timeouts especiales para SSE del pipeline.
- **backend** nunca es accesible directamente desde fuera del stack Docker.
- **db** solo es accesible desde `backend` dentro de la red Docker.

---

## ⚠️ Migración especial: consolidación del historial de Alembic (una sola vez, 2026-07-04)

> Este aviso aplica **una única vez**, la primera vez que se despliegue el commit que reemplaza
> las 15 migraciones viejas (`db5ece233ada` … `94c9b6697c6b`) por una sola `05e49443f9b5_initial_schema.py`.
> Borrar esta sección después de aplicarlo en producción.

El historial de Alembic fue consolidado en una sola migración cuyo contenido produce **el mismo
schema** que las 15 migraciones anteriores — verificado por diff de `pg_dump --schema-only` entre
una base de datos con el historial viejo aplicado completo y otra con solo la migración nueva.
La base de datos de producción ya tiene ese schema aplicado — no hay que crear nada, solo decirle
a Alembic que el punto de partida cambió de nombre.

> Diferencias menores encontradas y confirmadas inofensivas durante la verificación (no las
> introduce este cambio, ya existían): los nombres de algunas secuencias/constraints en `agreements`
> y `contracts` conservan nombres heredados de renames históricos (`institution_contracts_id_seq`,
> `contracts_global_pkey`, etc.) que no se tocan — nada en el código los referencia por nombre.
> También hay dos casos de *drift* modelo↔BD preexistentes: la columna `institutions.base_path`
> (huérfana, ningún código la usa) y el default `DEFAULT 10` a nivel de BD en `service_types.priority`
> (el modelo solo declara un default de Python, no `server_default`). Ninguno afecta este cambio.

**Antes de desplegar**, backup preventivo (como siempre):
```bash
./deploy.sh backup-full
```

**Desplegar el código nuevo normalmente:**
```bash
git pull origin main
docker compose -f docker-compose.prod.yml up -d --build backend
```

**El paso automático de `deploy.sh update` que corre `alembic upgrade head` va a fallar** con
`Can't locate revision identified by '94c9b6697c6b'` — es esperado y no daña nada (Alembic se
niega a actuar porque no reconoce el punto de partida, no intenta recrear tablas existentes).
Corregirlo con un `stamp --purge` (marca la revisión actual sin ejecutar ningún DDL, porque el
schema ya coincide — `--purge` es necesario porque un `stamp` normal también falla al no
reconocer la revisión vieja):

```bash
docker compose -f docker-compose.prod.yml exec backend alembic stamp --purge head
```

**Verificar:**
```bash
docker compose -f docker-compose.prod.yml exec backend alembic current
# Debe mostrar 05e49443f9b5 (head)
```

De ahí en adelante, `./deploy.sh update` vuelve a funcionar normal para futuras migraciones.

---

## Actualizar a una nueva versión

```bash
# 1. Verificar CI verde en GitHub Actions

# 2. Backup preventivo (obligatorio antes de cada deploy)
./deploy.sh backup pre-deploy   # ver sección Backups

# 3. Bajar los cambios
git pull origin main

# 4. Reconstruir, iniciar y migrar (solo reconstruye lo que cambió)
./deploy.sh update

# 5. Verificar
./deploy.sh health
curl http://localhost/health/db
./deploy.sh logs backend
```

---

## Rollback

### Sin migraciones de base de datos

```bash
git checkout <commit-anterior>
docker compose -f docker-compose.prod.yml up -d --build backend
```

### Con migraciones de base de datos

```bash
# 1. Restaurar backup de configuración tomado antes del deploy
./deploy.sh restore backups/pre-deploy_TIMESTAMP.sql

# 2. Revertir la migración de Alembic (sin wrapper — comando directo)
docker compose -f docker-compose.prod.yml exec backend alembic downgrade -1

# 3. Volver al código anterior
git checkout <commit-anterior>
docker compose -f docker-compose.prod.yml up -d --build backend
```

---

## Backups

Los datos críticos están en dos lugares: la base de datos PostgreSQL y el volumen `audit_data` (PDFs).

### Backup de tablas de configuración

Las tablas de configuración (instituciones, tipos de servicio, mapeos, reglas) son las más difíciles de reconstruir. Se recomienda hacer este backup antes de cada deploy y al terminar cada sesión de configuración.

```bash
./deploy.sh backup [nombre]
# → backups/<nombre>_TIMESTAMP.sql (default nombre: "config")
```

### Backup completo de la base de datos

Incluye todo: configuración + periodos + facturas + hallazgos.

```bash
./deploy.sh backup-full
# → backups/full_TIMESTAMP.dump
```

> El backup completo usa `--format=custom` (binario comprimido). Para restaurarlo se usa `pg_restore`, que es lo que hace `./deploy.sh restore-full`.

### Backup del volumen de auditoría (PDFs)

Sin wrapper — comando directo:

```bash
mkdir -p backups

docker run --rm \
  -v medical-audit-prod_audit_data:/data:ro \
  -v "$(pwd)/backups":/backups \
  alpine tar czf /backups/audit_data_$(date +%Y%m%d_%H%M%S).tar.gz -C /data .
```

---

### Restaurar tablas de configuración

```bash
./deploy.sh restore backups/config_TIMESTAMP.sql
```

### Restaurar base de datos completa

```bash
./deploy.sh restore-full backups/full_TIMESTAMP.dump
```

Detiene el `backend` antes de restaurar (evita escrituras concurrentes) y lo reinicia al terminar.

### Restaurar volumen de auditoría

Sin wrapper — comando directo:

```bash
docker run --rm \
  -v medical-audit-prod_audit_data:/data \
  -v "$(pwd)/backups":/backups \
  alpine tar xzf /backups/audit_data_TIMESTAMP.tar.gz -C /data
```

---

### Automatizar backups diarios

**En Linux (cron):**

```bash
crontab -e
```

Agregar:
```
0 2 * * * cd /ruta/al/proyecto && ./deploy.sh backup-full
```

**En Windows (Task Scheduler):**

Crear `scripts/backup-daily.bat`:
```bat
@echo off
cd /d C:\ruta\al\proyecto\medical-audit-v2
bash deploy.sh backup-full
```

Programar en **Task Scheduler → Create Basic Task → Daily → 02:00**.

---

## Gestión de contenedores

```bash
./deploy.sh ps                 # Ver estado de todos los contenedores
./deploy.sh logs                # Ver logs en tiempo real (todos los servicios)
./deploy.sh logs backend        # Ver logs solo del backend
./deploy.sh psql                 # Conectar directamente a la base de datos
./deploy.sh down                 # Detener todos los contenedores (conserva volúmenes y datos)
```

Sin wrapper — comandos directos:

```bash
# Reiniciar un servicio (sin reconstruir imagen)
docker compose -f docker-compose.prod.yml restart backend

# Abrir shell dentro del backend
docker compose -f docker-compose.prod.yml exec backend bash

# Detener y borrar TODOS los datos — IRREVERSIBLE
docker compose -f docker-compose.prod.yml down -v
```

---

## Acceso desde la red LAN

La aplicación es accesible desde cualquier dispositivo en la misma red local.

```bash
# Obtener la IP del servidor (Windows)
ipconfig   # buscar "IPv4 Address"

# Obtener la IP del servidor (Linux)
ip addr show | grep "inet "
```

Si no responde desde otros dispositivos, abrir el puerto 80 en el firewall:

```powershell
# Windows — ejecutar como Administrador
New-NetFirewallRule -DisplayName "Medical Audit Prod" -Direction Inbound -Protocol TCP -LocalPort 80 -Action Allow
```

```bash
# Ubuntu/Debian
sudo ufw allow 80/tcp
```

---

## Referencia de contenedores y volúmenes

| Contenedor | Imagen | Rol |
|---|---|---|
| `medical-audit-prod-db` | `postgres:16-alpine` | Base de datos PostgreSQL |
| `medical-audit-prod-backend` | `python:3.11-slim` + app | FastAPI + Ghostscript + Playwright |
| `medical-audit-prod-nginx` | `nginx:1.27-alpine` | Proxy inverso, rate limiting, gzip |

| Volumen | Contenido | Crítico |
|---|---|---|
| `medical-audit-prod_pgdata` | Datos de PostgreSQL | Sí — respaldar diariamente |
| `medical-audit-prod_audit_data` | PDFs auditados, ZIPs exportados | Medio — recuperables desde Drive |
