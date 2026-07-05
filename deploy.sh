#!/usr/bin/env bash
set -euo pipefail

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

COMPOSE="docker compose -f docker-compose.prod.yml"

# Check .env at startup
if [[ ! -f .env ]]; then
  echo -e "${YELLOW}[warn] .env not found — copy .env.example and fill in production values${NC}"
fi

CMD="${1:-help}"
shift || true  # remaining args available as "$@"

case "$CMD" in
  up)
    echo -e "${GREEN}Desplegando producción (con cache)...${NC}"
    $COMPOSE up --build -d
    $COMPOSE exec -T backend uv run alembic upgrade head
    $COMPOSE exec -T backend uv run python seeds/seed_data.py
    echo -e "${GREEN}Deploy completo.${NC}"
    ;;

  up-clean)
    echo -e "${GREEN}Desplegando producción (SIN cache)...${NC}"
    $COMPOSE build --no-cache
    $COMPOSE up -d
    $COMPOSE exec -T backend uv run alembic upgrade head
    $COMPOSE exec -T backend uv run python seeds/seed_data.py
    echo -e "${GREEN}Deploy completo.${NC}"
    ;;

  update)
    echo -e "${CYAN}Actualizando producción...${NC}"
    $COMPOSE up -d --build
    $COMPOSE exec -T backend uv run alembic upgrade head
    echo -e "${GREEN}Actualización completa.${NC}"
    ;;

  down)
    echo -e "${YELLOW}Deteniendo stack de producción...${NC}"
    $COMPOSE down
    ;;

  logs)
    $COMPOSE logs -f "$@"
    ;;

  ps)
    $COMPOSE ps
    ;;

  migrate)
    echo -e "${CYAN}Aplicando migraciones en producción...${NC}"
    $COMPOSE exec -T backend uv run alembic upgrade head
    echo -e "${GREEN}Migraciones aplicadas.${NC}"
    ;;

  seed)
    echo -e "${CYAN}Ejecutando seed en producción...${NC}"
    $COMPOSE exec -T backend uv run python seeds/seed_data.py
    echo -e "${GREEN}Seed completo.${NC}"
    ;;

  psql)
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    echo -e "${CYAN}Connecting to PostgreSQL (prod)...${NC}"
    $COMPOSE exec db psql -U "${POSTGRES_USER}" "${POSTGRES_DB}"
    ;;

  health)
    echo -e "${CYAN}Checking health...${NC}"
    curl -s http://localhost/health | python3 -m json.tool
    ;;

  backup)
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    mkdir -p backups
    STAMP=$(date +%Y%m%d_%H%M%S)
    LABEL="${1:-config}"
    FILE="backups/${LABEL}_${STAMP}.sql"
    echo -e "${CYAN}Creando snapshot de configuración → $FILE${NC}"
    $COMPOSE exec -T db pg_dump \
      -U "${POSTGRES_USER}" \
      --data-only \
      -t institutions \
      -t service_types -t doc_types -t folder_statuses \
      -t prefix_corrections \
      -t admins -t contracts -t services -t service_type_documents \
      "${POSTGRES_DB}" > "$FILE"
    echo -e "${GREEN}Snapshot guardado: $FILE${NC}"
    ;;

  backup-full)
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    mkdir -p backups
    STAMP=$(date +%Y%m%d_%H%M%S)
    FILE="backups/full_${STAMP}.dump"
    echo -e "${CYAN}Creando backup completo → $FILE${NC}"
    $COMPOSE exec -T db pg_dump -U "${POSTGRES_USER}" --format=custom "${POSTGRES_DB}" > "$FILE"
    echo -e "${GREEN}Backup guardado: $FILE${NC}"
    ;;

  restore)
    if [[ -z "${1:-}" ]]; then
      echo -e "${RED}[error] Uso: ./deploy.sh restore <archivo.sql>${NC}" >&2
      exit 1
    fi
    echo -e "${RED}${BOLD}WARNING: Esto sobreescribirá las tablas de configuración en producción con el contenido de $1.${NC}"
    read -rp "Escribe 'yes' para confirmar: " confirm
    if [[ "$confirm" != "yes" ]]; then
      echo "Abortado."
      exit 0
    fi
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    echo -e "${YELLOW}Restaurando configuración desde $1 ...${NC}"
    $COMPOSE exec -T db psql -U "${POSTGRES_USER}" "${POSTGRES_DB}" < "$1"
    echo -e "${GREEN}Restauración completa.${NC}"
    ;;

  restore-full)
    if [[ -z "${1:-}" ]]; then
      echo -e "${RED}[error] Uso: ./deploy.sh restore-full <archivo.dump>${NC}" >&2
      exit 1
    fi
    echo -e "${RED}${BOLD}WARNING: Esto DETIENE el backend y reemplaza TODA la base de datos con el contenido de $1. Esta acción no se puede deshacer.${NC}"
    read -rp "Escribe 'yes' para confirmar: " confirm
    if [[ "$confirm" != "yes" ]]; then
      echo "Abortado."
      exit 0
    fi
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    echo -e "${YELLOW}Deteniendo backend para restaurar sin escrituras concurrentes...${NC}"
    $COMPOSE stop backend
    echo -e "${YELLOW}Restaurando base de datos completa desde $1 ...${NC}"
    $COMPOSE exec -T db pg_restore -U "${POSTGRES_USER}" --clean --if-exists -d "${POSTGRES_DB}" < "$1"
    $COMPOSE start backend
    echo -e "${GREEN}Restauración completa.${NC}"
    ;;

  help|*)
    echo -e "${BOLD}Usage:${NC} ./deploy.sh <command> [args]"
    echo ""
    echo -e "${BOLD}Deploy:${NC}"
    echo -e "  ${GREEN}up${NC}                   Primer deploy: build (cache) + up + migrate + seed"
    echo -e "  ${GREEN}up-clean${NC}             Primer deploy SIN cache + up + migrate + seed"
    echo -e "  ${GREEN}update${NC}               Actualizar tras git pull: build (cache) + up + migrate"
    echo -e "  ${YELLOW}down${NC}                 Detener stack de producción (conserva volúmenes)"
    echo ""
    echo -e "${BOLD}Operación:${NC}"
    echo -e "  ${GREEN}ps${NC}                   Estado de los contenedores"
    echo -e "  ${GREEN}logs${NC} [svc]           Ver logs en tiempo real"
    echo -e "  ${GREEN}migrate${NC}              Aplicar migraciones Alembic pendientes"
    echo -e "  ${GREEN}seed${NC}                 Ejecutar seed de datos iniciales"
    echo -e "  ${GREEN}psql${NC}                 Conectar a PostgreSQL vía psql"
    echo -e "  ${GREEN}health${NC}               Verificar /health (localhost:80)"
    echo ""
    echo -e "${BOLD}Backups:${NC}"
    echo -e "  ${GREEN}backup${NC} [nombre]      Snapshot de tablas de configuración → backups/<nombre>_TIMESTAMP.sql"
    echo -e "  ${GREEN}backup-full${NC}          Backup completo (custom format) → backups/full_TIMESTAMP.dump"
    echo -e "  ${GREEN}restore${NC} <archivo>    Restaurar snapshot de configuración"
    echo -e "  ${GREEN}restore-full${NC} <arch>  Restaurar backup completo (detiene backend durante la restauración)"
    echo ""
    echo -e "${CYAN}Para desarrollo local usa ./dev.sh (ver ./dev.sh help)${NC}"
    echo ""
    if [[ "$CMD" != "help" ]]; then
      echo -e "${RED}[error] Unknown command: $CMD${NC}" >&2
      exit 1
    fi
    ;;
esac
