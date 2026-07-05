#!/usr/bin/env bash
set -euo pipefail

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

DEV_DOCKER="docker compose -f docker-compose.yml -f docker-compose.override.yml -f docker-compose.dev.yml"

# Check .env at startup
if [[ ! -f .env ]]; then
  echo -e "${YELLOW}[warn] .env not found — copy .env.example and fill in values${NC}"
fi

CMD="${1:-help}"
shift || true  # remaining args available as "$@"

case "$CMD" in
  db)
    echo -e "${GREEN}Starting database...${NC}"
    docker compose up -d
    ;;

  db-down)
    echo -e "${YELLOW}Stopping database...${NC}"
    docker compose down
    ;;

  start)
    echo -e "${GREEN}Starting database + backend...${NC}"
    docker compose up -d
    echo -e "${CYAN}Waiting for database to be ready...${NC}"
    sleep 2
    uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
    ;;

  serve)
    echo -e "${GREEN}Starting backend (native)...${NC}"
    uv run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
    ;;

  migrate)
    echo -e "${CYAN}Running migrations...${NC}"
    uv run alembic upgrade head
    echo -e "${GREEN}Migrations applied.${NC}"
    ;;

  migration)
    if [[ -z "${1:-}" ]]; then
      echo -e "${RED}[error] Usage: ./dev.sh migration <message>${NC}" >&2
      exit 1
    fi
    echo -e "${CYAN}Generating migration: $1${NC}"
    uv run alembic revision --autogenerate -m "$1"
    ;;

  seed)
    echo -e "${CYAN}Seeding database...${NC}"
    uv run python seeds/seed_data.py
    echo -e "${GREEN}Seed complete.${NC}"
    ;;

  docker-up)
    echo -e "${GREEN}Levantando backend en Docker (hot-reload)...${NC}"
    $DEV_DOCKER up -d --build
    $DEV_DOCKER exec -T backend uv run alembic upgrade head
    $DEV_DOCKER exec -T backend uv run python seeds/seed_data.py
    echo -e "${GREEN}Listo. http://localhost:8000${NC}"
    ;;

  docker-down)
    echo -e "${YELLOW}Deteniendo stack de desarrollo en Docker...${NC}"
    $DEV_DOCKER down
    ;;

  docker-logs)
    $DEV_DOCKER logs -f "$@"
    ;;

  docker-migrate)
    echo -e "${CYAN}Aplicando migraciones dentro del contenedor backend...${NC}"
    $DEV_DOCKER exec -T backend uv run alembic upgrade head
    echo -e "${GREEN}Migraciones aplicadas.${NC}"
    ;;

  docker-seed)
    echo -e "${CYAN}Sembrando datos dentro del contenedor backend...${NC}"
    $DEV_DOCKER exec -T backend uv run python seeds/seed_data.py
    echo -e "${GREEN}Seed completo.${NC}"
    ;;

  test)
    echo -e "${CYAN}Running tests...${NC}"
    uv run pytest "$@"
    ;;

  lint)
    echo -e "${CYAN}Linting...${NC}"
    uv run ruff check . && uv run ruff format --check .
    echo -e "${GREEN}Lint passed.${NC}"
    ;;

  format)
    echo -e "${CYAN}Formatting...${NC}"
    uv run ruff format .
    echo -e "${GREEN}Format complete.${NC}"
    ;;

  psql)
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    echo -e "${CYAN}Connecting to PostgreSQL...${NC}"
    docker compose exec db psql -U "${POSTGRES_USER}" "${POSTGRES_DB}"
    ;;

  health)
    echo -e "${CYAN}Checking health...${NC}"
    curl -s http://localhost:8000/health | python3 -m json.tool
    ;;

  backup)
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    mkdir -p backups
    STAMP=$(date +%Y%m%d_%H%M%S)
    LABEL="${1:-seeds}"
    FILE="backups/${LABEL}_${STAMP}.sql"
    echo -e "${CYAN}Creando snapshot → $FILE${NC}"
    docker compose exec -T db pg_dump \
      -U "${POSTGRES_USER}" \
      --data-only \
      -t institutions \
      -t service_types -t doc_types -t folder_statuses \
      -t prefix_corrections \
      -t admins -t contracts -t services -t service_type_documents \
      "${POSTGRES_DB}" > "$FILE"
    echo -e "${GREEN}Snapshot guardado: $FILE${NC}"
    ;;

  restore)
    if [[ -z "${1:-}" ]]; then
      echo -e "${RED}[error] Uso: ./dev.sh restore <archivo.sql>${NC}" >&2
      exit 1
    fi
    # shellcheck disable=SC1091
    source .env 2>/dev/null || true
    echo -e "${YELLOW}Restaurando desde $1 ...${NC}"
    docker compose exec -T db psql -U "${POSTGRES_USER}" "${POSTGRES_DB}" < "$1"
    echo -e "${GREEN}Restauración completa.${NC}"
    ;;

  nuke)
    echo -e "${RED}${BOLD}WARNING: This will destroy ALL volumes including the database.${NC}"
    read -rp "Type 'yes' to confirm: " confirm
    if [[ "$confirm" == "yes" ]]; then
      docker compose down -v
      echo -e "${GREEN}Done. All volumes removed.${NC}"
    else
      echo "Aborted."
    fi
    ;;

  help|*)
    echo -e "${BOLD}Usage:${NC} ./dev.sh <command> [args]"
    echo ""
    echo -e "${BOLD}Database (Docker):${NC}"
    echo -e "  ${GREEN}db${NC}                   Start PostgreSQL container"
    echo -e "  ${GREEN}db-down${NC}              Stop PostgreSQL container"
    echo -e "  ${GREEN}psql${NC}                 Connect to PostgreSQL via psql"
    echo -e "  ${GREEN}backup${NC} [nombre]      Snapshot de tablas base → backups/<nombre>_TIMESTAMP.sql"
    echo -e "  ${GREEN}restore${NC} <archivo>    Restaurar tablas desde un snapshot SQL"
    echo -e "  ${RED}nuke${NC}                 Destroy all volumes (asks confirmation)"
    echo ""
    echo -e "${BOLD}Backend (native):${NC}"
    echo -e "  ${GREEN}start${NC}                Start database + backend (todo en uno)
  ${GREEN}serve${NC}                Start uvicorn with hot-reload (0.0.0.0:8000)"
    echo -e "  ${GREEN}migrate${NC}              Apply pending Alembic migrations"
    echo -e "  ${GREEN}migration${NC} <msg>      Generate a new Alembic migration"
    echo -e "  ${GREEN}seed${NC}                 Run database seed script"
    echo -e "  ${GREEN}test${NC} [args]          Run pytest"
    echo -e "  ${GREEN}lint${NC}                 Run ruff check + format check"
    echo -e "  ${GREEN}format${NC}               Auto-format code with ruff"
    echo -e "  ${GREEN}health${NC}               Check /health endpoint (localhost:8000)"
    echo ""
    echo -e "${BOLD}Docker (backend contenedorizado, alternativa a serve):${NC}"
    echo -e "  ${GREEN}docker-up${NC}            Build + up (db+backend+extras) + migrate + seed, hot-reload"
    echo -e "  ${YELLOW}docker-down${NC}          Detener todo (db, backend, Adminer)"
    echo -e "  ${GREEN}docker-logs${NC} [svc]    Ver logs en tiempo real"
    echo -e "  ${GREEN}docker-migrate${NC}       Aplicar migraciones dentro del contenedor (sin uv local)"
    echo -e "  ${GREEN}docker-seed${NC}          Sembrar datos dentro del contenedor (sin uv local)"
    echo ""
    echo -e "${CYAN}Para producción usa ./deploy.sh (ver ./deploy.sh help)${NC}"
    echo ""
    if [[ "$CMD" != "help" ]]; then
      echo -e "${RED}[error] Unknown command: $CMD${NC}" >&2
      exit 1
    fi
    ;;
esac
