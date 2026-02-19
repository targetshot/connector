#!/bin/sh
set -eu

log() {
  printf '%s\n' "[mariadb-mirror-init] $*"
}

sql_escape() {
  # Escape single quotes for SQL string literals.
  printf "%s" "$1" | sed "s/'/''/g"
}

is_true() {
  case "$(printf "%s" "${1:-}" | tr '[:upper:]' '[:lower:]')" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

ROOT_PASSWORD="${MARIADB_ROOT_PASSWORD:-}"
if [ -z "$ROOT_PASSWORD" ]; then
  log "MARIADB_ROOT_PASSWORD fehlt, überspringe Bootstrap."
  exit 0
fi

MIRROR_USER="${TS_CONNECT_MIRROR_DB_USER:-debezium_sync}"
MIRROR_PASSWORD="${TS_CONNECT_MIRROR_DB_PASSWORD:-${MARIADB_PASSWORD:-}}"

if [ -n "$MIRROR_USER" ] && [ -n "$MIRROR_PASSWORD" ]; then
  user_esc="$(sql_escape "$MIRROR_USER")"
  pass_esc="$(sql_escape "$MIRROR_PASSWORD")"
  mariadb --protocol=socket -uroot "-p${ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS '${user_esc}'@'%' IDENTIFIED BY '${pass_esc}';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO '${user_esc}'@'%';
FLUSH PRIVILEGES;
SQL
  log "Debezium-Benutzer '${MIRROR_USER}' ist bereit."
else
  log "TS_CONNECT_MIRROR_DB_USER/TS_CONNECT_MIRROR_DB_PASSWORD nicht vollständig gesetzt, überspringe Benutzeranlage."
fi

SOURCE_HOST="${TS_CONNECT_SOURCE_DB_HOST:-}"
SOURCE_PORT="${TS_CONNECT_SOURCE_DB_PORT:-3306}"
SOURCE_USER="${TS_CONNECT_SOURCE_DB_REPL_USER:-}"
SOURCE_PASSWORD="${TS_CONNECT_SOURCE_DB_REPL_PASSWORD:-}"
CONNECT_RETRY="${TS_CONNECT_SOURCE_DB_CONNECT_RETRY:-10}"

if [ -z "$SOURCE_HOST" ]; then
  log "TS_CONNECT_SOURCE_DB_HOST ist leer, keine Replikation konfiguriert."
  exit 0
fi

if [ -z "$SOURCE_USER" ] || [ -z "$SOURCE_PASSWORD" ]; then
  log "Replikations-User/Passwort fehlen, Replikation wird nicht eingerichtet."
  exit 0
fi

source_host_esc="$(sql_escape "$SOURCE_HOST")"
source_user_esc="$(sql_escape "$SOURCE_USER")"
source_pass_esc="$(sql_escape "$SOURCE_PASSWORD")"

change_stmt=""
if is_true "${TS_CONNECT_SOURCE_DB_GTID_MODE:-true}"; then
  change_stmt="CHANGE MASTER TO MASTER_HOST='${source_host_esc}', MASTER_PORT=${SOURCE_PORT}, MASTER_USER='${source_user_esc}', MASTER_PASSWORD='${source_pass_esc}', MASTER_CONNECT_RETRY=${CONNECT_RETRY}, MASTER_USE_GTID=slave_pos;"
else
  SOURCE_LOG_FILE="${TS_CONNECT_SOURCE_DB_LOG_FILE:-}"
  SOURCE_LOG_POS="${TS_CONNECT_SOURCE_DB_LOG_POS:-}"
  if [ -z "$SOURCE_LOG_FILE" ] || [ -z "$SOURCE_LOG_POS" ]; then
    log "GTID aus und keine Log-Position angegeben (TS_CONNECT_SOURCE_DB_LOG_FILE/POS). Überspringe Replikations-Setup."
    exit 0
  fi
  source_log_file_esc="$(sql_escape "$SOURCE_LOG_FILE")"
  change_stmt="CHANGE MASTER TO MASTER_HOST='${source_host_esc}', MASTER_PORT=${SOURCE_PORT}, MASTER_USER='${source_user_esc}', MASTER_PASSWORD='${source_pass_esc}', MASTER_CONNECT_RETRY=${CONNECT_RETRY}, MASTER_LOG_FILE='${source_log_file_esc}', MASTER_LOG_POS=${SOURCE_LOG_POS};"
fi

set +e
mariadb --protocol=socket -uroot "-p${ROOT_PASSWORD}" <<SQL
STOP SLAVE;
RESET SLAVE ALL;
${change_stmt}
START SLAVE;
SQL
status=$?
set -e

if [ "$status" -ne 0 ]; then
  log "WARN: Replikation konnte nicht initialisiert werden. Prüfe Source-Host, Benutzerrechte und Binlog/GTID-Konfiguration."
  exit 0
fi

log "Replikation zur Vereins-Meyton-DB wurde eingerichtet."
