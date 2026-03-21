import asyncio
import html
import ipaddress
import json
import hashlib
import gzip
import logging
import platform
import os
import re
import secrets
import shutil
import socket
import sqlite3
import ssl
import stat
import string
import subprocess
from asyncio.subprocess import PIPE
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
import psycopg2
from psycopg2 import sql
from dotenv import dotenv_values
from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from starlette.background import BackgroundTask
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
import httpx
import operations_runtime as ops_runtime
from backup_runtime import (
    build_backup_filename,
    list_backup_files,
    next_daily_run,
    prune_backup_files,
    resolve_backup_dir,
    resolve_host_display_dir,
    scheduled_run_due,
)
from connector_config import CONNECT_SECRETS_PATH, build_connector_config
from file_utils import atomic_write_text as _atomic_write_text
from file_utils import fsync_directory as _fsync_directory
from file_utils import tmp_path_for as _tmp_path_for
from log_utils import (
    append_rotating_json_line,
    configure_rotating_logger,
    env_int_first,
    format_operation_message,
    make_operation_id,
    resolve_log_dir,
)
from security_bootstrap import (
    PASSWORD_PLACEHOLDER,
    PRIVATE_SECRET_FILE_MODE,
    UiSecurityBootstrap,
    require_admin_password,
    require_session_auth,
)
from licenses import (
    DEFAULT_LICENSE_TIER,
    DEFAULT_RETENTION_DAYS,
    LICENSE_RETENTION_DAYS,
    normalize_license_tier,
    plan_display_name,
    retention_for_license,
)
from host_agent_utils import get_host_agent_token
from update_agent_utils import get_update_agent_token
from update_state import UpdateStateManager

logger = logging.getLogger("ts-connect-ui")


def _env_int(name: str, default: int | None) -> int | None:
    value = os.getenv(name)
    if value is None:
        return default
    trimmed = value.strip()
    if not trimmed:
        return default
    try:
        return int(trimmed)
    except ValueError:
        logger.warning("Ungültiger Integer-Wert für %s: %s", name, trimmed)
        return default


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return None


_workspace_env_cache: dict[str, str] | None = None


def _workspace_env_values() -> dict[str, str]:
    global _workspace_env_cache
    if _workspace_env_cache is not None:
        return _workspace_env_cache
    env_path = WORKSPACE_PATH / ".env"
    if not env_path.exists():
        _workspace_env_cache = {}
        return _workspace_env_cache
    try:
        values = dotenv_values(env_path)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Konnte Workspace-.env nicht lesen: %s", exc)
        values = {}
    normalized: dict[str, str] = {}
    for key, value in values.items():
        if not key or value is None:
            continue
        normalized[str(key)] = str(value)
    _workspace_env_cache = normalized
    return _workspace_env_cache

APP_PORT = int(os.getenv("PORT", "8080"))
CONNECT_BASE_URL = os.getenv("CONNECT_BASE_URL", "http://kafka-connect:8083")
DEFAULT_CONNECTOR_NAME = os.getenv("DEFAULT_CONNECTOR_NAME", "targetshot-debezium")
BACKUP_CONNECTOR_NAME = os.getenv("BACKUP_CONNECTOR_NAME", f"{DEFAULT_CONNECTOR_NAME}-backup-sink")
TRUSTED_CIDRS = [c.strip() for c in os.getenv(
    "UI_TRUSTED_CIDRS",
    "192.168.0.0/16,10.0.0.0/8,172.16.0.0/12"
).split(",")]
UI_BIND_IP = (os.getenv("UI_BIND_IP", "0.0.0.0") or "0.0.0.0").strip() or "0.0.0.0"
WORKSPACE_PATH = Path(os.getenv("TS_CONNECT_WORKSPACE", "/workspace"))
WORKSPACE_HOST_PATH = (os.getenv("TS_CONNECT_WORKSPACE_HOST") or "").strip()
LOCAL_TIMEZONE = datetime.now().astimezone().tzinfo or timezone.utc
DATA_DIR = Path(os.getenv("TS_CONNECT_DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
security_bootstrap = UiSecurityBootstrap(
    DATA_DIR,
    logger=logger,
    atomic_write_text=_atomic_write_text,
    fsync_directory=_fsync_directory,
)
MACHINE_FINGERPRINT_FILE = DATA_DIR / "machine_fingerprint"
LOG_DIR = resolve_log_dir(data_dir=DATA_DIR)
UI_LOG_FILE = LOG_DIR / "ui.log"
HEALTH_LOG_FILE = LOG_DIR / "health.log"
LOG_MAX_BYTES = max(
    env_int_first(("TS_CONNECT_LOG_MAX_BYTES", "TS_CONNECT_UI_LOG_MAX_BYTES"), 5 * 1024 * 1024),
    1024,
)
LOG_BACKUP_COUNT = max(
    env_int_first(("TS_CONNECT_LOG_BACKUP_COUNT", "TS_CONNECT_UI_LOG_BACKUP_COUNT"), 5),
    1,
)
ADMIN_PASSWORD_FILE = security_bootstrap.admin_password_file
ADMIN_PASSWORD_GENERATED_FILE = security_bootstrap.admin_password_generated_file
SESSION_SECRET_FILE = security_bootstrap.session_secret_file


def _build_update_agent_urls() -> list[str]:
    raw_value = (os.getenv("TS_CONNECT_UPDATE_AGENT_URL") or "").strip()
    candidates = [
        raw_value,
        "http://ts-connect-update-agent:9000",
        "http://update-agent:9000",
    ]
    urls: list[str] = []
    for candidate in candidates:
        value = candidate.rstrip("/")
        if value and value not in urls:
            urls.append(value)
    return urls


UPDATE_AGENT_URLS = _build_update_agent_urls()
UPDATE_AGENT_URL = UPDATE_AGENT_URLS[0]
UPDATE_AGENT_TOKEN = get_update_agent_token(DATA_DIR)
HOST_AGENT_URL = os.getenv("TS_CONNECT_HOST_AGENT_URL", "").strip()
if HOST_AGENT_URL:
    HOST_AGENT_URL = HOST_AGENT_URL.rstrip("/")
HOST_AGENT_TOKEN = get_host_agent_token(DATA_DIR)
HOST_REBOOT_DELAY_SECONDS = int(os.getenv("TS_CONNECT_HOST_REBOOT_DELAY", "60"))
SECRETS_FILE_UID = _env_int("TS_CONNECT_SECRETS_UID", 1000)
SECRETS_FILE_GID = _env_int("TS_CONNECT_SECRETS_GID", 1000)


def _configure_logging() -> None:
    configure_rotating_logger(
        logger,
        UI_LOG_FILE,
        max_bytes=LOG_MAX_BYTES,
        backup_count=LOG_BACKUP_COUNT,
        level=logging.INFO,
    )


_configure_logging()
logger.info("UI logging enabled at %s", UI_LOG_FILE)


def _parse_trusted_networks(cidr_values: list[str]) -> tuple[list[ipaddress._BaseNetwork], list[str]]:
    networks: list[ipaddress._BaseNetwork] = []
    invalid: list[str] = []
    for raw in cidr_values:
        value = str(raw or "").strip()
        if not value:
            continue
        try:
            network = ipaddress.ip_network(value, strict=False)
        except ValueError:
            invalid.append(value)
            continue
        networks.append(network)
    for loopback in ("127.0.0.0/8", "::1/128"):
        network = ipaddress.ip_network(loopback, strict=False)
        if all(network != existing for existing in networks):
            networks.append(network)
    return networks, invalid


TRUSTED_NETWORKS, INVALID_TRUSTED_CIDRS = _parse_trusted_networks(TRUSTED_CIDRS)
if INVALID_TRUSTED_CIDRS:
    logger.warning("Ignoriere ungültige UI_TRUSTED_CIDRS-Einträge: %s", ", ".join(INVALID_TRUSTED_CIDRS))


def _summarize_network_exposure() -> dict[str, Any]:
    bind_ip = (UI_BIND_IP or "").strip() or "0.0.0.0"
    lowered_bind = bind_ip.lower()
    warnings: list[str] = []
    mode = "private-network"
    summary = "UI ist auf ein privates Netz bzw. eine interne Vereinsumgebung ausgerichtet."

    if lowered_bind in {"127.0.0.1", "::1", "localhost"}:
        mode = "localhost-only"
        summary = "UI ist auf lokalen Zugriff bzw. einen vorgeschalteten Reverse-Proxy/VPN begrenzt."
    elif lowered_bind in {"0.0.0.0", "::"}:
        mode = "all-interfaces"
        summary = "UI bindet an alle Interfaces und verlässt sich auf CIDR-, Firewall- oder Proxy-Schutz."
        warnings.append(
            "UI_BIND_IP ist auf allen Interfaces offen. Das ist nur für Vereinsnetz, VPN oder einen vorgeschalteten Reverse-Proxy gedacht."
        )
    else:
        try:
            bind_addr = ipaddress.ip_address(bind_ip)
        except ValueError:
            mode = "named-interface"
            summary = f"UI_BIND_IP nutzt einen benannten Host ({bind_ip}). Den effektiven Exposure-Pfad bitte bewusst prüfen."
        else:
            if bind_addr.is_loopback:
                mode = "localhost-only"
                summary = "UI ist auf lokalen Zugriff bzw. einen vorgeschalteten Reverse-Proxy/VPN begrenzt."
            elif bind_addr.is_private:
                mode = "private-network"
                summary = f"UI bindet explizit an ein privates Interface ({bind_ip})."
            else:
                mode = "public-interface"
                summary = f"UI bindet an eine öffentliche Adresse ({bind_ip})."
                warnings.append(
                    f"UI_BIND_IP zeigt auf eine öffentliche Adresse ({bind_ip}). Verwende bevorzugt 127.0.0.1 plus Reverse-Proxy/VPN oder eine explizite private LAN-IP."
                )

    broad_or_public_trust = [
        str(network)
        for network in TRUSTED_NETWORKS
        if str(network) in {"0.0.0.0/0", "::/0"} or not (network.is_private or network.is_loopback or network.is_link_local)
    ]
    if broad_or_public_trust:
        warnings.append(
            "UI_TRUSTED_CIDRS enthält öffentliche oder globale Netze: " + ", ".join(broad_or_public_trust[:4])
        )

    host_agent_url = (HOST_AGENT_URL or "").strip()
    host_agent_host = ""
    if host_agent_url:
        try:
            host_agent_host = (urlparse(host_agent_url).hostname or "").strip().lower()
        except ValueError:
            host_agent_host = ""
        if host_agent_host and host_agent_host not in {"127.0.0.1", "::1", "localhost", "host.docker.internal"}:
            warnings.append(
                f"Host-Agent URL zeigt nicht auf einen lokalen Host ({host_agent_host}). Der Host-Agent sollte nur lokal oder über einen bewusst abgesicherten Tunnel erreichbar sein."
            )

    return {
        "mode": mode,
        "bind_ip": bind_ip,
        "trusted_cidrs": [value for value in TRUSTED_CIDRS if value],
        "host_agent_url": host_agent_url or None,
        "summary": summary,
        "warnings": warnings,
        "ok": not warnings,
    }

def _ensure_private_file_permissions(path: Path) -> None:
    security_bootstrap.ensure_private_file_permissions(path)


AgentRequestError = ops_runtime.AgentRequestError


def _agent_error_status(exc: AgentRequestError, *, default_status: int = 502) -> int:
    return ops_runtime.agent_error_status(exc, default_status=default_status)


async def _update_agent_request(
    method: str,
    path: str,
    *,
    json_payload: dict | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    last_unavailable_error: Exception | None = None
    for index, candidate_url in enumerate(UPDATE_AGENT_URLS):
        try:
            return await ops_runtime.update_agent_request(
                method,
                path,
                update_agent_url=candidate_url,
                update_agent_token=UPDATE_AGENT_TOKEN,
                short_error_message=_short_error_message,
                json_payload=json_payload,
                timeout=timeout,
            )
        except ops_runtime.AgentRequestError as exc:
            if exc.unavailable and index + 1 < len(UPDATE_AGENT_URLS):
                last_unavailable_error = exc
                continue
            raise
    if last_unavailable_error is not None:
        raise last_unavailable_error
    raise RuntimeError("Update-Agent konnte nicht kontaktiert werden.")


async def _ping_update_agent(timeout: float = 3.0) -> bool:
    return await ops_runtime.ping_update_agent(
        update_agent_request_fn=_update_agent_request,
        timeout=timeout,
    )


async def _host_agent_request(
    method: str,
    path: str,
    *,
    json_payload: dict | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    return await ops_runtime.host_agent_request(
        method,
        path,
        host_agent_url=HOST_AGENT_URL,
        host_agent_token=HOST_AGENT_TOKEN,
        short_error_message=_short_error_message,
        json_payload=json_payload,
        timeout=timeout,
    )


async def _ping_host_agent(timeout: float = 3.0) -> bool:
    return await ops_runtime.ping_host_agent(
        host_agent_url=HOST_AGENT_URL,
        host_agent_request_fn=_host_agent_request,
        timeout=timeout,
    )


async def _container_status(name: str, *, timeout: float = 5.0) -> dict[str, Any]:
    return await _update_agent_request("GET", f"/api/v1/containers/{name}/status", timeout=timeout)


async def _restart_container(name: str, *, timeout: float = 30.0) -> dict[str, Any]:
    return await _update_agent_request("POST", f"/api/v1/containers/{name}/restart", timeout=timeout)


async def _stop_container(name: str, *, timeout: float = 30.0) -> dict[str, Any]:
    return await _update_agent_request("POST", f"/api/v1/containers/{name}/stop", timeout=timeout)

DEFAULT_BACKUP_HOST = os.getenv("TS_CONNECT_BACKUP_HOST", "backup-db")
DEFAULT_BACKUP_PORT = int(os.getenv("TS_CONNECT_BACKUP_PORT", "5432"))
DEFAULT_BACKUP_DB = os.getenv("TS_CONNECT_BACKUP_DB", "targetshot_backup")
DEFAULT_BACKUP_USER = os.getenv("TS_CONNECT_BACKUP_USER", "targetshot")
DEFAULT_MIRROR_DB_HOST = (os.getenv("TS_CONNECT_DEFAULT_DB_HOST", "mariadb-mirror") or "mariadb-mirror").strip() or "mariadb-mirror"
DEFAULT_MIRROR_DB_PORT = _env_int("TS_CONNECT_DEFAULT_DB_PORT", 3306) or 3306
if DEFAULT_MIRROR_DB_PORT <= 0:
    logger.warning("Ungültiger TS_CONNECT_DEFAULT_DB_PORT=%s, nutze 3306", DEFAULT_MIRROR_DB_PORT)
    DEFAULT_MIRROR_DB_PORT = 3306
DEFAULT_MIRROR_DB_USER = (os.getenv("TS_CONNECT_DEFAULT_DB_USER", "debezium_sync") or "debezium_sync").strip() or "debezium_sync"
DEFAULT_MIRROR_DB_NAME = (os.getenv("TS_CONNECT_MIRROR_DB_NAME", "SMDB") or "SMDB").strip() or "SMDB"
MIRROR_DB_PASSWORD = (os.getenv("TS_CONNECT_MIRROR_DB_PASSWORD") or os.getenv("MARIADB_PASSWORD") or "").strip()
MIRROR_DB_CONTAINER_NAME = (os.getenv("TS_CONNECT_MIRROR_DB_CONTAINER_NAME", "ts-mariadb-mirror") or "ts-mariadb-mirror").strip() or "ts-mariadb-mirror"
BACKUP_DB_CONTAINER_NAME = (os.getenv("TS_CONNECT_BACKUP_CONTAINER_NAME", "ts-backup-db") or "ts-backup-db").strip() or "ts-backup-db"
MIRROR_BACKUP_ENABLED = (os.getenv("TS_CONNECT_MIRROR_BACKUP_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"})
MIRROR_BACKUP_DIR = resolve_backup_dir(
    os.getenv("TS_CONNECT_MIRROR_BACKUP_DIR"),
    workspace_path=WORKSPACE_PATH,
    default_subdir="ui/backups/mirror-db",
)
MIRROR_BACKUP_HOST_DIR = resolve_host_display_dir(
    os.getenv("TS_CONNECT_MIRROR_BACKUP_HOST_DIR"),
    workspace_host=WORKSPACE_HOST_PATH,
    default_subdir="ui/backups/mirror-db",
)
MIRROR_BACKUP_RETENTION_DAYS = max(_env_int("TS_CONNECT_MIRROR_BACKUP_RETENTION_DAYS", 30) or 30, 1)
MIRROR_BACKUP_HOUR = min(max(_env_int("TS_CONNECT_MIRROR_BACKUP_HOUR", 2) or 2, 0), 23)
MIRROR_BACKUP_MINUTE = min(max(_env_int("TS_CONNECT_MIRROR_BACKUP_MINUTE", 15) or 15, 0), 59)
MIRROR_BACKUP_POLL_SECONDS = max(_env_int("TS_CONNECT_MIRROR_BACKUP_POLL_SECONDS", 60) or 60, 15)


def _workspace_host_root() -> str:
    host_root = (WORKSPACE_HOST_PATH or "").strip()
    if host_root:
        return host_root
    workspace_env = _workspace_env_values()
    host_root = (workspace_env.get("TS_CONNECT_WORKSPACE_HOST") or "").strip()
    if host_root:
        return host_root
    return str(WORKSPACE_PATH)


def _storage_target_definitions() -> list[dict[str, Any]]:
    host_root = Path(_workspace_host_root())
    return [
        {
            "key": "backup_db",
            "label": "Backup-DB",
            "runtime_path": WORKSPACE_PATH / "ui/data/backup-db",
            "server_path": host_root / "ui/data/backup-db",
            "container_name": BACKUP_DB_CONTAINER_NAME,
            "container_user": "postgres",
            "probe_relpaths": [
                Path("global"),
                Path("global/pg_filenode.map"),
            ],
        },
        {
            "key": "mirror_db",
            "label": "Mirror-MariaDB",
            "runtime_path": WORKSPACE_PATH / "ui/data/mariadb",
            "server_path": host_root / "ui/data/mariadb",
            "container_name": MIRROR_DB_CONTAINER_NAME,
            "container_user": "mysql",
            "probe_relpaths": [
                Path("mysql"),
                Path("ibdata1"),
                Path(DEFAULT_MIRROR_DB_NAME),
            ],
        },
    ]


def _storage_target_map() -> dict[str, dict[str, Any]]:
    return {target["key"]: target for target in _storage_target_definitions()}


def _storage_probe_paths(target: dict[str, Any]) -> list[Path]:
    runtime_path = target["runtime_path"]
    paths = [runtime_path]
    for relpath in target.get("probe_relpaths", []):
        candidate = runtime_path / relpath
        if candidate.exists():
            paths.append(candidate)
    return paths


def _inspect_container_user_ids_sync(container_name: str, user_name: str) -> tuple[int, int]:
    uid_result = subprocess.run(
        ["docker", "exec", container_name, "id", "-u", user_name],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if uid_result.returncode != 0:
        detail = (uid_result.stderr or uid_result.stdout or "uid lookup fehlgeschlagen").strip()
        raise RuntimeError(detail)
    gid_result = subprocess.run(
        ["docker", "exec", container_name, "id", "-g", user_name],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if gid_result.returncode != 0:
        detail = (gid_result.stderr or gid_result.stdout or "gid lookup fehlgeschlagen").strip()
        raise RuntimeError(detail)
    return int(uid_result.stdout.strip()), int(gid_result.stdout.strip())


def _format_owner_pair(uid: int | None, gid: int | None) -> str:
    if uid is None or gid is None:
        return "unbekannt"
    return f"{uid}:{gid}"


def _storage_issue_message(issue: dict[str, Any]) -> str:
    label = issue.get("label") or "Datenordner"
    server_path = issue.get("server_path") or issue.get("path") or "unbekannt"
    if issue.get("reason") == "missing-path":
        return f"{label}-Datenordner fehlt: {server_path}"
    if issue.get("reason") == "container-user-unresolved":
        return f"{label}-Besitzer konnte nicht geprüft werden: {issue.get('error') or 'Container-User nicht ermittelbar'}"
    if issue.get("reason") == "permission-error":
        return f"{label}-Datenordner ist nicht lesbar: {issue.get('error') or server_path}"
    expected = issue.get("expected_owner") or "unbekannt"
    actual = issue.get("actual_owner") or "unbekannt"
    probe_path = issue.get("probe_path") or server_path
    return f"{label}-Datenordner gehört {actual} statt {expected} ({probe_path})"


def _build_storage_issue(
    target: dict[str, Any],
    *,
    reason: str,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
    actual_uid: int | None = None,
    actual_gid: int | None = None,
    probe_path: Path | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    auto_fix_supported = reason in {"ownership-mismatch", "permission-error"} and expected_uid is not None and expected_gid is not None
    issue = {
        "key": target["key"],
        "label": target["label"],
        "reason": reason,
        "path": str(target["runtime_path"]),
        "server_path": str(target["server_path"]),
        "container_name": target["container_name"],
        "container_user": target["container_user"],
        "expected_uid": expected_uid,
        "expected_gid": expected_gid,
        "expected_owner": _format_owner_pair(expected_uid, expected_gid),
        "actual_uid": actual_uid,
        "actual_gid": actual_gid,
        "actual_owner": _format_owner_pair(actual_uid, actual_gid),
        "probe_path": str(probe_path) if probe_path else str(target["server_path"]),
        "error": error or "",
        "auto_fix_supported": auto_fix_supported and hasattr(os, "geteuid") and os.geteuid() == 0,
    }
    issue["summary"] = _storage_issue_message(issue)
    issue["repair_hint"] = (
        "Nur diesen Datenordner reparieren, nicht den gesamten ~/connector-Workspace."
        if issue["auto_fix_supported"]
        else "Wenn die Rechte falsch sind, nur diesen Datenordner korrigieren, nicht den gesamten ~/connector-Workspace."
    )
    return issue


def _check_storage_ownership_sync() -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    for target in _storage_target_definitions():
        runtime_path: Path = target["runtime_path"]
        if not runtime_path.exists():
            issues.append(
                _build_storage_issue(
                    target,
                    reason="missing-path",
                )
            )
            continue
        try:
            expected_uid, expected_gid = _inspect_container_user_ids_sync(
                target["container_name"],
                target["container_user"],
            )
        except Exception as exc:  # noqa: BLE001
            issues.append(
                _build_storage_issue(
                    target,
                    reason="container-user-unresolved",
                    error=_short_error_message(str(exc), 180),
                )
            )
            continue
        for probe_path in _storage_probe_paths(target):
            try:
                stat_result = probe_path.stat()
            except PermissionError as exc:
                issues.append(
                    _build_storage_issue(
                        target,
                        reason="permission-error",
                        expected_uid=expected_uid,
                        expected_gid=expected_gid,
                        probe_path=probe_path,
                        error=_short_error_message(str(exc), 180),
                    )
                )
                break
            except OSError:
                continue
            if stat_result.st_uid != expected_uid or stat_result.st_gid != expected_gid:
                issues.append(
                    _build_storage_issue(
                        target,
                        reason="ownership-mismatch",
                        expected_uid=expected_uid,
                        expected_gid=expected_gid,
                        actual_uid=stat_result.st_uid,
                        actual_gid=stat_result.st_gid,
                        probe_path=probe_path,
                    )
                )
                break
    return {
        "ok": not issues,
        "checked_at": _now_utc_iso(),
        "issues": issues,
    }


def _storage_issue_for_key_sync(key: str) -> dict[str, Any] | None:
    preflight = _check_storage_ownership_sync()
    for issue in preflight.get("issues", []):
        if issue.get("key") == key:
            return issue
    return None


def _apply_storage_ownership_tree_sync(path: Path, *, uid: int, gid: int) -> None:
    if not path.exists():
        return
    for root, dirnames, filenames in os.walk(path, topdown=False):
        root_path = Path(root)
        os.chown(root_path, uid, gid)
        for name in filenames:
            os.chown(root_path / name, uid, gid)
        for name in dirnames:
            os.chown(root_path / name, uid, gid)
    os.chown(path, uid, gid)


def _fix_storage_ownership_sync() -> dict[str, Any]:
    preflight = _check_storage_ownership_sync()
    issues = preflight.get("issues", [])
    if not issues:
        return {"ok": True, "fixed": [], "storage": preflight}
    target_map = _storage_target_map()
    fixed: list[str] = []
    for issue in issues:
        if not issue.get("auto_fix_supported"):
            raise RuntimeError(issue.get("summary") or "Datenordner-Reparatur nicht unterstützt.")
        target = target_map.get(issue.get("key") or "")
        if not target:
            raise RuntimeError(f"Unbekannter Datenordner-Schlüssel: {issue.get('key')}")
        expected_uid = issue.get("expected_uid")
        expected_gid = issue.get("expected_gid")
        if expected_uid is None or expected_gid is None:
            raise RuntimeError(issue.get("summary") or "Erwarteter Besitzer unbekannt.")
        _apply_storage_ownership_tree_sync(
            target["runtime_path"],
            uid=int(expected_uid),
            gid=int(expected_gid),
        )
        fixed.append(target["label"])
    refreshed = _check_storage_ownership_sync()
    return {"ok": refreshed.get("ok", False), "fixed": fixed, "storage": refreshed}


async def _get_storage_ownership_preflight() -> dict[str, Any]:
    return await asyncio.to_thread(_check_storage_ownership_sync)


async def _log_storage_ownership_preflight() -> None:
    preflight = await _get_storage_ownership_preflight()
    for issue in preflight.get("issues", []):
        logger.warning("Storage ownership preflight issue: %s", issue.get("summary"))


def _raise_for_storage_issue_sync(key: str) -> None:
    issue = _storage_issue_for_key_sync(key)
    if not issue:
        return
    raise RuntimeError(f"{issue.get('summary')} {issue.get('repair_hint')}".strip())


async def _ensure_storage_issue_free(key: str) -> None:
    await asyncio.to_thread(_raise_for_storage_issue_sync, key)
DEFAULT_SERVER_NAME = os.getenv("TS_CONNECT_DEFAULT_SERVER_NAME", "targetshot-mariadb-mirror")
DEFAULT_SOURCE_DB_HOST = os.getenv("TS_CONNECT_SOURCE_DB_HOST", "").strip()
DEFAULT_SOURCE_DB_PORT = int(os.getenv("TS_CONNECT_SOURCE_DB_PORT", "3306"))
DEFAULT_SOURCE_DB_REPL_USER = os.getenv("TS_CONNECT_SOURCE_DB_REPL_USER", "").strip()
DEFAULT_SOURCE_DB_GTID_MODE = os.getenv("TS_CONNECT_SOURCE_DB_GTID_MODE", "true").strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_SOURCE_DB_LOG_FILE = os.getenv("TS_CONNECT_SOURCE_DB_LOG_FILE", "").strip()
DEFAULT_SOURCE_DB_LOG_POS = _env_int("TS_CONNECT_SOURCE_DB_LOG_POS", None)
DEFAULT_SOURCE_DB_CONNECT_RETRY = int(os.getenv("TS_CONNECT_SOURCE_DB_CONNECT_RETRY", "10"))
SOURCE_DB_REPL_PASSWORD_KEY = "source_db_repl_password"
STREAMS_TARGET_PREFIX = (os.getenv("TS_STREAMS_TARGET_PREFIX", "ts.sds-test") or "ts.sds-test").strip() or "ts.sds-test"
MM2_INTERNAL_REPLICATION_FACTOR = max(_env_int("TS_CONNECT_MM2_INTERNAL_REPLICATION_FACTOR", 3) or 3, 1)
MM2_OFFSET_STORAGE_PARTITIONS = max(_env_int("TS_CONNECT_MM2_OFFSET_STORAGE_PARTITIONS", 5) or 5, 1)
MM2_STATUS_STORAGE_PARTITIONS = max(_env_int("TS_CONNECT_MM2_STATUS_STORAGE_PARTITIONS", 3) or 3, 1)
MM2_STATE_TOPIC_PREFIX = (os.getenv("TS_CONNECT_MM2_STATE_TOPIC_PREFIX", "_ts_mm2_v3") or "_ts_mm2_v3").strip() or "_ts_mm2_v3"
KEYGEN_ACCOUNT = (_env_first("TS_CONNECT_KEYGEN_ACCOUNT", "TS_KEYGEN_ACCOUNT", "KEYGEN_ACCOUNT") or "").strip()
KEYGEN_BASE_URL = (
    _env_first("TS_CONNECT_KEYGEN_API_URL", "TS_KEYGEN_API_URL", "KEYGEN_API_URL")
    or (f"https://api.keygen.sh/v1/accounts/{KEYGEN_ACCOUNT}" if KEYGEN_ACCOUNT else "")
).strip().rstrip("/")
KEYGEN_LICENSE_TOKEN = (_env_first("TS_CONNECT_KEYGEN_LICENSE_TOKEN", "TS_KEYGEN_LICENSE_TOKEN") or "").strip()
KEYGEN_POLICY_ID = (_env_first("TS_CONNECT_KEYGEN_POLICY_ID", "TS_KEYGEN_POLICY_ID", "KEYGEN_POLICY_ID") or "").strip()
KEYGEN_POLICY_NAME = (_env_first("TS_CONNECT_KEYGEN_POLICY_NAME", "TS_KEYGEN_POLICY_NAME", "KEYGEN_POLICY_NAME") or "").strip()
KEYGEN_MACHINE_NAME = (_env_first("TS_CONNECT_KEYGEN_MACHINE_NAME", "TS_KEYGEN_MACHINE_NAME") or "").strip()
KEYGEN_MACHINE_FINGERPRINT = (_env_first("TS_CONNECT_KEYGEN_MACHINE_FINGERPRINT", "TS_KEYGEN_MACHINE_FINGERPRINT") or "").strip()
KEYGEN_ACTIVATION_ENABLED = (
    (_env_first("TS_CONNECT_KEYGEN_AUTO_ACTIVATE", "TS_KEYGEN_AUTO_ACTIVATE") or "true").lower()
    in {"1", "true", "yes", "on"}
)

LICENSE_PROVIDER = "keygen"
LICENSE_MACHINE_ACTIVATION_ENABLED = KEYGEN_ACTIVATION_ENABLED


def _license_status_snapshot(settings: dict) -> dict:
    license_valid_iso = settings.get("license_valid_until")
    valid_dt = _parse_iso8601(license_valid_iso)
    days_remaining: int | None = None
    if valid_dt:
        days_remaining = (valid_dt.date() - datetime.now(timezone.utc).date()).days
    return {
        "timestamp": _now_utc_iso(),
        "license_key_present": bool(settings.get("license_key")),
        "license_status": settings.get("license_status"),
        "license_plan": settings.get("license_tier"),
        "license_valid_until": license_valid_iso,
        "days_remaining": days_remaining,
        "site": settings.get("topic_prefix"),
    }


def _normalize_iso8601(value: str | None) -> str | None:
    if not value:
        return None
    dt = _parse_iso8601(value)
    if not dt:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_license_expiry(value: str | None) -> str | None:
    normalized = _normalize_iso8601(value)
    if normalized or not value:
        return normalized
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
            return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        except ValueError:
            continue
    return None


KEYGEN_ACTIVATABLE_STATUSES = {
    "valid",
    "active",
    "no_machine",
    "no_machines",
    "fingerprint_scope_required",
    "fingerprint_scope_mismatch",
    "machine_scope_required",
    "machine_scope_mismatch",
}


def _normalize_provider_status(value: str | None) -> str:
    return (value or "").strip().lower().replace("-", "_").replace(" ", "_")


def _read_machine_fingerprint_file() -> str:
    try:
        return MACHINE_FINGERPRINT_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _write_machine_fingerprint_file(value: str) -> None:
    cleaned = (value or "").strip()
    if not cleaned:
        return
    _atomic_write_text(
        MACHINE_FINGERPRINT_FILE,
        cleaned + "\n",
        mode=stat.S_IRUSR | stat.S_IWUSR,
    )


def _compute_machine_fingerprint() -> str:
    machine_id = ""
    for candidate in (Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")):
        try:
            machine_id = candidate.read_text(encoding="utf-8").strip()
        except OSError:
            machine_id = ""
        if machine_id:
            break
    seed_parts = [
        machine_id,
        socket.gethostname(),
        platform.node(),
        platform.system(),
        platform.release(),
    ]
    seed = "|".join(part for part in seed_parts if part)
    if not seed:
        seed = "ts-connect"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


def _resolve_machine_fingerprint() -> str:
    explicit = (KEYGEN_MACHINE_FINGERPRINT or "").strip()
    if explicit:
        return explicit
    stored = _read_machine_fingerprint_file()
    if stored:
        return stored
    computed = _compute_machine_fingerprint()
    _write_machine_fingerprint_file(computed)
    return computed


def _resolve_machine_fingerprint_scope() -> list[str]:
    candidates: list[str] = []
    for value in (
        (KEYGEN_MACHINE_FINGERPRINT or "").strip(),
        _read_machine_fingerprint_file(),
        _compute_machine_fingerprint(),
    ):
        cleaned = (value or "").strip()
        if cleaned and cleaned not in candidates:
            candidates.append(cleaned)
    if not candidates:
        candidates.append(_resolve_machine_fingerprint())
    return candidates


def _resolve_machine_name(*, settings: dict[str, Any] | None = None, fallback: str | None = None) -> str:
    for value in (
        KEYGEN_MACHINE_NAME,
        fallback or "",
        str((settings or {}).get("server_name") or "").strip(),
        str((settings or {}).get("topic_prefix") or "").strip(),
        socket.gethostname(),
        platform.node(),
        "ts-connect",
    ):
        cleaned = (value or "").strip()
        if cleaned:
            return cleaned
    return "ts-connect"


def _extract_keygen_message(payload: dict[str, Any], meta: dict[str, Any]) -> str | None:
    if isinstance(meta.get("detail"), str) and meta.get("detail"):
        return str(meta.get("detail"))
    if isinstance(meta.get("message"), str) and meta.get("message"):
        return str(meta.get("message"))
    errors_obj = payload.get("errors")
    if isinstance(errors_obj, list) and errors_obj:
        first = errors_obj[0]
        if isinstance(first, dict):
            for key in ("detail", "title", "code"):
                if isinstance(first.get(key), str) and first.get(key):
                    return str(first.get(key))
        return str(first)
    if isinstance(errors_obj, dict) and errors_obj:
        return ", ".join(str(v) for v in errors_obj.values())
    if payload.get("error"):
        return str(payload.get("error"))
    if payload.get("message"):
        return str(payload.get("message"))
    return None


def _parse_keygen_license_validation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
    data_section = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    attributes = data_section.get("attributes") if isinstance(data_section.get("attributes"), dict) else {}
    relationships = data_section.get("relationships") if isinstance(data_section.get("relationships"), dict) else {}
    metadata = attributes.get("metadata") if isinstance(attributes.get("metadata"), dict) else {}

    raw_status = meta.get("code") or meta.get("status") or attributes.get("status")
    status = _normalize_provider_status(str(raw_status or "invalid"))
    valid_flag = meta.get("valid")
    valid = bool(valid_flag) if isinstance(valid_flag, bool) else status in {"valid", "active"}
    policy_rel = relationships.get("policy") if isinstance(relationships.get("policy"), dict) else {}
    policy_data = policy_rel.get("data") if isinstance(policy_rel.get("data"), dict) else {}
    policy_id = policy_data.get("id")
    expires_at = (
        attributes.get("expiry")
        or attributes.get("expires_at")
        or meta.get("expiry")
        or meta.get("expires_at")
    )
    customer_email = (
        metadata.get("customerEmail")
        or metadata.get("customer_email")
        or metadata.get("billingContactEmail")
        or metadata.get("billing_contact_email")
    )
    message = _extract_keygen_message(payload, meta)
    entitlements = metadata.get("entitlements")
    plan = "club_plus" if valid or status in KEYGEN_ACTIVATABLE_STATUSES else DEFAULT_LICENSE_TIER

    return {
        "valid": valid,
        "plan": normalize_license_tier(plan),
        "status": status or ("valid" if valid else "invalid"),
        "variant_id": str(policy_id).strip() if policy_id else KEYGEN_POLICY_ID or None,
        "variant_name": metadata.get("policyName") or metadata.get("policy_name") or KEYGEN_POLICY_NAME or None,
        "expires_at": _normalize_license_expiry(str(expires_at).strip() if expires_at else None),
        "raw_expires_at": expires_at,
        "customer_email": customer_email,
        "message": message,
        "license_id": str(data_section.get("id")).strip() if data_section.get("id") else None,
        "entitlements": entitlements,
        "payload": payload,
    }


def _keygen_can_activate(validation: dict[str, Any]) -> bool:
    return _normalize_provider_status(validation.get("status")) in KEYGEN_ACTIVATABLE_STATUSES


def _keygen_license_entitled(validation: dict[str, Any]) -> bool:
    if bool(validation.get("valid")):
        return True
    return _normalize_provider_status(validation.get("status")) in KEYGEN_ACTIVATABLE_STATUSES


def _build_keygen_machine_headers(license_key: str, *, include_content_type: bool = False) -> dict[str, str]:
    headers = {"Accept": "application/vnd.api+json"}
    if include_content_type:
        headers["Content-Type"] = "application/vnd.api+json"
    token = (KEYGEN_LICENSE_TOKEN or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
        return headers
    key = (license_key or "").strip()
    if not key:
        raise ValueError("Bitte einen Lizenzschlüssel eingeben.")
    headers["Authorization"] = f"License {key}"
    return headers


async def _find_keygen_machine(
    license_key: str,
    fingerprints: list[str],
) -> dict[str, Any] | None:
    if not KEYGEN_BASE_URL:
        return None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(
                f"{KEYGEN_BASE_URL}/machines?page[size]=100",
                headers=_build_keygen_machine_headers(license_key),
            )
    except httpx.RequestError:
        return None
    if response.status_code >= 400:
        return None
    try:
        payload = response.json()
    except json.JSONDecodeError:
        return None
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        return None
    for entry in data:
        if not isinstance(entry, dict):
            continue
        attributes = entry.get("attributes") if isinstance(entry.get("attributes"), dict) else {}
        if str(attributes.get("fingerprint") or "").strip() in fingerprints:
            return entry
    return None


def _write_json_log(filename: str, payload: dict) -> None:
    path = LOG_DIR / filename
    append_rotating_json_line(
        path,
        payload,
        max_bytes=LOG_MAX_BYTES,
        backup_count=LOG_BACKUP_COUNT,
    )


def _tail_log_lines(path: Path, line_limit: int) -> list[str]:
    if line_limit <= 0:
        return []
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()
    return [line.rstrip("\n") for line in lines[-line_limit:]]

def _load_version_defaults() -> tuple[str, str]:
    version = os.getenv("TS_CONNECT_VERSION")
    release = os.getenv("TS_CONNECT_RELEASE")
    version_file = WORKSPACE_PATH / "VERSION"
    release_file = WORKSPACE_PATH / "RELEASE"
    if (not version or not version.strip()) and version_file.exists():
        version = version_file.read_text(encoding="utf-8").strip()
    if (not release or not release.strip()) and release_file.exists():
        release = release_file.read_text(encoding="utf-8").strip()
    return (version or "dev"), (release or "Unbekannt")


def _resolve_update_image() -> str:
    for candidate in (
        os.getenv("TS_CONNECT_UPDATE_CHECK_IMAGE"),
        os.getenv("TS_CONNECT_UI_IMAGE"),
        DEFAULT_UPDATE_IMAGE,
    ):
        if candidate:
            trimmed = candidate.strip()
            if trimmed:
                return trimmed
    return DEFAULT_UPDATE_IMAGE


SESSION_SECRET = security_bootstrap.resolve_session_secret()
CONFLUENT_CLUSTER_URL = os.getenv(
    "TS_CONNECT_CLUSTER_URL",
    "https://pkc-w7d6j.germanywestcentral.azure.confluent.cloud",
)
CONFLUENT_CLUSTER_ID = os.getenv("TS_CONNECT_CLUSTER_ID", "lkc-g8p3n1")
CONFLUENT_BOOTSTRAP_DEFAULT = os.getenv(
    "TS_CONNECT_BOOTSTRAP_DEFAULT",
    "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092",
)
DOCS_URL = os.getenv("TS_CONNECT_DOCS_URL", "https://docs.targetshot.app/")

DB_PATH = DATA_DIR / "config.db"
SECRETS_PATH = Path(CONNECT_SECRETS_PATH)
APPLY_STATE_PATH = DATA_DIR / "connector_apply_state.json"
MM2_CONFIG_PATH = DATA_DIR / "mm2.properties"
LICENSE_KEY_FILE = DATA_DIR / "license.key"
APPLY_RETRY_SECONDS = int(os.getenv("TS_CONNECT_APPLY_RETRY_SECONDS", "60"))
UPDATE_STATE_PATH = DATA_DIR / "update_state.json"
UPDATE_CACHE_SECONDS = int(os.getenv("TS_CONNECT_UPDATE_CACHE_SECONDS", "3600"))
GITHUB_REPO_OVERRIDE = os.getenv("TS_CONNECT_GITHUB_REPO", "").strip()
GITHUB_TOKEN = os.getenv("TS_CONNECT_GITHUB_TOKEN", "").strip()
AUTO_UPDATE_DEFAULT_HOUR = int(os.getenv("TS_CONNECT_AUTO_UPDATE_HOUR", "1"))
AUTO_UPDATE_CHECK_SECONDS = int(os.getenv("TS_CONNECT_AUTO_UPDATE_POLL_SECONDS", "60"))
AUTO_UPDATE_FORCE_RELEASE = os.getenv("TS_CONNECT_AUTO_UPDATE_FORCE_RELEASE", "1").lower() in {"1", "true", "yes", "on"}
AUTO_UPDATE_STALE_SECONDS = int(os.getenv("TS_CONNECT_AUTO_UPDATE_STALE_SECONDS", "14400"))
PROJECT_NAME = os.getenv("COMPOSE_PROJECT_NAME", "ts-connect")
UI_CONTAINER_NAME = os.getenv("TS_CONNECT_UI_CONTAINER_NAME", "ts-connect-ui")
UPDATE_AGENT_CONTAINER_NAME = os.getenv("TS_CONNECT_UPDATE_AGENT_CONTAINER_NAME", "ts-connect-update-agent")
UPDATE_AGENT_SYNC_DELAY_SECONDS = max(_env_int("TS_CONNECT_UPDATE_AGENT_SYNC_DELAY", 12) or 12, 0)
OS_UPDATE_STATE_PATH = DATA_DIR / "os_update_state.json"
OS_UPDATE_MAX_AGE_SECONDS = int(os.getenv("TS_CONNECT_OS_UPDATE_MAX_AGE", "21600"))
OS_UPDATE_LOG_LIMIT = int(os.getenv("TS_CONNECT_OS_UPDATE_LOG_LIMIT", "400"))
OS_UPDATE_PACKAGE_LIMIT = int(os.getenv("TS_CONNECT_OS_UPDATE_PACKAGE_LIMIT", "80"))
MIRROR_BACKUP_STATE_PATH = DATA_DIR / "mirror_backup_state.json"
DEFAULT_UPDATE_IMAGE = "targetshot.azurecr.io/ts-connect:stable"

update_state_manager = UpdateStateManager(UPDATE_STATE_PATH)

_update_state_lock = asyncio.Lock()
_update_job_lock = asyncio.Lock()
_cached_repo_slug: str | None = None
_auto_update_task: asyncio.Task | None = None
_update_agent_sync_task: asyncio.Task | None = None
_update_agent_sync_lock = asyncio.Lock()
_os_update_state_lock = asyncio.Lock()
_os_update_refresh_lock = asyncio.Lock()
_os_update_task: asyncio.Task | None = None
_mirror_backup_state_lock = asyncio.Lock()
_mirror_backup_run_lock = asyncio.Lock()
_mirror_backup_task: asyncio.Task | None = None
_git_safe_configured = False

_apply_state_lock = asyncio.Lock()
_registry_login_lock = asyncio.Lock()
_logged_in_registries: set[str] = set()
LICENSE_FORM_KEY_SESSION = "license_form_key"


DeferredApplyError = ops_runtime.DeferredApplyError


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_apply_state() -> dict:
    return {
        "pending": False,
        "operation_id": None,
        "trigger": None,
        "current_action": None,
        "last_error": None,
        "last_attempt": None,
        "last_success": None,
        "next_retry": None,
    }


def _read_apply_state_unlocked() -> dict:
    if APPLY_STATE_PATH.exists():
        try:
            data = json.loads(APPLY_STATE_PATH.read_text(encoding="utf-8"))
            state = _default_apply_state()
            state.update({k: data.get(k) for k in state.keys()})
            return state
        except Exception:
            return _default_apply_state()
    return _default_apply_state()


async def get_apply_state() -> dict:
    async with _apply_state_lock:
        return _read_apply_state_unlocked()


async def merge_apply_state(**updates) -> dict:
    async with _apply_state_lock:
        state = _read_apply_state_unlocked()
        state.update(updates)
        _atomic_write_text(APPLY_STATE_PATH, json.dumps(state, ensure_ascii=False) + "\n")
        return state


def _next_retry_iso() -> str | None:
    if APPLY_RETRY_SECONDS <= 0:
        return None
    return (datetime.utcnow() + timedelta(seconds=APPLY_RETRY_SECONDS)).replace(microsecond=0).isoformat() + "Z"


def _classify_recovery_issue(
    message: str | None,
    *,
    operation_id: str | None = None,
    update_agent: dict[str, Any] | None = None,
) -> dict[str, str | None]:
    text = str(message or "").strip()
    lower = text.lower()
    agent_unavailable = bool(update_agent and update_agent.get("available") is False)
    agent_auth_error = bool(update_agent and update_agent.get("auth_error"))
    if not text and not agent_unavailable and not agent_auth_error:
        return {
            "category": None,
            "label": None,
            "hint": None,
            "next_step": None,
            "operation_id": operation_id,
        }
    if agent_auth_error or ("update-agent" in lower and ("401" in lower or "403" in lower or "unauthorized" in lower)):
        return {
            "category": "update-agent-auth",
            "label": "Update-Agent Auth-Fehler",
            "hint": "Update-Agent-Token zwischen UI und Update-Agent abgleichen.",
            "next_step": "TS_CONNECT_UPDATE_AGENT_TOKEN prüfen oder die gemeinsame Token-Datei neu synchronisieren und danach den Update-Status neu laden.",
            "operation_id": operation_id,
        }
    if agent_unavailable or ("update-agent" in lower and "nicht erreichbar" in lower):
        return {
            "category": "update-agent-unavailable",
            "label": "Update-Agent offline",
            "hint": "Update-Agent-Container, Port 9000 und Update-Agent-Token prüfen.",
            "next_step": "Update-Agent neu starten und danach den Update-Status erneut laden.",
            "operation_id": operation_id,
        }
    if "host-agent" in lower and "nicht erreichbar" in lower:
        return {
            "category": "host-agent-unavailable",
            "label": "Host-Agent offline",
            "hint": "Host-Agent-Dienst, Bind-Adresse und Host-Agent-Token prüfen.",
            "next_step": "Host-Agent-Dienst prüfen, Token abgleichen und die Host-Aktion erneut starten.",
            "operation_id": operation_id,
        }
    if (
        "connector rest" in lower
        or "/connectors" in lower
        or "kafka connect" in lower
        or "connect rest" in lower
    ):
        return {
            "category": "kafka-connect-unavailable",
            "label": "Kafka Connect offline",
            "hint": "Container ts-kafka-connect und dessen REST-Endpunkt prüfen.",
            "next_step": "Kafka Connect wieder gesund starten und danach den Worker-Status neu prüfen.",
            "operation_id": operation_id,
        }
    if "mirrormaker" in lower or "ts-mirror-maker" in lower or "health=unhealthy" in lower or "stabil laufend" in lower:
        return {
            "category": "mirror-maker-unhealthy",
            "label": "MirrorMaker instabil",
            "hint": "Container ts-mirror-maker, MM2-Logs und mm2.properties prüfen.",
            "next_step": "MirrorMaker-Logs prüfen, MM2-Konfiguration korrigieren und den Replikationslauf erneut auslösen.",
            "operation_id": operation_id,
        }
    if "keygen" in lower or "lizenz" in lower or "fingerprint scope" in lower or "policy scope" in lower:
        return {
            "category": "keygen-validation-failed",
            "label": "Keygen/Lizenzprüfung fehlgeschlagen",
            "hint": "Lizenzstatus, Keygen-Erreichbarkeit und lokale Maschinenaktivierung prüfen.",
            "next_step": "Lizenz in beta prüfen, danach lokal 'Lizenz prüfen' oder 'Installation aktivieren' erneut ausführen.",
            "operation_id": operation_id,
        }
    if "mirror-mariadb" in lower or "mariadb" in lower or "mysql" in lower or "datenbank" in lower:
        return {
            "category": "mirror-db-unavailable",
            "label": "Mirror-MariaDB nicht erreichbar",
            "hint": "Mirror-MariaDB-Status und Quell-Replikation prüfen.",
            "next_step": "Mirror-MariaDB wieder erreichbar machen und anschließend den Connector-Apply erneut starten.",
            "operation_id": operation_id,
        }
    return {
        "category": "unknown-recovery-error",
        "label": "Unklare Recovery-Störung",
        "hint": "Update-/Connector-Logs und operation_id gemeinsam prüfen.",
        "next_step": "Mit operation_id die Logs korrelieren und danach den betroffenen Ablauf gezielt erneut starten.",
        "operation_id": operation_id,
    }


def _default_os_update_state() -> dict:
    return {
        "check_in_progress": False,
        "update_in_progress": False,
        "current_action": None,
        "last_check": None,
        "last_check_error": None,
        "last_update": None,
        "last_update_error": None,
        "packages": [],
        "packages_total": 0,
        "pending_count": 0,
        "security_count": 0,
        "log": [],
    }


def _read_os_update_state_unlocked() -> dict:
    if OS_UPDATE_STATE_PATH.exists():
        try:
            data = json.loads(OS_UPDATE_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return _default_os_update_state()
        state = _default_os_update_state()
        state.update({k: data.get(k) for k in state.keys()})
        if not isinstance(state.get("log"), list):
            state["log"] = []
        if not isinstance(state.get("packages"), list):
            state["packages"] = []
        return state
    return _default_os_update_state()


async def get_os_update_state() -> dict:
    async with _os_update_state_lock:
        return _read_os_update_state_unlocked()


async def merge_os_update_state(
    *,
    log_append: list[str] | None = None,
    log_reset: bool = False,
    **updates: Any,
) -> dict:
    async with _os_update_state_lock:
        state = _read_os_update_state_unlocked()
        if log_reset:
            state["log"] = []
        if log_append:
            log = state.get("log")
            if not isinstance(log, list):
                log = []
            for line in log_append:
                if not isinstance(line, str):
                    continue
                log.append(line)
            if len(log) > OS_UPDATE_LOG_LIMIT:
                log = log[-OS_UPDATE_LOG_LIMIT :]
            state["log"] = log
        for key, value in updates.items():
            state[key] = value
        _atomic_write_text(OS_UPDATE_STATE_PATH, json.dumps(state, ensure_ascii=False) + "\n")
        return state


def _next_mirror_backup_run_iso(*, now: datetime | None = None) -> str | None:
    if not MIRROR_BACKUP_ENABLED:
        return None
    reference = now or datetime.now(timezone.utc)
    return next_daily_run(
        now=reference,
        hour=MIRROR_BACKUP_HOUR,
        minute=MIRROR_BACKUP_MINUTE,
        tz=LOCAL_TIMEZONE,
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_mirror_backup_state() -> dict[str, Any]:
    return {
        "enabled": MIRROR_BACKUP_ENABLED,
        "in_progress": False,
        "last_started": None,
        "last_success": None,
        "last_error": None,
        "last_file": None,
        "last_file_size": None,
        "last_run_slot": None,
        "next_run": _next_mirror_backup_run_iso(),
    }


def _read_mirror_backup_state_unlocked() -> dict[str, Any]:
    if MIRROR_BACKUP_STATE_PATH.exists():
        try:
            data = json.loads(MIRROR_BACKUP_STATE_PATH.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            data = {}
    else:
        data = {}
    state = _default_mirror_backup_state()
    if isinstance(data, dict):
        state.update({key: data.get(key) for key in state.keys() if key in data})
    state["enabled"] = MIRROR_BACKUP_ENABLED
    if not state.get("next_run"):
        state["next_run"] = _next_mirror_backup_run_iso()
    return state


async def get_mirror_backup_state() -> dict[str, Any]:
    async with _mirror_backup_state_lock:
        return _read_mirror_backup_state_unlocked()


async def merge_mirror_backup_state(**updates: Any) -> dict[str, Any]:
    async with _mirror_backup_state_lock:
        state = _read_mirror_backup_state_unlocked()
        for key, value in updates.items():
            state[key] = value
        if "enabled" not in updates:
            state["enabled"] = MIRROR_BACKUP_ENABLED
        if "next_run" not in updates:
            state["next_run"] = _next_mirror_backup_run_iso()
        _atomic_write_text(MIRROR_BACKUP_STATE_PATH, json.dumps(state, ensure_ascii=False) + "\n")
        return state


def _format_size(size_bytes: int | None) -> str:
    if not size_bytes or size_bytes <= 0:
        return "0 B"
    units = ("B", "KB", "MB", "GB", "TB")
    value = float(size_bytes)
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    if index == 0:
        return f"{int(value)} {units[index]}"
    return f"{value:.1f} {units[index]}"


def _mirror_backup_entries(limit: int = 8) -> list[dict[str, object]]:
    entries = list_backup_files(MIRROR_BACKUP_DIR, limit=limit)
    for entry in entries:
        entry["size_label"] = _format_size(int(entry.get("size_bytes") or 0))
        entry["download_url"] = f"/api/backup/mirror-db/download?name={quote(str(entry['name']))}"
    return entries


def _validate_backup_filename(name: str) -> str:
    candidate = (name or "").strip()
    if not candidate or Path(candidate).name != candidate or "/" in candidate or "\\" in candidate:
        raise HTTPException(status_code=400, detail="Ungültiger Backup-Dateiname.")
    if not candidate.startswith("mirror-db-backup-") or not candidate.endswith(".sql.gz"):
        raise HTTPException(status_code=400, detail="Ungültiger Backup-Dateiname.")
    return candidate


def _prune_mirror_backups_now() -> list[Path]:
    return prune_backup_files(
        MIRROR_BACKUP_DIR,
        retention_days=MIRROR_BACKUP_RETENTION_DAYS,
        now=datetime.now(timezone.utc),
    )


def _usable_secret(value: str | None) -> str:
    candidate = (value or "").strip()
    if not candidate:
        return ""
    if candidate.lower() in {"change-me-root", "change-me-db", "change-me", "no"}:
        return ""
    return candidate


def _inspect_container_env_sync(container_name: str) -> dict[str, str]:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}", container_name],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "docker inspect fehlgeschlagen").strip()
        raise RuntimeError(detail)
    env_map: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_map[key] = value
    return env_map


def _mirror_dump_candidates() -> list[tuple[str, str]]:
    workspace_env = _workspace_env_values()
    try:
        env_map = _inspect_container_env_sync(MIRROR_DB_CONTAINER_NAME)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Konnte Mirror-Container-Env nicht lesen: %s", exc)
        env_map = {}

    db_user_candidates = [
        (os.getenv("TS_CONNECT_MIRROR_DB_USER") or "").strip(),
        (env_map.get("TS_CONNECT_MIRROR_DB_USER") or "").strip(),
        (env_map.get("MARIADB_USER") or "").strip(),
        (workspace_env.get("TS_CONNECT_MIRROR_DB_USER") or "").strip(),
        (DEFAULT_MIRROR_DB_USER or "").strip(),
    ]
    db_password_candidates = [
        _usable_secret(os.getenv("TS_CONNECT_MIRROR_DB_PASSWORD")),
        _usable_secret(env_map.get("TS_CONNECT_MIRROR_DB_PASSWORD")),
        _usable_secret(env_map.get("MARIADB_PASSWORD")),
        _usable_secret(MIRROR_DB_PASSWORD),
        _usable_secret(workspace_env.get("TS_CONNECT_MIRROR_DB_PASSWORD")),
    ]
    candidates: list[tuple[str, str]] = []

    def add_candidate(user: str, password: str) -> None:
        username = (user or "").strip()
        secret = _usable_secret(password)
        if not username or not secret:
            return
        pair = (username, secret)
        if pair not in candidates:
            candidates.append(pair)

    add_candidate("root", os.getenv("TS_CONNECT_MIRROR_ROOT_PASSWORD") or "")
    add_candidate("root", env_map.get("MARIADB_ROOT_PASSWORD") or "")
    add_candidate("root", workspace_env.get("TS_CONNECT_MIRROR_ROOT_PASSWORD") or "")

    for username in db_user_candidates:
        for password in db_password_candidates:
            add_candidate(username, password)

    if not candidates:
        raise RuntimeError("Kein Mirror-DB-Passwort für den Vollbackup-Dump verfügbar.")
    return candidates


def _build_mirror_dump_command(
    dump_user: str,
    dump_password: str,
    *,
    include_routines: bool = True,
    include_events: bool = True,
    include_triggers: bool = True,
    auth_mode: str = "explicit",
) -> list[str]:
    command = ["docker", "exec"]
    if auth_mode == "explicit":
        command.extend(["-e", f"MYSQL_PWD={dump_password}", MIRROR_DB_CONTAINER_NAME, "mariadb-dump", "-h127.0.0.1", f"-u{dump_user}"])
    elif auth_mode == "socket-password":
        command.extend(["-e", f"MYSQL_PWD={dump_password}", MIRROR_DB_CONTAINER_NAME, "mariadb-dump", f"-u{dump_user}"])
    elif auth_mode != "container-root":
        raise RuntimeError(f"Unbekannter Mirror-Dump-Auth-Modus: {auth_mode}")
    else:
        command.extend([MIRROR_DB_CONTAINER_NAME, "mariadb-dump"])
    command.extend(
        [
            "--single-transaction",
            "--quick",
            "--hex-blob",
            "--default-character-set=utf8mb4",
            "--databases",
            DEFAULT_MIRROR_DB_NAME,
        ]
    )
    if include_routines:
        command.insert(-2, "--routines")
    if include_events:
        command.insert(-2, "--events")
    if include_triggers:
        command.insert(-2, "--triggers")
    return command


def _mirror_dump_variants(dump_user: str, *, auth_mode: str = "explicit") -> list[dict[str, Any]]:
    variants = [
        {
            "label": "full",
            "include_routines": True,
            "include_events": True,
            "include_triggers": True,
            "auth_mode": auth_mode,
        }
    ]
    variants.extend(
        [
            {
                "label": "without-events",
                "include_routines": True,
                "include_events": False,
                "include_triggers": True,
                "auth_mode": auth_mode,
            },
            {
                "label": "without-events-routines",
                "include_routines": False,
                "include_events": False,
                "include_triggers": True,
                "auth_mode": auth_mode,
            },
            {
                "label": "tables-only",
                "include_routines": False,
                "include_events": False,
                "include_triggers": False,
                "auth_mode": auth_mode,
            },
        ]
    )
    return variants


def _write_mirror_dump_temp_sync(
    temp_path: Path,
    *,
    dump_user: str,
    dump_password: str,
    include_routines: bool = True,
    include_events: bool = True,
    include_triggers: bool = True,
    auth_mode: str = "explicit",
) -> int:
    command = _build_mirror_dump_command(
        dump_user,
        dump_password,
        include_routines=include_routines,
        include_events=include_events,
        include_triggers=include_triggers,
        auth_mode=auth_mode,
    )
    stderr_output = ""
    try:
        with temp_path.open("wb") as raw_handle:
            with gzip.GzipFile(
                filename=temp_path.with_suffix("").name,
                mode="wb",
                fileobj=raw_handle,
                compresslevel=6,
                mtime=0,
            ) as gzip_handle:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                assert process.stdout is not None
                assert process.stderr is not None
                try:
                    while True:
                        chunk = process.stdout.read(1024 * 1024)
                        if not chunk:
                            break
                        gzip_handle.write(chunk)
                    stderr_output = process.stderr.read().decode("utf-8", errors="replace").strip()
                    return_code = process.wait(timeout=60)
                except Exception:  # noqa: BLE001
                    process.kill()
                    raise
            raw_handle.flush()
            os.fsync(raw_handle.fileno())
        if return_code != 0:
            detail = stderr_output or f"mariadb-dump exited with {return_code}"
            raise RuntimeError(detail)
        if temp_path.stat().st_size <= 0:
            raise RuntimeError("Der Mirror-DB-Dump ist leer.")
        return temp_path.stat().st_size
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _resolve_mirror_dump_credentials() -> tuple[str, str]:
    return _mirror_dump_candidates()[0]


def _is_mirror_dump_auth_error(detail: str) -> bool:
    lowered = (detail or "").strip().lower()
    if not lowered:
        return False
    if "access denied" in lowered and "to database" not in lowered and "show full tables" not in lowered:
        return True
    markers = (
        "using password",
        "authentication failed",
        "password incorrect",
    )
    return any(marker in lowered for marker in markers)


def _create_mirror_backup_sync(target_path: Path) -> int:
    _raise_for_storage_issue_sync("mirror_db")
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _tmp_path_for(target_path)
    errors: list[str] = []
    attempts: list[tuple[str, str, str]] = []
    for dump_user, dump_password in _mirror_dump_candidates():
        attempts.append((dump_user, dump_password, "explicit"))
        if dump_user == "root":
            attempts.append((dump_user, dump_password, "socket-password"))
    attempts.append(("root", "", "container-root"))
    seen_attempts: set[tuple[str, str, str]] = set()
    for dump_user, dump_password, auth_mode in attempts:
        attempt_key = (dump_user, dump_password, auth_mode)
        if attempt_key in seen_attempts:
            continue
        seen_attempts.add(attempt_key)
        for variant in _mirror_dump_variants(dump_user, auth_mode=auth_mode):
            try:
                file_size = _write_mirror_dump_temp_sync(
                    temp_path,
                    dump_user=dump_user,
                    dump_password=dump_password,
                    include_routines=variant["include_routines"],
                    include_events=variant["include_events"],
                    include_triggers=variant["include_triggers"],
                    auth_mode=variant["auth_mode"],
                )
                if variant["label"] != "full":
                    logger.warning(
                        "Mirror-DB-Dump mit reduziertem Umfang (%s) erfolgreich, weil Nutzer %s keine Vollrechte hat.",
                        variant["label"],
                        dump_user,
                    )
                temp_path.replace(target_path)
                _fsync_directory(target_path.parent)
                return file_size
            except Exception as exc:  # noqa: BLE001
                detail = str(exc).strip() or "Mirror-DB-Dump fehlgeschlagen"
                logger.warning(
                    "Mirror-DB-Dump mit Nutzer %s (%s/%s) fehlgeschlagen: %s",
                    dump_user,
                    auth_mode,
                    variant["label"],
                    detail,
                )
                errors.append(f"{dump_user}/{auth_mode}/{variant['label']}: {detail}")
                if _is_mirror_dump_auth_error(detail):
                    break
    raise RuntimeError(errors[-1] if errors else "Mirror-DB-Dump fehlgeschlagen.")


async def _run_mirror_backup(*, trigger: str, slot_id: str | None = None) -> dict[str, Any]:
    async with _mirror_backup_run_lock:
        if not MIRROR_BACKUP_ENABLED:
            raise RuntimeError("Mirror-DB-Backups sind deaktiviert.")
        started_at = _now_utc_iso()
        await merge_mirror_backup_state(
            in_progress=True,
            last_started=started_at,
            last_error=None,
        )
        logger.info("Starte Mirror-DB-Backup (%s) nach %s", trigger, MIRROR_BACKUP_DIR)
        try:
            removed_before = await asyncio.to_thread(_prune_mirror_backups_now)
            if removed_before:
                logger.info("Mirror-DB-Backup-Rotation entfernte %s alte Datei(en)", len(removed_before))
            filename = build_backup_filename("mirror-db-backup", datetime.now(timezone.utc), ".sql.gz")
            target_path = MIRROR_BACKUP_DIR / filename
            file_size = await asyncio.to_thread(_create_mirror_backup_sync, target_path)
            removed_after = await asyncio.to_thread(_prune_mirror_backups_now)
            if removed_after:
                logger.info("Mirror-DB-Backup-Rotation entfernte %s Datei(en) nach Backup", len(removed_after))
            next_run = _next_mirror_backup_run_iso()
            state = await merge_mirror_backup_state(
                in_progress=False,
                last_success=_now_utc_iso(),
                last_error=None,
                last_file=filename,
                last_file_size=file_size,
                last_run_slot=slot_id,
                next_run=next_run,
            )
            logger.info("Mirror-DB-Backup erfolgreich erstellt: %s (%s)", target_path, _format_size(file_size))
            return state
        except Exception as exc:  # noqa: BLE001
            detail = _short_error_message(str(exc), 240)
            next_run = _next_mirror_backup_run_iso()
            await merge_mirror_backup_state(
                in_progress=False,
                last_error=detail,
                last_run_slot=slot_id,
                next_run=next_run,
            )
            logger.warning("Mirror-DB-Backup fehlgeschlagen: %s", detail)
            raise


async def _refresh_mirror_backup_state() -> dict[str, Any]:
    if MIRROR_BACKUP_ENABLED:
        await asyncio.to_thread(_prune_mirror_backups_now)
    state = await merge_mirror_backup_state(enabled=MIRROR_BACKUP_ENABLED)
    state["last_file_size_bytes"] = state.get("last_file_size")
    state["last_file_size"] = _format_size(int(state.get("last_file_size") or 0)) if state.get("last_file_size") else None
    state["backups"] = _mirror_backup_entries()
    state["retention_days"] = MIRROR_BACKUP_RETENTION_DAYS
    state["directory"] = str(MIRROR_BACKUP_DIR)
    state["server_directory"] = MIRROR_BACKUP_HOST_DIR or str(MIRROR_BACKUP_DIR)
    state["schedule"] = {
        "hour": MIRROR_BACKUP_HOUR,
        "minute": MIRROR_BACKUP_MINUTE,
        "timezone": str(LOCAL_TIMEZONE),
    }
    return state


async def _maybe_run_scheduled_mirror_backup() -> None:
    if not MIRROR_BACKUP_ENABLED:
        return
    state = await get_mirror_backup_state()
    due, slot_id, next_run_dt = scheduled_run_due(
        now=datetime.now(timezone.utc),
        last_slot_id=state.get("last_run_slot"),
        hour=MIRROR_BACKUP_HOUR,
        minute=MIRROR_BACKUP_MINUTE,
        tz=LOCAL_TIMEZONE,
    )
    next_run = next_run_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    if state.get("next_run") != next_run:
        await merge_mirror_backup_state(next_run=next_run)
    if not due or state.get("in_progress"):
        return
    try:
        await _run_mirror_backup(trigger="auto", slot_id=slot_id)
    except Exception:  # noqa: BLE001
        logger.warning("Automatisches Mirror-DB-Backup fehlgeschlagen", exc_info=True)


async def _mirror_backup_worker() -> None:
    while True:
        try:
            await _maybe_run_scheduled_mirror_backup()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.warning("Mirror-DB-Backup-Worker fehlgeschlagen: %s", exc)
        await asyncio.sleep(MIRROR_BACKUP_POLL_SECONDS)


def _should_refresh_os_updates(state: dict) -> bool:
    if state.get("check_in_progress") or state.get("update_in_progress"):
        return False
    last_check = _parse_iso8601(state.get("last_check"))
    if not last_check:
        return True
    if OS_UPDATE_MAX_AGE_SECONDS <= 0:
        return False
    return (datetime.now(timezone.utc) - last_check) >= timedelta(seconds=OS_UPDATE_MAX_AGE_SECONDS)


def _ensure_ubuntu_host() -> None:
    os_release = Path("/etc/os-release")
    if not os_release.exists():
        raise RuntimeError("Betriebssystem konnte nicht erkannt werden (nur Ubuntu LTS wird unterstützt).")
    data = os_release.read_text(encoding="utf-8", errors="ignore").lower()
    if "id=ubuntu" not in data:
        raise RuntimeError("Dieses System wird nicht unterstützt. Bitte Ubuntu LTS für Betriebssystem-Updates verwenden.")


def _short_error_message(raw: str, max_len: int = 180) -> str:
    if not raw:
        return ""
    lines = [line.strip() for line in raw.strip().splitlines() if line.strip()]
    if not lines:
        return ""
    message = lines[0]
    for extra in lines[1:]:
        candidate = f"{message} | {extra}"
        if len(candidate) > max_len:
            message = message if len(message) <= max_len else message[: max_len - 1] + "…"
            break
        message = candidate
    else:
        if len(message) > max_len:
            message = message[: max_len - 1] + "…"
    return message


_HTTP_ERROR_PREFIX_RE = re.compile(r"^(GET|POST|PUT|DELETE)\s+\S+\s*->\s*\d{3}:\s*", re.IGNORECASE)
_APPLY_ERROR_HINTS = (
    (
        ("communications link failure", "unable to connect", "jdbc:mysql"),
        "Keine Verbindung zur lokalen Mirror-MariaDB. Bitte Host, Port oder Container-Status prüfen.",
    ),
    (
        ("connection refused",),
        "Die lokale Mirror-MariaDB lehnt Verbindungen ab. Ist der Dienst gestartet und der Port freigegeben?",
    ),
    (
        ("connect timed out", "connection timed out", "timeout"),
        "Zeitüberschreitung bei der Verbindung zur lokalen Mirror-MariaDB. Netzwerkpfad prüfen.",
    ),
)


def _format_apply_error(raw: str, *, max_len: int = 280) -> str:
    cleaned = _short_error_message(raw, max_len)
    cleaned = _HTTP_ERROR_PREFIX_RE.sub("", cleaned).strip(" -")
    if not cleaned:
        return _short_error_message(raw, max_len)
    lowered = cleaned.lower()
    for markers, hint in _APPLY_ERROR_HINTS:
        if any(marker in lowered for marker in markers):
            if hint.lower() in lowered:
                return cleaned
            return f"{hint} ({cleaned})"
    return cleaned


_APT_LIST_LINE_RE = re.compile(
    r"^(?P<name>[^/]+)/(?P<section>\S+)\s+(?P<version>\S+)\s+(?P<arch>\S+)\s+\[upgradable from: (?P<current>[^\]]+)\]",
    re.IGNORECASE,
)


def _parse_upgradable_list(output: str) -> list[dict[str, Any]]:
    packages: list[dict[str, Any]] = []
    if not output:
        return packages
    for line in output.splitlines():
        line = line.strip()
        if not line or line.lower().startswith("listing"):
            continue
        match = _APT_LIST_LINE_RE.match(line)
        if match:
            data = match.groupdict()
            name = data.get("name") or line
            section = data.get("section") or ""
            version = data.get("version") or ""
            current = data.get("current") or ""
            packages.append(
                {
                    "name": name,
                    "section": section,
                    "version": version,
                    "current": current,
                    "arch": data.get("arch") or "",
                    "security": "security" in section.lower(),
                }
            )
            continue
        first_token = line.split()[0]
        packages.append(
            {
                "name": first_token,
                "section": "",
                "version": "",
                "current": "",
                "arch": "",
                "security": False,
            }
        )
    return packages


_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html_tags(value: str) -> str:
    if not value:
        return ""
    if "<" not in value:
        return value.strip()
    text = _HTML_TAG_RE.sub(" ", value)
    text = html.unescape(text)
    return " ".join(text.split())


def _parse_iso8601(timestamp: str | None) -> datetime | None:
    if not timestamp:
        return None
    try:
        if timestamp.endswith("Z"):
            timestamp = timestamp[:-1] + "+00:00"
        return datetime.fromisoformat(timestamp)
    except ValueError:
        return None


def _format_local_timestamp(dt: datetime | None) -> str | None:
    if not dt:
        return None
    local_dt = _to_local(dt)
    if not local_dt:
        return None
    return local_dt.strftime("%d.%m.%Y, %H:%M Uhr")


async def configure_git_safety() -> None:
    try:
        await _ensure_git_safe_directory()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to configure git safe.directory: %s", exc)


async def ensure_update_state() -> dict[str, Any]:
    return await asyncio.to_thread(update_state_manager.ensure)


async def get_update_state_snapshot() -> dict[str, Any]:
    return await asyncio.to_thread(update_state_manager.read)


async def merge_update_state_async(**updates: Any) -> dict[str, Any]:
    return await asyncio.to_thread(update_state_manager.merge, **updates)


async def append_update_log(lines: list[str], *, reset: bool = False) -> dict[str, Any]:
    return await merge_update_state_async(log_append=lines, log_reset=reset)


def _parse_repo_slug(remote_url: str) -> str | None:
    remote_url = remote_url.strip()
    if remote_url.endswith(".git"):
        remote_url = remote_url[:-4]
    if remote_url.startswith("git@github.com:"):
        return remote_url[len("git@github.com:") :]
    if remote_url.startswith("https://github.com/"):
        return remote_url[len("https://github.com/") :]
    if remote_url.startswith("http://github.com/"):
        return remote_url[len("http://github.com/") :]
    if remote_url.startswith("ssh://git@github.com/"):
        return remote_url[len("ssh://git@github.com/") :]
    return None


def _parse_license_validation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return _parse_keygen_license_validation_payload(payload)


def _build_keygen_validate_meta(license_key: str, *, include_machine_scope: bool = True) -> dict[str, Any]:
    key = (license_key or "").strip()
    if not key:
        raise ValueError("Bitte einen Lizenzschlüssel eingeben.")
    meta: dict[str, Any] = {"key": key}
    scope: dict[str, Any] = {}
    if KEYGEN_POLICY_ID:
        scope["policy"] = KEYGEN_POLICY_ID
    if include_machine_scope:
        fingerprints = _resolve_machine_fingerprint_scope()
    else:
        fingerprints = []
    if fingerprints:
        scope["fingerprint"] = fingerprints[0]
        scope["fingerprints"] = fingerprints
    if scope:
        meta["scope"] = scope
    return meta


async def validate_license_key_remote(license_key: str, *, include_machine_scope: bool = True) -> dict[str, Any]:
    meta = _build_keygen_validate_meta(license_key, include_machine_scope=include_machine_scope)
    fingerprints = _resolve_machine_fingerprint_scope() if include_machine_scope else []
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"{KEYGEN_BASE_URL}/licenses/actions/validate-key",
                json={"meta": meta},
                headers={
                    "Accept": "application/vnd.api+json",
                    "Content-Type": "application/vnd.api+json",
                },
            )
    except httpx.RequestError as exc:  # noqa: BLE001
        raise RuntimeError(f"Lizenzserver nicht erreichbar: {exc}") from exc
    if response.status_code >= 400:
        raise RuntimeError(
            f"Lizenzprüfung fehlgeschlagen ({response.status_code}): {_extract_error_message(response)}"
        )
    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError("Lizenzserver lieferte eine ungültige Antwort.") from exc
    parsed = _parse_keygen_license_validation_payload(data if isinstance(data, dict) else {})
    if parsed.get("license_id") and not parsed.get("valid"):
        machine = await _find_keygen_machine(key, fingerprints)
        if machine:
            machine_attributes = machine.get("attributes") if isinstance(machine.get("attributes"), dict) else {}
            parsed["activation_id"] = machine.get("id")
            parsed["activation_at"] = (
                machine_attributes.get("created")
                or machine_attributes.get("createdAt")
                or machine_attributes.get("created_at")
                or machine_attributes.get("updated")
                or machine_attributes.get("updatedAt")
                or machine_attributes.get("updated_at")
            )
    if not parsed.get("message") and isinstance(data, dict):
        parsed["message"] = _extract_keygen_message(data, data.get("meta") if isinstance(data.get("meta"), dict) else {})
    if parsed.get("message"):
        parsed["error"] = parsed["message"]
    return parsed


async def activate_license_key_remote(
    license_key: str,
    *,
    instance_name: str,
    instance_id: str,
    validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    key = (license_key or "").strip()
    if not key:
        raise ValueError("Lizenzschlüssel fehlt für die Aktivierung.")
    if not KEYGEN_BASE_URL:
        raise RuntimeError("Keygen ist nicht konfiguriert.")
    validation = validation or {}
    license_id = str(validation.get("license_id") or "").strip()
    if not license_id:
        validation = await validate_license_key_remote(key)
        license_id = str(validation.get("license_id") or "").strip()
    if not license_id:
        raise RuntimeError("Keygen-Lizenz konnte nicht aufgelöst werden.")

    fingerprint = _resolve_machine_fingerprint()
    fingerprints = _resolve_machine_fingerprint_scope()
    machine_name = _resolve_machine_name(fallback=instance_name or instance_id)
    headers = _build_keygen_machine_headers(key, include_content_type=True)
    payload = {
        "data": {
            "type": "machines",
            "attributes": {
                "name": machine_name,
                "fingerprint": fingerprint,
                "platform": platform.platform(),
                "hostname": socket.gethostname(),
            },
            "relationships": {
                "license": {
                    "data": {
                        "type": "licenses",
                        "id": license_id,
                    },
                },
            },
        },
    }

    machine: dict[str, Any] | None = None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                f"{KEYGEN_BASE_URL}/machines",
                json=payload,
                headers=headers,
            )
    except httpx.RequestError as exc:  # noqa: BLE001
        raise RuntimeError(f"Lizenzaktivierung fehlgeschlagen: {exc}") from exc
    if response.status_code >= 400:
        detail = _extract_error_message(response)
        if "License key authentication is not allowed by policy" in detail and not KEYGEN_LICENSE_TOKEN:
            detail = (
                f"{detail} | Hinterlege TS_CONNECT_KEYGEN_LICENSE_TOKEN oder "
                "erlaube Lizenzschlüssel-Authentifizierung in der Keygen-Policy."
            )
        machine = await _find_keygen_machine(key, fingerprints)
        if not machine:
            raise RuntimeError(
                f"Lizenzaktivierung fehlgeschlagen ({response.status_code}): {detail}"
            )
    else:
        try:
            data = response.json()
        except json.JSONDecodeError as exc:
            raise RuntimeError("Lizenzaktivierung lieferte eine ungültige Antwort.") from exc
        payload_data = data.get("data") if isinstance(data, dict) and isinstance(data.get("data"), dict) else None
        machine = payload_data if isinstance(payload_data, dict) else None
        if not machine:
            machine = await _find_keygen_machine(key, fingerprints)

    machine = machine or await _find_keygen_machine(key, fingerprints)
    machine_attributes = machine.get("attributes") if isinstance(machine, dict) and isinstance(machine.get("attributes"), dict) else {}
    activated_at = (
        machine_attributes.get("created")
        or machine_attributes.get("createdAt")
        or machine_attributes.get("created_at")
        or _now_utc_iso()
    )
    return {
        "payload": machine,
        "activation_id": machine.get("id") if isinstance(machine, dict) else None,
        "status": machine_attributes.get("status") if isinstance(machine_attributes, dict) else "active",
        "activated": True,
        "activated_at": activated_at,
        "message": "Maschine in Keygen aktiviert.",
    }
async def _run_command_capture(cmd: list[str], *, cwd: Path | None = None, timeout: float | None = None) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=PIPE,
        stderr=PIPE,
        cwd=str(cwd) if cwd else None,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        raise
    return process.returncode, stdout.decode("utf-8", errors="replace"), stderr.decode("utf-8", errors="replace")


async def _run_command_text(cmd: list[str], *, cwd: Path | None = None, timeout: float | None = None) -> tuple[int, str]:
    code, stdout, stderr = await _run_command_capture(cmd, cwd=cwd, timeout=timeout)
    output = stdout if stdout.strip() else stderr
    return code, output.strip()


async def _docker_inspect_value(name: str, fmt: str) -> str | None:
    code, output = await _run_command_text(
        ["docker", "inspect", "--format", fmt, name],
        timeout=10,
    )
    if code != 0:
        return None
    value = output.strip()
    return value or None


async def _docker_image_id(image_ref: str) -> str | None:
    code, output = await _run_command_text(
        ["docker", "image", "inspect", "--format", "{{.Id}}", image_ref],
        timeout=20,
    )
    if code != 0:
        return None
    value = output.strip()
    return value or None


def _update_agent_compose_base() -> tuple[Path, list[str]]:
    compose_dir = WORKSPACE_PATH / "update-agent"
    cmd = ["docker", "compose"]
    if (WORKSPACE_PATH / ".env").exists():
        cmd += ["--env-file", "../.env"]
    return compose_dir, cmd


async def _update_agent_refresh_needed() -> tuple[bool, str]:
    return await ops_runtime.update_agent_refresh_needed(
        workspace_path=WORKSPACE_PATH,
        get_update_state_snapshot_fn=get_update_state_snapshot,
        default_update_image=DEFAULT_UPDATE_IMAGE,
        update_agent_container_name=UPDATE_AGENT_CONTAINER_NAME,
        docker_image_id_fn=_docker_image_id,
        docker_inspect_value_fn=_docker_inspect_value,
    )


def _command_log_lines(output: str) -> list[str]:
    return ops_runtime.command_log_lines(output)


async def _refresh_update_agent_if_needed() -> None:
    await ops_runtime.refresh_update_agent_if_needed(
        update_agent_sync_lock=_update_agent_sync_lock,
        update_agent_refresh_needed_fn=_update_agent_refresh_needed,
        default_update_image=DEFAULT_UPDATE_IMAGE,
        logger=logger,
        ensure_registry_login_for_release_fn=_ensure_registry_login_for_release,
        update_agent_compose_base_fn=_update_agent_compose_base,
        run_command_capture_fn=_run_command_capture,
        update_agent_container_name=UPDATE_AGENT_CONTAINER_NAME,
        ping_update_agent_fn=_ping_update_agent,
    )


async def _delayed_update_agent_sync() -> None:
    await ops_runtime.delayed_update_agent_sync(
        update_agent_sync_delay_seconds=UPDATE_AGENT_SYNC_DELAY_SECONDS,
        refresh_update_agent_if_needed_fn=_refresh_update_agent_if_needed,
    )


def _sanitize_hour(value: Any, default: int = AUTO_UPDATE_DEFAULT_HOUR) -> int:
    try:
        hour = int(value)
    except (TypeError, ValueError):
        return default
    return max(0, min(23, hour))


def _parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        val = value.strip().lower()
        if val in {"1", "true", "yes", "on"}:
            return True
        if val in {"0", "false", "no", "off"}:
            return False
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return default


def _now_local() -> datetime:
    return datetime.now(timezone.utc).astimezone(LOCAL_TIMEZONE)


def _to_local(dt: datetime | None) -> datetime | None:
    if not dt:
        return None
    return dt.astimezone(LOCAL_TIMEZONE)


def _calculate_next_auto_run(hour: int, last_run_iso: str | None) -> datetime:
    hour = _sanitize_hour(hour)
    now_local = _now_local()
    base_local = now_local
    if last_run_iso:
        last_dt = _parse_iso8601(last_run_iso)
        last_local = _to_local(last_dt)
        if last_local and last_local > base_local:
            base_local = last_local
    candidate_local = base_local.replace(hour=hour, minute=0, second=0, microsecond=0)
    if candidate_local <= base_local:
        candidate_local += timedelta(days=1)
    return candidate_local.astimezone(timezone.utc)


def _job_age_seconds(job_started: str | None) -> float | None:
    start_dt = _parse_iso8601(job_started)
    if not start_dt:
        return None
    return (datetime.now(timezone.utc) - start_dt).total_seconds()


async def _ensure_git_safe_directory() -> None:
    global _git_safe_configured
    if _git_safe_configured:
        return
    if not WORKSPACE_PATH.exists():
        return
    code, _ = await _run_command_text(["git", "config", "--global", "--add", "safe.directory", str(WORKSPACE_PATH)], cwd=WORKSPACE_PATH)
    if code == 0:
        _git_safe_configured = True


async def _determine_repo_slug(force: bool = False) -> str | None:
    global _cached_repo_slug
    if GITHUB_REPO_OVERRIDE:
        _cached_repo_slug = GITHUB_REPO_OVERRIDE
        return _cached_repo_slug or None
    if _cached_repo_slug and not force:
        return _cached_repo_slug
    if not WORKSPACE_PATH.exists() or not (WORKSPACE_PATH / ".git").exists():
        return None
    await _ensure_git_safe_directory()
    code, remote = await _run_command_text(["git", "remote", "get-url", "origin"], cwd=WORKSPACE_PATH)
    if code != 0 or not remote:
        return None
    remote = remote.strip()
    slug = _parse_repo_slug(remote)
    if slug and remote.startswith("git@github.com:"):
        https_url = f"https://github.com/{slug}.git"
        set_code, _ = await _run_command_text(["git", "remote", "set-url", "origin", https_url], cwd=WORKSPACE_PATH)
        if set_code == 0:
            remote = https_url
    slug = slug or _parse_repo_slug(remote)
    if slug:
        _cached_repo_slug = slug
    return slug


async def _read_local_ui_image_details() -> tuple[str | None, str | None]:
    container_name = (UI_CONTAINER_NAME or "ts-connect-ui").strip() or "ts-connect-ui"
    digest: str | None = None
    image_ref: str | None = None
    code, output = await _run_command_text(
        ["docker", "inspect", "--format", "{{.Image}}", container_name],
        cwd=WORKSPACE_PATH,
    )
    if code == 0 and output:
        digest = output.strip()
        code_img, img_output = await _run_command_text(
            ["docker", "inspect", "--format", "{{.Config.Image}}", container_name],
            cwd=WORKSPACE_PATH,
        )
        if code_img == 0 and img_output:
            image_ref = img_output.strip()
    if not digest:
        fallback_image = os.getenv("TS_CONNECT_UI_IMAGE", DEFAULT_UPDATE_IMAGE)
        code_img, img_digest = await _run_command_text(
            ["docker", "image", "inspect", "--format", "{{.Id}}", fallback_image],
            cwd=WORKSPACE_PATH,
        )
        if code_img == 0 and img_digest:
            digest = img_digest.strip()
            image_ref = fallback_image
    return digest, image_ref


async def _collect_workspace_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "path": str(WORKSPACE_PATH),
        "exists": WORKSPACE_PATH.exists(),
        "git": False,
        "dirty": None,
        "current_ref": None,
        "current_commit": None,
    }
    if not info["exists"]:
        return info
    git_dir = WORKSPACE_PATH / ".git"
    info["git"] = git_dir.exists()
    if not info["git"]:
        return info
    status_code, status_out, _ = await _run_command_capture(["git", "status", "--porcelain"], cwd=WORKSPACE_PATH)
    if status_code == 0:
        info["dirty"] = bool(status_out.strip())
    branch_code, branch = await _run_command_text(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=WORKSPACE_PATH)
    if branch_code == 0 and branch:
        info["current_ref"] = branch
    commit_code, commit = await _run_command_text(["git", "rev-parse", "HEAD"], cwd=WORKSPACE_PATH)
    if commit_code == 0 and commit:
        info["current_commit"] = commit
    return info


async def _detect_prerequisites(workspace_info: dict[str, Any]) -> dict[str, Any]:
    def _binary_found(name: str, extra_paths: tuple[str, ...] = ()) -> bool:
        path = shutil.which(name)
        if path and os.access(path, os.X_OK):
            return True
        for extra in extra_paths:
            p = Path(extra)
            if p.exists() and os.access(p, os.X_OK):
                return True
        return False

    git_available = _binary_found("git", ("/usr/bin/git", "/usr/local/bin/git"))
    workspace_ready = bool(workspace_info.get("exists") and workspace_info.get("git"))
    update_agent_status = await _read_update_agent_status(timeout=3.0)
    update_agent_ok = bool(update_agent_status and update_agent_status.get("available") and not update_agent_status.get("auth_error"))
    update_agent_error: str | None = None
    if update_agent_status and update_agent_status.get("error"):
        update_agent_error = _short_error_message(str(update_agent_status.get("error")), 160)
    host_agent_required = bool(HOST_AGENT_URL)
    host_agent_ok = True
    host_agent_error: str | None = None
    if host_agent_required:
        host_agent_ok = await _ping_host_agent()
        if not host_agent_ok:
            try:
                await _host_agent_request("GET", "/api/v1/health", timeout=3.0)
            except Exception as exc:  # noqa: BLE001
                host_agent_error = _short_error_message(str(exc), 160)
    storage_preflight = await _get_storage_ownership_preflight()
    storage_ok = bool(storage_preflight.get("ok"))
    storage_error: str | None = None
    if not storage_ok:
        summaries = [str(issue.get("summary") or "").strip() for issue in storage_preflight.get("issues", [])]
        summaries = [summary for summary in summaries if summary]
        storage_error = " | ".join(summaries[:2]) if summaries else "DB-Datenordner-Rechte prüfen"
    overall = git_available and workspace_ready and update_agent_ok and storage_ok
    if host_agent_required:
        overall = overall and host_agent_ok
    return {
        "git": git_available,
        "update_agent": update_agent_ok,
        "update_agent_error": update_agent_error,
        "workspace": workspace_ready,
        "storage": storage_ok,
        "storage_error": storage_error,
        "host_agent": host_agent_ok if host_agent_required else None,
        "host_agent_error": host_agent_error,
        "ok": overall,
    }


def _extract_manifest_version(manifest: dict[str, Any]) -> str | None:
    annotations = manifest.get("annotations") or {}
    for key in ("org.opencontainers.image.version", "org.opencontainers.image.ref.name"):
        value = annotations.get(key)
        if isinstance(value, str):
            candidate = value.strip()
            if candidate:
                return candidate
    return None


def _image_repo_from_ref(ref: str) -> str:
    base = ref.split("@", 1)[0]
    last_colon = base.rfind(":")
    last_slash = base.rfind("/")
    if last_colon > last_slash:
        return base[:last_colon]
    return base


def _registry_from_image(ref: str) -> str | None:
    base = ref.split("@", 1)[0]
    if "/" not in base:
        return None
    candidate = base.split("/", 1)[0]
    if "." in candidate or ":" in candidate:
        return candidate
    return None


def _parse_image_reference(ref: str) -> tuple[str, str, str]:
    if not ref:
        raise ValueError("Leerer Image-Ref")
    name = ref
    reference: str | None = None
    if "@" in name:
        name, reference = name.rsplit("@", 1)
    else:
        last_colon = name.rfind(":")
        last_slash = name.rfind("/")
        if last_colon > last_slash:
            name, reference = name[:last_colon], name[last_colon + 1 :]
    if not reference:
        reference = "latest"
    registry: str | None = None
    repository = name
    if "/" in name:
        candidate = name.split("/", 1)[0]
        if "." in candidate or ":" in candidate or candidate == "localhost":
            registry = candidate
            repository = name.split("/", 1)[1]
    if registry is None:
        registry = "registry-1.docker.io"
        if "/" not in repository:
            repository = f"library/{repository}"
    if not repository:
        raise ValueError(f"Ungültiger Image-Ref: {ref}")
    return registry, repository, reference


def _encode_repository_path(repository: str) -> str:
    parts = [quote(part, safe="") for part in repository.split("/") if part]
    return "/".join(parts)


def _encode_reference(reference: str) -> str:
    return quote(reference, safe=":@")


_REGISTRY_MANIFEST_ACCEPT = ",".join(
    (
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
    )
)
_REGISTRY_CONFIG_ACCEPT = ",".join(
    (
        "application/vnd.oci.image.config.v1+json",
        "application/vnd.docker.container.image.v1+json",
        "application/json",
    )
)
_AUTH_PARAM_RE = re.compile(r'(\w+)=(".*?"|[^,]+)')


async def _registry_http_get(
    registry: str,
    path: str,
    *,
    username: str | None = None,
    password: str | None = None,
    accept: str | None = None,
    scope: str | None = None,
) -> httpx.Response:
    url = f"https://{registry}{path}"
    headers: dict[str, str] = {}
    if accept:
        headers["Accept"] = accept
    auth = httpx.BasicAuth(username, password) if username and password else None
    bearer_token: str | None = None
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(url, headers=headers, auth=auth)
            if response.status_code == 401:
                challenge = response.headers.get("www-authenticate", "")
                params = _parse_www_authenticate_header(challenge)
                if params.get("scheme") == "bearer":
                    token = await _registry_fetch_bearer_token(
                        params,
                        username=username,
                        password=password,
                        scope_override=scope,
                    )
                    if token:
                        bearer_token = token
                        headers.pop("Authorization", None)
                        headers["Authorization"] = f"Bearer {token}"
                        response = await client.get(url, headers=headers)
    except httpx.RequestError as exc:  # noqa: BLE001
        raise RuntimeError(f"Registry {registry} nicht erreichbar: {_short_error_message(str(exc), 160)}") from exc
    if response.status_code >= 400:
        detail = _short_error_message(response.text, 160)
        auth_hint = ""
        if response.status_code == 401 and not bearer_token:
            auth_hint = " (Authentifizierung fehlgeschlagen)"
        raise RuntimeError(f"Registry {registry}{path} -> {response.status_code}: {detail}{auth_hint}")
    return response


def _parse_www_authenticate_header(header: str) -> dict[str, str]:
    if not header:
        return {}
    parts = header.split(" ", 1)
    scheme = parts[0].strip().lower()
    params_part = parts[1] if len(parts) > 1 else ""
    params: dict[str, str] = {"scheme": scheme}
    for match in _AUTH_PARAM_RE.finditer(params_part):
        key = match.group(1).lower()
        value = match.group(2).strip()
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        params[key] = value
    return params


async def _registry_fetch_bearer_token(
    auth_params: dict[str, str],
    *,
    username: str | None,
    password: str | None,
    scope_override: str | None,
) -> str | None:
    realm = auth_params.get("realm")
    if not realm:
        return None
    params: dict[str, str] = {}
    service = auth_params.get("service")
    if service:
        params["service"] = service
    scope = auth_params.get("scope") or scope_override
    if scope:
        params["scope"] = scope
    auth = httpx.BasicAuth(username, password) if username and password else None
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(realm, params=params or None, auth=auth)
    except httpx.RequestError:
        return None
    if response.status_code >= 400:
        return None
    try:
        data = response.json()
    except json.JSONDecodeError:
        return None
    token = data.get("access_token") or data.get("token")
    return token


async def _registry_fetch_manifest_json(
    registry: str,
    repository_path: str,
    reference: str,
    *,
    repository_scope: str | None = None,
    username: str | None = None,
    password: str | None = None,
) -> tuple[dict[str, Any], str]:
    path = f"/v2/{repository_path}/manifests/{_encode_reference(reference)}"
    scope = f"repository:{repository_scope}:pull" if repository_scope else None
    response = await _registry_http_get(
        registry,
        path,
        username=username,
        password=password,
        accept=_REGISTRY_MANIFEST_ACCEPT,
        scope=scope,
    )
    try:
        return response.json(), response.headers.get("content-type", "")
    except json.JSONDecodeError as exc:
        raise RuntimeError("Registry lieferte ein ungültiges Manifest") from exc


async def _registry_fetch_blob_json(
    registry: str,
    repository_path: str,
    digest: str,
    *,
    repository_scope: str | None = None,
    username: str | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    path = f"/v2/{repository_path}/blobs/{_encode_reference(digest)}"
    scope = f"repository:{repository_scope}:pull" if repository_scope else None
    response = await _registry_http_get(
        registry,
        path,
        username=username,
        password=password,
        accept=_REGISTRY_CONFIG_ACCEPT,
        scope=scope,
    )
    try:
        return response.json()
    except json.JSONDecodeError:
        # Some registries return gzip-compressed blob bytes without usable JSON headers.
        payload = response.content
        if payload.startswith(b"\x1f\x8b"):
            try:
                decompressed = gzip.decompress(payload)
                decoded = decompressed.decode("utf-8", "replace")
                return json.loads(decoded)
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                pass
        content_type = response.headers.get("content-type", "").strip() or "unbekannt"
        raise RuntimeError(f"Registry lieferte einen ungültigen Image-Config-Blob ({content_type})")


def _select_manifest_entry(manifest: dict[str, Any]) -> dict[str, Any] | None:
    entries = manifest.get("manifests")
    if not isinstance(entries, list):
        return None
    for entry in entries:
        platform = entry.get("platform") or {}
        if platform.get("architecture") == "amd64":
            return entry
    return entries[0] if entries else None


async def _ensure_registry_login_for_release(image_ref: str) -> None:
    registry = os.getenv("TS_CONNECT_ACR_REGISTRY") or _registry_from_image(image_ref)
    username = os.getenv("TS_CONNECT_ACR_USERNAME", "").strip()
    password = os.getenv("TS_CONNECT_ACR_PASSWORD", "")
    if not registry or not username or not password:
        return
    async with _registry_login_lock:
        if registry in _logged_in_registries:
            return
        process = await asyncio.create_subprocess_exec(
            "docker",
            "login",
            registry,
            "-u",
            username,
            "--password-stdin",
            stdin=PIPE,
            stdout=PIPE,
            stderr=PIPE,
        )
        stdout_data, stderr_data = await process.communicate(input=(password + "\n").encode("utf-8"))
        if process.returncode != 0:
            stdout = stdout_data.decode("utf-8", "replace").strip()
            stderr = stderr_data.decode("utf-8", "replace").strip()
            message = stderr or stdout or f"Docker-Login für {registry} fehlgeschlagen"
            raise RuntimeError(message)
        _logged_in_registries.add(registry)


async def _fetch_release_via_registry_http(image_ref: str) -> dict[str, Any]:
    registry, repository, reference = _parse_image_reference(image_ref)
    repository_path = _encode_repository_path(repository)
    username = os.getenv("TS_CONNECT_ACR_USERNAME", "").strip() or None
    password = os.getenv("TS_CONNECT_ACR_PASSWORD", "")
    manifest, content_type = await _registry_fetch_manifest_json(
        registry,
        repository_path,
        reference,
        repository_scope=repository,
        username=username,
        password=password,
    )
    media_type = (manifest.get("mediaType") or content_type or "").lower()
    if media_type.endswith("manifest.list.v2+json") or media_type.endswith("image.index.v1+json") or manifest.get("manifests"):
        entry = _select_manifest_entry(manifest)
        if not entry or not entry.get("digest"):
            raise RuntimeError("Registry Manifest enthält keine Plattform-Einträge")
        manifest, _ = await _registry_fetch_manifest_json(
            registry,
            repository_path,
            entry["digest"],
            repository_scope=repository,
            username=username,
            password=password,
        )
    config = manifest.get("config") or {}
    config_digest = config.get("digest")
    if not config_digest:
        raise RuntimeError("Registry Manifest enthält keinen Config Digest")
    config_blob = await _registry_fetch_blob_json(
        registry,
        repository_path,
        config_digest,
        repository_scope=repository,
        username=username,
        password=password,
    )
    labels: dict[str, str] = {}
    config_section = config_blob.get("config")
    if isinstance(config_section, dict):
        raw_labels = config_section.get("Labels")
        if isinstance(raw_labels, dict):
            labels = raw_labels
    created = config_blob.get("created")
    version = (labels.get("org.opencontainers.image.version") or "").strip()
    if not version:
        version = (labels.get("org.opencontainers.image.ref.name") or "").strip()
    if not version:
        version = _extract_manifest_version(manifest) or reference
    return {
        "tag_name": version,
        "name": f"Version {version}",
        "published_at": created,
        "html_url": None,
        "image": image_ref,
        "digest": config_digest,
        "source": "registry-api",
    }


async def _fetch_release_via_docker_cli(image_ref: str) -> dict[str, Any]:
    await _ensure_registry_login_for_release(image_ref)
    try:
        code, stdout, stderr = await _run_command_capture(["docker", "manifest", "inspect", image_ref])
    except FileNotFoundError as exc:  # pragma: no cover - system dependency
        raise RuntimeError("Docker CLI nicht gefunden (docker manifest)") from exc
    if code != 0:
        message = stderr.strip() or stdout.strip() or f"docker manifest inspect {image_ref} fehlgeschlagen"
        raise RuntimeError(message)
    try:
        manifest = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Docker Manifest konnte nicht geparsed werden") from exc
    manifest_list = manifest.get("manifests")
    child_digest: str | None = None
    if isinstance(manifest_list, list) and manifest_list:
        preferred = next(
            (entry for entry in manifest_list if (entry.get("platform") or {}).get("architecture") == "amd64"),
            manifest_list[0],
        )
        child_digest = preferred.get("digest")
    else:
        child_digest = manifest.get("config", {}).get("digest")
    if not child_digest:
        raise RuntimeError("Manifest Digest konnte nicht bestimmt werden.")
    repo_ref = _image_repo_from_ref(image_ref)
    child_ref = f"{repo_ref}@{child_digest}"
    code_child, child_out, child_err = await _run_command_capture(["docker", "manifest", "inspect", child_ref])
    if code_child != 0:
        message = child_err.strip() or child_out.strip() or f"docker manifest inspect {child_ref} fehlgeschlagen"
        raise RuntimeError(message)
    try:
        child_manifest = json.loads(child_out)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Child Manifest konnte nicht geparsed werden") from exc
    config_digest = child_manifest.get("config", {}).get("digest") or child_digest
    labels: dict[str, str] | None = None
    created: str | None = None
    try:
        code_cfg, cfg_out, cfg_err = await _run_command_capture(
            ["docker", "buildx", "imagetools", "inspect", child_ref, "--format", "{{json .Manifest.Config}}"]
        )
    except FileNotFoundError:  # pragma: no cover - system dependency
        code_cfg = -1
        cfg_out = ""
    if code_cfg == 0 and cfg_out.strip():
        try:
            config_blob = json.loads(cfg_out)
        except json.JSONDecodeError:
            config_blob = None
        else:
            labels = config_blob.get("Labels")
            created = config_blob.get("Created") or config_blob.get("created")
    if not labels:
        labels = {}
    version = (labels.get("org.opencontainers.image.version") or "").strip()
    if not version:
        version = _extract_manifest_version(child_manifest) or _extract_manifest_version(manifest)
    return {
        "tag_name": version,
        "name": f"Version {version}",
        "published_at": created,
        "html_url": None,
        "image": image_ref,
        "digest": config_digest,
        "source": "docker-cli",
    }


async def _fetch_latest_release() -> dict[str, Any] | None:
    image_ref = _resolve_update_image()
    registry_error: str | None = None
    try:
        return await _fetch_release_via_registry_http(image_ref)
    except Exception as exc:  # noqa: BLE001
        registry_error = _short_error_message(str(exc), 200)
        logger.debug("Registry-API fallback auf Docker CLI für %s (%s)", image_ref, registry_error)
    try:
        return await _fetch_release_via_docker_cli(image_ref)
    except Exception as exc:  # noqa: BLE001
        if registry_error:
            combined = f"{_short_error_message(str(exc), 200)} | Registry-API: {registry_error}"
            raise RuntimeError(combined) from exc
        raise


async def _ensure_latest_release(force: bool = False) -> dict[str, Any] | None:
    await ensure_update_state()
    state = await get_update_state_snapshot()
    latest_release = state.get("latest_release") if isinstance(state.get("latest_release"), dict) else None
    last_check = _parse_iso8601(state.get("last_check"))
    if latest_release and last_check and not force:
        age = datetime.now(timezone.utc) - last_check
        if age.total_seconds() < UPDATE_CACHE_SECONDS:
            return latest_release
    try:
        release = await _fetch_latest_release()
    except Exception as exc:  # noqa: BLE001
        await merge_update_state_async(
            last_check=_now_utc_iso(),
            last_check_error=_short_error_message(str(exc), 140),
        )
        return latest_release
    await merge_update_state_async(
        latest_release=release,
        last_check=_now_utc_iso(),
        last_check_error=None,
    )
    return release


def _detect_env_file_name() -> str | None:
    for candidate in (".env",):
        candidate_path = WORKSPACE_PATH / candidate
        if candidate_path.exists():
            return candidate
    return None


async def _read_update_agent_status(timeout: float = 3.0) -> dict[str, Any] | None:
    return await ops_runtime.read_update_agent_status(
        update_agent_request_fn=_update_agent_request,
        short_error_message=_short_error_message,
        timeout=timeout,
    )


async def _reconcile_stale_update_state() -> dict[str, Any]:
    return await ops_runtime.reconcile_stale_update_state(
        ensure_update_state_fn=ensure_update_state,
        get_update_state_snapshot_fn=get_update_state_snapshot,
        read_update_agent_status_fn=_read_update_agent_status,
        merge_update_state_async_fn=merge_update_state_async,
    )


async def _build_update_status(force: bool = False) -> dict[str, Any]:
    status = await ops_runtime.build_update_status(
        reconcile_stale_update_state_fn=_reconcile_stale_update_state,
        read_update_agent_status_fn=_read_update_agent_status,
        ensure_latest_release_fn=_ensure_latest_release,
        collect_workspace_info_fn=_collect_workspace_info,
        read_local_ui_image_details_fn=_read_local_ui_image_details,
        load_version_defaults_fn=_load_version_defaults,
        detect_prerequisites_fn=_detect_prerequisites,
        determine_repo_slug_fn=_determine_repo_slug,
        detect_env_file_name_fn=_detect_env_file_name,
        sanitize_hour_fn=_sanitize_hour,
        parse_iso8601_fn=_parse_iso8601,
        format_local_timestamp_fn=_format_local_timestamp,
        calculate_next_auto_run_fn=_calculate_next_auto_run,
        default_update_image=DEFAULT_UPDATE_IMAGE,
        force=force,
    )
    should_clear_stale_error = (
        force
        and not status.get("update_available")
        and not status.get("last_check_error")
        and status.get("status") != "running"
        and not status.get("current_action")
        and bool(status.get("last_error"))
        and not (status.get("update_agent") or {}).get("available") is False
    )
    if should_clear_stale_error:
        await merge_update_state_async(
            status="idle",
            current_action=None,
            last_error=None,
            operation_id=None,
        )
        status["status"] = "idle"
        status["current_action"] = None
        status["last_error"] = None
        status["operation_id"] = None
    status["recovery"] = _classify_recovery_issue(
        status.get("last_error"),
        operation_id=status.get("operation_id"),
        update_agent=status.get("update_agent"),
    )
    return status


def _os_cmd_to_str(cmd: list[str]) -> str:
    return " ".join(cmd)


async def _run_os_command_logged(label: str, cmd: list[str], *, timeout: float | None = None) -> str:
    await merge_os_update_state(log_append=[f"{label}: {_os_cmd_to_str(cmd)}"])
    code, stdout, stderr = await _run_command_capture(cmd, timeout=timeout)
    output = stdout.strip()
    error_output = stderr.strip()
    if output:
        await merge_os_update_state(log_append=output.splitlines())
    if code != 0:
        message = error_output or output or f"Exit {code}"
        raise RuntimeError(f"{label} fehlgeschlagen: {message}")
    if error_output:
        await merge_os_update_state(log_append=error_output.splitlines())
    return output


async def _refresh_os_updates_state(*, force: bool = False) -> dict:
    async with _os_update_refresh_lock:
        state = await get_os_update_state()
        if state.get("check_in_progress"):
            return state
        if not force and not _should_refresh_os_updates(state):
            return state
        await merge_os_update_state(
            check_in_progress=True,
            current_action="Paketquellen prüfen",
            last_check_error=None,
        )
    start_ts = _now_utc_iso()
    try:
        _ensure_ubuntu_host()
        await _run_os_command_logged(
            "apt-get update",
            ["bash", "-lc", "DEBIAN_FRONTEND=noninteractive apt-get update -qq"],
            timeout=600,
        )
        list_output = await _run_os_command_logged(
            "apt list --upgradable",
            ["bash", "-lc", "set -o pipefail; apt list --upgradable 2>/dev/null || true"],
            timeout=180,
        )
        packages = _parse_upgradable_list(list_output)
        packages.sort(key=lambda item: (not item.get("security"), item.get("name", "")))
        total = len(packages)
        security_total = sum(1 for pkg in packages if pkg.get("security"))
        limited = packages[:OS_UPDATE_PACKAGE_LIMIT]
        await merge_os_update_state(
            check_in_progress=False,
            current_action=None,
            last_check=start_ts,
            last_check_error=None,
            packages=limited,
            packages_total=total,
            pending_count=total,
            security_count=security_total,
        )
    except Exception as exc:  # noqa: BLE001
        message = _short_error_message(str(exc), 240)
        await merge_os_update_state(
            check_in_progress=False,
            current_action=None,
            last_check=start_ts,
            last_check_error=message,
        )
        return await get_os_update_state()
    return await get_os_update_state()


async def _run_os_updates_job() -> None:
    await merge_os_update_state(
        update_in_progress=True,
        last_update_error=None,
        current_action="Systemupdate gestartet",
        log_reset=True,
    )
    start_ts = _now_utc_iso()
    try:
        _ensure_ubuntu_host()
        await _run_os_command_logged(
            "apt-get update",
            ["bash", "-lc", "DEBIAN_FRONTEND=noninteractive apt-get update -qq"],
            timeout=600,
        )
        await _run_os_command_logged(
            "apt-get upgrade -y",
            ["bash", "-lc", "DEBIAN_FRONTEND=noninteractive apt-get -y upgrade"],
            timeout=1800,
        )
        await _run_os_command_logged(
            "apt-get autoremove -y",
            ["bash", "-lc", "DEBIAN_FRONTEND=noninteractive apt-get -y autoremove"],
            timeout=900,
        )
        await merge_os_update_state(
            update_in_progress=False,
            current_action=None,
            last_update=start_ts,
            last_update_error=None,
        )
        await _refresh_os_updates_state(force=True)
    except Exception as exc:  # noqa: BLE001
        message = _short_error_message(str(exc), 240)
        await merge_os_update_state(
            update_in_progress=False,
            current_action=None,
            last_update_error=message,
        )


def _os_update_task_done(task: asyncio.Task) -> None:
    global _os_update_task
    _os_update_task = None
    try:
        task.result()
    except Exception as exc:  # noqa: BLE001
        logger.exception("Systemupdate Hintergrundtask fehlgeschlagen: %s", exc)


def _update_agent_sync_task_done(task: asyncio.Task) -> None:
    global _update_agent_sync_task
    _update_agent_sync_task = None
    try:
        task.result()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Automatischer Update-Agent Sync fehlgeschlagen: %s", exc)


async def _start_update_runner(
    target_ref: str | None,
    repo_slug: str | None,
    env_file: str | None,
    operation_id: str | None,
) -> str:
    return await ops_runtime.start_update_runner(
        target_ref,
        repo_slug,
        env_file,
        operation_id=operation_id,
        project_name=PROJECT_NAME,
        update_agent_request_fn=_update_agent_request,
    )


async def _launch_update_job(
    *,
    target_ref: str | None,
    initiated_by: str,
    force_release_refresh: bool,
    reset_log: bool,
) -> dict[str, Any]:
    return await ops_runtime.launch_update_job(
        target_ref=target_ref,
        initiated_by=initiated_by,
        force_release_refresh=force_release_refresh,
        reset_log=reset_log,
        update_job_lock=_update_job_lock,
        get_update_state_snapshot_fn=get_update_state_snapshot,
        collect_workspace_info_fn=_collect_workspace_info,
        detect_prerequisites_fn=_detect_prerequisites,
        ensure_latest_release_fn=_ensure_latest_release,
        determine_repo_slug_fn=_determine_repo_slug,
        detect_env_file_name_fn=_detect_env_file_name,
        now_utc_iso_fn=_now_utc_iso,
        make_operation_id_fn=make_operation_id,
        format_operation_message_fn=format_operation_message,
        append_update_log_fn=append_update_log,
        merge_update_state_async_fn=merge_update_state_async,
        start_update_runner_fn=_start_update_runner,
        short_error_message=_short_error_message,
    )


async def _auto_update_worker() -> None:
    await ops_runtime.auto_update_worker(
        get_update_state_snapshot_fn=get_update_state_snapshot,
        merge_update_state_async_fn=merge_update_state_async,
        sanitize_hour_fn=_sanitize_hour,
        parse_iso8601_fn=_parse_iso8601,
        to_local_fn=_to_local,
        now_local_fn=_now_local,
        now_utc_iso_fn=_now_utc_iso,
        launch_update_job_fn=_launch_update_job,
        job_age_seconds_fn=_job_age_seconds,
        logger=logger,
        auto_update_check_seconds=AUTO_UPDATE_CHECK_SECONDS,
        auto_update_stale_seconds=AUTO_UPDATE_STALE_SECONDS,
        auto_update_force_release=AUTO_UPDATE_FORCE_RELEASE,
    )


TRANSIENT_HTTP_CODES = {500, 502, 503, 504}
TRANSIENT_ERROR_MARKERS = (
    "communications link failure",
    "connection refused",
    "connect timed out",
    "connection timed out",
    "could not connect",
    "no route to host",
    "unknown host",
    "temporarily unavailable",
    "service unavailable",
    "timeout after",
)


def _is_transient_status(status_code: int, message: str | None) -> bool:
    return ops_runtime.is_transient_status(
        status_code,
        message,
        transient_http_codes=TRANSIENT_HTTP_CODES,
        transient_error_markers=TRANSIENT_ERROR_MARKERS,
    )


def _is_transient_request_error(exc: httpx.RequestError) -> bool:
    return ops_runtime.is_transient_request_error(
        exc,
        transient_error_markers=TRANSIENT_ERROR_MARKERS,
    )


async def _schedule_retry(err_msg: str, *, operation_id: str | None = None) -> None:
    await ops_runtime.schedule_retry(
        err_msg,
        operation_id=operation_id,
        format_apply_error_fn=_format_apply_error,
        format_operation_message_fn=format_operation_message,
        merge_apply_state_fn=merge_apply_state,
        next_retry_iso_fn=_next_retry_iso,
        now_utc_iso_fn=_now_utc_iso,
        logger=logger,
    )


async def _mark_apply_success(*, operation_id: str | None = None, trigger: str | None = None) -> None:
    await ops_runtime.mark_apply_success(
        operation_id=operation_id,
        trigger=trigger,
        merge_apply_state_fn=merge_apply_state,
        now_utc_iso_fn=_now_utc_iso,
        format_operation_message_fn=format_operation_message,
        logger=logger,
    )


async def _connector_retry_worker() -> None:
    await ops_runtime.connector_retry_worker(
        apply_retry_seconds=APPLY_RETRY_SECONDS,
        get_apply_state_fn=get_apply_state,
        apply_connector_config_fn=apply_connector_config,
        merge_apply_state_fn=merge_apply_state,
        format_apply_error_fn=_format_apply_error,
        now_utc_iso_fn=_now_utc_iso,
        next_retry_iso_fn=_next_retry_iso,
        make_operation_id_fn=make_operation_id,
        format_operation_message_fn=format_operation_message,
        logger=logger,
    )

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")

# --------- Middleware: nur Vereinsnetz zulassen ----------
@app.middleware("http")
async def ip_allowlist(request: Request, call_next):
    client_ip = getattr(request.client, "host", None) or ""
    try:
        ip_obj = ipaddress.ip_address(client_ip)
    except ValueError:
        logger.warning("Verwerfe Anfrage mit ungültiger Client-IP: %s", client_ip)
        return JSONResponse({"detail": "Forbidden (invalid client IP)"}, status_code=403)
    allowed = ip_obj.is_loopback or any(ip_obj in network for network in TRUSTED_NETWORKS)
    if not allowed:
        return JSONResponse({"detail": "Forbidden (CIDR)"}, status_code=403)
    return await call_next(request)

# --------- DB Helpers ----------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY CHECK (id=1),
        db_host TEXT, db_port INTEGER, db_user TEXT,
        source_db_host TEXT, source_db_port INTEGER,
        source_db_repl_user TEXT, source_db_gtid_mode INTEGER,
        source_db_log_file TEXT, source_db_log_pos INTEGER,
        source_db_connect_retry INTEGER,
        confluent_bootstrap TEXT, confluent_sasl_username TEXT,
        topic_prefix TEXT, server_id INTEGER, server_name TEXT,
        offline_buffer_enabled INTEGER DEFAULT 0,
        license_tier TEXT,
        retention_days INTEGER,
        license_key TEXT,
        license_status TEXT,
        license_valid_until TEXT,
        license_last_checked TEXT,
        license_customer_email TEXT,
        license_activation_id TEXT,
        license_activated_at TEXT,
        backup_pg_host TEXT,
        backup_pg_port INTEGER,
        backup_pg_db TEXT,
        backup_pg_user TEXT
    )""")
    cur = conn.execute("SELECT COUNT(*) FROM settings")
    if cur.fetchone()[0] == 0:
        conn.execute(
            """
            INSERT INTO settings(
                id, db_host, db_port, db_user,
                source_db_host, source_db_port, source_db_repl_user,
                source_db_gtid_mode, source_db_log_file, source_db_log_pos, source_db_connect_retry,
                confluent_bootstrap, confluent_sasl_username,
                topic_prefix, server_id, server_name,
                offline_buffer_enabled, license_tier, retention_days,
                license_key, license_status, license_valid_until,
                license_last_checked, license_customer_email,
                license_activation_id, license_activated_at,
                backup_pg_host, backup_pg_port, backup_pg_db, backup_pg_user
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                DEFAULT_MIRROR_DB_HOST,
                DEFAULT_MIRROR_DB_PORT,
                DEFAULT_MIRROR_DB_USER,
                DEFAULT_SOURCE_DB_HOST,
                DEFAULT_SOURCE_DB_PORT,
                DEFAULT_SOURCE_DB_REPL_USER,
                1 if DEFAULT_SOURCE_DB_GTID_MODE else 0,
                DEFAULT_SOURCE_DB_LOG_FILE or None,
                DEFAULT_SOURCE_DB_LOG_POS,
                DEFAULT_SOURCE_DB_CONNECT_RETRY,
                CONFLUENT_BOOTSTRAP_DEFAULT,
                "YOUR-API-KEY",
                os.getenv("TS_CONNECT_DEFAULT_TOPIC_PREFIX", "413067"),
                int(os.getenv("TS_CONNECT_DEFAULT_SERVER_ID", "413067")),
                DEFAULT_SERVER_NAME,
                1,
                DEFAULT_LICENSE_TIER,
                DEFAULT_RETENTION_DAYS,
                "",
                "unknown",
                None,
                None,
                None,
                "",
                None,
                DEFAULT_BACKUP_HOST,
                DEFAULT_BACKUP_PORT,
                DEFAULT_BACKUP_DB,
                DEFAULT_BACKUP_USER,
            ),
        )
        conn.commit()
    _ensure_settings_schema(conn)
    return conn


def _ensure_settings_schema(conn: sqlite3.Connection) -> None:
    existing = {row["name"] for row in conn.execute("PRAGMA table_info(settings)").fetchall()}

    def add_column(name: str, ddl: str) -> None:
        if name not in existing:
            conn.execute(f"ALTER TABLE settings ADD COLUMN {ddl}")
            existing.add(name)

    add_column("offline_buffer_enabled", "offline_buffer_enabled INTEGER DEFAULT 0")
    add_column("license_tier", "license_tier TEXT")
    add_column("retention_days", "retention_days INTEGER")
    add_column("license_key", "license_key TEXT")
    add_column("license_status", "license_status TEXT")
    add_column("license_valid_until", "license_valid_until TEXT")
    add_column("license_last_checked", "license_last_checked TEXT")
    add_column("license_customer_email", "license_customer_email TEXT")
    add_column("license_activation_id", "license_activation_id TEXT")
    add_column("license_activated_at", "license_activated_at TEXT")
    add_column("backup_pg_host", "backup_pg_host TEXT")
    add_column("backup_pg_port", "backup_pg_port INTEGER")
    add_column("backup_pg_db", "backup_pg_db TEXT")
    add_column("backup_pg_user", "backup_pg_user TEXT")
    add_column("source_db_host", "source_db_host TEXT")
    add_column("source_db_port", "source_db_port INTEGER")
    add_column("source_db_repl_user", "source_db_repl_user TEXT")
    add_column("source_db_gtid_mode", "source_db_gtid_mode INTEGER DEFAULT 1")
    add_column("source_db_log_file", "source_db_log_file TEXT")
    add_column("source_db_log_pos", "source_db_log_pos INTEGER")
    add_column("source_db_connect_retry", "source_db_connect_retry INTEGER DEFAULT 10")
    add_column("shooter_count_cached", "shooter_count_cached INTEGER")
    add_column("shooter_count_checked_at", "shooter_count_checked_at TEXT")
    conn.commit()

    cur = conn.execute("SELECT COUNT(*) as cnt FROM settings")
    if cur.fetchone()["cnt"] == 0:
        return
    row = conn.execute("SELECT license_tier, retention_days FROM settings WHERE id=1").fetchone()
    license_value = normalize_license_tier(row["license_tier"] if row else None)
    retention_value = row["retention_days"] if row and row["retention_days"] else LICENSE_RETENTION_DAYS[license_value]
    conn.execute(
        """
        UPDATE settings
        SET
            offline_buffer_enabled = 1,
            license_tier = ?,
            retention_days = ?,
            license_status = COALESCE(NULLIF(TRIM(license_status), ''), 'unknown'),
            license_activation_id = COALESCE(license_activation_id, ''),
            backup_pg_host = COALESCE(NULLIF(TRIM(backup_pg_host), ''), ?),
            backup_pg_port = COALESCE(backup_pg_port, ?),
            backup_pg_db = COALESCE(NULLIF(TRIM(backup_pg_db), ''), ?),
            backup_pg_user = COALESCE(NULLIF(TRIM(backup_pg_user), ''), ?),
            source_db_port = COALESCE(source_db_port, ?),
            source_db_gtid_mode = COALESCE(source_db_gtid_mode, ?),
            source_db_connect_retry = COALESCE(source_db_connect_retry, ?)
        WHERE id=1
        """,
        (
            license_value,
            retention_value,
            DEFAULT_BACKUP_HOST,
            DEFAULT_BACKUP_PORT,
            DEFAULT_BACKUP_DB,
            DEFAULT_BACKUP_USER,
            DEFAULT_SOURCE_DB_PORT,
            1 if DEFAULT_SOURCE_DB_GTID_MODE else 0,
            DEFAULT_SOURCE_DB_CONNECT_RETRY,
        ),
    )
    conn.commit()


def ensure_backup_schema(*, host: str, port: int, database: str, user: str, password: str) -> None:
    try:
        with psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=5,
        ) as connection:
            connection.autocommit = True
            with connection.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS buffer_events (
                        id BIGSERIAL PRIMARY KEY,
                        topic TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_buffer_events_created_at ON buffer_events (created_at)
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_buffer_events_topic_created_at ON buffer_events (topic, created_at)
                    """
                )
    except psycopg2.Error as exc:  # noqa: BLE001
        raise RuntimeError(f"Backup-Datenbank nicht erreichbar: {exc}") from exc


def prune_backup_data(*, days: int, host: str, port: int, database: str, user: str, password: str) -> None:
    if days <= 0:
        return
    interval_literal = f"{int(days)} days"
    try:
        with psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=5,
        ) as connection:
            connection.autocommit = True
            with connection.cursor() as cur:
                cur.execute(
                    "DELETE FROM buffer_events WHERE created_at < NOW() - INTERVAL %s",
                    (interval_literal,),
                )
    except psycopg2.Error as exc:  # noqa: BLE001
        raise RuntimeError(f"Bereinigung der Backup-Datenbank fehlgeschlagen: {exc}") from exc


def _escape_jaas(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _probe_backup_connection(*, host: str, port: int, database: str, user: str, password: str, timeout: int = 3) -> None:
    with psycopg2.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        connect_timeout=timeout,
    ) as connection:
        with connection.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()


def _build_source_replication_payload(settings: dict, secrets: dict[str, str]) -> dict[str, Any] | None:
    host = str(settings.get("source_db_host") or "").strip()
    if not host:
        return None
    user = str(settings.get("source_db_repl_user") or "").strip()
    password = str(secrets.get(SOURCE_DB_REPL_PASSWORD_KEY) or "").strip()
    if not user or not password:
        raise ValueError("Replikations-Benutzer oder Passwort fehlt.")
    port = int(settings.get("source_db_port") or DEFAULT_SOURCE_DB_PORT)
    connect_retry = int(settings.get("source_db_connect_retry") or DEFAULT_SOURCE_DB_CONNECT_RETRY)
    gtid_mode = bool(settings.get("source_db_gtid_mode"))
    log_file = str(settings.get("source_db_log_file") or "").strip()
    log_pos = settings.get("source_db_log_pos")
    if not gtid_mode:
        if not log_file or log_pos is None:
            raise ValueError("Für non-GTID müssen Binlog-Datei und Position gesetzt sein.")
    payload: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "gtid_mode": gtid_mode,
        "connect_retry": max(1, connect_retry),
    }
    if not gtid_mode:
        payload["log_file"] = log_file
        payload["log_pos"] = int(log_pos)
    return payload


async def apply_source_replication_config(settings: dict, secrets: dict[str, str]) -> dict[str, Any]:
    return await ops_runtime.apply_source_replication_config(
        settings,
        secrets,
        build_source_replication_payload_fn=_build_source_replication_payload,
        update_agent_request_fn=_update_agent_request,
    )


async def source_replication_status_snapshot() -> dict[str, Any]:
    return await ops_runtime.source_replication_status_snapshot(
        update_agent_request_fn=_update_agent_request,
    )


def _write_mirror_maker_config(settings: dict, secrets: dict) -> None:
    ops_runtime.write_mirror_maker_config(
        settings,
        secrets,
        confluent_bootstrap_default=CONFLUENT_BOOTSTRAP_DEFAULT,
        stream_target_prefix=STREAMS_TARGET_PREFIX,
        mm2_internal_replication_factor=MM2_INTERNAL_REPLICATION_FACTOR,
        mm2_offset_storage_partitions=MM2_OFFSET_STORAGE_PARTITIONS,
        mm2_status_storage_partitions=MM2_STATUS_STORAGE_PARTITIONS,
        mm2_state_topic_prefix=MM2_STATE_TOPIC_PREFIX,
        config_path=MM2_CONFIG_PATH,
        atomic_write_text_fn=_atomic_write_text,
        secret_file_mode=stat.S_IRUSR | stat.S_IWUSR,
        secrets_file_uid=SECRETS_FILE_UID,
        secrets_file_gid=SECRETS_FILE_GID,
        escape_jaas_fn=_escape_jaas,
    )


def _build_backup_sink_config(settings: dict, secrets: dict) -> dict:
    return ops_runtime.build_backup_sink_config(
        settings,
        secrets,
        backup_connector_name=BACKUP_CONNECTOR_NAME,
    )


async def restart_mirror_maker() -> None:
    await ops_runtime.restart_mirror_maker(
        restart_container_fn=_restart_container,
        logger=logger,
    )


async def update_remote_replication_state(active: bool) -> None:
    await ops_runtime.update_remote_replication_state(
        active,
        container_status_fn=_container_status,
        restart_mirror_maker_fn=restart_mirror_maker,
        stop_container_fn=_stop_container,
        logger=logger,
    )


def fetch_settings() -> dict:
    conn = get_db()
    cur = conn.execute(
        """
        SELECT db_host, db_port, db_user,
               source_db_host, source_db_port, source_db_repl_user,
               source_db_gtid_mode, source_db_log_file, source_db_log_pos, source_db_connect_retry,
               confluent_bootstrap, confluent_sasl_username,
               topic_prefix, server_id, server_name,
               offline_buffer_enabled, license_tier, retention_days,
               license_key, license_status, license_valid_until, license_last_checked, license_customer_email,
               license_activation_id, license_activated_at,
               backup_pg_host, backup_pg_port, backup_pg_db, backup_pg_user,
               shooter_count_cached, shooter_count_checked_at
        FROM settings WHERE id=1
        """
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        raise RuntimeError("settings row missing")
    db_host = (os.getenv("TS_CONNECT_DEFAULT_DB_HOST") or "").strip() or (row["db_host"] or DEFAULT_MIRROR_DB_HOST)
    env_db_port = _env_int("TS_CONNECT_DEFAULT_DB_PORT", None)
    if env_db_port is not None and env_db_port > 0:
        db_port = env_db_port
    else:
        try:
            db_port = int(row["db_port"] or DEFAULT_MIRROR_DB_PORT)
        except (TypeError, ValueError):
            db_port = DEFAULT_MIRROR_DB_PORT
    db_user = (os.getenv("TS_CONNECT_DEFAULT_DB_USER") or "").strip() or (row["db_user"] or DEFAULT_MIRROR_DB_USER)
    license_tier = normalize_license_tier(row["license_tier"])
    retention_days = LICENSE_RETENTION_DAYS.get(license_tier, LICENSE_RETENTION_DAYS[DEFAULT_LICENSE_TIER])
    return {
        "db_host": db_host,
        "db_port": db_port,
        "db_user": db_user,
        "source_db_host": row["source_db_host"] or "",
        "source_db_port": row["source_db_port"] or DEFAULT_SOURCE_DB_PORT,
        "source_db_repl_user": row["source_db_repl_user"] or "",
        "source_db_gtid_mode": bool(row["source_db_gtid_mode"]) if row["source_db_gtid_mode"] is not None else DEFAULT_SOURCE_DB_GTID_MODE,
        "source_db_log_file": row["source_db_log_file"] or "",
        "source_db_log_pos": row["source_db_log_pos"],
        "source_db_connect_retry": row["source_db_connect_retry"] or DEFAULT_SOURCE_DB_CONNECT_RETRY,
        "confluent_bootstrap": row["confluent_bootstrap"],
        "confluent_sasl_username": row["confluent_sasl_username"],
        "topic_prefix": row["topic_prefix"],
        "server_id": row["server_id"],
        "server_name": row["server_name"],
        "offline_buffer_enabled": True,
        "license_tier": license_tier,
        "retention_days": retention_days,
        "license_key": row["license_key"] or "",
        "license_status": row["license_status"] or "unknown",
        "license_valid_until": row["license_valid_until"],
        "license_last_checked": row["license_last_checked"],
        "license_customer_email": row["license_customer_email"],
        "license_activation_id": row["license_activation_id"] or "",
        "license_activated_at": row["license_activated_at"],
        "backup_pg_host": row["backup_pg_host"] or DEFAULT_BACKUP_HOST,
        "backup_pg_port": row["backup_pg_port"] or DEFAULT_BACKUP_PORT,
        "backup_pg_db": row["backup_pg_db"] or DEFAULT_BACKUP_DB,
        "backup_pg_user": row["backup_pg_user"] or DEFAULT_BACKUP_USER,
        "shooter_count_cached": row["shooter_count_cached"],
        "shooter_count_checked_at": row["shooter_count_checked_at"],
    }


def _collect_shooter_stats_sync(settings: dict, secrets: dict) -> dict[str, Any]:
    verein_id = str(settings.get("topic_prefix") or settings.get("server_id") or "").strip()
    cached_count = settings.get("shooter_count_cached")
    cached_checked = settings.get("shooter_count_checked_at")
    result: dict[str, Any] = {
        "verein_id": verein_id,
        "count": cached_count,
        "ok": False,
        "error": None,
        "checked_at": cached_checked,
        "source": "cache" if cached_count is not None else None,
    }
    if not verein_id:
        result["error"] = "VereinsID nicht gesetzt."
        return result
    password = secrets.get("backup_pg_password")
    if not password:
        result["error"] = "Offline-Puffer Passwort fehlt."
        return result
    host = settings.get("backup_pg_host") or DEFAULT_BACKUP_HOST
    port = int(settings.get("backup_pg_port") or DEFAULT_BACKUP_PORT)
    database = settings.get("backup_pg_db") or DEFAULT_BACKUP_DB
    user = settings.get("backup_pg_user") or DEFAULT_BACKUP_USER

    def _query() -> int:
        with psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            connect_timeout=5,
        ) as connection:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(
                        DISTINCT COALESCE(
                            NULLIF(TRIM(payload -> 'after' ->> 'SportpassID'), ''),
                            CONCAT('ID:', payload -> 'after' ->> 'SchuetzeID')
                        )
                    )
                    FROM buffer_events
                    WHERE topic = 'ts.raw.schuetze'
                      AND payload ? 'after'
                      AND COALESCE(payload -> 'after' ->> 'VereinsID', '') = %s
                    """,
                    (verein_id,),
                )
                row = cur.fetchone()
        value = row[0] if row else 0
        return int(value or 0)

    try:
        count = _query()
    except Exception as exc:  # noqa: BLE001
        err = f"Offline-Puffer nicht erreichbar: {_short_error_message(str(exc), 160)}"
        result["error"] = err
        if cached_count is not None:
            result["ok"] = True
            result["source"] = "cache"
        return result

    checked_at = _now_utc_iso()
    result.update(
        {
            "count": count,
            "ok": True,
            "error": None,
            "checked_at": checked_at,
            "source": "buffer",
        }
    )
    try:
        conn = get_db()
        conn.execute(
            "UPDATE settings SET shooter_count_cached=?, shooter_count_checked_at=? WHERE id=1",
            (count, checked_at),
        )
        conn.commit()
        conn.close()
    except Exception:  # noqa: BLE001
        pass
    return result


def write_secrets_file(values: dict[str, str]) -> None:
    secrets_dir = SECRETS_PATH.parent
    tmp_path = _tmp_path_for(SECRETS_PATH)
    if not values:
        if SECRETS_PATH.exists():
            SECRETS_PATH.unlink()
            _fsync_directory(secrets_dir)
        if tmp_path.exists():
            tmp_path.unlink()
        return
    lines = [f"{key}={value}" for key, value in sorted(values.items()) if value is not None]
    payload = "\n".join(lines) + "\n"

    _atomic_write_text(
        SECRETS_PATH,
        payload,
        mode=PRIVATE_SECRET_FILE_MODE,
        uid=SECRETS_FILE_UID,
        gid=SECRETS_FILE_GID,
    )


def read_secrets_file() -> dict:
    if not SECRETS_PATH.exists():
        return {}
    _ensure_private_file_permissions(SECRETS_PATH)
    data: dict[str, str] = {}
    for line in SECRETS_PATH.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def _resolve_mirror_db_password(secrets_data: dict[str, str] | None = None) -> str:
    env_password = (os.getenv("TS_CONNECT_MIRROR_DB_PASSWORD") or os.getenv("MARIADB_PASSWORD") or "").strip()
    if env_password:
        return env_password
    if not secrets_data:
        return ""
    return str(secrets_data.get("db_password") or "").strip()


def _ensure_mirror_db_secret(secrets_data: dict[str, str]) -> tuple[str, dict[str, str]]:
    resolved_password = _resolve_mirror_db_password(secrets_data)
    if not resolved_password:
        return "", secrets_data
    if secrets_data.get("db_password") == resolved_password:
        return resolved_password, secrets_data
    updated = dict(secrets_data)
    updated["db_password"] = resolved_password
    write_secrets_file(updated)
    return resolved_password, updated


def store_license_key(value: str) -> None:
    value = (value or "").strip()
    if value:
        _atomic_write_text(
            LICENSE_KEY_FILE,
            value + "\n",
            mode=stat.S_IRUSR | stat.S_IWUSR,
        )
        return
    tmp_path = _tmp_path_for(LICENSE_KEY_FILE)
    if LICENSE_KEY_FILE.exists():
        LICENSE_KEY_FILE.unlink()
        _fsync_directory(LICENSE_KEY_FILE.parent)
    if tmp_path.exists():
        tmp_path.unlink()


def sync_license_key_file(settings: dict | None = None) -> bool:
    """Ensure license.key reflects the currently stored license value.

    Returns True when the file was changed (written or removed).
    """
    if settings is None:
        settings = fetch_settings()
    desired = (settings.get("license_key") or "").strip()
    existing = ""
    if LICENSE_KEY_FILE.exists():
        try:
            existing = LICENSE_KEY_FILE.read_text(encoding="utf-8").strip()
        except OSError:
            existing = ""
    if desired:
        if existing != desired:
            store_license_key(desired)
            return True
        return False
    if LICENSE_KEY_FILE.exists():
        store_license_key("")
        return True
    return False


def _license_meta_path() -> Path:
    return DATA_DIR / "license_meta.json"


def read_license_meta() -> dict[str, Any]:
    path = _license_meta_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_license_meta(data: dict[str, Any]) -> None:
    path = _license_meta_path()
    if not data:
        if path.exists():
            path.unlink()
            _fsync_directory(path.parent)
        tmp_path = _tmp_path_for(path)
        if tmp_path.exists():
            tmp_path.unlink()
        return
    _atomic_write_text(
        path,
        json.dumps(data, ensure_ascii=False, indent=2) + "\n",
        mode=stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH,
    )


def _set_license_form_key(request: Request, value: str | None) -> None:
    key = (value or "").strip()
    if key:
        request.session[LICENSE_FORM_KEY_SESSION] = key
    else:
        request.session.pop(LICENSE_FORM_KEY_SESSION, None)


def _get_license_form_key(request: Request) -> str:
    return str(request.session.get(LICENSE_FORM_KEY_SESSION) or "").strip()


def _resolve_license_form_state(stored_key: str, draft_key: str) -> tuple[str, str, bool]:
    stored = (stored_key or "").strip()
    draft = (draft_key or "").strip()
    if draft and draft == stored:
        draft = ""
    return (draft or stored), draft, bool(draft and draft != stored)


def _license_is_active(settings: dict) -> bool:
    key = (settings.get("license_key") or "").strip()
    if not key:
        return False
    status = _normalize_provider_status(settings.get("license_status"))
    if status in {"revoked", "cancelled", "disabled", "invalid"}:
        return False
    activation_id = (settings.get("license_activation_id") or "").strip()
    if not activation_id:
        return False
    expires_iso = settings.get("license_valid_until")
    expires_dt = _parse_iso8601(expires_iso)
    if expires_dt and expires_dt < datetime.now(timezone.utc):
        return False
    return True


def _license_validation_state(validation: dict[str, Any]) -> tuple[str, int, str, str | None, str]:
    entitled = _keygen_license_entitled(validation)
    plan = normalize_license_tier(validation.get("plan") if entitled else DEFAULT_LICENSE_TIER)
    retention_days = LICENSE_RETENTION_DAYS.get(plan, DEFAULT_RETENTION_DAYS)
    status = validation.get("status") or ("valid" if validation.get("valid") else "unbekannt")
    expires_at_norm = _normalize_iso8601(validation.get("expires_at") or validation.get("raw_expires_at"))
    last_checked = _now_utc_iso()
    return plan, retention_days, status, expires_at_norm, last_checked


def _rotate_backup_password(*, settings: dict, current_password: str, new_password: str) -> None:
    try:
        with psycopg2.connect(
            host=settings["backup_pg_host"],
            port=settings["backup_pg_port"],
            dbname=settings["backup_pg_db"],
            user=settings["backup_pg_user"],
            password=current_password,
            connect_timeout=5,
        ) as connection:
            connection.autocommit = True
            with connection.cursor() as cur:
                cur.execute(
                    sql.SQL("ALTER ROLE {} WITH PASSWORD %s").format(
                        sql.Identifier(settings["backup_pg_user"])
                    ),
                    (new_password,),
                )
    except psycopg2.Error as exc:  # noqa: BLE001
        raise RuntimeError(f"Backup-Passwort konnte nicht gesetzt werden: {exc}") from exc


def _ensure_backup_password(settings: dict, secrets_data: dict) -> tuple[str, dict]:
    password = (secrets_data.get("backup_pg_password") or "").strip()
    if password:
        if password == "targetshot":
            logger.warning(
                "Unsicheres Backup-Passwort in secrets.properties erkannt; erneute Initialisierung wird erzwungen."
            )
        else:
            return password, secrets_data
    fallback_candidates = [
        (os.getenv("TS_CONNECT_BACKUP_PASSWORD") or "").strip(),
        (os.getenv("POSTGRES_PASSWORD") or "").strip(),
    ]
    new_password = secrets.token_urlsafe(32)
    last_error: Exception | None = None
    for candidate in fallback_candidates:
        if not candidate:
            continue
        try:
            _rotate_backup_password(settings=settings, current_password=candidate, new_password=new_password)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            continue
        secrets_data["backup_pg_password"] = new_password
        write_secrets_file(secrets_data)
        return new_password, secrets_data

    if last_error is not None:
        raise RuntimeError(
            "Backup-Passwort konnte nicht initialisiert werden. Bitte aktuelles Passwort explizit über "
            "TS_CONNECT_BACKUP_PASSWORD oder POSTGRES_PASSWORD bereitstellen."
        ) from last_error
    raise RuntimeError(
        "Backup-Passwort fehlt. Bitte aktuelles Passwort explizit über "
        "TS_CONNECT_BACKUP_PASSWORD oder POSTGRES_PASSWORD bereitstellen."
    )


async def ensure_offline_buffer_ready() -> None:
    conn = get_db()
    conn.execute("UPDATE settings SET offline_buffer_enabled=1 WHERE id=1")
    conn.commit()
    conn.close()

    try:
        await _ensure_storage_issue_free("backup_db")
    except Exception as exc:  # noqa: BLE001
        logger.error("Offline-Puffer Storage-Preflight fehlgeschlagen: %s", exc)
        return

    settings = fetch_settings()
    secrets_data = read_secrets_file()
    try:
        password, secrets_data = _ensure_backup_password(settings, secrets_data)
    except Exception as exc:  # noqa: BLE001
        logger.error("Offline-Puffer Passwort-Initialisierung fehlgeschlagen: %s", exc)
        return

    try:
        await asyncio.to_thread(
            ensure_backup_schema,
            host=settings["backup_pg_host"],
            port=settings["backup_pg_port"],
            database=settings["backup_pg_db"],
            user=settings["backup_pg_user"],
            password=password,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Offline-Puffer Schema-Initialisierung fehlgeschlagen: %s", exc)
        return

    try:
        await asyncio.to_thread(
            prune_backup_data,
            days=settings["retention_days"],
            host=settings["backup_pg_host"],
            port=settings["backup_pg_port"],
            database=settings["backup_pg_db"],
            user=settings["backup_pg_user"],
            password=password,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Offline-Puffer Aufbewahrungsbereinigung fehlgeschlagen: %s", exc)


def set_admin_password(new_password: str) -> None:
    security_bootstrap.set_admin_password(new_password)


def _read_admin_password_record() -> str:
    return security_bootstrap.read_admin_password_record()


def ensure_admin_password_file() -> None:
    security_bootstrap.ensure_admin_password_file()


def verify_admin_password(candidate: str) -> bool:
    return security_bootstrap.verify_admin_password(candidate)


@app.on_event("startup")
async def init_admin_password() -> None:
    ensure_admin_password_file()
    await _log_storage_ownership_preflight()
    await ensure_offline_buffer_ready()
    current_settings = fetch_settings()
    try:
        if sync_license_key_file(current_settings):
            logger.info("Synchronized license.key with stored license settings")
    except Exception as exc:  # noqa: BLE001
        logger.warning("License file sync failed: %s", exc)
    cloud_replication_active = _license_is_active(current_settings)
    if cloud_replication_active:
        try:
            await apply_connector_config(trigger="startup")
        except DeferredApplyError as exc:
            logger.warning("Automatic connector apply deferred on startup: %s", exc)
        except ValueError as exc:
            logger.info("Skipping automatic connector apply on startup: %s", exc)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Automatic connector apply failed on startup: %s", exc)
    await update_remote_replication_state(cloud_replication_active)
    await configure_git_safety()
    await ensure_update_state()
    state = await get_update_state_snapshot()
    updates: dict[str, Any] = {}
    if state.get("auto_update_hour") is None:
        updates["auto_update_hour"] = AUTO_UPDATE_DEFAULT_HOUR
    if updates:
        await merge_update_state_async(**updates)
    asyncio.create_task(_connector_retry_worker())
    global _auto_update_task
    if _auto_update_task is None:
        _auto_update_task = asyncio.create_task(_auto_update_worker())
    global _update_agent_sync_task
    if _update_agent_sync_task is None:
        _update_agent_sync_task = asyncio.create_task(_delayed_update_agent_sync())
        _update_agent_sync_task.add_done_callback(_update_agent_sync_task_done)
    global _mirror_backup_task
    await _refresh_mirror_backup_state()
    if MIRROR_BACKUP_ENABLED and _mirror_backup_task is None:
        _mirror_backup_task = asyncio.create_task(_mirror_backup_worker())


def _extract_error_message(resp: httpx.Response) -> str:
    fallback = f"{resp.status_code} {getattr(resp, 'reason_phrase', '')}".strip()

    def _clean(value: str) -> str:
        cleaned = _strip_html_tags(str(value))
        if not cleaned:
            return fallback
        shortened = _short_error_message(cleaned, 160)
        return shortened or fallback

    try:
        payload = resp.json()
    except Exception:  # noqa: BLE001
        return _clean(resp.text)
    if isinstance(payload, dict):
        for key in ("message", "error", "detail", "trace"):
            value = payload.get(key)
            if value:
                return _clean(str(value))
    return _clean(resp.text)


async def _connect_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    json_payload: dict | None = None,
    allow_defer: bool,
    operation_id: str | None = None,
    ok_statuses: tuple[int, ...] = (200, 201, 202, 204),
) -> httpx.Response:
    return await ops_runtime.connect_request(
        client,
        method,
        url,
        allow_defer=allow_defer,
        operation_id=operation_id,
        schedule_retry_fn=_schedule_retry,
        short_error_message=_short_error_message,
        extract_error_message_fn=_extract_error_message,
        is_transient_status_fn=_is_transient_status,
        is_transient_request_error_fn=_is_transient_request_error,
        json_payload=json_payload,
        ok_statuses=ok_statuses,
    )


async def _ensure_connector(
    client: httpx.AsyncClient,
    *,
    name: str,
    config: dict,
    allow_defer: bool,
    operation_id: str | None = None,
) -> None:
    await ops_runtime.ensure_connector(
        client,
        name=name,
        config=config,
        allow_defer=allow_defer,
        operation_id=operation_id,
        connect_base_url=CONNECT_BASE_URL,
        connect_request_fn=_connect_request,
    )


async def _delete_connector_if_exists(
    client: httpx.AsyncClient,
    *,
    name: str,
    allow_defer: bool,
    operation_id: str | None = None,
) -> None:
    await ops_runtime.delete_connector_if_exists(
        client,
        name=name,
        allow_defer=allow_defer,
        operation_id=operation_id,
        connect_base_url=CONNECT_BASE_URL,
        connect_request_fn=_connect_request,
    )


async def apply_connector_config(
    *,
    allow_defer: bool = True,
    operation_id: str | None = None,
    trigger: str = "manual",
) -> None:
    await ops_runtime.apply_connector_config(
        allow_defer=allow_defer,
        operation_id=operation_id,
        trigger=trigger,
        ensure_offline_buffer_ready_fn=ensure_offline_buffer_ready,
        fetch_settings_fn=fetch_settings,
        read_secrets_file_fn=read_secrets_file,
        ensure_mirror_db_secret_fn=_ensure_mirror_db_secret,
        build_connector_config_fn=build_connector_config,
        build_backup_sink_config_fn=_build_backup_sink_config,
        default_connector_name=DEFAULT_CONNECTOR_NAME,
        backup_connector_name=BACKUP_CONNECTOR_NAME,
        connect_base_url=CONNECT_BASE_URL,
        ensure_connector_fn=_ensure_connector,
        delete_connector_if_exists_fn=_delete_connector_if_exists,
        write_mirror_maker_config_fn=_write_mirror_maker_config,
        restart_mirror_maker_fn=restart_mirror_maker,
        mm2_config_path=MM2_CONFIG_PATH,
        mark_apply_success_fn=_mark_apply_success,
        merge_apply_state_fn=merge_apply_state,
        make_operation_id_fn=make_operation_id,
        format_operation_message_fn=format_operation_message,
        logger=logger,
    )

# --------- Views ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not request.session.get("authenticated"):
        return RedirectResponse("/login", status_code=303)
    context = build_index_context(request)
    return templates.TemplateResponse("index.html", context)

def require_admin(pw: str, *, raise_exc: bool = True) -> bool:
    return require_admin_password(verify_admin_password, pw, raise_exc=raise_exc)

def require_session(request: Request):
    require_session_auth(request)


def build_index_context(request: Request) -> dict:
    data = fetch_settings().copy()
    secrets_data = read_secrets_file()
    stored_license_key = str(data.get("license_key") or "").strip()
    draft_license_key = _get_license_form_key(request)
    input_license_key, resolved_draft_key, has_pending_license_key = _resolve_license_form_state(
        stored_license_key,
        draft_license_key,
    )
    if draft_license_key and not resolved_draft_key:
        _set_license_form_key(request, "")
    data["retention_days"] = LICENSE_RETENTION_DAYS.get(
        data["license_tier"],
        LICENSE_RETENTION_DAYS[DEFAULT_LICENSE_TIER],
    )
    connect_version, connect_release = _load_version_defaults()
    placeholders = {
        "YOUR-BOOTSTRAP:9092",
        "YOUR-BOOTSTRAP",
        "pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092",
    }
    if data["confluent_bootstrap"] in placeholders or not data["confluent_bootstrap"]:
        data["confluent_bootstrap"] = CONFLUENT_BOOTSTRAP_DEFAULT

    has_secrets = SECRETS_PATH.exists()
    source_repl_password_saved = bool(secrets_data.get(SOURCE_DB_REPL_PASSWORD_KEY))
    flash_message = request.session.pop("flash_message", None)
    error_message = request.session.pop("error_message", None)
    license_valid_iso = data.get("license_valid_until")
    license_valid_dt = _parse_iso8601(license_valid_iso)
    license_valid_display: str | None = None
    license_days_remaining: int | None = None
    if license_valid_dt:
        license_valid_display = license_valid_dt.astimezone(timezone.utc).strftime("%d.%m.%Y")
        license_days_remaining = (license_valid_dt.date() - datetime.now(timezone.utc).date()).days
    license_last_checked_iso = data.get("license_last_checked")
    license_last_checked_display: str | None = None
    license_last_checked_date_display: str | None = None
    license_last_checked_time_display: str | None = None
    if license_last_checked_iso:
        last_checked_dt = _parse_iso8601(license_last_checked_iso)
        if last_checked_dt:
            last_checked_local = _to_local(last_checked_dt)
            if last_checked_local:
                license_last_checked_display = last_checked_local.strftime("%d.%m.%Y, %H:%M Uhr")
                license_last_checked_date_display = last_checked_local.strftime("%d.%m.%Y")
                license_last_checked_time_display = last_checked_local.strftime("%H:%M Uhr")
    status_raw = (data.get("license_status") or "unknown").lower()
    status_labels = {
        "active": "Aktiv",
        "valid": "Aktiv",
        "expired": "Abgelaufen",
        "revoked": "Widerrufen",
        "cancelled": "Gekündigt",
        "disabled": "Deaktiviert",
        "inactive": "Inaktiv",
        "pending": "In Prüfung",
        "unknown": "Unbekannt",
        "invalid": "Ungültig",
        "no_machine": "Noch nicht aktiviert",
        "no_machines": "Noch nicht aktiviert",
        "fingerprint_scope_required": "Maschinenabgleich erforderlich",
        "fingerprint_scope_mismatch": "Falsche Maschine",
        "machine_scope_required": "Maschinenabgleich erforderlich",
        "machine_scope_mismatch": "Falsche Maschine",
    }
    license_status_label = status_labels.get(status_raw, (data.get("license_status") or "Unbekannt").capitalize())
    activation_id = data.get("license_activation_id") or ""
    activation_iso = data.get("license_activated_at")
    activation_dt = _parse_iso8601(activation_iso)
    activation_display: str | None = None
    if activation_dt:
        activation_display = activation_dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    license_meta = read_license_meta()
    license_info = {
        "key": input_license_key,
        "stored_key": stored_license_key,
        "draft_key": resolved_draft_key,
        "has_pending_key": has_pending_license_key,
        "status": data.get("license_status", "unknown"),
        "status_label": license_status_label,
        "plan": data.get("license_tier", DEFAULT_LICENSE_TIER),
        "plan_label": plan_display_name(data.get("license_tier")),
        "valid_until": license_valid_iso,
        "valid_until_display": license_valid_display,
        "days_remaining": license_days_remaining,
        "last_checked": license_last_checked_iso,
        "last_checked_display": license_last_checked_display,
        "last_checked_date_display": license_last_checked_date_display,
        "last_checked_time_display": license_last_checked_time_display,
        "customer_email": data.get("license_customer_email"),
        "status_raw": status_raw,
        "activation_id": activation_id,
        "activation_at": activation_iso,
        "activation_at_display": activation_display,
        "provider": LICENSE_PROVIDER,
        "provider_label": "Keygen",
        "meta": license_meta,
    }

    verein_identifier = str(data.get("topic_prefix") or data.get("server_id") or "").strip()
    data["verein_id"] = verein_identifier

    return {
        "request": request,
        "data": data,
        "has_secrets": has_secrets,
        "connect_version": connect_version,
        "connect_release": connect_release,
        "confluent_cluster_url": CONFLUENT_CLUSTER_URL,
        "confluent_cluster_id": CONFLUENT_CLUSTER_ID,
        "flash_message": flash_message,
        "error_message": error_message,
        "docs_url": DOCS_URL,
        "source_repl_password_placeholder": PASSWORD_PLACEHOLDER if source_repl_password_saved else "",
        "source_repl_password_saved": source_repl_password_saved,
        "confluent_password_placeholder": PASSWORD_PLACEHOLDER if secrets_data.get("confluent_sasl_password") else "",
        "confluent_password_saved": bool(secrets_data.get("confluent_sasl_password")),
        "default_server_name": DEFAULT_SERVER_NAME,
        "license": license_info,
        "license_activation_enabled": LICENSE_MACHINE_ACTIVATION_ENABLED,
        "host_agent_configured": bool(HOST_AGENT_URL),
    }


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if request.session.get("authenticated"):
        return RedirectResponse("/", status_code=303)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
async def login_submit(request: Request, pw: str = Form(...)):
    if not verify_admin_password(pw):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "error": "Falsches Passwort. Bitte erneut versuchen.",
            },
            status_code=401,
        )
    request.session["authenticated"] = True
    return RedirectResponse("/", status_code=303)


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=303)

# --------- Preflight Tests ----------
@app.get("/api/test/db", dependencies=[Depends(require_session)])
async def test_db(
    host: str | None = None,
    port: int | None = None,
    user: str | None = None,
    password: str | None = None,
):
    import pymysql
    settings = fetch_settings()
    secrets_data = read_secrets_file()
    resolved_password, secrets_data = _ensure_mirror_db_secret(secrets_data)
    host_value = (host or settings.get("db_host") or "").strip()
    user_value = (user or settings.get("db_user") or "").strip()
    port_value = int(port or settings.get("db_port") or DEFAULT_MIRROR_DB_PORT)
    submitted_password = (password or "").strip()
    if submitted_password == PASSWORD_PLACEHOLDER:
        submitted_password = str(secrets_data.get("db_password") or "").strip()
    password_value = submitted_password or resolved_password
    if not host_value or not user_value or port_value <= 0:
        return {"ok": False, "msg": "Mirror-MariaDB-Konfiguration unvollständig"}
    if not password_value:
        return {"ok": False, "msg": "Mirror-MariaDB-Passwort fehlt (TS_CONNECT_MIRROR_DB_PASSWORD)."}
    try:
        conn = pymysql.connect(host=host_value, port=port_value, user=user_value, password=password_value,
                               connect_timeout=3, read_timeout=3, write_timeout=3)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        return {"ok": True, "msg": "DB OK"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


@app.get("/api/test/source-db", dependencies=[Depends(require_session)])
async def test_source_db(host: str, port: int, user: str, password: str):
    import pymysql
    secrets_data = read_secrets_file()
    if password == PASSWORD_PLACEHOLDER and secrets_data.get(SOURCE_DB_REPL_PASSWORD_KEY):
        password = secrets_data[SOURCE_DB_REPL_PASSWORD_KEY]
    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            connect_timeout=3,
            read_timeout=3,
            write_timeout=3,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        return {"ok": True, "msg": "MainDB erreichbar"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "msg": str(exc)}


@app.get("/api/test/confluent", dependencies=[Depends(require_session)])
async def test_confluent(bootstrap: str):
    # reachability + TLS handshake only (Credentials prüft später der Connector)
    try:
        bootstrap_value = bootstrap.strip()
        for scheme in ("https://", "http://"):
            if bootstrap_value.startswith(scheme):
                bootstrap_value = bootstrap_value[len(scheme):]
                break
        bootstrap_value = bootstrap_value.rstrip("/")
        if ":" in bootstrap_value:
            host, port_raw = bootstrap_value.rsplit(":", 1)
            port = int(port_raw)
        else:
            host, port = bootstrap_value, 9092
        ctx = ssl.create_default_context()
        with socket.create_connection((host, port), timeout=3) as sock:
            with ctx.wrap_socket(sock, server_hostname=host):
                # handshake ok
                return {"ok": True, "msg": "TLS handshake OK"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}


@app.get("/api/test/backup-db", dependencies=[Depends(require_session)])
async def test_backup_db():
    try:
        await _ensure_storage_issue_free("backup_db")
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "msg": _short_error_message(str(exc), 220)}
    await ensure_offline_buffer_ready()
    settings = fetch_settings()
    secrets_data = read_secrets_file()
    password = secrets_data.get("backup_pg_password")
    if not password:
        return {"ok": False, "msg": "Kein Backup-Passwort gespeichert"}
    try:
        await asyncio.to_thread(
            _probe_backup_connection,
            host=settings["backup_pg_host"],
            port=int(settings["backup_pg_port"]),
            database=settings["backup_pg_db"],
            user=settings["backup_pg_user"],
            password=password,
            timeout=3,
        )
        return {"ok": True, "msg": "Backup-DB OK"}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "msg": str(exc)}


@app.get("/api/offline-buffer/export", dependencies=[Depends(require_session)])
@app.get("/api/backup/export", dependencies=[Depends(require_session)])
async def export_offline_buffer():
    try:
        await _ensure_storage_issue_free("backup_db")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=_short_error_message(str(exc), 220)) from exc
    await ensure_offline_buffer_ready()
    settings = fetch_settings()
    secrets_data = read_secrets_file()
    password = secrets_data.get("backup_pg_password")
    if not password:
        raise HTTPException(status_code=400, detail="Kein Backup-Passwort gespeichert.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    export_path = DATA_DIR / f"backup-export-{timestamp}-{secrets.token_hex(4)}.ndjson"

    try:
        with export_path.open("w", encoding="utf-8") as handle:
            handle.write(
                json.dumps(
                    {
                        "meta": {
                            "exported_at": _now_utc_iso(),
                            "host": settings["backup_pg_host"],
                            "database": settings["backup_pg_db"],
                        }
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            try:
                with psycopg2.connect(
                    host=settings["backup_pg_host"],
                    port=settings["backup_pg_port"],
                    dbname=settings["backup_pg_db"],
                    user=settings["backup_pg_user"],
                    password=password,
                    connect_timeout=5,
                ) as connection:
                    try:
                        connection.set_session(readonly=True, autocommit=False)
                    except Exception:  # noqa: BLE001
                        pass
                    with connection.cursor(name="buffer_export") as cur:
                        cur.itersize = 1000
                        cur.execute(
                            """
                            SELECT id, topic, payload, created_at
                            FROM buffer_events
                            ORDER BY id
                            """
                        )
                        for row in cur:
                            payload_value = row[2]
                            if isinstance(payload_value, (bytes, bytearray, memoryview)):
                                payload_bytes = payload_value.tobytes() if isinstance(payload_value, memoryview) else bytes(payload_value)
                                try:
                                    payload_value = json.loads(payload_bytes.decode("utf-8"))
                                except Exception:  # noqa: BLE001
                                    payload_value = payload_bytes.decode("utf-8", errors="ignore")
                            elif isinstance(payload_value, str):
                                try:
                                    payload_value = json.loads(payload_value)
                                except Exception:  # noqa: BLE001
                                    # leave as raw string
                                    pass
                            created_at = row[3]
                            if isinstance(created_at, datetime):
                                created_at = created_at.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                            handle.write(
                                json.dumps(
                                    {
                                        "id": row[0],
                                        "topic": row[1],
                                        "payload": payload_value,
                                        "created_at": created_at,
                                    },
                                    ensure_ascii=False,
                                )
                                + "\n"
                            )
            except psycopg2.Error as exc:  # noqa: BLE001
                raise HTTPException(status_code=503, detail=f"Offline-Puffer-Export fehlgeschlagen: {exc}") from exc
    except Exception:
        if export_path.exists():
            export_path.unlink(missing_ok=True)
        raise

    return FileResponse(
        export_path,
        media_type="application/x-ndjson",
        filename=export_path.name,
        background=BackgroundTask(export_path.unlink),
    )


@app.get("/api/backup/mirror-db", dependencies=[Depends(require_session)])
async def mirror_backup_status():
    return await _refresh_mirror_backup_state()


@app.get("/api/storage/preflight", dependencies=[Depends(require_session)])
async def storage_preflight_status():
    return await _get_storage_ownership_preflight()


@app.post("/api/storage/preflight/fix", dependencies=[Depends(require_session)])
async def storage_preflight_fix(pw: str = Form(...)):
    require_admin(pw)
    try:
        result = await asyncio.to_thread(_fix_storage_ownership_sync)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=_short_error_message(str(exc), 220))
    return result


@app.post("/api/backup/mirror-db/run", dependencies=[Depends(require_session)])
async def run_mirror_backup():
    try:
        await _run_mirror_backup(trigger="manual")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=_short_error_message(str(exc), 240))
    return await _refresh_mirror_backup_state()


@app.get("/api/backup/mirror-db/download", dependencies=[Depends(require_session)])
async def download_mirror_backup(name: str):
    candidate = _validate_backup_filename(name)
    backup_path = (MIRROR_BACKUP_DIR / candidate).resolve()
    allowed_root = MIRROR_BACKUP_DIR.resolve()
    if backup_path.parent != allowed_root:
        raise HTTPException(status_code=400, detail="Ungültiger Backup-Dateiname.")
    if not backup_path.exists() or not backup_path.is_file():
        raise HTTPException(status_code=404, detail="Mirror-DB-Backup nicht gefunden.")
    return FileResponse(
        backup_path,
        media_type="application/gzip",
        filename=backup_path.name,
    )


def _friendly_meyton_error(exc: Exception, host: str | None = None) -> str:
    message = str(exc)
    lowered = message.lower()
    args = getattr(exc, "args", ())
    code: int | None = None
    if isinstance(args, (tuple, list)) and args:
        first = args[0]
        if isinstance(first, int):
            code = first
    host_hint = f" ({host})" if host else ""
    if code == 2003 or "can't connect" in lowered or "timed out" in lowered:
        return f"Keine Antwort von der Mirror-MariaDB{host_hint}. Netzwerk oder Container prüfen."
    if code == 2005 or "getaddrinfo failed" in lowered or "name or service not known" in lowered:
        return f"Mirror-MariaDB-Host{host_hint} nicht gefunden. Adresse kontrollieren."
    if code == 1045 or "access denied" in lowered:
        return "Mirror-MariaDB-Anmeldung abgelehnt. Benutzer oder Passwort prüfen."
    if "unknown database" in lowered:
        return "Mirror-MariaDB-Datenbank nicht gefunden. Datenbanknamen prüfen."
    return f"Mirror-MariaDB-Fehler: {_short_error_message(message, 120)}"


async def _check_database_health() -> dict[str, str]:
    settings = fetch_settings()
    host = settings.get("db_host")
    port = settings.get("db_port")
    user = settings.get("db_user")
    if not host or not user or not port:
        return {"status": "unknown", "message": "Nicht konfiguriert"}
    secrets = read_secrets_file()
    password, _ = _ensure_mirror_db_secret(secrets)
    if not password:
        return {"status": "warn", "message": "Mirror-DB-Passwort fehlt (TS_CONNECT_MIRROR_DB_PASSWORD)"}

    def _connect() -> None:
        import pymysql  # local import to avoid global dependency at import time

        conn = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            connect_timeout=3,
            read_timeout=3,
            write_timeout=3,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()

    try:
        await asyncio.to_thread(_connect)
        return {"status": "ok", "message": "Verbindung aktiv"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": _friendly_meyton_error(exc, host)}


async def _check_confluent_health() -> dict[str, str]:
    settings = fetch_settings()
    bootstrap = settings.get("confluent_bootstrap") or CONFLUENT_BOOTSTRAP_DEFAULT
    if not bootstrap:
        return {"status": "unknown", "message": "Nicht konfiguriert"}
    result = await test_confluent(bootstrap)
    if result.get("ok"):
        return {"status": "ok", "message": result.get("msg", "Erreichbar")}
    return {"status": "error", "message": _short_error_message(str(result.get("msg", "Fehler")), 140)}


async def _check_backup_health() -> dict[str, str]:
    settings = fetch_settings()
    secrets = read_secrets_file()
    password = secrets.get("backup_pg_password")
    if not password:
        return {"status": "warn", "message": "Passwort fehlt"}
    try:
        await asyncio.to_thread(
            _probe_backup_connection,
            host=settings["backup_pg_host"],
            port=int(settings["backup_pg_port"]),
            database=settings["backup_pg_db"],
            user=settings["backup_pg_user"],
            password=password,
        )
        return {"status": "ok", "message": "Backup-DB erreichbar"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": _short_error_message(str(exc), 140)}


async def _check_license_health() -> dict[str, str]:
    settings = fetch_settings()
    license_key = (settings.get("license_key") or "").strip()
    if not license_key:
        return {"status": "warn", "message": "Keine Lizenz hinterlegt"}
    status = (settings.get("license_status") or "unbekannt").lower()
    valid_until = _parse_iso8601(settings.get("license_valid_until"))
    now = datetime.now(timezone.utc)
    if valid_until:
        if valid_until < now:
            return {"status": "error", "message": "Lizenz abgelaufen"}
        days_left = (valid_until.date() - now.date()).days
        if days_left <= 3:
            message = f"Läuft in {days_left} Tagen ab"
        else:
            message = f"{days_left} Tage verbleiben"
    else:
        message = "Gültigkeit unbekannt"
    if status in {"revoked", "expired", "disabled", "cancelled", "invalid"}:
        return {"status": "error", "message": f"Status: {status}"}
    if status in {"pending", "inactive"}:
        return {"status": "warn", "message": f"Status: {status}"}
    if status in {"unknown", ""}:
        return {"status": "warn", "message": message}
    return {"status": "ok", "message": message}


async def _check_connector_health() -> dict[str, str]:
    return await ops_runtime.check_connector_health(
        connect_base_url=CONNECT_BASE_URL,
        default_connector_name=DEFAULT_CONNECTOR_NAME,
        extract_error_message_fn=_extract_error_message,
        short_error_message=_short_error_message,
    )


# --------- Save & Apply ----------
@app.post("/save", dependencies=[Depends(require_session)])
async def save(
    request: Request,
    section: str = Form(...),
    pw: str | None = Form(default=None),
    db_host: str | None = Form(default=None),
    db_port: int | None = Form(default=None),
    db_user: str | None = Form(default=None),
    db_password: str | None = Form(default=None),
    source_db_host: str | None = Form(default=None),
    source_db_port: str | None = Form(default=None),
    source_db_repl_user: str | None = Form(default=None),
    source_db_repl_password: str | None = Form(default=None),
    source_db_gtid_mode: str | None = Form(default="true"),
    source_db_log_file: str | None = Form(default=None),
    source_db_log_pos: str | None = Form(default=None),
    source_db_connect_retry: str | None = Form(default=None),
    confluent_bootstrap: str | None = Form(default=None),
    confluent_sasl_username: str | None = Form(default=None),
    confluent_sasl_password: str | None = Form(default=None),
    topic_prefix: str | None = Form(default=None),
    server_id: int | None = Form(default=None),
    server_name: str | None = Form(default=None),
    verein_id: str | None = Form(default=None),
    license_key: str | None = Form(default=None),
    license_action: str | None = Form(default="validate"),
    new_admin_password: str = Form(default=""),
    confirm_admin_password: str = Form(default=""),
):
    section_key = section.strip().lower()
    settings = fetch_settings()
    secrets_data = read_secrets_file()

    if section_key == "admin":
        if not pw or not require_admin(pw, raise_exc=False):
            request.session["error_message"] = "Aktuelles Admin-Passwort ist ungültig."
            return RedirectResponse("/", status_code=303)
        new_admin_password = new_admin_password.strip()
        confirm_admin_password = confirm_admin_password.strip()
        if not new_admin_password:
            request.session["error_message"] = "Bitte ein neues Admin-Passwort eingeben."
            return RedirectResponse("/", status_code=303)
        if new_admin_password != confirm_admin_password:
            request.session["error_message"] = "Neues Admin-Passwort stimmt nicht überein."
            return RedirectResponse("/", status_code=303)
        if len(new_admin_password) < 8:
            request.session["error_message"] = "Das neue Admin-Passwort muss mindestens 8 Zeichen enthalten."
            return RedirectResponse("/", status_code=303)
        set_admin_password(new_admin_password)
        security_bootstrap.clear_generated_admin_password_file()
        request.session["flash_message"] = "Admin-Passwort aktualisiert."
        return RedirectResponse("/", status_code=303)

    if section_key == "license":
        license_key_value = (license_key or "").strip()
        license_action_value = (license_action or "validate").strip().lower()
        prune_error: str | None = None
        current_license = (settings.get("license_key") or "").strip()
        current_activation_id = settings.get("license_activation_id") or ""
        current_activation_at = settings.get("license_activated_at")
        verein_identifier = str(settings.get("topic_prefix") or settings.get("server_id") or "").strip()

        if not license_key_value:
            _set_license_form_key(request, "")
            plan = DEFAULT_LICENSE_TIER
            retention_days = LICENSE_RETENTION_DAYS[plan]
            now_iso = _now_utc_iso()
            conn = get_db()
            conn.execute(
                """
                UPDATE settings
                SET license_key='', license_tier=?, retention_days=?,
                    license_status='unknown', license_valid_until=NULL,
                    license_last_checked=?, license_customer_email=NULL,
                    license_activation_id='', license_activated_at=NULL
                WHERE id=1
                """,
                (
                    plan,
                    retention_days,
                    now_iso,
                ),
            )
            conn.commit()
            conn.close()
            store_license_key("")
            write_license_meta({})
            if settings.get("offline_buffer_enabled") and secrets_data.get("backup_pg_password"):
                try:
                    updated_settings = fetch_settings()
                    prune_backup_data(
                        days=retention_days,
                        host=updated_settings["backup_pg_host"],
                        port=updated_settings["backup_pg_port"],
                        database=updated_settings["backup_pg_db"],
                        user=updated_settings["backup_pg_user"],
                        password=secrets_data["backup_pg_password"],
                    )
                except Exception as exc:  # noqa: BLE001
                    prune_error = f"Backup-Bereinigung nach Lizenz-Reset fehlgeschlagen: {exc}"
            request.session["flash_message"] = f"Lizenz entfernt. Plan auf {plan_display_name(plan)} zurückgesetzt."
            if prune_error:
                request.session.setdefault("error_message", prune_error)
            await update_remote_replication_state(False)
            return RedirectResponse("/", status_code=303)

        try:
            validation = await validate_license_key_remote(license_key_value, include_machine_scope=False)
        except Exception as exc:  # noqa: BLE001
            _set_license_form_key(request, license_key_value)
            request.session["error_message"] = f"Lizenzprüfung fehlgeschlagen: {exc}"
            return RedirectResponse("/", status_code=303)
        write_license_meta(validation)

        entitled = _keygen_license_entitled(validation)
        plan, retention_days, status, expires_at_norm, last_checked = _license_validation_state(validation)

        new_activation_id = current_activation_id
        new_activation_at = current_activation_at
        if license_key_value != current_license:
            new_activation_id = ""
            new_activation_at = None

        activation_feedback: dict[str, Any] | None = None
        activation_error: str | None = None
        activation_requested = license_action_value == "activate"
        if activation_requested:
            if not LICENSE_MACHINE_ACTIVATION_ENABLED:
                activation_error = "Lizenzaktivierungen sind deaktiviert."
            elif new_activation_id:
                activation_error = "Lizenz ist bereits aktiviert."
            elif not _keygen_can_activate(validation):
                activation_error = "Lizenz kann erst nach erfolgreicher Prüfung mit Keygen aktiviert werden."
            else:
                instance_id_value = verein_identifier
                instance_name_value = verein_identifier or settings.get("server_name") or "ts-connect"
                try:
                    activation_feedback = await activate_license_key_remote(
                        license_key_value,
                        instance_name=instance_name_value,
                        instance_id=str(instance_id_value),
                        validation=validation,
                    )
                except Exception as exc:  # noqa: BLE001
                    activation_error = _short_error_message(str(exc), 180)
                if activation_feedback and activation_feedback.get("activated"):
                    new_activation_id = activation_feedback.get("activation_id") or ""
                    new_activation_at = activation_feedback.get("activated_at") or _now_utc_iso()
                    try:
                        validation = await validate_license_key_remote(license_key_value)
                        write_license_meta(validation)
                        entitled = _keygen_license_entitled(validation)
                        plan, retention_days, status, expires_at_norm, last_checked = _license_validation_state(validation)
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Keygen validation after activation failed: %s", exc)
                elif activation_feedback and activation_feedback.get("message") and not activation_error:
                    activation_error = str(activation_feedback.get("message"))

        conn = get_db()
        conn.execute(
            """
            UPDATE settings
            SET license_key=?, license_tier=?, retention_days=?, license_status=?, license_valid_until=?, license_last_checked=?,
                license_customer_email=?, license_activation_id=?, license_activated_at=?
            WHERE id=1
            """,
            (
                license_key_value,
                plan,
                retention_days,
                status,
                expires_at_norm,
                last_checked,
                validation.get("customer_email"),
                new_activation_id,
                new_activation_at,
            ),
        )
        conn.commit()
        conn.close()

        store_license_key(license_key_value)
        write_license_meta(validation)
        _set_license_form_key(request, "")

        if settings.get("offline_buffer_enabled") and secrets_data.get("backup_pg_password"):
            try:
                updated_settings = fetch_settings()
                prune_backup_data(
                    days=retention_days,
                    host=updated_settings["backup_pg_host"],
                    port=updated_settings["backup_pg_port"],
                    database=updated_settings["backup_pg_db"],
                    user=updated_settings["backup_pg_user"],
                    password=secrets_data["backup_pg_password"],
                )
            except Exception as exc:  # noqa: BLE001
                prune_error = f"Backup-Bereinigung nach Lizenz-Update fehlgeschlagen: {exc}"

        updated_settings = fetch_settings()
        await update_remote_replication_state(_license_is_active(updated_settings))

        message_parts = [f"Plan: {plan_display_name(plan)}"]
        if expires_at_norm:
            message_parts.append(f"gültig bis {expires_at_norm}")
        if new_activation_id:
            message_parts.append("Lizenz aktiviert")
        message = " | ".join(message_parts)

        if entitled:
            request.session["flash_message"] = f"Lizenz erfolgreich geprüft. {message}"
            if prune_error:
                request.session["error_message"] = prune_error
            if activation_error:
                extra = request.session.get("error_message")
                activation_msg = f"Lizenzaktivierung fehlgeschlagen: {activation_error}"
                request.session["error_message"] = f"{extra} | {activation_msg}" if extra else activation_msg
            elif activation_requested and new_activation_id:
                extra = request.session.get("flash_message")
                activation_msg = "Lizenz erfolgreich aktiviert."
                request.session["flash_message"] = f"{extra} {activation_msg}" if extra else activation_msg
            elif LICENSE_MACHINE_ACTIVATION_ENABLED and not new_activation_id:
                extra = request.session.get("flash_message")
                activation_msg = "Lizenz gespeichert. Diese Installation muss noch mit Keygen aktiviert werden."
                request.session["flash_message"] = f"{extra} {activation_msg}" if extra else activation_msg
        else:
            reason = validation.get("message") or validation.get("error") or "Lizenz ungültig."
            full_reason = f"Lizenz ungültig: {reason} ({message})"
            if prune_error:
                full_reason = f"{full_reason} | {prune_error}"
            request.session["error_message"] = full_reason
        return RedirectResponse("/", status_code=303)

    if section_key == "db":
        request.session["flash_message"] = (
            "Mirror-MariaDB-Verbindung wird intern aus der .env gelesen und ist in der UI nicht editierbar."
        )
        return RedirectResponse("/", status_code=303)

    if section_key == "source":
        source_host_value = (source_db_host or "").strip()
        source_port_raw = (source_db_port or "").strip()
        source_port_value = DEFAULT_SOURCE_DB_PORT
        if source_port_raw:
            try:
                source_port_value = int(source_port_raw)
            except ValueError:
                request.session["error_message"] = "Source-Port muss eine Zahl sein."
                return RedirectResponse("/", status_code=303)
        source_user_value = (source_db_repl_user or "").strip()
        source_gtid_mode_value = True
        source_log_file_value = ""
        source_log_pos_value = None
        source_connect_retry_value = DEFAULT_SOURCE_DB_CONNECT_RETRY

        if source_port_value <= 0 or source_port_value > 65535:
            request.session["error_message"] = "Source-Port muss zwischen 1 und 65535 liegen."
            return RedirectResponse("/", status_code=303)

        submitted_password = (source_db_repl_password or "").strip() if source_db_repl_password is not None else ""
        existing_password = secrets_data.get(SOURCE_DB_REPL_PASSWORD_KEY, "")
        if submitted_password == PASSWORD_PLACEHOLDER and existing_password:
            source_password_value = existing_password
        else:
            source_password_value = submitted_password

        if source_host_value:
            if not source_user_value:
                request.session["error_message"] = "Bitte den Replikations-User der Vereins-MainDB angeben."
                return RedirectResponse("/", status_code=303)
            if not source_password_value:
                request.session["error_message"] = "Bitte das Replikations-Passwort der Vereins-MainDB angeben."
                return RedirectResponse("/", status_code=303)
        else:
            source_user_value = ""
            source_password_value = ""

        conn = get_db()
        conn.execute(
            """
            UPDATE settings
            SET source_db_host=?, source_db_port=?, source_db_repl_user=?,
                source_db_gtid_mode=?, source_db_log_file=?, source_db_log_pos=?, source_db_connect_retry=?
            WHERE id=1
            """,
            (
                source_host_value,
                source_port_value,
                source_user_value,
                1 if source_gtid_mode_value else 0,
                source_log_file_value or None,
                source_log_pos_value,
                source_connect_retry_value,
            ),
        )
        conn.commit()
        conn.close()

        if source_host_value and source_password_value:
            secrets_data[SOURCE_DB_REPL_PASSWORD_KEY] = source_password_value
        else:
            secrets_data.pop(SOURCE_DB_REPL_PASSWORD_KEY, None)
        write_secrets_file(secrets_data)

        try:
            result = await apply_source_replication_config(fetch_settings(), secrets_data)
        except Exception as exc:  # noqa: BLE001
            request.session["flash_message"] = "MainDB-Replikations-Einstellungen wurden gespeichert."
            request.session["error_message"] = (
                "Replikation konnte nicht angewendet werden: "
                f"{_short_error_message(str(exc), 220)}"
            )
            return RedirectResponse("/", status_code=303)

        message = str(result.get("message") or "").strip()
        if source_host_value:
            base = "Vereins-MainDB-Replikation gespeichert und auf der Mirror-MariaDB angewendet."
        else:
            base = "Vereins-MainDB-Replikation deaktiviert."
        request.session["flash_message"] = f"{base} {message}".strip()
        return RedirectResponse("/", status_code=303)

    if section_key == "offline":
        await ensure_offline_buffer_ready()
        try:
            await apply_connector_config(trigger="offline")
        except DeferredApplyError as exc:
            request.session["flash_message"] = (
                "Offline-Puffer wird erneut angewendet, sobald die lokale Mirror-MariaDB erreichbar ist. "
                f"Letzter Fehler: {exc}"
            )
        except Exception as exc:  # noqa: BLE001
            request.session["error_message"] = f"Offline-Puffer Aktualisierung fehlgeschlagen: {exc}"
        else:
            request.session["flash_message"] = "Offline-Puffer ist aktiv und wurde aktualisiert."
        return RedirectResponse("/", status_code=303)

    if section_key == "confluent":
        confluent_sasl_username = (confluent_sasl_username or "").strip()
        submitted_confluent_password = (confluent_sasl_password or "").strip()
        existing_confluent_password = str(secrets_data.get("confluent_sasl_password") or "").strip()
        if submitted_confluent_password == PASSWORD_PLACEHOLDER and existing_confluent_password:
            confluent_password_value = existing_confluent_password
        else:
            confluent_password_value = submitted_confluent_password
        bootstrap_value = (confluent_bootstrap or settings["confluent_bootstrap"] or CONFLUENT_BOOTSTRAP_DEFAULT).strip()

        if not confluent_sasl_username or not confluent_password_value:
            request.session["error_message"] = "Bitte API Key und Secret für Confluent ausfüllen."
            return RedirectResponse("/", status_code=303)

        verein_id_value = (verein_id or topic_prefix or "").strip()
        if not verein_id_value:
            request.session["error_message"] = "Bitte die VereinsID angeben."
            return RedirectResponse("/", status_code=303)
        if not verein_id_value.isdigit():
            request.session["error_message"] = "Die VereinsID darf nur Ziffern enthalten."
            return RedirectResponse("/", status_code=303)
        try:
            server_id_value = int(verein_id_value)
        except ValueError:
            request.session["error_message"] = "Die VereinsID muss eine gültige Zahl sein."
            return RedirectResponse("/", status_code=303)
        topic_prefix_value = verein_id_value
        server_name_value = (settings.get("server_name") or DEFAULT_SERVER_NAME).strip() or DEFAULT_SERVER_NAME

        conn = get_db()
        conn.execute(
            """
            UPDATE settings
            SET confluent_bootstrap=?, confluent_sasl_username=?, topic_prefix=?,
                server_id=?, server_name=?
            WHERE id=1
            """,
            (
                bootstrap_value,
                confluent_sasl_username,
                topic_prefix_value,
                server_id_value,
                server_name_value,
            ),
        )
        conn.commit()
        conn.close()

        settings = fetch_settings()
        db_password_val, secrets_data = _ensure_mirror_db_secret(secrets_data)
        if not db_password_val:
            request.session["error_message"] = (
                "Mirror-MariaDB-Passwort fehlt. Bitte TS_CONNECT_MIRROR_DB_PASSWORD in der .env setzen."
            )
            return RedirectResponse("/", status_code=303)

        secrets_data.update(
            {
                "db_password": db_password_val,
                "confluent_bootstrap": settings["confluent_bootstrap"],
                "confluent_sasl_username": confluent_sasl_username,
                "confluent_sasl_password": confluent_password_value,
            }
        )
        write_secrets_file(secrets_data)

        try:
            await apply_connector_config(trigger="confluent")
        except DeferredApplyError as exc:
            request.session["flash_message"] = (
                "Confluent-Einstellungen gespeichert. Connector-Update wird automatisch erneut versucht, "
                "sobald die lokale Mirror-MariaDB erreichbar ist. "
                f"Letzter Fehler: {exc}"
            )
            return RedirectResponse("/", status_code=303)
        except Exception as exc:
            request.session["error_message"] = f"Connector-Update fehlgeschlagen: {exc}"
            return RedirectResponse("/", status_code=303)

        request.session["flash_message"] = "Confluent-Einstellungen gespeichert & Connector aktualisiert."
        return RedirectResponse("/", status_code=303)

    request.session["error_message"] = "Unbekannter Abschnitt."
    return RedirectResponse("/", status_code=303)


# --------- License Utilities ----------
@app.post("/api/license/refresh", dependencies=[Depends(require_session)])
async def refresh_license_file():
    settings = fetch_settings()
    license_value = (settings.get("license_key") or "").strip()
    if not license_value:
        raise HTTPException(status_code=400, detail="Keine Lizenz hinterlegt.")
    updated = sync_license_key_file(settings)
    return {
        "ok": True,
        "updated": updated,
        "path": str(LICENSE_KEY_FILE),
        "exists": LICENSE_KEY_FILE.exists(),
    }


# --------- Connector Control ----------
# --------- Connector Control ----------
@app.post("/api/connector/control/{action}", dependencies=[Depends(require_session)])
async def connector_control(action: str, pw: str = Form(...)):
    require_admin(pw)
    valid = {"pause", "resume", "restart"}
    if action not in valid:
        raise HTTPException(400, "invalid action")
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/{action}")
        return {"ok": r.status_code in (200, 202), "status": r.status_code}


# --------- Secrets ----------
@app.post("/api/secrets/view", dependencies=[Depends(require_session)])
async def secrets_view(pw: str = Form(...)):
    require_admin(pw)
    if not SECRETS_PATH.exists():
        return {"ok": False, "exists": False, "content": ""}
    text = SECRETS_PATH.read_text(encoding="utf-8")
    modified = datetime.utcfromtimestamp(SECRETS_PATH.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
    return {"ok": True, "exists": True, "content": text, "modified": modified}


@app.post("/api/connector/resnapshot", dependencies=[Depends(require_session)])
async def connector_resnapshot(pw: str = Form(...)):
    require_admin(pw)
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.delete(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}")
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Connector REST nicht erreichbar: {exc}")
        if resp.status_code not in (200, 202, 204, 404):
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
    try:
        await apply_connector_config(trigger="resnapshot")
    except DeferredApplyError as exc:
        return {
            "ok": True,
            "pending": True,
            "message": (
                "Connector-Neuanlage wurde geplant. Sobald die lokale Mirror-MariaDB erreichbar ist, "
                "startet Debezium automatisch. "
                f"Letzter Fehler: {exc}"
            ),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Connector-Neuanlage fehlgeschlagen: {exc}")
    return {"ok": True, "pending": False}

# --------- Status ----------
@app.get("/api/status", dependencies=[Depends(require_session)])
async def status():
    apply_state = await get_apply_state()
    result: dict[str, object] = {
        "applyState": apply_state,
        "operation_id": apply_state.get("operation_id"),
    }
    preset_worker = "pending" if apply_state.get("pending") else None
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            w = await client.get(f"{CONNECT_BASE_URL}/connectors")
            if w.status_code != 200:
                result["worker"] = preset_worker or "unavailable"
                result["error"] = _extract_error_message(w)
                result["recovery"] = _classify_recovery_issue(
                    str(result.get("error") or ""),
                    operation_id=apply_state.get("operation_id"),
                )
                return result
            s = await client.get(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/status")
            result["connectorStatus"] = s.json() if s.status_code == 200 else None
            result["worker"] = preset_worker or "ok"
            if preset_worker and apply_state.get("last_error"):
                result.setdefault("error", apply_state.get("last_error"))
            result["recovery"] = _classify_recovery_issue(
                str(result.get("error") or apply_state.get("last_error") or ""),
                operation_id=apply_state.get("operation_id"),
            )
            return result
        except Exception as e:
            result["worker"] = preset_worker or "unavailable"
            result["error"] = str(e)
            if preset_worker and apply_state.get("last_error"):
                result.setdefault("error", apply_state.get("last_error"))
            result["recovery"] = _classify_recovery_issue(
                str(result.get("error") or apply_state.get("last_error") or ""),
                operation_id=apply_state.get("operation_id"),
            )
            return result


@app.get("/api/connector/config", dependencies=[Depends(require_session)])
async def connector_config():
    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.get(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}")
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"Connector REST nicht erreichbar: {exc}")
    if resp.status_code == 404:
        return {"exists": False, "config": None}
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return {"exists": True, "config": resp.json()}


@app.get("/api/update/status", dependencies=[Depends(require_session)])
async def update_status(force: bool = False):
    try:
        status = await _build_update_status(force=force)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=_short_error_message(str(exc), 180))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=_short_error_message(str(exc), 180))
    return status


@app.get("/api/health/summary", dependencies=[Depends(require_session)])
async def health_summary():
    database, confluent, connector, backup, license, apply_state, storage = await asyncio.gather(
        _check_database_health(),
        _check_confluent_health(),
        _check_connector_health(),
        _check_backup_health(),
        _check_license_health(),
        get_apply_state(),
        _get_storage_ownership_preflight(),
    )
    network = _summarize_network_exposure()
    storage_issues = {str(issue.get("key") or ""): issue for issue in storage.get("issues", [])}
    mirror_storage_issue = storage_issues.get("mirror_db")
    if mirror_storage_issue:
        database = {
            "status": "error",
            "message": str(mirror_storage_issue.get("summary") or "Mirror-MariaDB-Datenordner-Rechte prüfen"),
        }
    backup_storage_issue = storage_issues.get("backup_db")
    if backup_storage_issue:
        backup = {
            "status": "error",
            "message": str(backup_storage_issue.get("summary") or "Backup-DB-Datenordner-Rechte prüfen"),
        }
    connector_issue_message = connector.get("message") if connector.get("status") != "ok" else None
    connector_recovery = _classify_recovery_issue(
        apply_state.get("last_error") or connector_issue_message,
        operation_id=apply_state.get("operation_id"),
    )
    if connector_recovery.get("category"):
        connector = dict(connector)
        connector["recovery"] = connector_recovery
        if connector.get("status") == "ok":
            connector["status"] = "warn"
    if apply_state.get("pending"):
        connector = dict(connector)
        if connector.get("status") == "ok":
            connector["status"] = "warn"
        if not connector.get("message"):
            connector["message"] = "Connector-Konfiguration wartet auf Wiederholung"
    snapshot = {
        "timestamp": _now_utc_iso(),
        "database": database,
        "confluent": confluent,
        "connector": connector,
        "backup": backup,
        "license": license,
        "network": network,
    }
    try:
        _write_json_log("health.log", snapshot)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Failed to write health log: %s", exc)
    return {
        "database": database,
        "confluent": confluent,
        "connector": connector,
        "backup": backup,
        "license": license,
        "storage": storage,
        "network": network,
    }


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/api/source-replication/status", dependencies=[Depends(require_session)])
async def source_replication_status():
    settings = fetch_settings()
    configured = bool(str(settings.get("source_db_host") or "").strip())
    try:
        snapshot = await source_replication_status_snapshot()
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "configured": configured,
            "message": _short_error_message(str(exc), 180),
        }
    snapshot["configured"] = configured
    if configured:
        snapshot.setdefault("configured_host", settings.get("source_db_host"))
    return snapshot


@app.get("/api/logs/ui", dependencies=[Depends(require_session)])
async def ui_logs(lines: int = 200):
    line_limit = max(10, min(lines, 500))
    if not UI_LOG_FILE.exists():
        return {
            "path": str(UI_LOG_FILE),
            "exists": False,
            "updated_at": None,
            "lines": [],
        }
    try:
        log_lines = await asyncio.to_thread(_tail_log_lines, UI_LOG_FILE, line_limit)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=_short_error_message(str(exc), 180))
    modified = datetime.utcfromtimestamp(UI_LOG_FILE.stat().st_mtime).replace(microsecond=0).isoformat() + "Z"
    return {
        "path": str(UI_LOG_FILE),
        "exists": True,
        "updated_at": modified,
        "lines": log_lines,
    }


@app.post("/api/update/config", dependencies=[Depends(require_session)])
async def update_config(pw: str = Form(...), auto_enabled: str = Form("0"), auto_hour: str = Form("1")):
    require_admin(pw)
    enabled = _parse_bool(auto_enabled, default=False)
    hour = _sanitize_hour(auto_hour)
    await merge_update_state_async(
        auto_update_enabled=enabled,
        auto_update_hour=hour,
    )
    return {
        "ok": True,
        "auto_update": {
            "enabled": enabled,
            "hour": hour,
        },
    }


@app.post("/api/update/run", dependencies=[Depends(require_session)])
async def trigger_update(pw: str = Form(...), target: str | None = Form(None)):
    require_admin(pw)
    target_ref = (target or "").strip() or None
    result = await _launch_update_job(
        target_ref=target_ref,
        initiated_by="manual",
        force_release_refresh=not target_ref,
        reset_log=True,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=result.get("code", 500), detail=result.get("error"))
    return result


@app.get("/api/os-updates/status", dependencies=[Depends(require_session)])
async def os_updates_status(refresh: bool = False):
    if refresh:
        return await _refresh_os_updates_state(force=True)
    state = await get_os_update_state()
    if _should_refresh_os_updates(state):
        return await _refresh_os_updates_state(force=False)
    return state


@app.post("/api/os-updates/apply", dependencies=[Depends(require_session)])
async def os_updates_apply(pw: str = Form(...)):
    require_admin(pw)
    state = await get_os_update_state()
    if state.get("update_in_progress"):
        raise HTTPException(status_code=409, detail="Systemupdate läuft bereits.")
    global _os_update_task
    if _os_update_task and not _os_update_task.done():
        raise HTTPException(status_code=409, detail="Systemupdate läuft bereits.")
    _os_update_task = asyncio.create_task(_run_os_updates_job())
    _os_update_task.add_done_callback(_os_update_task_done)
    return {"ok": True}


def _ensure_host_agent_configured() -> None:
    ops_runtime.ensure_host_agent_configured(host_agent_url=HOST_AGENT_URL)


def _raise_http_for_agent_exception(exc: Exception, *, default_status: int = 500, max_len: int = 200) -> None:
    ops_runtime.raise_http_for_agent_exception(
        exc,
        short_error_message=_short_error_message,
        default_status=default_status,
        max_len=max_len,
    )


@app.get("/api/host/status", dependencies=[Depends(require_session)])
async def host_status():
    _ensure_host_agent_configured()
    try:
        return await _host_agent_request("GET", "/api/v1/status", timeout=10)
    except Exception as exc:  # noqa: BLE001
        _raise_http_for_agent_exception(exc, default_status=503)


@app.post("/api/host/os-refresh", dependencies=[Depends(require_session)])
async def host_os_refresh():
    _ensure_host_agent_configured()
    try:
        return await _host_agent_request("POST", "/api/v1/os/refresh", timeout=5)
    except Exception as exc:  # noqa: BLE001
        _raise_http_for_agent_exception(exc, default_status=500)


@app.post("/api/host/os-update", dependencies=[Depends(require_session)])
async def host_os_update(pw: str = Form(...)):
    require_admin(pw)
    _ensure_host_agent_configured()
    try:
        return await _host_agent_request("POST", "/api/v1/os/update", timeout=5)
    except Exception as exc:  # noqa: BLE001
        _raise_http_for_agent_exception(exc, default_status=500)


@app.post("/api/host/reboot", dependencies=[Depends(require_session)])
async def host_reboot(pw: str = Form(...), delay: int = Form(HOST_REBOOT_DELAY_SECONDS)):
    require_admin(pw)
    _ensure_host_agent_configured()
    payload = {"delay": max(0, min(int(delay), 3600))}
    try:
        return await _host_agent_request("POST", "/api/v1/reboot", json_payload=payload, timeout=5)
    except Exception as exc:  # noqa: BLE001
        _raise_http_for_agent_exception(exc, default_status=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=APP_PORT)
