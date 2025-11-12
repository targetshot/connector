import asyncio
import html
import ipaddress
import json
import hashlib
import logging
import os
import re
import secrets
import shutil
import socket
import sqlite3
import ssl
import stat
import string
from asyncio.subprocess import PIPE
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
import psycopg2
from psycopg2 import sql
from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from starlette.background import BackgroundTask
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
import httpx
from connector_config import CONNECT_SECRETS_PATH, build_connector_config
from file_utils import atomic_write_text as _atomic_write_text
from file_utils import fsync_directory as _fsync_directory
from file_utils import tmp_path_for as _tmp_path_for
from licenses import (
    DEFAULT_LICENSE_TIER,
    DEFAULT_RETENTION_DAYS,
    LICENSE_RETENTION_DAYS,
    normalize_license_tier,
    plan_allows_shooter_count,
    plan_display_name,
    plan_limit_label,
    required_plan_for_shooter_count,
    retention_for_license,
)
from host_agent_utils import get_host_agent_token
from update_agent_utils import get_update_agent_token
from update_state import UpdateStateManager

logger = logging.getLogger("ts-connect-ui")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


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

APP_PORT = int(os.getenv("PORT", "8080"))
CONNECT_BASE_URL = os.getenv("CONNECT_BASE_URL", "http://kafka-connect:8083")
DEFAULT_CONNECTOR_NAME = os.getenv("DEFAULT_CONNECTOR_NAME", "targetshot-debezium")
BACKUP_CONNECTOR_NAME = os.getenv("BACKUP_CONNECTOR_NAME", f"{DEFAULT_CONNECTOR_NAME}-backup-sink")
DEFAULT_ADMIN_PASSWORD = "change-me"
PASSWORD_PLACEHOLDER = "********"
DEFAULT_SESSION_SECRET = "targetshot-connect-ui-secret"
TRUSTED_CIDRS = [c.strip() for c in os.getenv(
    "UI_TRUSTED_CIDRS",
    "192.168.0.0/16,10.0.0.0/8,172.16.0.0/12"
).split(",")]
WORKSPACE_PATH = Path(os.getenv("TS_CONNECT_WORKSPACE", "/workspace"))
LOCAL_TIMEZONE = datetime.now().astimezone().tzinfo or timezone.utc
DATA_DIR = Path(os.getenv("TS_CONNECT_DATA_DIR", "/app/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
ADMIN_PASSWORD_FILE = DATA_DIR / "admin_password.txt"
ADMIN_PASSWORD_GENERATED_FILE = DATA_DIR / "admin_password.generated"
SESSION_SECRET_FILE = DATA_DIR / "session_secret"
UPDATE_AGENT_URL = os.getenv("TS_CONNECT_UPDATE_AGENT_URL", "http://update-agent:9000").rstrip("/")
UPDATE_AGENT_TOKEN = get_update_agent_token(DATA_DIR)
HOST_AGENT_URL = os.getenv("TS_CONNECT_HOST_AGENT_URL", "").strip()
if HOST_AGENT_URL:
    HOST_AGENT_URL = HOST_AGENT_URL.rstrip("/")
HOST_AGENT_TOKEN = get_host_agent_token(DATA_DIR)
HOST_REBOOT_DELAY_SECONDS = int(os.getenv("TS_CONNECT_HOST_REBOOT_DELAY", "60"))
SECRETS_FILE_UID = _env_int("TS_CONNECT_SECRETS_UID", 1000)
SECRETS_FILE_GID = _env_int("TS_CONNECT_SECRETS_GID", 1000)


def _generate_admin_password(length: int = 24) -> str:
    alphabet = string.ascii_letters + string.digits + "-_"
    return "".join(secrets.choice(alphabet) for _ in range(length))


def _resolve_env_admin_password() -> str | None:
    value = os.getenv("UI_ADMIN_PASSWORD")
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed or trimmed == DEFAULT_ADMIN_PASSWORD:
        return None
    return trimmed


def _read_generated_admin_password() -> str | None:
    if not ADMIN_PASSWORD_GENERATED_FILE.exists():
        return None
    try:
        data = ADMIN_PASSWORD_GENERATED_FILE.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return data or None


def _remember_generated_admin_password(password: str) -> None:
    _atomic_write_text(
        ADMIN_PASSWORD_GENERATED_FILE,
        password + "\n",
        mode=stat.S_IRUSR | stat.S_IWUSR,
    )


def _clear_generated_admin_password_file() -> None:
    if ADMIN_PASSWORD_GENERATED_FILE.exists():
        try:
            ADMIN_PASSWORD_GENERATED_FILE.unlink()
        except OSError:
            pass
        else:
            _fsync_directory(ADMIN_PASSWORD_GENERATED_FILE.parent)


def _resolve_session_secret() -> str:
    env_secret = os.getenv("UI_SESSION_SECRET", "").strip()
    if env_secret and env_secret != DEFAULT_SESSION_SECRET:
        return env_secret
    try:
        stored = SESSION_SECRET_FILE.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        stored = ""
    except OSError:
        stored = ""
    if stored:
        return stored
    generated = secrets.token_urlsafe(48)
    try:
        _atomic_write_text(
            SESSION_SECRET_FILE,
            generated + "\n",
            mode=stat.S_IRUSR | stat.S_IWUSR,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Session-Secret konnte nicht persistiert werden: %s", exc)
    else:
        logger.warning(
            "UI_SESSION_SECRET nicht gesetzt – zufälliges Secret wurde in %s hinterlegt.",
            SESSION_SECRET_FILE,
        )
    return generated


async def _update_agent_request(
    method: str,
    path: str,
    *,
    json_payload: dict | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    if not UPDATE_AGENT_URL:
        raise RuntimeError("Update-Agent URL nicht konfiguriert")
    url = f"{UPDATE_AGENT_URL}{path}"
    headers: dict[str, str] = {}
    if UPDATE_AGENT_TOKEN:
        headers["X-Update-Agent-Token"] = UPDATE_AGENT_TOKEN
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, url, json=json_payload, headers=headers)
    except httpx.RequestError as exc:
        raise RuntimeError(f"Update-Agent nicht erreichbar: {_short_error_message(str(exc), 140)}") from exc
    if response.status_code >= 400:
        text = _short_error_message(response.text, 200)
        raise RuntimeError(f"Update-Agent {response.status_code}: {text}")
    if response.headers.get("content-type", "").startswith("application/json"):
        return response.json()
    return {"ok": True, "body": response.text}


async def _ping_update_agent(timeout: float = 3.0) -> bool:
    try:
        result = await _update_agent_request("GET", "/api/v1/health", timeout=timeout)
    except Exception:
        return False
    return bool(result.get("ok"))


async def _host_agent_request(
    method: str,
    path: str,
    *,
    json_payload: dict | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    if not HOST_AGENT_URL:
        raise RuntimeError("Host-Agent URL nicht konfiguriert")
    url = f"{HOST_AGENT_URL}{path}"
    headers: dict[str, str] = {}
    token = HOST_AGENT_TOKEN
    if token:
        headers["X-Host-Agent-Token"] = token
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, url, json=json_payload, headers=headers)
    except httpx.RequestError as exc:
        raise RuntimeError(f"Host-Agent nicht erreichbar: {_short_error_message(str(exc), 140)}") from exc
    if response.status_code >= 400:
        text = _short_error_message(response.text, 200)
        raise RuntimeError(f"Host-Agent {response.status_code}: {text}")
    if response.headers.get("content-type", "").startswith("application/json"):
        return response.json()
    return {"ok": True, "body": response.text}


async def _ping_host_agent(timeout: float = 3.0) -> bool:
    if not HOST_AGENT_URL:
        return False
    try:
        result = await _host_agent_request("GET", "/api/v1/health", timeout=timeout)
    except Exception:
        return False
    return bool(result.get("ok"))


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
DEFAULT_SERVER_NAME = os.getenv("TS_CONNECT_DEFAULT_SERVER_NAME", "targetshot-mysql")
STREAMS_TARGET_PREFIX = (os.getenv("TS_STREAMS_TARGET_PREFIX", "ts.sds-test") or "ts.sds-test").strip() or "ts.sds-test"
LEMON_LICENSE_API_URL = os.getenv(
    "TS_LICENSE_API_URL",
    "https://api.lemonsqueezy.com/v1/licenses/validate",
).strip()
LEMON_ACTIVATION_URL = os.getenv(
    "TS_LICENSE_ACTIVATION_URL",
    "https://api.lemonsqueezy.com/v1/licenses/activate",
).strip()
LEMON_LICENSE_API_KEY = os.getenv("TS_LICENSE_API_KEY", "").strip()
LEMON_VARIANT_PLAN_MAP_RAW = os.getenv("TS_LICENSE_VARIANT_PLAN_MAP", "")
LEMON_INSTANCE_NAME = os.getenv("TS_LICENSE_INSTANCE_NAME", "").strip()
LEMON_INSTANCE_ID = os.getenv("TS_LICENSE_INSTANCE_ID", "").strip()
LEMON_ACTIVATION_ENABLED = os.getenv("TS_LICENSE_AUTO_ACTIVATE", "true").lower() in {"1", "true", "yes", "on"}
ELASTIC_AGENT_ENABLED = os.getenv("ELASTIC_AGENT_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


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


def _parse_variant_plan_map(raw: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in raw.split(","):
        if not entry.strip():
            continue
        if "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        key = key.strip()
        value = normalize_license_tier(value)
        if key:
            mapping[key.lower()] = value
    return mapping


_DEFAULT_VARIANT_MAP = {
    "1056369": "basic",
    "1056375": "plus",
    "1056379": "pro",
}
LEMON_VARIANT_PLAN_MAP = _parse_variant_plan_map(LEMON_VARIANT_PLAN_MAP_RAW)


def _write_json_log(filename: str, payload: dict) -> None:
    path = LOG_DIR / filename
    line = json.dumps(payload, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")

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


SESSION_SECRET = _resolve_session_secret()
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
OS_UPDATE_STATE_PATH = DATA_DIR / "os_update_state.json"
OS_UPDATE_MAX_AGE_SECONDS = int(os.getenv("TS_CONNECT_OS_UPDATE_MAX_AGE", "21600"))
OS_UPDATE_LOG_LIMIT = int(os.getenv("TS_CONNECT_OS_UPDATE_LOG_LIMIT", "400"))
OS_UPDATE_PACKAGE_LIMIT = int(os.getenv("TS_CONNECT_OS_UPDATE_PACKAGE_LIMIT", "80"))
DEFAULT_UPDATE_IMAGE = "targetshot.azurecr.io/ts-connect:stable"

update_state_manager = UpdateStateManager(UPDATE_STATE_PATH)

_update_state_lock = asyncio.Lock()
_update_job_lock = asyncio.Lock()
_cached_repo_slug: str | None = None
_auto_update_task: asyncio.Task | None = None
_os_update_state_lock = asyncio.Lock()
_os_update_refresh_lock = asyncio.Lock()
_os_update_task: asyncio.Task | None = None
_git_safe_configured = False

_apply_state_lock = asyncio.Lock()
_registry_login_lock = asyncio.Lock()
_logged_in_registries: set[str] = set()


class DeferredApplyError(RuntimeError):
    """Raised when connector apply is deferred for a retry."""


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_apply_state() -> dict:
    return {
        "pending": False,
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
        "Keine Verbindung zur Vereinsdatenbank (MySQL). Bitte Host, Port, VPN oder Firewall prüfen.",
    ),
    (
        ("connection refused",),
        "Die Vereinsdatenbank lehnt Verbindungen ab. Ist der Dienst gestartet und der Port freigegeben?",
    ),
    (
        ("connect timed out", "connection timed out", "timeout"),
        "Zeitüberschreitung bei der Verbindung zur Vereinsdatenbank. Netzwerkpfad prüfen.",
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


def _determine_plan_from_license(
    *,
    variant_id: str | None,
    variant_name: str | None,
    plan_hint: str | None,
) -> str:
    for candidate in (variant_id, variant_name, plan_hint):
        if not candidate:
            continue
        key = str(candidate).strip().lower()
        if not key:
            continue
        mapped = LEMON_VARIANT_PLAN_MAP.get(key) or _DEFAULT_VARIANT_MAP.get(key)
        if mapped:
            return mapped
    for candidate in (plan_hint, variant_name):
        if not candidate:
            continue
        lowered = str(candidate).strip().lower()
        for plan in LICENSE_RETENTION_DAYS:
            if plan in lowered:
                return plan
    return DEFAULT_LICENSE_TIER


def _parse_license_validation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    meta = payload.get("meta") or {}
    data_section = payload.get("data") or {}
    attributes = data_section.get("attributes") or {}
    relationships = data_section.get("relationships") or {}
    valid_flag: bool | None = None
    if isinstance(meta, dict) and "valid" in meta:
        valid_flag = bool(meta.get("valid"))
    if valid_flag is None and "valid" in payload:
        valid_flag = bool(payload.get("valid"))
    status_raw = (meta.get("status") if isinstance(meta, dict) else None) or attributes.get("status")
    status = (status_raw or "").strip() or ("valid" if valid_flag else "")
    variant_id = attributes.get("variant_id") or meta.get("variant_id")
    if not variant_id and isinstance(relationships, dict):
        variant_rel = relationships.get("variant") or {}
        if isinstance(variant_rel, dict):
            variant_data = variant_rel.get("data") or {}
            if isinstance(variant_data, dict):
                variant_id = variant_data.get("id")
    variant_name = meta.get("variant_name") if isinstance(meta, dict) else None
    if not variant_name:
        variant_name = attributes.get("variant_name")
    plan_hint = (
        meta.get("plan") if isinstance(meta, dict) else None
    ) or (meta.get("plan_name") if isinstance(meta, dict) else None) or attributes.get("plan")
    plan = _determine_plan_from_license(
        variant_id=str(variant_id).strip() if variant_id else None,
        variant_name=variant_name,
        plan_hint=plan_hint,
    )
    expires_at = None
    if isinstance(meta, dict):
        expires_at = meta.get("expires_at") or meta.get("renews_at") or meta.get("renewal_at")
    if not expires_at:
        expires_at = attributes.get("expires_at")
    if not expires_at and isinstance(payload, dict):
        license_key_data = payload.get("license_key")
        if isinstance(license_key_data, dict):
            expires_at = license_key_data.get("expires_at")
    customer_email = None
    if isinstance(meta, dict):
        customer_email = meta.get("customer_email") or meta.get("email")
    if not customer_email and isinstance(relationships, dict):
        customer_rel = relationships.get("customer") or {}
        if isinstance(customer_rel, dict):
            customer_data = customer_rel.get("data") or {}
            if isinstance(customer_data, dict):
                customer_attr = customer_data.get("attributes") or {}
                if isinstance(customer_attr, dict):
                    customer_email = customer_attr.get("email")
    error_message = None
    if isinstance(payload, dict):
        if payload.get("error"):
            error_message = str(payload.get("error"))
        elif payload.get("message"):
            error_message = str(payload.get("message"))
        elif payload.get("errors"):
            errors_obj = payload.get("errors")
            if isinstance(errors_obj, list) and errors_obj:
                error_message = str(errors_obj[0])
            elif isinstance(errors_obj, dict):
                error_message = ", ".join(str(v) for v in errors_obj.values())
    message = (meta.get("message") if isinstance(meta, dict) else None) or error_message
    normalized_expiry = _normalize_license_expiry(expires_at)
    valid = bool(valid_flag if valid_flag is not None else status.lower() in {"active", "valid"})
    return {
        "valid": valid,
        "plan": normalize_license_tier(plan),
        "status": status or ("valid" if valid else "unbekannt"),
        "variant_id": str(variant_id).strip() if variant_id else None,
        "variant_name": variant_name,
        "expires_at": normalized_expiry,
        "raw_expires_at": expires_at,
        "customer_email": customer_email,
        "message": message,
        "payload": payload,
    }


async def validate_license_key_remote(license_key: str) -> dict[str, Any]:
    key = (license_key or "").strip()
    if not key:
        raise ValueError("Bitte einen Lizenzschlüssel eingeben.")
    headers = {"Accept": "application/json"}
    if LEMON_LICENSE_API_KEY:
        headers["Authorization"] = f"Bearer {LEMON_LICENSE_API_KEY}"
    payload: dict[str, Any] = {"license_key": key}
    if LEMON_INSTANCE_NAME:
        payload["instance_name"] = LEMON_INSTANCE_NAME
    if LEMON_INSTANCE_ID:
        payload["instance_id"] = LEMON_INSTANCE_ID
    payload["options"] = {"increment_uses_count": False}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(
                LEMON_LICENSE_API_URL or "https://api.lemonsqueezy.com/v1/licenses/validate",
                json=payload,
                headers=headers,
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
    parsed = _parse_license_validation_payload(data if isinstance(data, dict) else {})
    if not parsed.get("message") and isinstance(data, dict):
        parsed["message"] = data.get("message")
    if isinstance(data, dict) and data.get("error"):
        parsed["error"] = str(data.get("error"))
    elif parsed.get("message"):
        parsed["error"] = parsed["message"]
    return parsed


async def activate_license_key_remote(
    license_key: str,
    *,
    instance_name: str,
    instance_id: str,
) -> dict[str, Any]:
    key = (license_key or "").strip()
    if not key:
        raise ValueError("Lizenzschlüssel fehlt für die Aktivierung.")
    instance_id = (instance_id or "").strip()
    if not instance_id:
        raise ValueError("Lizenzaktivierung benötigt eine Instance ID.")
    payload: dict[str, Any] = {
        "license_key": key,
        "instance_id": instance_id,
    }
    if instance_name:
        payload["instance_name"] = instance_name.strip()
    headers = {"Accept": "application/json"}
    if LEMON_LICENSE_API_KEY:
        headers["Authorization"] = f"Bearer {LEMON_LICENSE_API_KEY}"
    url = LEMON_ACTIVATION_URL or "https://api.lemonsqueezy.com/v1/activations"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(url, json=payload, headers=headers)
    except httpx.RequestError as exc:  # noqa: BLE001
        raise RuntimeError(f"Lizenzaktivierung fehlgeschlagen: {exc}") from exc
    if response.status_code >= 400:
        raise RuntimeError(
            f"Lizenzaktivierung fehlgeschlagen ({response.status_code}): {_extract_error_message(response)}"
        )
    try:
        data = response.json()
    except json.JSONDecodeError as exc:
        raise RuntimeError("Lizenzaktivierung lieferte eine ungültige Antwort.") from exc
    result: dict[str, Any] = {"payload": data}
    if isinstance(data, dict):
        payload_data = data.get("data") if isinstance(data.get("data"), dict) else None
        if payload_data:
            attributes = payload_data.get("attributes") if isinstance(payload_data.get("attributes"), dict) else {}
            result.update(
                {
                    "activation_id": payload_data.get("id"),
                    "status": attributes.get("status") or data.get("status"),
                    "activated": bool(attributes.get("activated")),
                    "activated_at": attributes.get("created_at") or attributes.get("activated_at"),
                    "message": attributes.get("message") or data.get("message"),
                }
            )
        else:
            result.update(
                {
                    "activated": bool(data.get("activated")),
                    "activation_id": data.get("id"),
                    "status": data.get("status"),
                    "activated_at": data.get("created_at") or data.get("activated_at"),
                    "message": data.get("message"),
                }
            )
    return result
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
    update_agent_ok = await _ping_update_agent()
    host_agent_required = bool(HOST_AGENT_URL)
    host_agent_ok = True
    if host_agent_required:
        host_agent_ok = await _ping_host_agent()
    overall = git_available and workspace_ready and update_agent_ok
    if host_agent_required:
        overall = overall and host_agent_ok
    return {
        "git": git_available,
        "update_agent": update_agent_ok,
        "workspace": workspace_ready,
        "host_agent": host_agent_ok if host_agent_required else None,
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
_REGISTRY_CONFIG_ACCEPT = "application/vnd.docker.container.image.v1+json"
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
    except json.JSONDecodeError as exc:
        raise RuntimeError("Registry lieferte einen ungültigen Image-Config-Blob") from exc


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
        logger.info("Registry-API für %s konnte nicht gelesen werden (%s), versuche Docker CLI", image_ref, registry_error)
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


async def _build_update_status(force: bool = False) -> dict[str, Any]:
    await ensure_update_state()
    state = await get_update_state_snapshot()
    release = await _ensure_latest_release(force=force)
    workspace_info = await _collect_workspace_info()
    local_image_digest, local_image_ref = await _read_local_ui_image_details()
    connect_version, connect_release = _load_version_defaults()
    prereq = await _detect_prerequisites(workspace_info)
    repo_slug = await _determine_repo_slug()
    compose_env_exists = (WORKSPACE_PATH / "compose.env").exists()
    current_marker = connect_version or workspace_info.get("current_ref") or workspace_info.get("current_commit")
    latest_tag = release.get("tag_name") if isinstance(release, dict) else None
    remote_digest = release.get("digest") if isinstance(release, dict) else None
    update_available = False
    if latest_tag and current_marker and latest_tag != current_marker:
        update_available = True
    elif remote_digest and local_image_digest and remote_digest != local_image_digest:
        update_available = True
    elif remote_digest and not local_image_digest:
        update_available = True
    auto_enabled = bool(state.get("auto_update_enabled"))
    auto_hour = _sanitize_hour(state.get("auto_update_hour"))
    auto_last_run = state.get("auto_update_last_run")
    next_auto_run_iso = None
    auto_last_run_local_display: str | None = None
    if auto_last_run:
        last_run_dt = _parse_iso8601(auto_last_run)
        auto_last_run_local_display = _format_local_timestamp(last_run_dt)
    next_auto_run_local_display: str | None = None
    if auto_enabled:
        next_dt_utc = _calculate_next_auto_run(auto_hour, auto_last_run)
        next_auto_run_iso = next_dt_utc.isoformat().replace("+00:00", "Z")
        next_auto_run_local_display = _format_local_timestamp(next_dt_utc)
    status: dict[str, Any] = {
        "ok": True,
        "status": state.get("status", "idle"),
        "current_action": state.get("current_action"),
        "current_version": connect_version,
        "current_release": connect_release,
        "current_image": local_image_ref,
        "current_image_digest": local_image_digest,
        "latest_release": release,
        "latest_release_digest": remote_digest,
        "update_available": update_available,
        "last_check": state.get("last_check"),
        "last_check_error": state.get("last_check_error"),
        "last_success": state.get("last_success"),
        "last_error": state.get("last_error"),
        "update_target": state.get("update_target"),
        "log": state.get("log", []),
        "workspace": workspace_info,
        "prerequisites": prereq,
        "repo_slug": repo_slug,
        "compose_env": compose_env_exists,
        "job_started": state.get("job_started"),
        "current_image": os.getenv("TS_CONNECT_UI_IMAGE", DEFAULT_UPDATE_IMAGE),
        "auto_update": {
            "enabled": auto_enabled,
            "hour": auto_hour,
            "last_run": auto_last_run,
            "last_run_local": auto_last_run_local_display,
            "next_run": next_auto_run_iso,
            "next_run_local": next_auto_run_local_display,
        },
    }
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


async def _start_update_runner(target_ref: str | None, repo_slug: str | None, compose_env: bool) -> str:
    payload = {
        "target_ref": target_ref,
        "repo_slug": repo_slug,
        "compose_env": compose_env,
        "project_name": PROJECT_NAME,
    }
    result = await _update_agent_request("POST", "/api/v1/run", json_payload=payload, timeout=10)
    return result.get("job_id") or "update-agent"


async def _launch_update_job(
    *,
    target_ref: str | None,
    initiated_by: str,
    force_release_refresh: bool,
    reset_log: bool,
) -> dict[str, Any]:
    label = "Automatisches" if initiated_by == "auto" else "Manuelles"
    async with _update_job_lock:
        state = await get_update_state_snapshot()
        if state.get("status") == "running":
            return {"ok": False, "error": "Ein Update läuft bereits", "code": 409}
        workspace_info = await _collect_workspace_info()
        prereq = await _detect_prerequisites(workspace_info)
        if not prereq.get("ok"):
            missing = [name for name, available in prereq.items() if name != "ok" and not available]
            detail = "Voraussetzungen fehlen: " + ", ".join(missing)
            return {"ok": False, "error": detail, "code": 400}
        release = await _ensure_latest_release(force=force_release_refresh)
        repo_slug = await _determine_repo_slug()
        compose_env_exists = (WORKSPACE_PATH / "compose.env").exists()
        selected_target = target_ref or (release.get("tag_name") if isinstance(release, dict) else None)
        job_started = _now_utc_iso()
        log_lines = [f"{label} Update ausgelöst um {job_started}", f"Initiator: {initiated_by}"]
        await append_update_log(log_lines, reset=reset_log)
        updates: dict[str, Any] = {
            "status": "running",
            "update_in_progress": True,
            "current_action": "Starte Update Runner",
            "last_error": None,
            "job_started": job_started,
            "update_target": selected_target,
        }
        if initiated_by == "auto":
            updates["auto_update_last_run"] = job_started
        await merge_update_state_async(**updates)
        try:
            job_identifier = await _start_update_runner(selected_target, repo_slug, compose_env_exists)
        except Exception as exc:  # noqa: BLE001
            message = _short_error_message(str(exc), 200)
            result_updates: dict[str, Any] = {
                "status": "error",
                "update_in_progress": False,
                "current_action": None,
                "last_error": message,
                "log_append": [f"FEHLER: {message}"],
            }
            if initiated_by == "auto":
                result_updates.setdefault("auto_update_last_run", job_started)
            await merge_update_state_async(**result_updates)
            return {"ok": False, "error": message, "code": 500}
        await merge_update_state_async(
            current_action="Update-Agent gestartet",
            log_append=[f"Update-Agent Job: {job_identifier}"],
        )
        return {"ok": True, "container": job_identifier, "target": selected_target}


async def _auto_update_worker() -> None:
    await asyncio.sleep(15)
    while True:
        try:
            state = await get_update_state_snapshot()
            if state.get("status") == "running":
                if AUTO_UPDATE_STALE_SECONDS > 0:
                    age_seconds = _job_age_seconds(state.get("job_started"))
                    if age_seconds is None or age_seconds >= AUTO_UPDATE_STALE_SECONDS:
                        minutes = None
                        if age_seconds is not None and age_seconds > 0:
                            minutes = max(1, int(age_seconds // 60))
                        suffix = f" (letzte Aktivität vor {minutes} Minuten)" if minutes is not None else ""
                        reset_updates: dict[str, Any] = {
                            "status": "idle",
                            "update_in_progress": False,
                            "current_action": None,
                            "last_error": "Update-Status nach Timeout zurückgesetzt",
                        }
                        if state.get("auto_update_enabled"):
                            reset_updates["auto_update_last_run"] = None
                        await merge_update_state_async(
                            log_append=[f"Update-Status automatisch auf idle gesetzt{suffix}"],
                            **reset_updates,
                        )
                        state = await get_update_state_snapshot()
                    else:
                        await asyncio.sleep(AUTO_UPDATE_CHECK_SECONDS)
                        continue
                else:
                    await asyncio.sleep(AUTO_UPDATE_CHECK_SECONDS)
                    continue
            if state.get("auto_update_enabled"):
                auto_hour = _sanitize_hour(state.get("auto_update_hour"))
                last_run_iso = state.get("auto_update_last_run")
                now_local = _now_local()
                today_run_local = now_local.replace(hour=auto_hour, minute=0, second=0, microsecond=0)
                last_run_dt = _parse_iso8601(last_run_iso)
                last_run_local = _to_local(last_run_dt)
                already_today = last_run_local and last_run_local.date() == now_local.date()
                if now_local >= today_run_local and not already_today:
                    logger.info("Auto-Update: Starte geplanten Lauf für %s", today_run_local.isoformat())
                    result = await _launch_update_job(
                        target_ref=None,
                        initiated_by="auto",
                        force_release_refresh=AUTO_UPDATE_FORCE_RELEASE,
                        reset_log=False,
                    )
                    if not result.get("ok"):
                        await merge_update_state_async(
                            log_append=[
                                "Automatisches Update fehlgeschlagen: "
                                + str(result.get("error")),
                            ],
                            auto_update_last_run=_now_utc_iso(),
                        )
                    await asyncio.sleep(60)
                    continue
            await asyncio.sleep(AUTO_UPDATE_CHECK_SECONDS)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Auto-Update Worker Fehler: %s", exc)
            await asyncio.sleep(max(120, AUTO_UPDATE_CHECK_SECONDS))


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
    if status_code in TRANSIENT_HTTP_CODES:
        return True
    if status_code == 400 and message:
        lowered = message.lower()
        return any(marker in lowered for marker in TRANSIENT_ERROR_MARKERS)
    if status_code == 409:
        return True  # connector already in desired state - treat as non-fatal
    return False


def _is_transient_request_error(exc: httpx.RequestError) -> bool:
    lowered = str(exc).lower()
    return any(marker in lowered for marker in TRANSIENT_ERROR_MARKERS)


async def _schedule_retry(err_msg: str) -> None:
    message = _format_apply_error(err_msg)
    await merge_apply_state(
        pending=True,
        last_error=message,
        last_attempt=_now_utc_iso(),
        next_retry=_next_retry_iso(),
    )
    logger.warning("Connector apply deferred: %s", message)


async def _mark_apply_success() -> None:
    await merge_apply_state(
        pending=False,
        last_error=None,
        last_attempt=_now_utc_iso(),
        last_success=_now_utc_iso(),
        next_retry=None,
    )


async def _connector_retry_worker() -> None:
    if APPLY_RETRY_SECONDS <= 0:
        logger.info("Connector retry worker disabled (APPLY_RETRY_SECONDS=%s)", APPLY_RETRY_SECONDS)
        return
    await asyncio.sleep(APPLY_RETRY_SECONDS)
    while True:
        state = await get_apply_state()
        if state.get("pending"):
            try:
                await apply_connector_config(allow_defer=False)
            except Exception as exc:  # noqa: BLE001
                await merge_apply_state(
                    pending=True,
                    last_error=_format_apply_error(str(exc)),
                    last_attempt=_now_utc_iso(),
                    next_retry=_next_retry_iso(),
                )
                logger.warning("Retrying connector apply failed: %s", exc)
            else:
                logger.info("Deferred connector apply succeeded on retry")
        await asyncio.sleep(APPLY_RETRY_SECONDS)

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")

# --------- Middleware: nur Vereinsnetz zulassen ----------
@app.middleware("http")
async def ip_allowlist(request: Request, call_next):
    client_ip = request.client.host
    ip_obj = ipaddress.ip_address(client_ip)
    allowed = any(ip_obj in ipaddress.ip_network(cidr) for cidr in TRUSTED_CIDRS)
    if not allowed and client_ip != "127.0.0.1":
        return JSONResponse({"detail": "Forbidden (CIDR)"}, status_code=403)
    return await call_next(request)

# --------- DB Helpers ----------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY CHECK (id=1),
        db_host TEXT, db_port INTEGER, db_user TEXT,
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
                confluent_bootstrap, confluent_sasl_username,
                topic_prefix, server_id, server_name,
                offline_buffer_enabled, license_tier, retention_days,
                license_key, license_status, license_valid_until,
                license_last_checked, license_customer_email,
                license_activation_id, license_activated_at,
                backup_pg_host, backup_pg_port, backup_pg_db, backup_pg_user
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                os.getenv("TS_CONNECT_DEFAULT_DB_HOST", "192.168.10.200"),
                3306,
                os.getenv("TS_CONNECT_DEFAULT_DB_USER", "debezium_sync"),
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
            backup_pg_user = COALESCE(NULLIF(TRIM(backup_pg_user), ''), ?)
        WHERE id=1
        """,
        (
            license_value,
            retention_value,
            DEFAULT_BACKUP_HOST,
            DEFAULT_BACKUP_PORT,
            DEFAULT_BACKUP_DB,
            DEFAULT_BACKUP_USER,
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


def _write_mirror_maker_config(settings: dict, secrets: dict) -> None:
    confluent_bootstrap = settings.get("confluent_bootstrap") or secrets.get("confluent_bootstrap") or CONFLUENT_BOOTSTRAP_DEFAULT
    sasl_user = secrets.get("confluent_sasl_username")
    sasl_password = secrets.get("confluent_sasl_password")
    if not (confluent_bootstrap and sasl_user and sasl_password):
        raise RuntimeError("MirrorMaker benötigt gültige Confluent Zugangsdaten (Bootstrap, API Key & Secret).")
    config_lines = [
        "clusters = local,remote",
        "local.bootstrap.servers = redpanda:9092",
        f"remote.bootstrap.servers = {confluent_bootstrap}",
        "local.security.protocol = PLAINTEXT",
        "remote.security.protocol = SASL_SSL",
        "remote.sasl.mechanism = PLAIN",
        (
            "remote.sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{_escape_jaas(sasl_user)}' password='{_escape_jaas(sasl_password)}';"
        ),
        "remote.ssl.endpoint.identification.algorithm = https",
        "local->remote.enabled = true",
        "remote->local.enabled = false",
        f"local->remote.enabled = true",
        f"local->remote.topics = {STREAMS_TARGET_PREFIX}.*",
        "local->remote.groups = _ts.*",
        "local->remote.emit.heartbeats.interval.seconds = 15",
        "offset.storage.topic = _ts_mm2_offsets",
        "offset.storage.partitions = 5",
        "offset.storage.cluster.alias = local",
        "config.storage.topic = _ts_mm2_configs",
        "config.storage.cluster.alias = local",
        "status.storage.topic = _ts_mm2_status",
        "status.storage.partitions = 3",
        "status.storage.cluster.alias = local",
        "checkpoint.topic.replication.factor = 1",
        "heartbeats.topic.replication.factor = 1",
        "offset.syncs.topic.replication.factor = 1",
        "replication.factor = 1",
        "tasks.max = 1",
        "sync.topic.acls.enabled = false",
        "emit.checkpoints.enabled = true",
        "refresh.topics.interval.seconds = 30",
    ]
    _atomic_write_text(
        MM2_CONFIG_PATH,
        "\n".join(config_lines) + "\n",
        mode=stat.S_IRUSR | stat.S_IWUSR,
    )


def _build_backup_sink_config(settings: dict, secrets: dict) -> dict:
    backup_password = secrets.get("backup_pg_password", "")
    if not backup_password:
        raise ValueError("Backup-Datenbank Passwort fehlt in secrets.properties (backup_pg_password).")
    host = settings["backup_pg_host"]
    port = settings["backup_pg_port"]
    database = settings["backup_pg_db"]
    user = settings["backup_pg_user"]
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    return {
        "name": BACKUP_CONNECTOR_NAME,
        "config": {
            "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            "topics.regex": r"ts\.raw\..+",
            "connection.url": jdbc_url,
            "connection.user": user,
            "connection.password": backup_password,
            "dialect.name": "PostgreSqlDatabaseDialect",
            "table.name.format": "buffer_events",
            "auto.create": "false",
            "auto.evolve": "false",
            "insert.mode": "insert",
            "pk.mode": "none",
            "delete.enabled": "false",
            "max.batch.size": "500",
            "max.retries": "6",
            "retry.backoff.ms": "5000",
            "behavior.on.null.values": "ignore",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "transforms": "HoistPayload,AddTopic",
            "transforms.HoistPayload.type": "org.apache.kafka.connect.transforms.HoistField$Value",
            "transforms.HoistPayload.field": "payload",
            "transforms.AddTopic.type": "org.apache.kafka.connect.transforms.InsertField$Value",
            "transforms.AddTopic.topic.field": "topic",
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true",
            "errors.deadletterqueue.topic.name": "_ts_backup_dlq",
            "errors.deadletterqueue.topic.replication.factor": "1",
        },
    }


async def restart_mirror_maker() -> None:
    try:
        result = await _restart_container("ts-mirror-maker")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"MirrorMaker Neustart fehlgeschlagen: {exc}") from exc
    if not result.get("found", True):
        logger.info("MirrorMaker Container ist nicht vorhanden – kein Neustart erforderlich.")


async def update_remote_replication_state(active: bool) -> None:
    try:
        status = await _container_status("ts-mirror-maker")
    except Exception as exc:  # noqa: BLE001
        logger.warning("MirrorMaker Status konnte nicht abgefragt werden: %s", exc)
        return
    has_container = bool(status.get("exists"))
    is_running = bool(status.get("running"))

    if active:
        if not has_container:
            logger.warning("MirrorMaker Container nicht gefunden. Cloud-Replikation kann nicht aktiviert werden.")
            return
        if is_running:
            return
        try:
            await restart_mirror_maker()
        except Exception as exc:  # noqa: BLE001
            logger.error("MirrorMaker konnte nicht gestartet werden: %s", exc)
    else:
        if not has_container or not is_running:
            return
        try:
            await _stop_container("ts-mirror-maker")
        except Exception as exc:  # noqa: BLE001
            logger.warning("MirrorMaker konnte nicht gestoppt werden: %s", exc)


def fetch_settings() -> dict:
    conn = get_db()
    cur = conn.execute(
        """
        SELECT db_host, db_port, db_user,
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
    license_tier = normalize_license_tier(row["license_tier"])
    retention_days = LICENSE_RETENTION_DAYS.get(license_tier, LICENSE_RETENTION_DAYS[DEFAULT_LICENSE_TIER])
    return {
        "db_host": row["db_host"],
        "db_port": row["db_port"],
        "db_user": row["db_user"],
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
        mode=stat.S_IRUSR | stat.S_IWUSR,
        uid=SECRETS_FILE_UID,
        gid=SECRETS_FILE_GID,
    )


def read_secrets_file() -> dict:
    if not SECRETS_PATH.exists():
        return {}
    data: dict[str, str] = {}
    for line in SECRETS_PATH.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


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


def _license_is_active(settings: dict) -> bool:
    key = (settings.get("license_key") or "").strip()
    if not key:
        return False
    status = (settings.get("license_status") or "").strip().lower()
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
        return password, secrets_data
    fallback_candidates = [
        (os.getenv("TS_CONNECT_BACKUP_PASSWORD") or "").strip(),
        (os.getenv("POSTGRES_PASSWORD") or "").strip(),
        "targetshot",
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

    # Rotation fehlgeschlagen – versuche mit vorhandenem Passwort weiterzuarbeiten
    for candidate in fallback_candidates:
        if not candidate:
            continue
        secrets_data["backup_pg_password"] = candidate
        write_secrets_file(secrets_data)
        logger.warning(
            "Backup-Passwort Rotation fehlgeschlagen, nutze vorhandenes Passwort weiter (keine Verbindung zur Rotation möglich)."
        )
        return candidate, secrets_data

    raise RuntimeError("Backup-Passwort konnte nicht initialisiert werden") from last_error


async def ensure_offline_buffer_ready() -> None:
    conn = get_db()
    conn.execute("UPDATE settings SET offline_buffer_enabled=1 WHERE id=1")
    conn.commit()
    conn.close()

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


def _hash_admin_password(plain: str, salt_hex: str | None = None) -> tuple[str, str]:
    if not plain:
        raise ValueError("Admin-Passwort darf nicht leer sein")
    if salt_hex is None:
        salt_bytes = secrets.token_bytes(16)
    else:
        salt_bytes = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt_bytes, 260000)
    return salt_bytes.hex(), digest.hex()


def set_admin_password(new_password: str) -> None:
    salt_hex, hash_hex = _hash_admin_password(new_password)
    _atomic_write_text(
        ADMIN_PASSWORD_FILE,
        f"{salt_hex}:{hash_hex}\n",
        mode=stat.S_IRUSR | stat.S_IWUSR,
    )


def _read_admin_password_record() -> str:
    if not ADMIN_PASSWORD_FILE.exists():
        return ""
    return ADMIN_PASSWORD_FILE.read_text(encoding="utf-8").strip()


def ensure_admin_password_file() -> None:
    if ADMIN_PASSWORD_FILE.exists():
        return
    env_password = _resolve_env_admin_password()
    if env_password:
        set_admin_password(env_password)
        _clear_generated_admin_password_file()
        logger.info("Admin-Passwort aus UI_ADMIN_PASSWORD initialisiert.")
        return
    generated_password = _read_generated_admin_password()
    if not generated_password:
        generated_password = _generate_admin_password()
        _remember_generated_admin_password(generated_password)
    set_admin_password(generated_password)
    logger.warning(
        "Es wurde ein zufälliges Admin-Passwort erzeugt und in %s abgelegt.",
        ADMIN_PASSWORD_GENERATED_FILE,
    )


def verify_admin_password(candidate: str) -> bool:
    ensure_admin_password_file()
    record = _read_admin_password_record()
    if not record:
        return False
    if ":" not in record:
        return secrets.compare_digest(record, candidate)
    salt_hex, hash_hex = record.split(":", 1)
    try:
        _, candidate_hash = _hash_admin_password(candidate, salt_hex=salt_hex)
    except ValueError:
        return False
    return secrets.compare_digest(hash_hex, candidate_hash)


@app.on_event("startup")
async def init_admin_password() -> None:
    ensure_admin_password_file()
    await ensure_offline_buffer_ready()
    current_settings = fetch_settings()
    try:
        if sync_license_key_file(current_settings):
            logger.info("Synchronized license.key with stored license settings")
    except Exception as exc:  # noqa: BLE001
        logger.warning("License file sync failed: %s", exc)
    await update_remote_replication_state(_license_is_active(current_settings))
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
    ok_statuses: tuple[int, ...] = (200, 201, 202, 204),
) -> httpx.Response:
    try:
        resp = await client.request(method, url, json=json_payload)
    except httpx.RequestError as exc:
        if allow_defer and _is_transient_request_error(exc):
            await _schedule_retry(str(exc))
            raise DeferredApplyError(_short_error_message(str(exc))) from exc
        raise RuntimeError(f"{method} {url} fehlgeschlagen: {exc}") from exc

    if resp.status_code not in ok_statuses:
        message = _extract_error_message(resp)
        if allow_defer and _is_transient_status(resp.status_code, message):
            await _schedule_retry(message)
            raise DeferredApplyError(_short_error_message(message))
        raise RuntimeError(
            f"{method} {url} -> {resp.status_code}: {message.strip() or 'Unbekannter Fehler'}"
        )
    return resp


async def _ensure_connector(
    client: httpx.AsyncClient,
    *,
    name: str,
    config: dict,
    allow_defer: bool,
) -> None:
    url_base = f"{CONNECT_BASE_URL}/connectors/{name}"
    resp = await _connect_request(
        client,
        "GET",
        url_base,
        allow_defer=allow_defer,
        ok_statuses=(200, 404),
    )
    if resp.status_code == 200:
        await _connect_request(
            client,
            "PUT",
            f"{url_base}/pause",
            allow_defer=allow_defer,
            ok_statuses=(200, 202, 204, 409),
        )
        await _connect_request(
            client,
            "PUT",
            f"{url_base}/config",
            json_payload=config,
            allow_defer=allow_defer,
        )
        await _connect_request(
            client,
            "PUT",
            f"{url_base}/resume",
            allow_defer=allow_defer,
            ok_statuses=(200, 202, 204, 409),
        )
        return
    await _connect_request(
        client,
        "POST",
        f"{CONNECT_BASE_URL}/connectors",
        json_payload={"name": name, "config": config},
        allow_defer=allow_defer,
        ok_statuses=(200, 201, 202),
    )


async def _delete_connector_if_exists(
    client: httpx.AsyncClient,
    *,
    name: str,
    allow_defer: bool,
) -> None:
    await _connect_request(
        client,
        "DELETE",
        f"{CONNECT_BASE_URL}/connectors/{name}",
        allow_defer=allow_defer,
        ok_statuses=(200, 202, 204, 404),
    )


async def apply_connector_config(*, allow_defer: bool = True) -> None:
    await ensure_offline_buffer_ready()
    settings = fetch_settings()
    secrets = read_secrets_file()
    offline_enabled = True

    required = {
        "db_password": "DB-Passwort",
        "confluent_bootstrap": "Confluent Bootstrap",
        "confluent_sasl_username": "Confluent API Key",
        "confluent_sasl_password": "Confluent API Secret",
    }
    if offline_enabled:
        required["backup_pg_password"] = "Backup-Datenbank Passwort"
    missing = [label for key, label in required.items() if not secrets.get(key)]
    if missing:
        raise ValueError("Es fehlen Werte in secrets.properties: " + ", ".join(missing))

    connectors = [
        {
            "name": DEFAULT_CONNECTOR_NAME,
            "config": build_connector_config(settings, offline_mode=offline_enabled),
        }
    ]
    if offline_enabled:
        connectors.append(_build_backup_sink_config(settings, secrets))

    async with httpx.AsyncClient(timeout=10) as client:
        for connector in connectors:
            await _ensure_connector(
                client,
                name=connector["name"],
                config=connector["config"],
                allow_defer=allow_defer,
            )
        if not offline_enabled:
            await _delete_connector_if_exists(
                client,
                name=BACKUP_CONNECTOR_NAME,
                allow_defer=allow_defer,
            )

    if offline_enabled:
        try:
            _write_mirror_maker_config(settings, secrets)
            await restart_mirror_maker()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"MirrorMaker-Konfiguration fehlgeschlagen: {exc}") from exc
    else:
        if MM2_CONFIG_PATH.exists():
            try:
                MM2_CONFIG_PATH.unlink()
            except OSError:
                pass

    await _mark_apply_success()

# --------- Views ----------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    if not request.session.get("authenticated"):
        return RedirectResponse("/login", status_code=303)
    context = build_index_context(request)
    return templates.TemplateResponse("index.html", context)

def require_admin(pw: str, *, raise_exc: bool = True) -> bool:
    if verify_admin_password(pw):
        return True
    if raise_exc:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return False

def require_session(request: Request):
    if not request.session.get("authenticated"):
        raise HTTPException(status_code=401, detail="Unauthorized")


def build_index_context(request: Request) -> dict:
    data = fetch_settings().copy()
    secrets_data = read_secrets_file()
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
    db_password_saved = bool(secrets_data.get("db_password"))
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
        "key": data.get("license_key", ""),
        "status": data.get("license_status", "unknown"),
        "status_label": license_status_label,
        "plan": data.get("license_tier", DEFAULT_LICENSE_TIER),
        "plan_label": normalize_license_tier(data.get("license_tier")).capitalize(),
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
        "meta": license_meta,
    }

    verein_identifier = str(data.get("topic_prefix") or data.get("server_id") or "").strip()
    data["verein_id"] = verein_identifier

    shooter_stats = _collect_shooter_stats_sync(data, secrets_data)
    shooter_count = shooter_stats.get("count")
    required_plan = required_plan_for_shooter_count(shooter_count)
    plan_ok = plan_allows_shooter_count(license_info["plan"], shooter_count)
    license_info.update(
        {
            "shooter_stats": shooter_stats,
            "shooter_plan_ok": plan_ok,
            "shooter_required_plan": required_plan,
            "shooter_required_plan_label": plan_display_name(required_plan) if required_plan else None,
            "shooter_limit_label": plan_limit_label(license_info["plan"]),
        }
    )

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
        "db_password_placeholder": PASSWORD_PLACEHOLDER if db_password_saved else "",
        "db_password_saved": db_password_saved,
        "default_server_name": DEFAULT_SERVER_NAME,
        "license": license_info,
        "license_activation_enabled": LEMON_ACTIVATION_ENABLED,
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
async def test_db(host: str, port: int, user: str, password: str):
    import pymysql
    secrets_data = read_secrets_file()
    if password == PASSWORD_PLACEHOLDER and secrets_data.get("db_password"):
        password = secrets_data["db_password"]
    try:
        conn = pymysql.connect(host=host, port=port, user=user, password=password,
                               connect_timeout=3, read_timeout=3, write_timeout=3)
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        return {"ok": True, "msg": "DB OK"}
    except Exception as e:
        return {"ok": False, "msg": str(e)}

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


@app.get("/api/backup/export", dependencies=[Depends(require_session)])
async def export_backup():
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
                raise HTTPException(status_code=503, detail=f"Backup-Export fehlgeschlagen: {exc}") from exc
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
        return f"Keine Antwort von der MeytonDB{host_hint}. Netzwerk oder IP prüfen."
    if code == 2005 or "getaddrinfo failed" in lowered or "name or service not known" in lowered:
        return f"MeytonDB-Host{host_hint} nicht gefunden. Adresse kontrollieren."
    if code == 1045 or "access denied" in lowered:
        return "MeytonDB-Anmeldung abgelehnt. Benutzer oder Passwort prüfen."
    if "unknown database" in lowered:
        return "MeytonDB-Datenbank nicht gefunden. Datenbanknamen prüfen."
    return f"MeytonDB-Fehler: {_short_error_message(message, 120)}"


async def _check_database_health() -> dict[str, str]:
    settings = fetch_settings()
    host = settings.get("db_host")
    port = settings.get("db_port")
    user = settings.get("db_user")
    if not host or not user or not port:
        return {"status": "unknown", "message": "Nicht konfiguriert"}
    secrets = read_secrets_file()
    password = secrets.get("db_password")
    if not password:
        return {"status": "warn", "message": "Kein Passwort gespeichert"}

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


async def _check_elastic_agent_health() -> dict[str, str]:
    if not ELASTIC_AGENT_ENABLED:
        return {"status": "skipped", "message": "Deaktiviert"}
    try:
        status = await _container_status("ts-elastic-agent")
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": _short_error_message(str(exc), 140)}
    if not status.get("exists"):
        return {"status": "warn", "message": "Agent nicht gestartet"}
    status_text = (status.get("status") or "").strip()
    if "Up" in status_text:
        return {"status": "ok", "message": status_text}
    if "Exited" in status_text or "Dead" in status_text:
        return {"status": "error", "message": status_text}
    return {"status": "warn", "message": status_text}


async def _check_connector_health() -> dict[str, str]:
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            overview = await client.get(f"{CONNECT_BASE_URL}/connectors")
            if overview.status_code != 200:
                if overview.status_code == 404:
                    return {
                        "status": "error",
                        "message": "Kafka Connect REST (/connectors) antwortet mit 404 – läuft der Connect-Container?",
                    }
                return {
                    "status": "error",
                    "message": _extract_error_message(overview),
                }
            status_resp = await client.get(
                f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/status"
            )
            if status_resp.status_code == 200:
                data = status_resp.json()
                state = (data.get("connector") or {}).get("state")
                if state in {"RUNNING", "UP"}:
                    return {"status": "ok", "message": "Connector läuft"}
                if state:
                    return {"status": "warn", "message": f"Zustand: {state}"}
                return {"status": "warn", "message": "Status unbekannt"}
            if status_resp.status_code == 404:
                return {"status": "warn", "message": "Connector nicht angelegt"}
            return {
                "status": "error",
                "message": _extract_error_message(status_resp),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": _short_error_message(str(exc), 140)}


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
        _clear_generated_admin_password_file()
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
            request.session["flash_message"] = (
                f"Lizenz entfernt. Plan auf {plan.capitalize()} zurückgesetzt."
            )
            if prune_error:
                request.session.setdefault("error_message", prune_error)
            await update_remote_replication_state(False)
            return RedirectResponse("/", status_code=303)

        try:
            validation = await validate_license_key_remote(license_key_value)
        except Exception as exc:  # noqa: BLE001
            request.session["error_message"] = f"Lizenzprüfung fehlgeschlagen: {exc}"
            return RedirectResponse("/", status_code=303)
        write_license_meta(validation)

        plan = validation.get("plan") or DEFAULT_LICENSE_TIER
        if not validation.get("valid"):
            plan = DEFAULT_LICENSE_TIER
        plan = normalize_license_tier(plan)
        retention_days = LICENSE_RETENTION_DAYS.get(plan, DEFAULT_RETENTION_DAYS)
        status = validation.get("status") or ("valid" if validation.get("valid") else "unbekannt")
        expires_at_norm = _normalize_iso8601(validation.get("expires_at") or validation.get("raw_expires_at"))
        last_checked = _now_utc_iso()

        new_activation_id = current_activation_id
        new_activation_at = current_activation_at
        if license_key_value != current_license:
            new_activation_id = ""
            new_activation_at = None

        shooter_stats = await asyncio.to_thread(_collect_shooter_stats_sync, settings, secrets_data)
        shooter_error = shooter_stats.get("error")
        shooter_count = shooter_stats.get("count")
        if shooter_error and shooter_count is None:
            request.session["error_message"] = f"Schützenprüfung fehlgeschlagen: {shooter_error}"
            return RedirectResponse("/", status_code=303)
        if shooter_count is not None and not plan_allows_shooter_count(plan, shooter_count):
            required_plan = required_plan_for_shooter_count(shooter_count)
            required_label = plan_display_name(required_plan)
            limit_label = plan_limit_label(plan)
            request.session["error_message"] = (
                f"Plan {plan.capitalize()} reicht nicht aus (Limit: {limit_label}). "
                f"Für {shooter_count} Schützen wird mindestens der Plan {required_label} benötigt."
            )
            return RedirectResponse("/", status_code=303)

        activation_feedback: dict[str, Any] | None = None
        activation_error: str | None = None
        activation_requested = license_action_value == "activate"
        if activation_requested:
            if not LEMON_ACTIVATION_ENABLED:
                activation_error = "Lizenzaktivierungen sind deaktiviert."
            elif new_activation_id:
                activation_error = "Lizenz ist bereits aktiviert."
            elif not validation.get("valid"):
                activation_error = "Lizenz kann erst nach erfolgreicher Prüfung aktiviert werden."
            else:
                instance_id_value = LEMON_INSTANCE_ID or verein_identifier
                instance_name_value = LEMON_INSTANCE_NAME or verein_identifier or settings.get("server_name") or "ts-connect"
                if not instance_id_value:
                    activation_error = "Keine Instance ID für die Lizenzaktivierung konfiguriert."
                else:
                    try:
                        activation_feedback = await activate_license_key_remote(
                            license_key_value,
                            instance_name=instance_name_value,
                            instance_id=str(instance_id_value),
                        )
                    except Exception as exc:  # noqa: BLE001
                        activation_error = _short_error_message(str(exc), 180)
                if activation_feedback and activation_feedback.get("activated"):
                    new_activation_id = activation_feedback.get("activation_id") or ""
                    new_activation_at = activation_feedback.get("activated_at") or _now_utc_iso()
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

        message_parts = [f"Plan: {plan.capitalize()}"]
        if expires_at_norm:
            message_parts.append(f"gültig bis {expires_at_norm}")
        if new_activation_id:
            message_parts.append("Lizenz aktiviert")
        message = " | ".join(message_parts)

        if validation.get("valid"):
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
        else:
            reason = validation.get("message") or validation.get("error") or "Lizenz ungültig."
            full_reason = f"Lizenz ungültig: {reason} ({message})"
            if prune_error:
                full_reason = f"{full_reason} | {prune_error}"
            request.session["error_message"] = full_reason
        return RedirectResponse("/", status_code=303)

    if section_key == "db":
        if not all([db_host, db_port is not None, db_user]):
            request.session["error_message"] = "Bitte alle MeytonDB-Felder ausfüllen."
            return RedirectResponse("/", status_code=303)

        db_host = db_host.strip()
        db_user = db_user.strip()
        submitted_password = (db_password or "").strip() if db_password is not None else ""
        existing_password = secrets_data.get("db_password", "")
        if submitted_password == PASSWORD_PLACEHOLDER and existing_password:
            db_password_value = existing_password
        else:
            db_password_value = submitted_password
        if not db_password_value:
            request.session["error_message"] = "Bitte das MeytonDB-Passwort angeben."
            return RedirectResponse("/", status_code=303)

        conn = get_db()
        conn.execute(
            "UPDATE settings SET db_host=?, db_port=?, db_user=? WHERE id=1",
            (db_host, db_port, db_user),
        )
        conn.commit()
        conn.close()

        settings = fetch_settings()
        confluent_bootstrap_val = settings["confluent_bootstrap"] or CONFLUENT_BOOTSTRAP_DEFAULT
        confluent_user_val = settings["confluent_sasl_username"] or secrets_data.get("confluent_sasl_username", "")
        confluent_pass_val = secrets_data.get("confluent_sasl_password", "")

        secrets_data.update(
            {
                "db_password": db_password_value,
                "confluent_bootstrap": confluent_bootstrap_val,
                "confluent_sasl_username": confluent_user_val,
                "confluent_sasl_password": confluent_pass_val,
            }
        )
        write_secrets_file(secrets_data)

        try:
            await apply_connector_config()
            request.session["flash_message"] = "MeytonDB-Einstellungen gespeichert & Connector aktualisiert."
        except DeferredApplyError as exc:
            request.session["flash_message"] = (
                "MeytonDB-Einstellungen gespeichert. Connector-Update wird automatisch erneut versucht, "
                "sobald die Vereinsdatenbank erreichbar ist. "
                f"Letzter Fehler: {exc}"
            )
        except ValueError as exc:
            request.session["flash_message"] = (
                "MeytonDB-Einstellungen gespeichert. Connector-Update übersprungen: "
                f"{exc}"
            )
        except Exception as exc:
            request.session["error_message"] = f"Connector-Update fehlgeschlagen: {exc}"
        return RedirectResponse("/", status_code=303)

    if section_key == "offline":
        await ensure_offline_buffer_ready()
        try:
            await apply_connector_config()
        except DeferredApplyError as exc:
            request.session["flash_message"] = (
                "Offline-Puffer wird erneut angewendet, sobald die Vereinsdatenbank erreichbar ist. "
                f"Letzter Fehler: {exc}"
            )
        except Exception as exc:  # noqa: BLE001
            request.session["error_message"] = f"Offline-Puffer Aktualisierung fehlgeschlagen: {exc}"
        else:
            request.session["flash_message"] = "Offline-Puffer ist aktiv und wurde aktualisiert."
        return RedirectResponse("/", status_code=303)

    if section_key == "confluent":
        confluent_sasl_username = (confluent_sasl_username or "").strip()
        confluent_sasl_password = (confluent_sasl_password or "").strip()
        bootstrap_value = (confluent_bootstrap or settings["confluent_bootstrap"] or CONFLUENT_BOOTSTRAP_DEFAULT).strip()

        if not confluent_sasl_username or not confluent_sasl_password:
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
        submitted_password = (db_password or "").strip()
        if submitted_password == PASSWORD_PLACEHOLDER and secrets_data.get("db_password"):
            db_password_val = secrets_data.get("db_password")
        elif submitted_password:
            db_password_val = submitted_password
        else:
            db_password_val = secrets_data.get("db_password")
        if not db_password_val:
            request.session["error_message"] = "Bitte zuerst die MeytonDB-Zugangsdaten speichern (DB-Passwort fehlt)."
            return RedirectResponse("/", status_code=303)

        secrets_data.update(
            {
                "db_password": db_password_val,
                "confluent_bootstrap": settings["confluent_bootstrap"],
                "confluent_sasl_username": confluent_sasl_username,
                "confluent_sasl_password": confluent_sasl_password,
            }
        )
        write_secrets_file(secrets_data)

        try:
            await apply_connector_config()
        except DeferredApplyError as exc:
            request.session["flash_message"] = (
                "Confluent-Einstellungen gespeichert. Connector-Update wird automatisch erneut versucht, "
                "sobald die Vereinsdatenbank erreichbar ist. "
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
        await apply_connector_config()
    except DeferredApplyError as exc:
        return {
            "ok": True,
            "pending": True,
            "message": (
                "Connector-Neuanlage wurde geplant. Sobald die Vereinsdatenbank erreichbar ist, "
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
    result: dict[str, object] = {"applyState": apply_state}
    preset_worker = "pending" if apply_state.get("pending") else None
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            w = await client.get(f"{CONNECT_BASE_URL}/connectors")
            if w.status_code != 200:
                result["worker"] = preset_worker or "unavailable"
                result["error"] = _extract_error_message(w)
                return result
            s = await client.get(f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}/status")
            result["connectorStatus"] = s.json() if s.status_code == 200 else None
            result["worker"] = preset_worker or "ok"
            if preset_worker and apply_state.get("last_error"):
                result.setdefault("error", apply_state.get("last_error"))
            return result
        except Exception as e:
            result["worker"] = preset_worker or "unavailable"
            result["error"] = str(e)
            if preset_worker and apply_state.get("last_error"):
                result.setdefault("error", apply_state.get("last_error"))
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
    database, confluent, connector, backup, license, elastic_agent = await asyncio.gather(
        _check_database_health(),
        _check_confluent_health(),
        _check_connector_health(),
        _check_backup_health(),
        _check_license_health(),
        _check_elastic_agent_health(),
    )
    snapshot = {
        "timestamp": _now_utc_iso(),
        "database": database,
        "confluent": confluent,
        "connector": connector,
        "backup": backup,
        "license": license,
        "elastic_agent": elastic_agent,
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
        "elastic_agent": elastic_agent,
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
    if not HOST_AGENT_URL:
        raise HTTPException(status_code=503, detail="Host-Agent nicht konfiguriert.")


@app.get("/api/host/status", dependencies=[Depends(require_session)])
async def host_status():
    _ensure_host_agent_configured()
    try:
        return await _host_agent_request("GET", "/api/v1/status", timeout=10)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=_short_error_message(str(exc), 200))


@app.post("/api/host/os-refresh", dependencies=[Depends(require_session)])
async def host_os_refresh():
    _ensure_host_agent_configured()
    try:
        return await _host_agent_request("POST", "/api/v1/os/refresh", timeout=5)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=_short_error_message(str(exc), 200))


@app.post("/api/host/os-update", dependencies=[Depends(require_session)])
async def host_os_update(pw: str = Form(...)):
    require_admin(pw)
    _ensure_host_agent_configured()
    try:
        return await _host_agent_request("POST", "/api/v1/os/update", timeout=5)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=_short_error_message(str(exc), 200))


@app.post("/api/host/reboot", dependencies=[Depends(require_session)])
async def host_reboot(pw: str = Form(...), delay: int = Form(HOST_REBOOT_DELAY_SECONDS)):
    require_admin(pw)
    _ensure_host_agent_configured()
    payload = {"delay": max(0, min(int(delay), 3600))}
    try:
        return await _host_agent_request("POST", "/api/v1/reboot", json_payload=payload, timeout=5)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=_short_error_message(str(exc), 200))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=APP_PORT)
