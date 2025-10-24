import asyncio
import json
import logging
import os
import socket
import ssl
import ipaddress
import stat
import sqlite3
import hashlib
import secrets
import shutil
import time
from asyncio.subprocess import PIPE
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
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
from update_state import UpdateStateManager

APP_PORT = int(os.getenv("PORT", "8080"))
CONNECT_BASE_URL = os.getenv("CONNECT_BASE_URL", "http://kafka-connect:8083")
DEFAULT_CONNECTOR_NAME = os.getenv("DEFAULT_CONNECTOR_NAME", "targetshot-debezium")
BACKUP_CONNECTOR_NAME = os.getenv("BACKUP_CONNECTOR_NAME", f"{DEFAULT_CONNECTOR_NAME}-backup-sink")
ADMIN_PASSWORD = os.getenv("UI_ADMIN_PASSWORD", "change-me")
PASSWORD_PLACEHOLDER = "********"
TRUSTED_CIDRS = [c.strip() for c in os.getenv(
    "UI_TRUSTED_CIDRS",
    "192.168.0.0/16,10.0.0.0/8,172.16.0.0/12"
).split(",")]
WORKSPACE_PATH = Path(os.getenv("TS_CONNECT_WORKSPACE", "/workspace"))

LICENSE_RETENTION_DAYS = {
    "basic": 14,
    "plus": 30,
    "pro": 90,
}
DEFAULT_LICENSE_TIER = os.getenv("TS_CONNECT_DEFAULT_LICENSE_TIER", "basic").strip().lower()
if DEFAULT_LICENSE_TIER not in LICENSE_RETENTION_DAYS:
    DEFAULT_LICENSE_TIER = "basic"
DEFAULT_RETENTION_DAYS = LICENSE_RETENTION_DAYS[DEFAULT_LICENSE_TIER]
DEFAULT_BACKUP_HOST = os.getenv("TS_CONNECT_BACKUP_HOST", "backup-db")
DEFAULT_BACKUP_PORT = int(os.getenv("TS_CONNECT_BACKUP_PORT", "5432"))
DEFAULT_BACKUP_DB = os.getenv("TS_CONNECT_BACKUP_DB", "targetshot_backup")
DEFAULT_BACKUP_USER = os.getenv("TS_CONNECT_BACKUP_USER", "targetshot")
LEMON_LICENSE_API_URL = os.getenv(
    "TS_LICENSE_API_URL",
    "https://api.lemonsqueezy.com/v1/licenses/validate",
).strip()
LEMON_LICENSE_API_KEY = os.getenv("TS_LICENSE_API_KEY", "").strip()
LEMON_VARIANT_PLAN_MAP_RAW = os.getenv("TS_LICENSE_VARIANT_PLAN_MAP", "")
LEMON_INSTANCE_NAME = os.getenv("TS_LICENSE_INSTANCE_NAME", "").strip()
LEMON_INSTANCE_ID = os.getenv("TS_LICENSE_INSTANCE_ID", "").strip()


def _normalize_license_tier(value: str | None) -> str:
    if not value:
        return DEFAULT_LICENSE_TIER
    normalized = value.strip().lower()
    if normalized not in LICENSE_RETENTION_DAYS:
        return DEFAULT_LICENSE_TIER
    return normalized


def _retention_for_license(value: str | None) -> int:
    return LICENSE_RETENTION_DAYS.get(_normalize_license_tier(value), DEFAULT_RETENTION_DAYS)


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


def _parse_variant_plan_map(raw: str) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in raw.split(","):
        if not entry.strip():
            continue
        if "=" not in entry:
            continue
        key, value = entry.split("=", 1)
        key = key.strip()
        value = _normalize_license_tier(value)
        if key:
            mapping[key.lower()] = value
    return mapping


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


SESSION_SECRET = os.getenv("UI_SESSION_SECRET", "targetshot-connect-ui-secret")
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

DATA_DIR = Path("/app/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "config.db"
SECRETS_PATH = Path(CONNECT_SECRETS_PATH)
ADMIN_PASSWORD_FILE = DATA_DIR / "admin_password.txt"
APPLY_STATE_PATH = DATA_DIR / "connector_apply_state.json"
MM2_CONFIG_PATH = DATA_DIR / "mm2.properties"
APPLY_RETRY_SECONDS = int(os.getenv("TS_CONNECT_APPLY_RETRY_SECONDS", "60"))
UPDATE_STATE_PATH = DATA_DIR / "update_state.json"
UPDATE_CACHE_SECONDS = int(os.getenv("TS_CONNECT_UPDATE_CACHE_SECONDS", "3600"))
GITHUB_REPO_OVERRIDE = os.getenv("TS_CONNECT_GITHUB_REPO", "").strip()
GITHUB_TOKEN = os.getenv("TS_CONNECT_GITHUB_TOKEN", "").strip()
UPDATE_RUNNER_IMAGE = os.getenv("TS_CONNECT_UPDATE_IMAGE", "").strip()
UPDATE_CONTAINER_PREFIX = os.getenv("TS_CONNECT_UPDATE_CONTAINER_PREFIX", "ts-connect-update")
DOCKER_SOCKET_PATH = Path(os.getenv("TS_CONNECT_DOCKER_SOCKET", "/var/run/docker.sock"))
AUTO_UPDATE_DEFAULT_HOUR = int(os.getenv("TS_CONNECT_AUTO_UPDATE_HOUR", "1"))
AUTO_UPDATE_CHECK_SECONDS = int(os.getenv("TS_CONNECT_AUTO_UPDATE_POLL_SECONDS", "60"))
AUTO_UPDATE_FORCE_RELEASE = os.getenv("TS_CONNECT_AUTO_UPDATE_FORCE_RELEASE", "1").lower() in {"1", "true", "yes", "on"}
PROJECT_NAME = os.getenv("COMPOSE_PROJECT_NAME", "ts-connect")

update_state_manager = UpdateStateManager(UPDATE_STATE_PATH)

_update_state_lock = asyncio.Lock()
_update_job_lock = asyncio.Lock()
_cached_repo_slug: str | None = None
_auto_update_task: asyncio.Task | None = None
_git_safe_configured = False

logger = logging.getLogger("ts-connect-ui")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

_apply_state_lock = asyncio.Lock()


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
        APPLY_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False) + "\n", encoding="utf-8")
        return state


def _next_retry_iso() -> str | None:
    if APPLY_RETRY_SECONDS <= 0:
        return None
    return (datetime.utcnow() + timedelta(seconds=APPLY_RETRY_SECONDS)).replace(microsecond=0).isoformat() + "Z"


def _short_error_message(raw: str, max_len: int = 180) -> str:
    if not raw:
        return ""
    text = raw.strip().splitlines()[0]
    if len(text) > max_len:
        return text[: max_len - 1] + "…"
    return text


def _parse_iso8601(timestamp: str | None) -> datetime | None:
    if not timestamp:
        return None
    try:
        if timestamp.endswith("Z"):
            timestamp = timestamp[:-1] + "+00:00"
        return datetime.fromisoformat(timestamp)
    except ValueError:
        return None


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
        mapped = LEMON_VARIANT_PLAN_MAP.get(key)
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
    normalized_expiry = _normalize_iso8601(expires_at)
    valid = bool(valid_flag if valid_flag is not None else status.lower() in {"active", "valid"})
    return {
        "valid": valid,
        "plan": _normalize_license_tier(plan),
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


def _calculate_next_auto_run(hour: int, last_run_iso: str | None) -> datetime:
    hour = _sanitize_hour(hour)
    now = datetime.now(timezone.utc)
    base = now
    if last_run_iso:
        last_dt = _parse_iso8601(last_run_iso)
        if last_dt and last_dt > base:
            base = last_dt
    candidate = base.replace(hour=hour, minute=0, second=0, microsecond=0)
    if candidate <= base:
        candidate += timedelta(days=1)
    return candidate


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


def _detect_prerequisites(workspace_info: dict[str, Any]) -> dict[str, Any]:
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
    docker_available = _binary_found("docker", ("/usr/bin/docker", "/usr/local/bin/docker"))
    docker_socket = DOCKER_SOCKET_PATH.exists()
    workspace_ready = bool(workspace_info.get("exists") and workspace_info.get("git"))
    return {
        "git": git_available,
        "docker": docker_available,
        "docker_socket": docker_socket,
        "workspace": workspace_ready,
        "ok": git_available and docker_available and docker_socket and workspace_ready,
    }


async def _inspect_container_mounts() -> tuple[dict[str, str], str | None]:
    if shutil.which("docker") is None:
        return {}, None
    container_id = os.getenv("HOSTNAME")
    if not container_id:
        return {}, None
    try:
        code, stdout, _ = await _run_command_capture(["docker", "inspect", container_id])
    except FileNotFoundError:
        return {}, None
    if code != 0:
        return {}, None
    try:
        data = json.loads(stdout)
    except json.JSONDecodeError:
        return {}, None
    if not data:
        return {}, None
    first = data[0]
    mounts: dict[str, str] = {}
    for mount in first.get("Mounts", []):
        dest = mount.get("Destination")
        source = mount.get("Source")
        if dest and source:
            mounts[dest] = source
    image = first.get("Config", {}).get("Image")
    return mounts, image


async def _fetch_latest_release(repo_slug: str) -> dict[str, Any] | None:
    if not repo_slug:
        return None
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    url = f"https://api.github.com/repos/{repo_slug}/releases/latest"
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(url, headers=headers)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    payload = response.json()
    return {
        "tag_name": payload.get("tag_name"),
        "name": payload.get("name") or payload.get("tag_name"),
        "published_at": payload.get("published_at"),
        "html_url": payload.get("html_url"),
    }


async def _ensure_latest_release(force: bool = False) -> dict[str, Any] | None:
    await ensure_update_state()
    state = await get_update_state_snapshot()
    latest_release = state.get("latest_release") if isinstance(state.get("latest_release"), dict) else None
    last_check = _parse_iso8601(state.get("last_check"))
    if latest_release and last_check and not force:
        age = datetime.now(timezone.utc) - last_check
        if age.total_seconds() < UPDATE_CACHE_SECONDS:
            return latest_release
    repo_slug = await _determine_repo_slug(force=force)
    if not repo_slug:
        await merge_update_state_async(last_check=_now_utc_iso(), last_check_error="Repository konnte nicht bestimmt werden")
        return latest_release
    try:
        release = await _fetch_latest_release(repo_slug)
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
    connect_version, connect_release = _load_version_defaults()
    prereq = _detect_prerequisites(workspace_info)
    repo_slug = await _determine_repo_slug()
    compose_env_exists = (WORKSPACE_PATH / "compose.env").exists()
    current_marker = connect_version or workspace_info.get("current_ref") or workspace_info.get("current_commit")
    latest_tag = release.get("tag_name") if isinstance(release, dict) else None
    update_available = bool(latest_tag and current_marker and latest_tag != current_marker)
    auto_enabled = bool(state.get("auto_update_enabled"))
    auto_hour = _sanitize_hour(state.get("auto_update_hour"))
    auto_last_run = state.get("auto_update_last_run")
    next_auto_run_iso = None
    if auto_enabled:
        next_auto_run_iso = _calculate_next_auto_run(auto_hour, auto_last_run).isoformat().replace("+00:00", "Z")
    status: dict[str, Any] = {
        "ok": True,
        "status": state.get("status", "idle"),
        "current_action": state.get("current_action"),
        "current_version": connect_version,
        "current_release": connect_release,
        "latest_release": release,
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
        "auto_update": {
            "enabled": auto_enabled,
            "hour": auto_hour,
            "last_run": auto_last_run,
            "next_run": next_auto_run_iso,
        },
    }
    return status


async def _start_update_runner(target_ref: str | None, repo_slug: str | None, compose_env: bool) -> str:
    mounts, image = await _inspect_container_mounts()
    workspace_host = mounts.get("/workspace") or mounts.get(str(WORKSPACE_PATH))
    data_host = mounts.get("/app/data")
    if not workspace_host or not data_host:
        raise RuntimeError("Update benötigt Bind-Mounts für /workspace und /app/data")
    runner_image = UPDATE_RUNNER_IMAGE or image
    if not runner_image:
        raise RuntimeError("Update-Runner Image konnte nicht bestimmt werden")
    socket_path = str(DOCKER_SOCKET_PATH)
    if not os.path.exists(socket_path):
        raise RuntimeError(f"Docker Socket nicht gefunden: {socket_path}")
    container_name = f"{UPDATE_CONTAINER_PREFIX}-{int(time.time())}"
    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-v",
        f"{workspace_host}:/workspace",
        "-v",
        f"{data_host}:/app/data",
        "-v",
        f"{socket_path}:{socket_path}",
        "-e",
        "TS_CONNECT_WORKSPACE=/workspace",
        "-e",
        "TS_CONNECT_DATA_DIR=/app/data",
    ]
    if PROJECT_NAME:
        cmd += ["-e", f"COMPOSE_PROJECT_NAME={PROJECT_NAME}"]
    if repo_slug:
        cmd += ["-e", f"TS_CONNECT_GITHUB_REPO={repo_slug}"]
    if target_ref:
        cmd += ["-e", f"TS_CONNECT_UPDATE_REF={target_ref}"]
    if compose_env:
        cmd += ["-e", "TS_CONNECT_UPDATE_COMPOSE_ENV=compose.env"]
    cmd += [runner_image, "python", "-m", "update_runner"]
    code, stdout, stderr = await _run_command_capture(cmd)
    if code != 0:
        message = stderr.strip() or stdout.strip() or "docker run Fehler"
        raise RuntimeError(message)
    return stdout.strip()


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
        prereq = _detect_prerequisites(workspace_info)
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
            container_id = await _start_update_runner(selected_target, repo_slug, compose_env_exists)
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
            try:
                await _run_command_capture(["docker", "rm", "-f", "ts-kafka-connect", "ts-connect-ui"], cwd=WORKSPACE_PATH)
            except Exception:  # noqa: BLE001
                pass
            return {"ok": False, "error": message, "code": 500}
        await merge_update_state_async(
            current_action="Update-Runner läuft",
            log_append=[f"Runner Container: {container_id}"],
        )
        return {"ok": True, "container": container_id, "target": selected_target}


async def _auto_update_worker() -> None:
    await asyncio.sleep(15)
    while True:
        try:
            state = await get_update_state_snapshot()
            if state.get("auto_update_enabled"):
                auto_hour = _sanitize_hour(state.get("auto_update_hour"))
                last_run_iso = state.get("auto_update_last_run")
                now = datetime.now(timezone.utc)
                today_run = now.replace(hour=auto_hour, minute=0, second=0, microsecond=0)
                last_run_dt = _parse_iso8601(last_run_iso)
                already_today = last_run_dt and last_run_dt.date() == now.date()
                if now >= today_run and not already_today:
                    logger.info("Auto-Update: Starte geplanten Lauf für %s", today_run.isoformat())
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
    message = _short_error_message(err_msg, 300)
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
                    last_error=_short_error_message(str(exc), 300),
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
                backup_pg_host, backup_pg_port, backup_pg_db, backup_pg_user
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                os.getenv("TS_CONNECT_DEFAULT_SERVER_NAME", "targetshot-mysql"),
                1,
                DEFAULT_LICENSE_TIER,
                DEFAULT_RETENTION_DAYS,
                "",
                "unknown",
                None,
                None,
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
    add_column("backup_pg_host", "backup_pg_host TEXT")
    add_column("backup_pg_port", "backup_pg_port INTEGER")
    add_column("backup_pg_db", "backup_pg_db TEXT")
    add_column("backup_pg_user", "backup_pg_user TEXT")
    conn.commit()

    cur = conn.execute("SELECT COUNT(*) as cnt FROM settings")
    if cur.fetchone()["cnt"] == 0:
        return
    row = conn.execute("SELECT license_tier, retention_days FROM settings WHERE id=1").fetchone()
    license_value = _normalize_license_tier(row["license_tier"] if row else None)
    retention_value = row["retention_days"] if row and row["retention_days"] else LICENSE_RETENTION_DAYS[license_value]
    conn.execute(
        """
        UPDATE settings
        SET
            offline_buffer_enabled = 1,
            license_tier = ?,
            retention_days = ?,
            license_key = COALESCE(license_key, ''),
            license_status = COALESCE(NULLIF(TRIM(license_status), ''), 'unknown'),
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
        "local->remote.topics = ts.raw.*",
        "local->remote.groups = _ts.*",
        "local->remote.emit.heartbeats.interval.seconds = 15",
        "offset.storage.topic = _ts_mm2_offsets",
        "config.storage.topic = _ts_mm2_configs",
        "status.storage.topic = _ts_mm2_status",
        "checkpoint.topic.replication.factor = 1",
        "heartbeats.topic.replication.factor = 1",
        "offset.syncs.topic.replication.factor = 1",
        "replication.factor = 1",
        "tasks.max = 1",
        "sync.topic.acls.enabled = false",
        "emit.checkpoints.enabled = true",
        "refresh.topics.interval.seconds = 30",
    ]
    MM2_CONFIG_PATH.write_text("\n".join(config_lines) + "\n", encoding="utf-8")


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
    code, output = await _run_command_text(
        ["docker", "ps", "-aq", "--filter", "name=ts-mirror-maker"],
        timeout=10,
    )
    if code != 0:
        raise RuntimeError(f"docker ps für mirror-maker fehlgeschlagen: {output}")
    if not output.strip():
        # Container existiert nicht - vermutlich offline Buffer nicht aktiviert.
        return
    code, restart_out = await _run_command_text(
        ["docker", "restart", "ts-mirror-maker"],
        timeout=30,
    )
    if code != 0:
        raise RuntimeError(f"docker restart ts-mirror-maker fehlgeschlagen: {restart_out}")


def fetch_settings() -> dict:
    conn = get_db()
    cur = conn.execute(
        """
        SELECT db_host, db_port, db_user,
               confluent_bootstrap, confluent_sasl_username,
               topic_prefix, server_id, server_name,
               offline_buffer_enabled, license_tier, retention_days,
               license_key, license_status, license_valid_until, license_last_checked, license_customer_email,
               backup_pg_host, backup_pg_port, backup_pg_db, backup_pg_user
        FROM settings WHERE id=1
        """
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        raise RuntimeError("settings row missing")
    license_tier = _normalize_license_tier(row["license_tier"])
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
        "backup_pg_host": row["backup_pg_host"] or DEFAULT_BACKUP_HOST,
        "backup_pg_port": row["backup_pg_port"] or DEFAULT_BACKUP_PORT,
        "backup_pg_db": row["backup_pg_db"] or DEFAULT_BACKUP_DB,
        "backup_pg_user": row["backup_pg_user"] or DEFAULT_BACKUP_USER,
    }


def write_secrets_file(values: dict[str, str]) -> None:
    if not values:
        if SECRETS_PATH.exists():
            SECRETS_PATH.unlink()
        return
    lines = [f"{key}={value}" for key, value in sorted(values.items()) if value is not None]
    SECRETS_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(SECRETS_PATH, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)  # 0644 allows read across containers


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
    ADMIN_PASSWORD_FILE.write_text(f"{salt_hex}:{hash_hex}\n", encoding="utf-8")
    os.chmod(ADMIN_PASSWORD_FILE, stat.S_IRUSR | stat.S_IWUSR)


def _read_admin_password_record() -> str:
    if not ADMIN_PASSWORD_FILE.exists():
        return ""
    return ADMIN_PASSWORD_FILE.read_text(encoding="utf-8").strip()


def ensure_admin_password_file() -> None:
    if ADMIN_PASSWORD_FILE.exists():
        return
    set_admin_password(ADMIN_PASSWORD)


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
    try:
        payload = resp.json()
    except Exception:  # noqa: BLE001
        return resp.text
    if isinstance(payload, dict):
        for key in ("message", "error", "detail", "trace"):
            value = payload.get(key)
            if value:
                return str(value)
    return resp.text


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
        license_valid_display = license_valid_dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
        license_days_remaining = (license_valid_dt.date() - datetime.now(timezone.utc).date()).days
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
    license_info = {
        "key": data.get("license_key", ""),
        "status": data.get("license_status", "unknown"),
        "status_label": license_status_label,
        "plan": data.get("license_tier", DEFAULT_LICENSE_TIER),
        "plan_label": _normalize_license_tier(data.get("license_tier")).capitalize(),
        "valid_until": license_valid_iso,
        "valid_until_display": license_valid_display,
        "days_remaining": license_days_remaining,
        "last_checked": data.get("license_last_checked"),
        "customer_email": data.get("license_customer_email"),
        "status_raw": status_raw,
    }

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
        "license": license_info,
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
            with ctx.wrap_socket(sock, server_hostname=host) as ssock:
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
                    connection.autocommit = True
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
        return {"status": "error", "message": _short_error_message(str(exc), 140)}


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
    try:
        code, status_output = await _run_command_text(
            ["docker", "ps", "--filter", "name=ts-elastic-agent", "--format", "{{.Status}}"],
            timeout=5,
        )
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": _short_error_message(str(exc), 140)}
    if code != 0 or not status_output.strip():
        return {"status": "warn", "message": "Agent nicht gestartet"}
    status_text = status_output.strip()
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
    license_key: str | None = Form(default=None),
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
        request.session["flash_message"] = "Admin-Passwort aktualisiert."
        return RedirectResponse("/", status_code=303)

    if section_key == "license":
        license_key_value = (license_key or "").strip()
        prune_error: str | None = None
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
                    license_last_checked=?, license_customer_email=NULL
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
            return RedirectResponse("/", status_code=303)
        try:
            validation = await validate_license_key_remote(license_key_value)
        except Exception as exc:  # noqa: BLE001
            request.session["error_message"] = f"Lizenzprüfung fehlgeschlagen: {exc}"
            return RedirectResponse("/", status_code=303)
        plan = validation.get("plan") or DEFAULT_LICENSE_TIER
        if not validation.get("valid"):
            plan = DEFAULT_LICENSE_TIER
        plan = _normalize_license_tier(plan)
        retention_days = LICENSE_RETENTION_DAYS.get(plan, DEFAULT_RETENTION_DAYS)
        status = validation.get("status") or ("valid" if validation.get("valid") else "unbekannt")
        expires_at_norm = _normalize_iso8601(validation.get("expires_at") or validation.get("raw_expires_at"))
        last_checked = _now_utc_iso()
        conn = get_db()
        conn.execute(
            """
            UPDATE settings
            SET license_key=?, license_tier=?, retention_days=?, license_status=?, license_valid_until=?, license_last_checked=?, license_customer_email=?
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
            ),
        )
        conn.commit()
        conn.close()

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

        message_parts = [f"Plan: {plan.capitalize()}"]
        if expires_at_norm:
            message_parts.append(f"gültig bis {expires_at_norm}")
        message = " | ".join(message_parts)
        if validation.get("valid"):
            request.session["flash_message"] = f"Lizenz erfolgreich geprüft. {message}"
            if prune_error:
                request.session["error_message"] = prune_error
        else:
            reason = validation.get("message") or validation.get("error") or "Lizenz ungültig."
            full_reason = f"Lizenz ungültig: {reason} ({message})"
            if prune_error:
                full_reason = f"{full_reason} | {prune_error}"
            request.session["error_message"] = full_reason
        return RedirectResponse("/", status_code=303)

    if section_key == "db":
        if not all([db_host, db_port is not None, db_user]):
            request.session["error_message"] = "Bitte alle MariaDB-Felder ausfüllen."
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
            request.session["error_message"] = "Bitte das MariaDB-Passwort angeben."
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
            request.session["flash_message"] = "MariaDB-Einstellungen gespeichert & Connector aktualisiert."
        except DeferredApplyError as exc:
            request.session["flash_message"] = (
                "MariaDB-Einstellungen gespeichert. Connector-Update wird automatisch erneut versucht, "
                "sobald die Vereinsdatenbank erreichbar ist. "
                f"Letzter Fehler: {exc}"
            )
        except ValueError as exc:
            request.session["flash_message"] = (
                "MariaDB-Einstellungen gespeichert. Connector-Update übersprungen: "
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
        topic_prefix = (topic_prefix or "").strip()
        server_name = (server_name or "").strip()
        bootstrap_value = (confluent_bootstrap or settings["confluent_bootstrap"] or CONFLUENT_BOOTSTRAP_DEFAULT).strip()

        if not confluent_sasl_username or not confluent_sasl_password:
            request.session["error_message"] = "Bitte API Key und Secret für Confluent ausfüllen."
            return RedirectResponse("/", status_code=303)

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
                topic_prefix,
                server_id if server_id is not None else settings["server_id"],
                server_name or settings["server_name"],
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
            request.session["error_message"] = "Bitte zuerst die MariaDB-Zugangsdaten speichern (DB-Passwort fehlt)."
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

# --------- Connector Control ----------
# --------- Connector Control ----------
@app.post("/api/connector/control/{action}", dependencies=[Depends(require_session)])
async def connector_control(action: str, pw: str = Form(...)):
    require_admin(pw)
    valid = {"pause", "resume", "restart"}
    if action not in valid: raise HTTPException(400, "invalid action")
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=APP_PORT)
