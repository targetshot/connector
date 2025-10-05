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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
import httpx
from connector_config import CONNECT_SECRETS_PATH, build_connector_config
from update_state import UpdateStateManager

APP_PORT = int(os.getenv("PORT", "8080"))
CONNECT_BASE_URL = os.getenv("CONNECT_BASE_URL", "http://kafka-connect:8083")
DEFAULT_CONNECTOR_NAME = os.getenv("DEFAULT_CONNECTOR_NAME", "targetshot-debezium")
ADMIN_PASSWORD = os.getenv("UI_ADMIN_PASSWORD", "change-me")
PASSWORD_PLACEHOLDER = "********"
TRUSTED_CIDRS = [c.strip() for c in os.getenv(
    "UI_TRUSTED_CIDRS",
    "192.168.0.0/16,10.0.0.0/8,172.16.0.0/12"
).split(",")]
CONNECT_VERSION = os.getenv("TS_CONNECT_VERSION", "v0.2.17-beta")
CONNECT_RELEASE = os.getenv("TS_CONNECT_RELEASE", "Beta")
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
DB_PATH = DATA_DIR / "config.db"
SECRETS_PATH = Path(CONNECT_SECRETS_PATH)
ADMIN_PASSWORD_FILE = DATA_DIR / "admin_password.txt"
APPLY_STATE_PATH = DATA_DIR / "connector_apply_state.json"
APPLY_RETRY_SECONDS = int(os.getenv("TS_CONNECT_APPLY_RETRY_SECONDS", "60"))
UPDATE_STATE_PATH = DATA_DIR / "update_state.json"
WORKSPACE_PATH = Path(os.getenv("TS_CONNECT_WORKSPACE", "/workspace"))
UPDATE_CACHE_SECONDS = int(os.getenv("TS_CONNECT_UPDATE_CACHE_SECONDS", "3600"))
GITHUB_REPO_OVERRIDE = os.getenv("TS_CONNECT_GITHUB_REPO", "").strip()
GITHUB_TOKEN = os.getenv("TS_CONNECT_GITHUB_TOKEN", "").strip()
UPDATE_RUNNER_IMAGE = os.getenv("TS_CONNECT_UPDATE_IMAGE", "").strip()
UPDATE_CONTAINER_PREFIX = os.getenv("TS_CONNECT_UPDATE_CONTAINER_PREFIX", "ts-connect-update")
DOCKER_SOCKET_PATH = Path(os.getenv("TS_CONNECT_DOCKER_SOCKET", "/var/run/docker.sock"))

update_state_manager = UpdateStateManager(UPDATE_STATE_PATH)

_update_state_lock = asyncio.Lock()
_update_job_lock = asyncio.Lock()
_cached_repo_slug: str | None = None

logger = logging.getLogger("ts-connect-ui")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

_apply_state_lock = asyncio.Lock()


class DeferredApplyError(RuntimeError):
    """Raised when connector apply is deferred for a retry."""


def _now_utc_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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


async def _determine_repo_slug(force: bool = False) -> str | None:
    global _cached_repo_slug
    if GITHUB_REPO_OVERRIDE:
        _cached_repo_slug = GITHUB_REPO_OVERRIDE
        return _cached_repo_slug or None
    if _cached_repo_slug and not force:
        return _cached_repo_slug
    if not WORKSPACE_PATH.exists() or not (WORKSPACE_PATH / ".git").exists():
        return None
    code, remote = await _run_command_text(["git", "remote", "get-url", "origin"], cwd=WORKSPACE_PATH)
    if code != 0 or not remote:
        return None
    slug = _parse_repo_slug(remote)
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
        if shutil.which(name):
            return True
        for path in extra_paths:
            if Path(path).exists():
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
        age = datetime.utcnow() - last_check
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
    prereq = _detect_prerequisites(workspace_info)
    repo_slug = await _determine_repo_slug()
    compose_env_exists = (WORKSPACE_PATH / "compose.env").exists()
    current_marker = CONNECT_VERSION or workspace_info.get("current_ref") or workspace_info.get("current_commit")
    latest_tag = release.get("tag_name") if isinstance(release, dict) else None
    update_available = bool(latest_tag and current_marker and latest_tag != current_marker)
    status: dict[str, Any] = {
        "ok": True,
        "status": state.get("status", "idle"),
        "current_action": state.get("current_action"),
        "current_version": CONNECT_VERSION,
        "current_release": CONNECT_RELEASE,
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
    conn.execute("""CREATE TABLE IF NOT EXISTS settings (
        id INTEGER PRIMARY KEY CHECK (id=1),
        db_host TEXT, db_port INTEGER, db_user TEXT,
        confluent_bootstrap TEXT, confluent_sasl_username TEXT,
        topic_prefix TEXT, server_id INTEGER, server_name TEXT
    )""")
    cur = conn.execute("SELECT COUNT(*) FROM settings")
    if cur.fetchone()[0] == 0:
        conn.execute(
            """
            INSERT INTO settings(
                id, db_host, db_port, db_user,
                confluent_bootstrap, confluent_sasl_username,
                topic_prefix, server_id, server_name
            )
            VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                os.getenv("TS_CONNECT_DEFAULT_DB_HOST", "192.168.10.200"),
                3306,
                os.getenv("TS_CONNECT_DEFAULT_DB_USER", "debezium_sync"),
                CONFLUENT_BOOTSTRAP_DEFAULT,
                "YOUR-API-KEY",
                os.getenv("TS_CONNECT_DEFAULT_TOPIC_PREFIX", "413067"),
                int(os.getenv("TS_CONNECT_DEFAULT_SERVER_ID", "413067")),
                os.getenv("TS_CONNECT_DEFAULT_SERVER_NAME", "targetshot-mysql"),
            ),
        )
        conn.commit()
    return conn


def fetch_settings() -> dict:
    conn = get_db()
    cur = conn.execute(
        """
        SELECT db_host, db_port, db_user,
               confluent_bootstrap, confluent_sasl_username,
               topic_prefix, server_id, server_name
        FROM settings WHERE id=1
        """
    )
    row = cur.fetchone()
    conn.close()
    return {
        "db_host": row[0],
        "db_port": row[1],
        "db_user": row[2],
        "confluent_bootstrap": row[3],
        "confluent_sasl_username": row[4],
        "topic_prefix": row[5],
        "server_id": row[6],
        "server_name": row[7],
    }

def write_secrets_file(db_password: str, confluent_bootstrap: str,
                       confluent_user: str, confluent_pass: str):
    lines = [
        f"db_password={db_password}",
        f"confluent_bootstrap={confluent_bootstrap}",
        f"confluent_sasl_username={confluent_user}",
        f"confluent_sasl_password={confluent_pass}"
    ]
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
    await ensure_update_state()
    asyncio.create_task(_connector_retry_worker())


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


async def apply_connector_config(*, allow_defer: bool = True) -> None:
    settings = fetch_settings()
    secrets = read_secrets_file()

    required = {
        "db_password": "DB-Passwort",
        "confluent_bootstrap": "Confluent Bootstrap",
        "confluent_sasl_username": "Confluent API Key",
        "confluent_sasl_password": "Confluent API Secret",
    }
    missing = [label for key, label in required.items() if not secrets.get(key)]
    if missing:
        raise ValueError("Es fehlen Werte in secrets.properties: " + ", ".join(missing))

    connector_cfg = {
        "name": DEFAULT_CONNECTOR_NAME,
        "config": build_connector_config(settings),
    }

    async with httpx.AsyncClient(timeout=10) as client:
        url_base = f"{CONNECT_BASE_URL}/connectors/{DEFAULT_CONNECTOR_NAME}"
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
                json_payload=connector_cfg["config"],
                allow_defer=allow_defer,
            )
            await _connect_request(
                client,
                "PUT",
                f"{url_base}/resume",
                allow_defer=allow_defer,
                ok_statuses=(200, 202, 204, 409),
            )
        else:
            await _connect_request(
                client,
                "POST",
                f"{CONNECT_BASE_URL}/connectors",
                json_payload=connector_cfg,
                allow_defer=allow_defer,
                ok_statuses=(200, 201, 202),
            )

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

    return {
        "request": request,
        "data": data,
        "has_secrets": has_secrets,
        "connect_version": CONNECT_VERSION,
        "connect_release": CONNECT_RELEASE,
        "confluent_cluster_url": CONFLUENT_CLUSTER_URL,
        "confluent_cluster_id": CONFLUENT_CLUSTER_ID,
        "flash_message": flash_message,
        "error_message": error_message,
        "docs_url": DOCS_URL,
        "db_password_placeholder": PASSWORD_PLACEHOLDER if db_password_saved else "",
        "db_password_saved": db_password_saved,
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

        write_secrets_file(db_password_value, confluent_bootstrap_val, confluent_user_val, confluent_pass_val)

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

        write_secrets_file(
            db_password_val,
            settings["confluent_bootstrap"],
            confluent_sasl_username,
            confluent_sasl_password,
        )

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


@app.post("/api/update/run", dependencies=[Depends(require_session)])
async def trigger_update(pw: str = Form(...), target: str | None = Form(None)):
    require_admin(pw)
    async with _update_job_lock:
        state = await get_update_state_snapshot()
        if state.get("status") == "running":
            raise HTTPException(status_code=409, detail="Ein Update läuft bereits")
        workspace_info = await _collect_workspace_info()
        prereq = _detect_prerequisites(workspace_info)
        if not prereq.get("ok"):
            missing = [name for name, enabled in prereq.items() if name != "ok" and not enabled]
            detail = "Voraussetzungen fehlen: " + ", ".join(missing)
            raise HTTPException(status_code=400, detail=detail)
        release = await _ensure_latest_release(force=not target)
        repo_slug = await _determine_repo_slug()
        compose_env_exists = (WORKSPACE_PATH / "compose.env").exists()
        target_ref = target or (release.get("tag_name") if isinstance(release, dict) else None)
        job_started = _now_utc_iso()
        await append_update_log([f"Update ausgelöst um {job_started}"], reset=True)
        await merge_update_state_async(
            status="running",
            update_in_progress=True,
            current_action="Starte Update Runner",
            last_error=None,
            job_started=job_started,
            update_target=target_ref,
        )
        try:
            container_id = await _start_update_runner(target_ref, repo_slug, compose_env_exists)
        except Exception as exc:  # noqa: BLE001
            message = _short_error_message(str(exc), 200)
            await merge_update_state_async(
                status="error",
                update_in_progress=False,
                current_action=None,
                last_error=message,
                log_append=[f"FEHLER: {message}"],
            )
            raise HTTPException(status_code=500, detail=message)
        await merge_update_state_async(
            current_action="Update-Runner läuft",
            log_append=[f"Runner Container: {container_id}"],
        )
        return {"ok": True, "container": container_id, "target": target_ref}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=APP_PORT)
