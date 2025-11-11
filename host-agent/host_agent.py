from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

APP = FastAPI(title="TargetShot Host Agent", version="0.1.0")


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

STATE_PATH = Path(os.getenv("TS_HOST_AGENT_STATE_PATH", "/var/lib/ts-connect-host-agent/state.json"))
STATE_PATH.parent.mkdir(parents=True, exist_ok=True)

TOKEN_FILE = Path(os.getenv("TS_HOST_AGENT_TOKEN_FILE", "/etc/ts-connect-host-agent/token"))

WORKSPACE_PATH = Path(os.getenv("TS_HOST_WORKSPACE", "/opt/ts-connect"))
COMPOSE_ENV = os.getenv("TS_HOST_COMPOSE_ENV", "compose.env")
UPDATE_AGENT_PATH = Path(os.getenv("TS_HOST_UPDATE_AGENT_PATH", WORKSPACE_PATH / "update-agent"))
UPDATE_AGENT_ENV = os.getenv("TS_HOST_UPDATE_AGENT_ENV", "../compose.env")
COMPOSE_TIMEOUT = int(os.getenv("TS_HOST_AGENT_COMPOSE_TIMEOUT", "120"))
REBOOT_DELAY_SECONDS = int(os.getenv("TS_HOST_AGENT_REBOOT_DELAY", "60"))
HOST_LOG_LIMIT = int(os.getenv("TS_HOST_AGENT_LOG_LIMIT", "400"))
OS_PACKAGE_LIMIT = int(os.getenv("TS_HOST_AGENT_PACKAGE_LIMIT", "80"))

HOST_AGENT_TOKEN = os.getenv("TS_HOST_AGENT_TOKEN") or ""
if not HOST_AGENT_TOKEN:
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    if TOKEN_FILE.exists():
        try:
            HOST_AGENT_TOKEN = TOKEN_FILE.read_text(encoding="utf-8").strip()
        except OSError:
            HOST_AGENT_TOKEN = ""
    if not HOST_AGENT_TOKEN:
        HOST_AGENT_TOKEN = secrets.token_urlsafe(48)
        tmp_path = TOKEN_FILE.with_name(f".{TOKEN_FILE.name}.tmp")
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(HOST_AGENT_TOKEN + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, TOKEN_FILE)
        os.chmod(TOKEN_FILE, 0o600)


# ------------------------------------------------------------------------------
# State handling
# ------------------------------------------------------------------------------

def _default_os_state() -> dict[str, Any]:
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


def _default_reboot_state() -> dict[str, Any]:
    return {
        "pending": False,
        "current_action": None,
        "last_request": None,
        "last_success": None,
        "last_error": None,
        "scheduled_for": None,
        "log": [],
    }


def _default_state() -> dict[str, Any]:
    return {
        "os": _default_os_state(),
        "reboot": _default_reboot_state(),
    }


def _atomic_write_text(path: Path, content: str) -> None:
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)
    directory_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


_STATE_LOCK = threading.Lock()


def _read_state() -> dict[str, Any]:
    if not STATE_PATH.exists():
        return _default_state()
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return _default_state()
    state = _default_state()
    for key in state.keys():
        if key in data and isinstance(data[key], dict):
            state[key].update(data[key])
    return state


def _write_state(state: dict[str, Any]) -> dict[str, Any]:
    _atomic_write_text(STATE_PATH, json.dumps(state, ensure_ascii=False, indent=2))
    return state


def _merge_state(section: str, updates: dict[str, Any], log_append: list[str] | None = None, log_limit: int = HOST_LOG_LIMIT) -> dict[str, Any]:
    with _STATE_LOCK:
        state = _read_state()
        part = state.get(section) or {}
        part.update(updates)
        if log_append:
            log = part.get("log")
            if not isinstance(log, list):
                log = []
            for line in log_append:
                if isinstance(line, str):
                    log.append(line)
            if len(log) > log_limit:
                log = log[-log_limit:]
            part["log"] = log
        state[section] = part
        return _write_state(state)


def _add_log(section: str, line: str) -> None:
    _merge_state(section, {}, [line])


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _run_command(cmd: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None, timeout: int | None = None, section: str = "os", label: str | None = None) -> str:
    display = " ".join(cmd)
    _add_log(section, f"$ {display}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(cwd) if cwd else None,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Befehl nicht gefunden: {cmd[0]}") from exc
    output = (result.stdout or "").strip()
    if output:
        _add_log(section, output)
    if result.returncode != 0:
        raise RuntimeError(f"{label or display} fehlgeschlagen (Exit {result.returncode})")
    return output


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
            packages.append(
                {
                    "name": data.get("name") or "",
                    "section": data.get("section") or "",
                    "version": data.get("version") or "",
                    "current": data.get("current") or "",
                    "arch": data.get("arch") or "",
                    "security": "security" in (data.get("section") or "").lower(),
                }
            )
        else:
            first = line.split()[0]
            packages.append(
                {
                    "name": first,
                    "section": "",
                    "version": "",
                    "current": "",
                    "arch": "",
                    "security": False,
                }
            )
    return packages


_job_lock = threading.Lock()


def _apt_environment() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("DEBIAN_FRONTEND", "noninteractive")
    return env


def _docker_environment() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("COMPOSE_HTTP_TIMEOUT", "240")
    return env


def _run_os_refresh() -> None:
    start_ts = _now_iso()
    _merge_state(
        "os",
        {
            "check_in_progress": True,
            "current_action": "Paketquellen prüfen",
            "last_check_error": None,
        },
        ["Systemcheck gestartet"],
    )
    try:
        env = _apt_environment()
        _run_command(["apt-get", "update", "-qq"], env=env, timeout=600, section="os", label="apt-get update")
        listing = _run_command(
            ["bash", "-lc", "set -o pipefail; apt list --upgradable 2>/dev/null || true"],
            env=env,
            timeout=240,
            section="os",
            label="apt list --upgradable",
        )
        packages = _parse_upgradable_list(listing)
        packages.sort(key=lambda pkg: (not pkg.get("security"), pkg.get("name", "")))
        limited = packages[:OS_PACKAGE_LIMIT]
        _merge_state(
            "os",
            {
                "check_in_progress": False,
                "current_action": None,
                "last_check": start_ts,
                "packages": limited,
                "packages_total": len(packages),
                "pending_count": len(packages),
                "security_count": sum(1 for pkg in packages if pkg.get("security")),
            },
            ["Systemcheck abgeschlossen"],
        )
    except Exception as exc:  # noqa: BLE001
        _merge_state(
            "os",
            {
                "check_in_progress": False,
                "current_action": None,
                "last_check": start_ts,
                "last_check_error": str(exc),
            },
            [f"FEHLER: {exc}"],
        )


def _run_os_update() -> None:
    start_ts = _now_iso()
    _merge_state(
        "os",
        {
            "update_in_progress": True,
            "current_action": "Systemupdate gestartet",
            "last_update_error": None,
        },
        ["Systemupdate gestartet"],
    )
    try:
        env = _apt_environment()
        _run_command(["apt-get", "update", "-qq"], env=env, timeout=600, section="os", label="apt-get update")
        _merge_state("os", {"current_action": "Pakete aktualisieren"})
        _run_command(["apt-get", "-y", "upgrade"], env=env, timeout=1800, section="os", label="apt-get upgrade")
        _merge_state("os", {"current_action": "Bereinige Pakete"})
        _run_command(["apt-get", "-y", "autoremove"], env=env, timeout=900, section="os", label="apt-get autoremove")
        _merge_state(
            "os",
            {
                "update_in_progress": False,
                "current_action": None,
                "last_update": start_ts,
                "last_update_error": None,
            },
            ["Systemupdate abgeschlossen"],
        )
    except Exception as exc:  # noqa: BLE001
        _merge_state(
            "os",
            {
                "update_in_progress": False,
                "current_action": None,
                "last_update": start_ts,
                "last_update_error": str(exc),
            },
            [f"FEHLER: {exc}"],
        )
    finally:
        _run_os_refresh()


def _compose_down(path: Path, env_file: str | None) -> None:
    if not path.exists():
        _add_log("reboot", f"Überspringe docker compose down – Verzeichnis {path} nicht gefunden.")
        return
    cmd = ["docker", "compose"]
    if env_file:
        candidate = path / env_file
        if not candidate.exists():
            _add_log("reboot", f"Env-Datei {candidate} nicht gefunden – starte Compose ohne --env-file.")
        else:
            cmd += ["--env-file", env_file]
    cmd += ["down", "--remove-orphans", "--timeout", str(COMPOSE_TIMEOUT)]
    _run_command(cmd, cwd=path, env=_docker_environment(), timeout=COMPOSE_TIMEOUT + 60, section="reboot", label="docker compose down")


def _run_reboot(delay: int) -> None:
    start_ts = _now_iso()
    _merge_state(
        "reboot",
        {
            "pending": True,
            "current_action": "Bereite Neustart vor",
            "last_error": None,
            "last_request": start_ts,
        },
        ["Reboot angefordert"],
    )
    try:
        if WORKSPACE_PATH.exists():
            _add_log("reboot", f"Stoppe Haupt-Stack in {WORKSPACE_PATH}")
            _compose_down(WORKSPACE_PATH, COMPOSE_ENV)
        update_agent_dir = UPDATE_AGENT_PATH
        if update_agent_dir.exists():
            _add_log("reboot", f"Stoppe Update-Agent in {update_agent_dir}")
            _compose_down(update_agent_dir, UPDATE_AGENT_ENV)
        _add_log("reboot", "Synchronisiere Dateisystem")
        _run_command(["sync"], section="reboot", label="sync")
        schedule_time = datetime.now(timezone.utc) + timedelta(seconds=delay)
        reason = "TargetShot Host Agent Neustart"
        if delay <= 0:
            delay_arg = "now"
        elif delay < 60:
            delay_arg = "+1"
        else:
            delay_arg = f"+{max(1, delay // 60)}"
        try:
            _run_command(
                ["shutdown", "-r", delay_arg, reason],
                section="reboot",
                label="shutdown -r",
            )
        except RuntimeError:
            _run_command(["systemctl", "reboot"], section="reboot", label="systemctl reboot")
        _merge_state(
            "reboot",
            {
                "pending": True,
                "current_action": "Neustart initiiert",
                "scheduled_for": schedule_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            },
            ["Neustart ausgelöst"],
        )
    except Exception as exc:  # noqa: BLE001
        _merge_state(
            "reboot",
            {
                "pending": False,
                "current_action": None,
                "last_error": str(exc),
            },
            [f"FEHLER: {exc}"],
        )
    else:
        _merge_state(
            "reboot",
            {
                "pending": False,
                "current_action": None,
                "last_success": start_ts,
            },
        )


def _require_token(request: Request) -> None:
    if not HOST_AGENT_TOKEN:
        return
    header = request.headers.get("X-Host-Agent-Token")
    if not header or not secrets.compare_digest(header.strip(), HOST_AGENT_TOKEN):
        raise HTTPException(status_code=401, detail="Unauthorized")


@APP.middleware("http")
async def token_middleware(request: Request, call_next):
    try:
        _require_token(request)
    except HTTPException as exc:
        return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)
    return await call_next(request)


@APP.get("/api/v1/health")
async def health():
    return {
        "ok": True,
        "workspace": str(WORKSPACE_PATH),
    }


@APP.get("/api/v1/status")
async def status():
    return _read_state()


@APP.post("/api/v1/os/refresh")
async def trigger_refresh():
    state = _read_state()
    if state["os"].get("check_in_progress") or state["os"].get("update_in_progress"):
        raise HTTPException(status_code=409, detail="Systemcheck oder Update läuft bereits.")
    thread = threading.Thread(target=_run_os_refresh, name="os-refresh", daemon=True)
    thread.start()
    return {"ok": True}


@APP.post("/api/v1/os/update")
async def trigger_update():
    state = _read_state()
    if state["os"].get("update_in_progress"):
        raise HTTPException(status_code=409, detail="Systemupdate läuft bereits.")
    thread = threading.Thread(target=_run_os_update, name="os-update", daemon=True)
    thread.start()
    return {"ok": True}


@APP.post("/api/v1/reboot")
async def schedule_reboot(payload: dict[str, Any] | None = None):
    with _job_lock:
        state = _read_state()
        if state["reboot"].get("pending"):
            raise HTTPException(status_code=409, detail="Es liegt bereits ein Neustart an.")
        delay = int((payload or {}).get("delay") or REBOOT_DELAY_SECONDS)
        thread = threading.Thread(target=_run_reboot, args=(delay,), name="reboot", daemon=True)
        thread.start()
    return {"ok": True, "delay": delay}


def main() -> None:
    parser = argparse.ArgumentParser(description="TargetShot Host Agent")
    parser.add_argument("--host", default=os.getenv("TS_HOST_AGENT_LISTEN", "0.0.0.0"))
    parser.add_argument("--port", default=int(os.getenv("TS_HOST_AGENT_PORT", "9010")), type=int)
    args = parser.parse_args()

    import uvicorn

    uvicorn.run("host_agent:APP", host=args.host, port=args.port, workers=1)


if __name__ == "__main__":
    main()
