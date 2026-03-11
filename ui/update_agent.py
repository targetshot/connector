from __future__ import annotations

import asyncio
import logging
import os
import secrets
import sys
from pathlib import Path
from typing import Any

from asyncio.subprocess import PIPE

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel

from update_agent_utils import get_update_agent_token

logger = logging.getLogger("ts-update-agent")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

WORKSPACE_PATH = Path(os.getenv("TS_CONNECT_WORKSPACE", "/workspace"))
DATA_DIR = Path(os.getenv("TS_CONNECT_DATA_DIR", "/app/data"))
MODULE_DIR = Path(__file__).resolve().parent
TOKEN = get_update_agent_token(DATA_DIR)
MIRROR_CONTAINER_NAME = os.getenv("TS_CONNECT_MIRROR_CONTAINER_NAME", "ts-mariadb-mirror").strip() or "ts-mariadb-mirror"

app = FastAPI(title="TargetShot Update Agent")

_job_lock = asyncio.Lock()
_current_job_id: str | None = None
_job_task: asyncio.Task | None = None


def _require_workspace() -> None:
    if not WORKSPACE_PATH.exists():
        raise RuntimeError(f"Workspace nicht gefunden: {WORKSPACE_PATH}")


class RunRequest(BaseModel):
    target_ref: str | None = None
    repo_slug: str | None = None
    env_file: str | None = None
    compose_env: bool | None = None
    project_name: str | None = None


class MirrorReplicationApplyRequest(BaseModel):
    host: str
    port: int = 3306
    user: str
    password: str
    gtid_mode: bool = True
    log_file: str | None = None
    log_pos: int | None = None
    connect_retry: int = 10


class DockerCommandError(RuntimeError):
    pass


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


async def _run_subprocess(cmd: list[str], *, env: dict[str, str] | None = None) -> int:
    process = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(WORKSPACE_PATH),
        env=env,
    )
    return await process.wait()


async def _docker_command(args: list[str], *, timeout: float | None = None) -> tuple[int, str, str]:
    process = await asyncio.create_subprocess_exec("docker", *args, stdout=PIPE, stderr=PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        process.kill()
        raise
    return process.returncode, stdout.decode("utf-8", errors="replace"), stderr.decode("utf-8", errors="replace")


def _parse_replication_status(output: str) -> dict[str, Any]:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        return {
            "configured": False,
            "running": False,
            "message": "Keine Replikation konfiguriert.",
        }
    data: dict[str, str] = {}
    for line in lines:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip()
    io_running = data.get("Slave_IO_Running", "").lower() == "yes"
    sql_running = data.get("Slave_SQL_Running", "").lower() == "yes"
    seconds_raw = (data.get("Seconds_Behind_Master") or "").strip()
    seconds: int | None = None
    if seconds_raw and seconds_raw.upper() != "NULL":
        try:
            seconds = int(seconds_raw)
        except ValueError:
            seconds = None
    last_io_error = data.get("Last_IO_Error") or ""
    last_sql_error = data.get("Last_SQL_Error") or ""
    running = io_running and sql_running
    if running:
        message = "Replikation läuft."
    else:
        details = [part for part in (last_io_error, last_sql_error) if part]
        if details:
            message = details[0]
        elif io_running or sql_running:
            message = "Replikation teilweise aktiv."
        else:
            message = "Replikation gestoppt."
    return {
        "configured": True,
        "running": running,
        "slave_io_running": io_running,
        "slave_sql_running": sql_running,
        "seconds_behind_master": seconds,
        "master_host": data.get("Master_Host") or "",
        "master_port": data.get("Master_Port") or "",
        "last_io_error": last_io_error,
        "last_sql_error": last_sql_error,
        "message": message,
    }


async def _ensure_idle() -> None:
    async with _job_lock:
        if _current_job_id:
            raise HTTPException(status_code=409, detail="Update läuft bereits")


async def _auth_guard(request: Request) -> None:
    if not TOKEN:
        return
    header = request.headers.get("X-Update-Agent-Token")
    if header != TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


def _build_runner_env(payload: RunRequest) -> dict[str, str]:
    env = os.environ.copy()
    env["TS_CONNECT_WORKSPACE"] = str(WORKSPACE_PATH)
    env["TS_CONNECT_DATA_DIR"] = str(DATA_DIR)
    existing_pythonpath = env.get("PYTHONPATH", "").strip()
    if existing_pythonpath:
        env["PYTHONPATH"] = os.pathsep.join((str(MODULE_DIR), existing_pythonpath))
    else:
        env["PYTHONPATH"] = str(MODULE_DIR)
    if payload.repo_slug:
        env["TS_CONNECT_GITHUB_REPO"] = payload.repo_slug
    else:
        env.pop("TS_CONNECT_GITHUB_REPO", None)
    if payload.target_ref:
        env["TS_CONNECT_UPDATE_REF"] = payload.target_ref
    else:
        env.pop("TS_CONNECT_UPDATE_REF", None)
    env_file = (payload.env_file or "").strip()
    if not env_file and payload.compose_env:
        # Legacy payload field: map to the unified .env convention.
        env_file = ".env"
    if env_file:
        env["TS_CONNECT_UPDATE_COMPOSE_ENV"] = env_file
    else:
        env.pop("TS_CONNECT_UPDATE_COMPOSE_ENV", None)
    if payload.project_name:
        env["COMPOSE_PROJECT_NAME"] = payload.project_name
    return env


async def _run_update_job(job_id: str, payload: RunRequest) -> None:
    global _current_job_id, _job_task
    logger.info("Starte Update-Runner Job %s", job_id)
    env = _build_runner_env(payload)
    try:
        return_code = await _run_subprocess([sys.executable, "-m", "update_runner"], env=env)
        if return_code != 0:
            logger.error("Update-Runner Job %s endete mit Code %s", job_id, return_code)
        else:
            logger.info("Update-Runner Job %s abgeschlossen", job_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Update-Runner Job %s fehlgeschlagen: %s", job_id, exc)
    finally:
        async with _job_lock:
            _current_job_id = None
            _job_task = None


@app.get("/api/v1/health")
async def agent_health(_: Any = Depends(_auth_guard)):
    return {"ok": True, "running": bool(_current_job_id)}


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/api/v1/status")
async def agent_status(_: Any = Depends(_auth_guard)):
    return {"running": bool(_current_job_id), "job_id": _current_job_id}


@app.post("/api/v1/run")
async def agent_run(request: RunRequest, _: Any = Depends(_auth_guard)):
    global _current_job_id, _job_task
    _require_workspace()
    async with _job_lock:
        if _current_job_id:
            raise HTTPException(status_code=409, detail="Update läuft bereits")
        job_id = secrets.token_hex(8)
        _current_job_id = job_id
        _job_task = asyncio.create_task(_run_update_job(job_id, request))
    return {"ok": True, "job_id": job_id}


def _container_filter(name: str) -> list[str]:
    return ["--filter", f"name={name}"]


async def _container_exists(name: str) -> tuple[bool, str]:
    code, stdout, stderr = await _docker_command(["ps", "-a", *_container_filter(name), "--format", "{{.Status}}"])
    if code != 0:
        raise DockerCommandError(stderr.strip() or stdout.strip() or "docker ps -a fehlgeschlagen")
    status = stdout.strip()
    return bool(status), status


async def _container_env(name: str) -> dict[str, str]:
    code, stdout, stderr = await _docker_command(
        ["inspect", "--format", "{{range .Config.Env}}{{println .}}{{end}}", name],
        timeout=10,
    )
    if code != 0:
        raise DockerCommandError(stderr.strip() or stdout.strip() or "docker inspect fehlgeschlagen")
    env_map: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_map[key] = value
    return env_map


async def _mirror_root_password(name: str) -> str:
    env_map = await _container_env(name)
    password = (env_map.get("MARIADB_ROOT_PASSWORD") or "").strip()
    if not password:
        raise DockerCommandError("MARIADB_ROOT_PASSWORD im Mirror-Container nicht gefunden")
    return password


async def _run_mirror_sql(sql: str, *, name: str) -> str:
    root_password = await _mirror_root_password(name)
    code, stdout, stderr = await _docker_command(
        [
            "exec",
            name,
            "mariadb",
            "--protocol=socket",
            "-uroot",
            f"-p{root_password}",
            "-e",
            sql,
        ],
        timeout=30,
    )
    if code != 0:
        raise DockerCommandError(stderr.strip() or stdout.strip() or "MariaDB-Befehl fehlgeschlagen")
    return stdout


async def _mirror_replication_status(name: str) -> dict[str, Any]:
    output = await _run_mirror_sql("SHOW SLAVE STATUS\\G", name=name)
    return _parse_replication_status(output)


def _build_change_master_sql(request: MirrorReplicationApplyRequest) -> str:
    host = _sql_escape(request.host)
    user = _sql_escape(request.user)
    password = _sql_escape(request.password)
    connect_retry = max(1, int(request.connect_retry or 10))
    port = int(request.port or 3306)
    if request.gtid_mode:
        return (
            "CHANGE MASTER TO "
            f"MASTER_HOST='{host}', "
            f"MASTER_PORT={port}, "
            f"MASTER_USER='{user}', "
            f"MASTER_PASSWORD='{password}', "
            f"MASTER_CONNECT_RETRY={connect_retry}, "
            "MASTER_SSL=0, "
            "MASTER_USE_GTID=slave_pos;"
        )
    if not request.log_file or request.log_pos is None:
        raise DockerCommandError("Für non-GTID werden log_file und log_pos benötigt.")
    log_file = _sql_escape(request.log_file)
    log_pos = int(request.log_pos)
    if log_pos <= 0:
        raise DockerCommandError("log_pos muss größer als 0 sein.")
    return (
        "CHANGE MASTER TO "
        f"MASTER_HOST='{host}', "
        f"MASTER_PORT={port}, "
        f"MASTER_USER='{user}', "
        f"MASTER_PASSWORD='{password}', "
        f"MASTER_CONNECT_RETRY={connect_retry}, "
        "MASTER_SSL=0, "
        f"MASTER_LOG_FILE='{log_file}', "
        f"MASTER_LOG_POS={log_pos};"
    )


@app.post("/api/v1/containers/{name}/restart")
async def restart_container(name: str, _: Any = Depends(_auth_guard)):
    try:
        exists, _ = await _container_exists(name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not exists:
        return {"ok": True, "found": False}
    code, stdout, stderr = await _docker_command(["restart", name], timeout=60)
    if code != 0:
        raise HTTPException(status_code=500, detail=stderr.strip() or stdout.strip() or "docker restart fehlgeschlagen")
    return {"ok": True, "found": True, "output": stdout.strip()}


@app.post("/api/v1/containers/{name}/stop")
async def stop_container(name: str, _: Any = Depends(_auth_guard)):
    try:
        exists, _ = await _container_exists(name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not exists:
        return {"ok": True, "found": False}
    code, stdout, stderr = await _docker_command(["stop", name], timeout=60)
    if code != 0:
        raise HTTPException(status_code=500, detail=stderr.strip() or stdout.strip() or "docker stop fehlgeschlagen")
    return {"ok": True, "found": True, "output": stdout.strip()}


@app.get("/api/v1/containers/{name}/status")
async def container_status(name: str, _: Any = Depends(_auth_guard)):
    try:
        exists, status = await _container_exists(name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    running = False
    if exists:
        run_code, run_stdout, _ = await _docker_command(["ps", *_container_filter(name), "--format", "{{.Status}}"])
        if run_code == 0 and run_stdout.strip():
            running = "Up" in run_stdout.strip()
    return {
        "exists": exists,
        "status": status,
        "running": running,
    }


@app.get("/api/v1/mirror-replication/status")
async def mirror_replication_status(_: Any = Depends(_auth_guard)):
    container_name = MIRROR_CONTAINER_NAME
    try:
        exists, status = await _container_exists(container_name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not exists:
        return {
            "ok": False,
            "exists": False,
            "container": container_name,
            "status": status,
            "configured": False,
            "running": False,
            "message": "Mirror-Container nicht gefunden.",
        }
    try:
        snapshot = await _mirror_replication_status(container_name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "ok": True,
        "exists": True,
        "container": container_name,
        "status": status,
        **snapshot,
    }


@app.post("/api/v1/mirror-replication/apply")
async def mirror_replication_apply(request: MirrorReplicationApplyRequest, _: Any = Depends(_auth_guard)):
    if not request.host.strip():
        raise HTTPException(status_code=400, detail="Source-Host fehlt.")
    if request.port <= 0 or request.port > 65535:
        raise HTTPException(status_code=400, detail="Source-Port muss zwischen 1 und 65535 liegen.")
    if not request.user.strip() or not request.password.strip():
        raise HTTPException(status_code=400, detail="Replikations-Benutzer und Passwort sind erforderlich.")
    if not request.gtid_mode:
        if not (request.log_file and request.log_file.strip()) or request.log_pos is None:
            raise HTTPException(status_code=400, detail="Für non-GTID müssen log_file und log_pos gesetzt sein.")
    container_name = MIRROR_CONTAINER_NAME
    try:
        exists, _ = await _container_exists(container_name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not exists:
        raise HTTPException(status_code=404, detail="Mirror-Container nicht gefunden.")

    try:
        change_master_sql = _build_change_master_sql(request)
        sql = "\n".join(
            [
                "STOP SLAVE;",
                "RESET SLAVE ALL;",
                change_master_sql,
                "START SLAVE;",
            ]
        )
        await _run_mirror_sql(sql, name=container_name)
        snapshot = await _mirror_replication_status(container_name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return {
        "ok": True,
        "applied": True,
        "container": container_name,
        **snapshot,
    }


@app.post("/api/v1/mirror-replication/disable")
async def mirror_replication_disable(_: Any = Depends(_auth_guard)):
    container_name = MIRROR_CONTAINER_NAME
    try:
        exists, _ = await _container_exists(container_name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not exists:
        return {
            "ok": True,
            "applied": False,
            "container": container_name,
            "configured": False,
            "running": False,
            "message": "Mirror-Container nicht gefunden.",
        }
    try:
        await _run_mirror_sql("STOP SLAVE; RESET SLAVE ALL;", name=container_name)
    except DockerCommandError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "ok": True,
        "applied": True,
        "container": container_name,
        "configured": False,
        "running": False,
        "message": "Replikation deaktiviert.",
    }
