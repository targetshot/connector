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
TOKEN = get_update_agent_token(DATA_DIR)

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
    compose_env: bool = False
    project_name: str | None = None


class DockerCommandError(RuntimeError):
    pass


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
    if payload.repo_slug:
        env["TS_CONNECT_GITHUB_REPO"] = payload.repo_slug
    else:
        env.pop("TS_CONNECT_GITHUB_REPO", None)
    if payload.target_ref:
        env["TS_CONNECT_UPDATE_REF"] = payload.target_ref
    else:
        env.pop("TS_CONNECT_UPDATE_REF", None)
    if payload.compose_env:
        env["TS_CONNECT_UPDATE_COMPOSE_ENV"] = "compose.env"
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
