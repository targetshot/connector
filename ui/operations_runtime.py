from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx
from fastapi import HTTPException


class AgentRequestError(RuntimeError):
    def __init__(
        self,
        service: str,
        message: str,
        *,
        status_code: int | None = None,
        unavailable: bool = False,
    ) -> None:
        super().__init__(message)
        self.service = service
        self.status_code = status_code
        self.unavailable = unavailable


def agent_error_status(exc: AgentRequestError, *, default_status: int = 502) -> int:
    if exc.status_code:
        return exc.status_code
    if exc.unavailable:
        return 503
    return default_status


async def update_agent_request(
    method: str,
    path: str,
    *,
    update_agent_url: str,
    update_agent_token: str,
    short_error_message: Callable[[str, int], str],
    json_payload: dict | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    if not update_agent_url:
        raise AgentRequestError("update-agent", "Update-Agent URL nicht konfiguriert", status_code=503, unavailable=True)
    url = f"{update_agent_url}{path}"
    headers: dict[str, str] = {}
    if update_agent_token:
        headers["X-Update-Agent-Token"] = update_agent_token
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, url, json=json_payload, headers=headers)
    except httpx.RequestError as exc:
        raise AgentRequestError(
            "update-agent",
            f"Update-Agent nicht erreichbar: {short_error_message(str(exc), 140)}",
            status_code=503,
            unavailable=True,
        ) from exc
    if response.status_code >= 400:
        text = short_error_message(response.text, 200)
        raise AgentRequestError(
            "update-agent",
            f"Update-Agent {response.status_code}: {text}",
            status_code=response.status_code,
        )
    if response.headers.get("content-type", "").startswith("application/json"):
        return response.json()
    return {"ok": True, "body": response.text}


async def ping_update_agent(
    *,
    update_agent_request_fn: Callable[..., Awaitable[dict[str, Any]]],
    timeout: float = 3.0,
) -> bool:
    try:
        result = await update_agent_request_fn("GET", "/api/v1/health", timeout=timeout)
    except Exception:
        return False
    return bool(result.get("ok"))


async def host_agent_request(
    method: str,
    path: str,
    *,
    host_agent_url: str,
    host_agent_token: str,
    short_error_message: Callable[[str, int], str],
    json_payload: dict | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    if not host_agent_url:
        raise AgentRequestError("host-agent", "Host-Agent URL nicht konfiguriert", status_code=503, unavailable=True)
    url = f"{host_agent_url}{path}"
    headers: dict[str, str] = {}
    if host_agent_token:
        headers["X-Host-Agent-Token"] = host_agent_token
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.request(method, url, json=json_payload, headers=headers)
    except httpx.RequestError as exc:
        raise AgentRequestError(
            "host-agent",
            f"Host-Agent nicht erreichbar: {short_error_message(str(exc), 140)}",
            status_code=503,
            unavailable=True,
        ) from exc
    if response.status_code >= 400:
        text = short_error_message(response.text, 200)
        raise AgentRequestError(
            "host-agent",
            f"Host-Agent {response.status_code}: {text}",
            status_code=response.status_code,
        )
    if response.headers.get("content-type", "").startswith("application/json"):
        return response.json()
    return {"ok": True, "body": response.text}


async def ping_host_agent(
    *,
    host_agent_url: str,
    host_agent_request_fn: Callable[..., Awaitable[dict[str, Any]]],
    timeout: float = 3.0,
) -> bool:
    if not host_agent_url:
        return False
    try:
        result = await host_agent_request_fn("GET", "/api/v1/health", timeout=timeout)
    except Exception:
        return False
    return bool(result.get("ok"))


async def read_update_agent_status(
    *,
    update_agent_request_fn: Callable[..., Awaitable[dict[str, Any]]],
    short_error_message: Callable[[str, int], str],
    timeout: float = 3.0,
) -> dict[str, Any] | None:
    try:
        result = await update_agent_request_fn("GET", "/api/v1/status", timeout=timeout)
    except AgentRequestError as exc:
        return {
            "available": False,
            "running": False,
            "job_id": None,
            "error": short_error_message(str(exc), 180),
            "status_code": agent_error_status(exc),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "available": False,
            "running": False,
            "job_id": None,
            "error": short_error_message(str(exc), 180),
            "status_code": 503,
        }
    payload = result if isinstance(result, dict) else {}
    return {
        "available": True,
        "running": bool(payload.get("running")),
        "job_id": payload.get("job_id"),
        "error": None,
        "status_code": 200,
    }


async def reconcile_stale_update_state(
    *,
    ensure_update_state_fn: Callable[[], Awaitable[dict[str, Any]]],
    get_update_state_snapshot_fn: Callable[[], Awaitable[dict[str, Any]]],
    read_update_agent_status_fn: Callable[[], Awaitable[dict[str, Any] | None]],
    merge_update_state_async_fn: Callable[..., Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    await ensure_update_state_fn()
    state = await get_update_state_snapshot_fn()
    if state.get("status") != "running":
        return state
    agent_status = await read_update_agent_status_fn()
    if agent_status and agent_status.get("available") is False:
        next_action = "Update-Agent nicht erreichbar"
        next_error = agent_status.get("error") or "Update-Agent nicht erreichbar"
        if state.get("current_action") != next_action or state.get("last_error") != next_error:
            await merge_update_state_async_fn(
                current_action=next_action,
                last_error=next_error,
            )
            return await get_update_state_snapshot_fn()
        return state
    if agent_status and not agent_status.get("running"):
        await merge_update_state_async_fn(
            status="error",
            update_in_progress=False,
            current_action=None,
            job_started=None,
            last_error="Update-Agent meldet keinen laufenden Job mehr.",
            log_append=["Update-Agent meldet keinen laufenden Job mehr; Update-Status wurde auf Fehler gesetzt."],
        )
        return await get_update_state_snapshot_fn()
    return state


async def start_update_runner(
    target_ref: str | None,
    repo_slug: str | None,
    env_file: str | None,
    *,
    project_name: str,
    update_agent_request_fn: Callable[..., Awaitable[dict[str, Any]]],
) -> str:
    payload = {
        "target_ref": target_ref,
        "repo_slug": repo_slug,
        "env_file": env_file,
        "compose_env": bool(env_file),
        "project_name": project_name,
    }
    result = await update_agent_request_fn("POST", "/api/v1/run", json_payload=payload, timeout=10)
    return result.get("job_id") or "update-agent"


async def launch_update_job(
    *,
    target_ref: str | None,
    initiated_by: str,
    force_release_refresh: bool,
    reset_log: bool,
    update_job_lock: asyncio.Lock,
    get_update_state_snapshot_fn: Callable[[], Awaitable[dict[str, Any]]],
    collect_workspace_info_fn: Callable[[], Awaitable[dict[str, Any]]],
    detect_prerequisites_fn: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ensure_latest_release_fn: Callable[..., Awaitable[dict[str, Any] | None]],
    determine_repo_slug_fn: Callable[..., Awaitable[str | None]],
    detect_env_file_name_fn: Callable[[], str | None],
    now_utc_iso_fn: Callable[[], str],
    append_update_log_fn: Callable[..., Awaitable[dict[str, Any]]],
    merge_update_state_async_fn: Callable[..., Awaitable[dict[str, Any]]],
    start_update_runner_fn: Callable[[str | None, str | None, str | None], Awaitable[str]],
    short_error_message: Callable[[str, int], str],
) -> dict[str, Any]:
    label = "Automatisches" if initiated_by == "auto" else "Manuelles"
    async with update_job_lock:
        state = await get_update_state_snapshot_fn()
        if state.get("status") == "running":
            return {"ok": False, "error": "Ein Update läuft bereits", "code": 409}
        workspace_info = await collect_workspace_info_fn()
        prereq = await detect_prerequisites_fn(workspace_info)
        if not prereq.get("ok"):
            missing = [name for name, available in prereq.items() if name != "ok" and not available]
            detail = "Voraussetzungen fehlen: " + ", ".join(missing)
            return {"ok": False, "error": detail, "code": 400}
        release = await ensure_latest_release_fn(force=force_release_refresh)
        repo_slug = await determine_repo_slug_fn()
        env_file_name = detect_env_file_name_fn()
        selected_target = target_ref or (release.get("tag_name") if isinstance(release, dict) else None)
        job_started = now_utc_iso_fn()
        log_lines = [f"{label} Update ausgelöst um {job_started}", f"Initiator: {initiated_by}"]
        await append_update_log_fn(log_lines, reset=reset_log)
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
        await merge_update_state_async_fn(**updates)
        try:
            job_identifier = await start_update_runner_fn(selected_target, repo_slug, env_file_name)
        except AgentRequestError as exc:
            message = short_error_message(str(exc), 200)
            result_updates: dict[str, Any] = {
                "status": "error",
                "update_in_progress": False,
                "current_action": None,
                "last_error": message,
                "log_append": [f"FEHLER: {message}"],
            }
            if initiated_by == "auto":
                result_updates.setdefault("auto_update_last_run", job_started)
            await merge_update_state_async_fn(**result_updates)
            return {"ok": False, "error": message, "code": agent_error_status(exc)}
        except Exception as exc:  # noqa: BLE001
            message = short_error_message(str(exc), 200)
            result_updates = {
                "status": "error",
                "update_in_progress": False,
                "current_action": None,
                "last_error": message,
                "log_append": [f"FEHLER: {message}"],
            }
            if initiated_by == "auto":
                result_updates.setdefault("auto_update_last_run", job_started)
            await merge_update_state_async_fn(**result_updates)
            return {"ok": False, "error": message, "code": 500}
        await merge_update_state_async_fn(
            current_action="Update-Agent gestartet",
            log_append=[f"Update-Agent Job: {job_identifier}"],
        )
        return {"ok": True, "container": job_identifier, "target": selected_target}


async def update_agent_refresh_needed(
    *,
    workspace_path: Path,
    get_update_state_snapshot_fn: Callable[[], Awaitable[dict[str, Any]]],
    default_update_image: str,
    update_agent_container_name: str,
    docker_image_id_fn: Callable[[str], Awaitable[str | None]],
    docker_inspect_value_fn: Callable[[str, str], Awaitable[str | None]],
) -> tuple[bool, str]:
    compose_dir = workspace_path / "update-agent"
    if not compose_dir.exists():
        return (False, f"Update-Agent Verzeichnis fehlt: {compose_dir}")
    state = await get_update_state_snapshot_fn()
    if state.get("status") == "running":
        return (False, "Update läuft noch")
    desired_image = (os.getenv("TS_CONNECT_UI_IMAGE", default_update_image) or default_update_image).strip() or default_update_image
    desired_image_id = await docker_image_id_fn(desired_image)
    current_agent_image_id = await docker_inspect_value_fn(update_agent_container_name, "{{.Image}}")
    if current_agent_image_id is None:
        return (True, "Update-Agent Container fehlt")
    if desired_image_id and current_agent_image_id != desired_image_id:
        return (True, "Update-Agent nutzt noch ein altes Image")
    return (False, "Update-Agent ist bereits aktuell")


def command_log_lines(output: str) -> list[str]:
    return [line for line in output.splitlines() if line.strip()]


async def refresh_update_agent_if_needed(
    *,
    update_agent_sync_lock: asyncio.Lock,
    update_agent_refresh_needed_fn: Callable[[], Awaitable[tuple[bool, str]]],
    default_update_image: str,
    logger: Any,
    ensure_registry_login_for_release_fn: Callable[[str], Awaitable[Any]],
    update_agent_compose_base_fn: Callable[[], tuple[Path, list[str]]],
    run_command_capture_fn: Callable[..., Awaitable[tuple[int, str, str]]],
    update_agent_container_name: str,
    ping_update_agent_fn: Callable[..., Awaitable[bool]],
) -> None:
    async with update_agent_sync_lock:
        needs_refresh, reason = await update_agent_refresh_needed_fn()
        if not needs_refresh:
            logger.info("Update-Agent Sync übersprungen: %s", reason)
            return
        desired_image = (os.getenv("TS_CONNECT_UI_IMAGE", default_update_image) or default_update_image).strip() or default_update_image
        logger.info("Starte automatischen Update-Agent Sync: %s", reason)
        try:
            await ensure_registry_login_for_release_fn(desired_image)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Update-Agent Sync: Docker-Login fehlgeschlagen: %s", exc)
        compose_dir, compose_cmd = update_agent_compose_base_fn()
        pull_code, pull_stdout, pull_stderr = await run_command_capture_fn(
            compose_cmd + ["pull", "update-agent"],
            cwd=compose_dir,
            timeout=300,
        )
        if pull_code != 0:
            message = pull_stderr.strip() or pull_stdout.strip() or f"Exit {pull_code}"
            raise RuntimeError(f"Update-Agent Pull fehlgeschlagen: {message}")
        for line in command_log_lines(pull_stdout) + command_log_lines(pull_stderr):
            logger.info("Update-Agent Sync: %s", line)
        rm_code, rm_stdout, rm_stderr = await run_command_capture_fn(
            ["docker", "rm", "-f", update_agent_container_name],
            timeout=60,
        )
        if rm_code == 0:
            for line in command_log_lines(rm_stdout) + command_log_lines(rm_stderr):
                logger.info("Update-Agent Sync: %s", line)
        up_code, up_stdout, up_stderr = await run_command_capture_fn(
            compose_cmd + ["up", "-d", "update-agent"],
            cwd=compose_dir,
            timeout=300,
        )
        if up_code != 0:
            message = up_stderr.strip() or up_stdout.strip() or f"Exit {up_code}"
            raise RuntimeError(f"Update-Agent Start fehlgeschlagen: {message}")
        for line in command_log_lines(up_stdout) + command_log_lines(up_stderr):
            logger.info("Update-Agent Sync: %s", line)
        deadline = asyncio.get_running_loop().time() + 60
        while asyncio.get_running_loop().time() < deadline:
            if await ping_update_agent_fn(timeout=3.0):
                logger.info("Update-Agent Sync erfolgreich abgeschlossen")
                return
            await asyncio.sleep(2)
        raise RuntimeError("Update-Agent wurde nach dem automatischen Sync nicht erreichbar")


async def delayed_update_agent_sync(
    *,
    update_agent_sync_delay_seconds: int,
    refresh_update_agent_if_needed_fn: Callable[[], Awaitable[None]],
) -> None:
    if update_agent_sync_delay_seconds > 0:
        await asyncio.sleep(update_agent_sync_delay_seconds)
    await refresh_update_agent_if_needed_fn()


async def build_update_status(
    *,
    reconcile_stale_update_state_fn: Callable[[], Awaitable[dict[str, Any]]],
    read_update_agent_status_fn: Callable[..., Awaitable[dict[str, Any] | None]],
    ensure_latest_release_fn: Callable[..., Awaitable[dict[str, Any] | None]],
    collect_workspace_info_fn: Callable[[], Awaitable[dict[str, Any]]],
    read_local_ui_image_details_fn: Callable[[], Awaitable[tuple[str | None, str | None]]],
    load_version_defaults_fn: Callable[[], tuple[str, str]],
    detect_prerequisites_fn: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    determine_repo_slug_fn: Callable[..., Awaitable[str | None]],
    detect_env_file_name_fn: Callable[[], str | None],
    sanitize_hour_fn: Callable[[Any], int],
    parse_iso8601_fn: Callable[[str | None], Any],
    format_local_timestamp_fn: Callable[[Any], str | None],
    calculate_next_auto_run_fn: Callable[[int, str | None], Any],
    default_update_image: str,
    force: bool = False,
) -> dict[str, Any]:
    state = await reconcile_stale_update_state_fn()
    agent_status = await read_update_agent_status_fn()
    release = await ensure_latest_release_fn(force=force)
    workspace_info = await collect_workspace_info_fn()
    local_image_digest, local_image_ref = await read_local_ui_image_details_fn()
    connect_version, connect_release = load_version_defaults_fn()
    prereq = await detect_prerequisites_fn(workspace_info)
    repo_slug = await determine_repo_slug_fn()
    env_file_name = detect_env_file_name_fn()
    compose_env_exists = env_file_name is not None
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
    auto_hour = sanitize_hour_fn(state.get("auto_update_hour"))
    auto_last_run = state.get("auto_update_last_run")
    next_auto_run_iso = None
    auto_last_run_local_display: str | None = None
    if auto_last_run:
        last_run_dt = parse_iso8601_fn(auto_last_run)
        auto_last_run_local_display = format_local_timestamp_fn(last_run_dt)
    next_auto_run_local_display: str | None = None
    if auto_enabled:
        next_dt_utc = calculate_next_auto_run_fn(auto_hour, auto_last_run)
        next_auto_run_iso = next_dt_utc.isoformat().replace("+00:00", "Z")
        next_auto_run_local_display = format_local_timestamp_fn(next_dt_utc)
    display_status = state.get("status", "idle")
    display_action = state.get("current_action")
    display_error = state.get("last_error")
    if agent_status and agent_status.get("available") is False and display_status != "running":
        display_status = "degraded"
        display_action = "Update-Agent nicht erreichbar"
        display_error = display_error or agent_status.get("error")
    return {
        "ok": True,
        "status": display_status,
        "current_action": display_action,
        "current_version": connect_version,
        "current_release": connect_release,
        "current_image": os.getenv("TS_CONNECT_UI_IMAGE", default_update_image),
        "current_image_digest": local_image_digest,
        "latest_release": release,
        "latest_release_digest": remote_digest,
        "update_available": update_available,
        "last_check": state.get("last_check"),
        "last_check_error": state.get("last_check_error"),
        "last_success": state.get("last_success"),
        "last_error": display_error,
        "update_target": state.get("update_target"),
        "log": state.get("log", []),
        "workspace": workspace_info,
        "prerequisites": prereq,
        "update_agent": agent_status,
        "repo_slug": repo_slug,
        "compose_env": compose_env_exists,
        "env_file": env_file_name,
        "job_started": state.get("job_started"),
        "auto_update": {
            "enabled": auto_enabled,
            "hour": auto_hour,
            "last_run": auto_last_run,
            "last_run_local": auto_last_run_local_display,
            "next_run": next_auto_run_iso,
            "next_run_local": next_auto_run_local_display,
        },
    }


async def auto_update_worker(
    *,
    get_update_state_snapshot_fn: Callable[[], Awaitable[dict[str, Any]]],
    merge_update_state_async_fn: Callable[..., Awaitable[dict[str, Any]]],
    sanitize_hour_fn: Callable[[Any], int],
    parse_iso8601_fn: Callable[[str | None], Any],
    to_local_fn: Callable[[Any], Any],
    now_local_fn: Callable[[], Any],
    now_utc_iso_fn: Callable[[], str],
    launch_update_job_fn: Callable[..., Awaitable[dict[str, Any]]],
    job_age_seconds_fn: Callable[[str | None], float | None],
    logger: Any,
    auto_update_check_seconds: int,
    auto_update_stale_seconds: int,
    auto_update_force_release: bool,
) -> None:
    await asyncio.sleep(15)
    while True:
        try:
            state = await get_update_state_snapshot_fn()
            if state.get("status") == "running":
                if auto_update_stale_seconds > 0:
                    age_seconds = job_age_seconds_fn(state.get("job_started"))
                    if age_seconds is None or age_seconds >= auto_update_stale_seconds:
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
                        await merge_update_state_async_fn(
                            log_append=[f"Update-Status automatisch auf idle gesetzt{suffix}"],
                            **reset_updates,
                        )
                        state = await get_update_state_snapshot_fn()
                    else:
                        await asyncio.sleep(auto_update_check_seconds)
                        continue
                else:
                    await asyncio.sleep(auto_update_check_seconds)
                    continue
            if state.get("auto_update_enabled"):
                auto_hour = sanitize_hour_fn(state.get("auto_update_hour"))
                last_run_iso = state.get("auto_update_last_run")
                now_local = now_local_fn()
                today_run_local = now_local.replace(hour=auto_hour, minute=0, second=0, microsecond=0)
                last_run_dt = parse_iso8601_fn(last_run_iso)
                last_run_local = to_local_fn(last_run_dt)
                already_today = last_run_local and last_run_local.date() == now_local.date()
                if now_local >= today_run_local and not already_today:
                    logger.info("Auto-Update: Starte geplanten Lauf für %s", today_run_local.isoformat())
                    result = await launch_update_job_fn(
                        target_ref=None,
                        initiated_by="auto",
                        force_release_refresh=auto_update_force_release,
                        reset_log=False,
                    )
                    if not result.get("ok"):
                        await merge_update_state_async_fn(
                            log_append=[
                                "Automatisches Update fehlgeschlagen: "
                                + str(result.get("error")),
                            ],
                            auto_update_last_run=now_utc_iso_fn(),
                        )
                    await asyncio.sleep(60)
                    continue
            await asyncio.sleep(auto_update_check_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Auto-Update Worker Fehler: %s", exc)
            await asyncio.sleep(max(120, auto_update_check_seconds))


def ensure_host_agent_configured(*, host_agent_url: str) -> None:
    if not host_agent_url:
        raise HTTPException(status_code=503, detail="Host-Agent nicht konfiguriert.")


def raise_http_for_agent_exception(
    exc: Exception,
    *,
    short_error_message: Callable[[str, int], str],
    default_status: int = 500,
    max_len: int = 200,
) -> None:
    if isinstance(exc, AgentRequestError):
        raise HTTPException(status_code=agent_error_status(exc, default_status=default_status), detail=short_error_message(str(exc), max_len))
    raise HTTPException(status_code=default_status, detail=short_error_message(str(exc), max_len))


async def schedule_retry(
    err_msg: str,
    *,
    format_apply_error_fn: Callable[[str], str],
    merge_apply_state_fn: Callable[..., Awaitable[dict[str, Any]]],
    next_retry_iso_fn: Callable[[], str | None],
    now_utc_iso_fn: Callable[[], str],
    logger: Any,
) -> None:
    message = format_apply_error_fn(err_msg)
    await merge_apply_state_fn(
        pending=True,
        last_error=message,
        last_attempt=now_utc_iso_fn(),
        next_retry=next_retry_iso_fn(),
    )
    logger.warning("Connector apply deferred: %s", message)


async def mark_apply_success(
    *,
    merge_apply_state_fn: Callable[..., Awaitable[dict[str, Any]]],
    now_utc_iso_fn: Callable[[], str],
) -> None:
    timestamp = now_utc_iso_fn()
    await merge_apply_state_fn(
        pending=False,
        last_error=None,
        last_attempt=timestamp,
        last_success=timestamp,
        next_retry=None,
    )


async def connector_retry_worker(
    *,
    apply_retry_seconds: int,
    get_apply_state_fn: Callable[[], Awaitable[dict[str, Any]]],
    apply_connector_config_fn: Callable[..., Awaitable[Any]],
    merge_apply_state_fn: Callable[..., Awaitable[dict[str, Any]]],
    format_apply_error_fn: Callable[[str], str],
    now_utc_iso_fn: Callable[[], str],
    next_retry_iso_fn: Callable[[], str | None],
    logger: Any,
) -> None:
    if apply_retry_seconds <= 0:
        logger.info("Connector retry worker disabled (APPLY_RETRY_SECONDS=%s)", apply_retry_seconds)
        return
    await asyncio.sleep(apply_retry_seconds)
    while True:
        state = await get_apply_state_fn()
        if state.get("pending"):
            try:
                await apply_connector_config_fn(allow_defer=False)
            except Exception as exc:  # noqa: BLE001
                await merge_apply_state_fn(
                    pending=True,
                    last_error=format_apply_error_fn(str(exc)),
                    last_attempt=now_utc_iso_fn(),
                    next_retry=next_retry_iso_fn(),
                )
                logger.warning("Retrying connector apply failed: %s", exc)
            else:
                logger.info("Deferred connector apply succeeded on retry")
        await asyncio.sleep(apply_retry_seconds)


class DeferredApplyError(RuntimeError):
    """Raised when connector apply is deferred for a retry."""


def is_transient_status(
    status_code: int,
    message: str | None,
    *,
    transient_http_codes: set[int],
    transient_error_markers: tuple[str, ...],
) -> bool:
    if status_code in transient_http_codes:
        return True
    if status_code == 400 and message:
        lowered = message.lower()
        return any(marker in lowered for marker in transient_error_markers)
    if status_code == 409:
        return True
    return False


def is_transient_request_error(
    exc: Exception,
    *,
    transient_error_markers: tuple[str, ...],
) -> bool:
    lowered = str(exc).lower()
    return any(marker in lowered for marker in transient_error_markers)


async def connect_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    allow_defer: bool,
    schedule_retry_fn: Callable[[str], Awaitable[None]],
    short_error_message: Callable[[str, int], str],
    extract_error_message_fn: Callable[[Any], str],
    is_transient_status_fn: Callable[[int, str | None], bool],
    is_transient_request_error_fn: Callable[[Exception], bool],
    json_payload: dict | None = None,
    ok_statuses: tuple[int, ...] = (200, 201, 202, 204),
) -> Any:
    try:
        resp = await client.request(method, url, json=json_payload)
    except httpx.RequestError as exc:
        if allow_defer and is_transient_request_error_fn(exc):
            await schedule_retry_fn(str(exc))
            raise DeferredApplyError(short_error_message(str(exc), 180)) from exc
        raise RuntimeError(f"{method} {url} fehlgeschlagen: {exc}") from exc

    if resp.status_code not in ok_statuses:
        message = extract_error_message_fn(resp)
        if allow_defer and is_transient_status_fn(resp.status_code, message):
            await schedule_retry_fn(message)
            raise DeferredApplyError(short_error_message(message, 180))
        raise RuntimeError(
            f"{method} {url} -> {resp.status_code}: {message.strip() or 'Unbekannter Fehler'}"
        )
    return resp


async def ensure_connector(
    client: httpx.AsyncClient,
    *,
    name: str,
    config: dict[str, Any],
    allow_defer: bool,
    connect_base_url: str,
    connect_request_fn: Callable[..., Awaitable[Any]],
) -> None:
    url_base = f"{connect_base_url}/connectors/{name}"
    resp = await connect_request_fn(
        client,
        "GET",
        url_base,
        allow_defer=allow_defer,
        ok_statuses=(200, 404),
    )
    if resp.status_code == 200:
        await connect_request_fn(
            client,
            "PUT",
            f"{url_base}/pause",
            allow_defer=allow_defer,
            ok_statuses=(200, 202, 204, 409),
        )
        await connect_request_fn(
            client,
            "PUT",
            f"{url_base}/config",
            json_payload=config,
            allow_defer=allow_defer,
        )
        await connect_request_fn(
            client,
            "PUT",
            f"{url_base}/resume",
            allow_defer=allow_defer,
            ok_statuses=(200, 202, 204, 409),
        )
        return
    await connect_request_fn(
        client,
        "POST",
        f"{connect_base_url}/connectors",
        json_payload={"name": name, "config": config},
        allow_defer=allow_defer,
        ok_statuses=(200, 201, 202),
    )


async def delete_connector_if_exists(
    client: httpx.AsyncClient,
    *,
    name: str,
    allow_defer: bool,
    connect_base_url: str,
    connect_request_fn: Callable[..., Awaitable[Any]],
) -> None:
    await connect_request_fn(
        client,
        "DELETE",
        f"{connect_base_url}/connectors/{name}",
        allow_defer=allow_defer,
        ok_statuses=(200, 202, 204, 404),
    )


async def apply_source_replication_config(
    settings: dict[str, Any],
    secrets: dict[str, str],
    *,
    build_source_replication_payload_fn: Callable[[dict[str, Any], dict[str, str]], dict[str, Any] | None],
    update_agent_request_fn: Callable[..., Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    payload = build_source_replication_payload_fn(settings, secrets)
    if payload is None:
        return await update_agent_request_fn("POST", "/api/v1/mirror-replication/disable", timeout=25)
    return await update_agent_request_fn(
        "POST",
        "/api/v1/mirror-replication/apply",
        json_payload=payload,
        timeout=35,
    )


async def source_replication_status_snapshot(
    *,
    update_agent_request_fn: Callable[..., Awaitable[dict[str, Any]]],
) -> dict[str, Any]:
    return await update_agent_request_fn("GET", "/api/v1/mirror-replication/status", timeout=12)


def write_mirror_maker_config(
    settings: dict[str, Any],
    secrets: dict[str, Any],
    *,
    confluent_bootstrap_default: str,
    stream_target_prefix: str,
    mm2_internal_replication_factor: int,
    mm2_offset_storage_partitions: int,
    mm2_status_storage_partitions: int,
    mm2_state_topic_prefix: str,
    config_path: Path,
    atomic_write_text_fn: Callable[..., Any],
    secret_file_mode: int,
    secrets_file_uid: int | None,
    secrets_file_gid: int | None,
    escape_jaas_fn: Callable[[str], str],
) -> None:
    confluent_bootstrap = settings.get("confluent_bootstrap") or secrets.get("confluent_bootstrap") or confluent_bootstrap_default
    sasl_user = secrets.get("confluent_sasl_username")
    sasl_password = secrets.get("confluent_sasl_password")
    if not (confluent_bootstrap and sasl_user and sasl_password):
        raise RuntimeError("MirrorMaker benötigt gültige Confluent Zugangsdaten (Bootstrap, API Key & Secret).")
    mm2_rf = str(mm2_internal_replication_factor)
    config_lines = [
        "clusters = local,remote",
        "local.bootstrap.servers = redpanda:9092",
        f"remote.bootstrap.servers = {confluent_bootstrap}",
        "local.security.protocol = PLAINTEXT",
        "remote.security.protocol = SASL_SSL",
        "remote.sasl.mechanism = PLAIN",
        (
            "remote.sasl.jaas.config = org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{escape_jaas_fn(sasl_user)}' password='{escape_jaas_fn(sasl_password)}';"
        ),
        "remote.ssl.endpoint.identification.algorithm = https",
        "local->remote.enabled = true",
        "remote->local.enabled = false",
        f"local->remote.topics = {stream_target_prefix}.*",
        "emit.heartbeats.enabled = false",
        "emit.checkpoints.enabled = false",
        "sync.group.offsets.enabled = false",
        "refresh.groups.enabled = false",
        "emit.heartbeats.interval.seconds = -1",
        "local->remote.emit.heartbeats.enabled = false",
        "local->remote.emit.checkpoints.enabled = false",
        "local->remote.sync.group.offsets.enabled = false",
        "local->remote.refresh.groups.enabled = false",
        "local->remote.emit.heartbeats.interval.seconds = -1",
        "remote->local.emit.heartbeats.enabled = false",
        "remote->local.emit.checkpoints.enabled = false",
        "remote->local.sync.group.offsets.enabled = false",
        "remote->local.refresh.groups.enabled = false",
        "remote->local.emit.heartbeats.interval.seconds = -1",
        f"offset.storage.topic = {mm2_state_topic_prefix}_offsets",
        f"offset.storage.partitions = {mm2_offset_storage_partitions}",
        f"offset.storage.replication.factor = {mm2_rf}",
        "offset.storage.cluster.alias = local",
        f"config.storage.topic = {mm2_state_topic_prefix}_configs",
        f"config.storage.replication.factor = {mm2_rf}",
        "config.storage.cluster.alias = local",
        f"status.storage.topic = {mm2_state_topic_prefix}_status",
        f"status.storage.partitions = {mm2_status_storage_partitions}",
        f"status.storage.replication.factor = {mm2_rf}",
        "status.storage.cluster.alias = local",
        f"checkpoint.topic.replication.factor = {mm2_rf}",
        f"heartbeats.topic.replication.factor = {mm2_rf}",
        f"offset.syncs.topic.replication.factor = {mm2_rf}",
        f"replication.factor = {mm2_rf}",
        "replication.policy.class = org.apache.kafka.connect.mirror.IdentityReplicationPolicy",
        "tasks.max = 1",
        "sync.topic.acls.enabled = false",
        "refresh.topics.interval.seconds = 30",
    ]
    atomic_write_text_fn(
        config_path,
        "\n".join(config_lines) + "\n",
        mode=secret_file_mode,
        uid=secrets_file_uid,
        gid=secrets_file_gid,
    )


def build_backup_sink_config(
    settings: dict[str, Any],
    secrets: dict[str, Any],
    *,
    backup_connector_name: str,
) -> dict[str, Any]:
    backup_password = secrets.get("backup_pg_password", "")
    if not backup_password:
        raise ValueError("Backup-Datenbank Passwort fehlt in secrets.properties (backup_pg_password).")
    host = settings["backup_pg_host"]
    port = settings["backup_pg_port"]
    database = settings["backup_pg_db"]
    user = settings["backup_pg_user"]
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
    return {
        "name": backup_connector_name,
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


async def restart_mirror_maker(
    *,
    restart_container_fn: Callable[..., Awaitable[dict[str, Any]]],
    logger: Any,
) -> None:
    try:
        result = await restart_container_fn("ts-mirror-maker")
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"MirrorMaker Neustart fehlgeschlagen: {exc}") from exc
    if not result.get("found", True):
        logger.info("MirrorMaker Container ist nicht vorhanden – kein Neustart erforderlich.")


async def update_remote_replication_state(
    active: bool,
    *,
    container_status_fn: Callable[..., Awaitable[dict[str, Any]]],
    restart_mirror_maker_fn: Callable[[], Awaitable[None]],
    stop_container_fn: Callable[..., Awaitable[dict[str, Any]]],
    logger: Any,
) -> None:
    try:
        status = await container_status_fn("ts-mirror-maker")
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
            await restart_mirror_maker_fn()
        except Exception as exc:  # noqa: BLE001
            logger.error("MirrorMaker konnte nicht gestartet werden: %s", exc)
    else:
        if not has_container or not is_running:
            return
        try:
            await stop_container_fn("ts-mirror-maker")
        except Exception as exc:  # noqa: BLE001
            logger.warning("MirrorMaker konnte nicht gestoppt werden: %s", exc)


async def apply_connector_config(
    *,
    allow_defer: bool,
    ensure_offline_buffer_ready_fn: Callable[[], Awaitable[None]],
    fetch_settings_fn: Callable[[], dict[str, Any]],
    read_secrets_file_fn: Callable[[], dict[str, Any]],
    ensure_mirror_db_secret_fn: Callable[[dict[str, Any]], tuple[str, dict[str, Any]]],
    build_connector_config_fn: Callable[..., dict[str, Any]],
    build_backup_sink_config_fn: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]],
    default_connector_name: str,
    backup_connector_name: str,
    connect_base_url: str,
    ensure_connector_fn: Callable[..., Awaitable[None]],
    delete_connector_if_exists_fn: Callable[..., Awaitable[None]],
    write_mirror_maker_config_fn: Callable[[dict[str, Any], dict[str, Any]], None],
    restart_mirror_maker_fn: Callable[[], Awaitable[None]],
    mm2_config_path: Path,
    mark_apply_success_fn: Callable[[], Awaitable[None]],
) -> None:
    await ensure_offline_buffer_ready_fn()
    settings = fetch_settings_fn()
    secrets = read_secrets_file_fn()
    db_password, secrets = ensure_mirror_db_secret_fn(secrets)
    if not db_password:
        raise ValueError("Mirror-MariaDB-Passwort fehlt. Bitte TS_CONNECT_MIRROR_DB_PASSWORD setzen.")
    offline_enabled = True

    required = {
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
            "name": default_connector_name,
            "config": build_connector_config_fn(settings, offline_mode=offline_enabled),
        }
    ]
    if offline_enabled:
        connectors.append(build_backup_sink_config_fn(settings, secrets))

    async with httpx.AsyncClient(timeout=10) as client:
        for connector in connectors:
            await ensure_connector_fn(
                client,
                name=connector["name"],
                config=connector["config"],
                allow_defer=allow_defer,
            )
        if not offline_enabled:
            await delete_connector_if_exists_fn(
                client,
                name=backup_connector_name,
                allow_defer=allow_defer,
            )

    if offline_enabled:
        try:
            write_mirror_maker_config_fn(settings, secrets)
            await restart_mirror_maker_fn()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"MirrorMaker-Konfiguration fehlgeschlagen: {exc}") from exc
    else:
        if mm2_config_path.exists():
            try:
                mm2_config_path.unlink()
            except OSError:
                pass

    await mark_apply_success_fn()


async def check_connector_health(
    *,
    connect_base_url: str,
    default_connector_name: str,
    extract_error_message_fn: Callable[[Any], str],
    short_error_message: Callable[[str, int], str],
) -> dict[str, str]:
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            overview = await client.get(f"{connect_base_url}/connectors")
            if overview.status_code != 200:
                if overview.status_code == 404:
                    return {
                        "status": "error",
                        "message": "Kafka Connect REST (/connectors) antwortet mit 404 – läuft der Connect-Container?",
                    }
                return {
                    "status": "error",
                    "message": extract_error_message_fn(overview),
                }
            status_resp = await client.get(
                f"{connect_base_url}/connectors/{default_connector_name}/status"
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
                return {"status": "warn", "message": "Connector noch nicht angelegt"}
            return {
                "status": "error",
                "message": extract_error_message_fn(status_resp),
            }
        except Exception as exc:  # noqa: BLE001
            return {"status": "error", "message": short_error_message(str(exc), 140)}
