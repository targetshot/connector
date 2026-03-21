from __future__ import annotations

import argparse
import base64
import logging
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterable

from log_utils import configure_rotating_logger, env_int_first, format_operation_message, resolve_log_dir
from update_state import UpdateStateManager

logger = logging.getLogger("ts-update-runner")
UPDATE_OPERATION_ID = (os.getenv("TS_CONNECT_UPDATE_OPERATION_ID") or "").strip() or None


class CommandError(RuntimeError):
    pass


def _env_truthy(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _registry_from_image(image: str | None) -> str | None:
    if not image or "/" not in image:
        return None
    candidate = image.split("/", 1)[0]
    if "." in candidate or ":" in candidate:
        return candidate
    return None


def _docker_login(registry: str, username: str, password: str, *, cwd: Path, manager: UpdateStateManager) -> None:
    if not registry or not username or not password:
        return
    message = f"Docker Login bei {registry}"
    manager.merge(log_append=[message], current_action="Docker Login")
    logger.info(format_operation_message(message, operation_id=UPDATE_OPERATION_ID))
    cmd = ["docker", "login", registry, "-u", username, "--password-stdin"]
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        input=f"{password}\n",
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    lines: list[str] = []
    if result.stdout:
        lines.extend(line for line in result.stdout.splitlines() if line.strip())
    if result.stderr:
        lines.extend(line for line in result.stderr.splitlines() if line.strip())
    if lines:
        manager.merge(log_append=lines)
        for line in lines:
            logger.info(format_operation_message(line, operation_id=UPDATE_OPERATION_ID))
    if result.returncode != 0:
        logger.error(format_operation_message(f"Docker-Login fehlgeschlagen bei {registry}", operation_id=UPDATE_OPERATION_ID))
        raise CommandError("Docker-Login fehlgeschlagen")


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TargetShot Connect Update Runner")
    parser.add_argument("--workspace", default=os.getenv("TS_CONNECT_WORKSPACE", "/workspace"))
    parser.add_argument("--data-dir", default=os.getenv("TS_CONNECT_DATA_DIR", "/app/data"))
    parser.add_argument("--ref", default=os.getenv("TS_CONNECT_UPDATE_REF"))
    parser.add_argument("--compose-env", default=os.getenv("TS_CONNECT_UPDATE_COMPOSE_ENV"))
    parser.add_argument("--repo-slug", default=os.getenv("TS_CONNECT_GITHUB_REPO"))
    return parser.parse_args()


def _cmd_to_str(cmd: Iterable[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def _command_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("GIT_TERMINAL_PROMPT", "0")
    if "COMPOSE_PROJECT_NAME" not in env:
        env["COMPOSE_PROJECT_NAME"] = os.getenv("COMPOSE_PROJECT_NAME", "ts-connect")
    return env


def _origin_remote_url(workspace: Path) -> str | None:
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=str(workspace),
        text=True,
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None


def _github_auth_git_prefix(workspace: Path) -> list[str]:
    token = (os.getenv("TS_CONNECT_GITHUB_TOKEN") or "").strip()
    if not token:
        return ["git"]
    remote = _origin_remote_url(workspace)
    slug = _parse_repo_slug(remote or "")
    if not slug:
        return ["git"]
    remote_lc = (remote or "").lower()
    if "github.com" not in remote_lc:
        return ["git"]
    basic = base64.b64encode(f"x-access-token:{token}".encode("utf-8")).decode("ascii")
    return [
        "git",
        "-c",
        f"http.https://github.com/.extraheader=AUTHORIZATION: basic {basic}",
    ]


def _run_command(cmd: list[str], *, cwd: Path, manager: UpdateStateManager) -> str:
    command_line = f"$ {_cmd_to_str(cmd)}"
    manager.merge(log_append=[command_line])
    logger.info(format_operation_message(command_line, operation_id=UPDATE_OPERATION_ID))
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_command_env(),
    )
    output_lines: list[str] = []
    assert process.stdout is not None
    for line in process.stdout:
        line = line.rstrip()
        output_lines.append(line)
        if line:
            manager.merge(log_append=[line])
            logger.info(format_operation_message(line, operation_id=UPDATE_OPERATION_ID))
    return_code = process.wait()
    if return_code != 0:
        detail = ""
        for line in reversed(output_lines):
            stripped = line.strip()
            if stripped:
                detail = stripped
                break
        logger.error(
            format_operation_message(
                f"Befehl fehlgeschlagen ({return_code}): {_cmd_to_str(cmd)}",
                operation_id=UPDATE_OPERATION_ID,
            )
        )
        if detail:
            raise CommandError(f"Befehl fehlgeschlagen ({return_code}): {_cmd_to_str(cmd)} | {detail}")
        raise CommandError(f"Befehl fehlgeschlagen ({return_code}): {_cmd_to_str(cmd)}")
    return "\n".join(output_lines)


def _inspect_container_details(name: str, *, cwd: Path) -> tuple[str | None, str | None]:
    result = subprocess.run(
        [
            "docker",
            "inspect",
            name,
            "--format",
            "{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{else}}{{end}}",
        ],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    if result.returncode != 0:
        return (None, None)
    raw = result.stdout.strip()
    if not raw:
        return (None, None)
    status, _, health = raw.partition("|")
    return (status or None, health or None)


def _inspect_container_state(name: str, *, cwd: Path) -> str | None:
    status, health = _inspect_container_details(name, cwd=cwd)
    return health or status


def _wait_for_container_state(
    name: str,
    *,
    expected: set[str],
    timeout_seconds: int,
    cwd: Path,
    manager: UpdateStateManager,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_state = _inspect_container_state(name, cwd=cwd)
    while time.monotonic() < deadline:
        if last_state in expected:
            manager.merge(log_append=[f"Container {name} ist {last_state}."])
            return
        time.sleep(2)
        last_state = _inspect_container_state(name, cwd=cwd)
    raise CommandError(
        f"Container {name} wurde nach {timeout_seconds}s nicht bereit "
        f"(letzter Status: {last_state or 'nicht gefunden'})"
    )


def _wait_for_container_running_stable(
    name: str,
    *,
    timeout_seconds: int,
    stable_seconds: int,
    require_healthy: bool = False,
    cwd: Path,
    manager: UpdateStateManager,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    stable_since: float | None = None
    last_status: str | None = None
    last_health: str | None = None
    while time.monotonic() < deadline:
        status, health = _inspect_container_details(name, cwd=cwd)
        last_status = status
        last_health = health
        healthy_enough = health == "healthy" if require_healthy and health is not None else health != "unhealthy"
        if status == "running" and healthy_enough:
            if stable_since is None:
                stable_since = time.monotonic()
            elif time.monotonic() - stable_since >= stable_seconds:
                health_suffix = f", health={health}" if health else ""
                manager.merge(log_append=[f"Container {name} läuft stabil (status=running{health_suffix})."])
                return
        else:
            stable_since = None
        time.sleep(2)
    details = []
    if last_status:
        details.append(f"status={last_status}")
    if last_health:
        details.append(f"health={last_health}")
    suffix = ", ".join(details) if details else "nicht gefunden"
    raise CommandError(f"Container {name} wurde nach {timeout_seconds}s nicht stabil laufend ({suffix})")


def _mirror_maker_config_active(data_dir: Path) -> bool:
    path = data_dir / "mm2.properties"
    if not path.exists():
        return False
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return False
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("remote.bootstrap.servers"):
            _key, _sep, value = stripped.partition("=")
            return bool(value.strip())
    return False


def _tail_container_logs(name: str, *, cwd: Path, tail_lines: int = 40) -> list[str]:
    result = subprocess.run(
        ["docker", "logs", "--tail", str(tail_lines), name],
        cwd=str(cwd),
        text=True,
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    lines: list[str] = []
    for source in (result.stdout, result.stderr):
        if not source:
            continue
        lines.extend(line.rstrip() for line in source.splitlines() if line.strip())
    return lines[-tail_lines:]


def _stabilize_mirror_maker_after_update(
    compose_cmd: list[str],
    *,
    workspace: Path,
    data_dir: Path,
    manager: UpdateStateManager,
) -> None:
    if not _mirror_maker_config_active(data_dir):
        manager.merge(log_append=["Überspringe MirrorMaker-Stabilisierung: keine aktive MM2-Konfiguration erkannt."])
        return

    attempts = (
        "Starte MirrorMaker nach den Kern-Healthchecks gezielt neu.",
        "Erster MirrorMaker-Start blieb instabil. Ein einmaliger Wiederherstellungsversuch wird ausgeführt.",
    )
    last_error: CommandError | None = None

    for message in attempts:
        manager.merge(log_append=[message], current_action="MirrorMaker stabilisieren")
        _run_command(compose_cmd + ["up", "-d", "--force-recreate", "mirror-maker"], cwd=workspace, manager=manager)
        try:
            _wait_for_container_running_stable(
                "ts-mirror-maker",
                timeout_seconds=180,
                stable_seconds=20,
                require_healthy=True,
                cwd=workspace,
                manager=manager,
            )
            return
        except CommandError as exc:
            last_error = exc
            log_lines = _tail_container_logs("ts-mirror-maker", cwd=workspace)
            if log_lines:
                manager.merge(
                    log_append=["MirrorMaker Diagnose (letzte Logzeilen):", *log_lines],
                    current_action="MirrorMaker Diagnose",
                )

    message = str(last_error) if last_error else "MirrorMaker Stabilisierung nach Update fehlgeschlagen"
    raise CommandError(f"MirrorMaker Stabilisierung nach Update fehlgeschlagen: {message}")


def _ensure_workspace(workspace: Path) -> None:
    if not workspace.exists():
        raise RuntimeError(f"Arbeitsverzeichnis nicht gefunden: {workspace}")
    if not (workspace / ".git").exists():
        raise RuntimeError(f"Kein Git-Repository unter {workspace}")


def _parse_repo_slug(remote_url: str) -> str | None:
    remote_url = remote_url.strip()
    if remote_url.endswith(".git"):
        remote_url = remote_url[:-4]
    prefixes = (
        "git@github.com:",
        "https://github.com/",
        "http://github.com/",
        "ssh://git@github.com/",
    )
    for prefix in prefixes:
        if remote_url.startswith(prefix):
            return remote_url[len(prefix) :]
    return None


def _ensure_https_remote(workspace: Path, manager: UpdateStateManager) -> None:
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=str(workspace),
        text=True,
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    if result.returncode != 0:
        manager.merge(log_append=["Warnung: git remote get-url origin fehlgeschlagen"], current_action="Git Remote prüfen")
        return
    remote = result.stdout.strip()
    slug = _parse_repo_slug(remote)
    if slug and remote.startswith("git@github.com:"):
        https_url = f"https://github.com/{slug}.git"
        manager.merge(log_append=[f"Setze Remote auf {https_url}"], current_action="Git Remote setzen")
        _run_command(["git", "remote", "set-url", "origin", https_url], cwd=workspace, manager=manager)


def _ensure_clean_repo(workspace: Path, manager: UpdateStateManager) -> None:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=str(workspace),
        text=True,
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError("Git-Status konnte nicht geprüft werden")
    if result.stdout.strip():
        manager.merge(log_append=["Arbeitsverzeichnis enthält lokale Änderungen:"])
        for line in result.stdout.splitlines():
            manager.merge(log_append=[line])
        raise RuntimeError("Bitte lokale Änderungen committen oder verwerfen, bevor ein Update ausgeführt wird")


def _detect_branch(workspace: Path) -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=str(workspace),
        text=True,
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    if result.returncode != 0:
        return None
    branch = result.stdout.strip()
    if branch == "HEAD" or not branch:
        return None
    return branch


def _current_commit(workspace: Path) -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=str(workspace),
        text=True,
        capture_output=True,
        env=_command_env(),
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _compose_base(compose_env: str | None) -> list[str]:
    base = ["docker", "compose"]
    if compose_env:
        base += ["--env-file", compose_env]
    return base


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return values
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def _require_host_workspace_path(*, workspace: Path, compose_env: str | None) -> Path | None:
    if workspace != Path("/workspace"):
        return None
    if not compose_env:
        raise RuntimeError(
            "TS_CONNECT_WORKSPACE_HOST fehlt. Setze in .env einen absoluten Host-Pfad wie /opt/ts-connect, "
            "damit In-App-Updates Docker-Bind-Mounts korrekt auflösen."
        )
    env_path = workspace / compose_env
    env_values = _parse_env_file(env_path)
    raw_value = env_values.get("TS_CONNECT_WORKSPACE_HOST", "").strip()
    if not raw_value:
        raise RuntimeError(
            "TS_CONNECT_WORKSPACE_HOST fehlt in .env. Setze einen absoluten Host-Pfad wie /opt/ts-connect, "
            "damit In-App-Updates Docker-Bind-Mounts korrekt auflösen."
        )
    host_path = Path(raw_value)
    if not host_path.is_absolute():
        raise RuntimeError(
            f"TS_CONNECT_WORKSPACE_HOST muss absolut sein (aktueller Wert: {raw_value}). "
            "Beispiel: /opt/ts-connect"
        )
    return host_path


def run_update() -> int:
    args = _parse_args()
    workspace = Path(args.workspace).resolve()
    data_dir = Path(args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    log_dir = resolve_log_dir(data_dir=data_dir)
    update_runner_log_file = log_dir / "update-runner.log"
    log_max_bytes = max(
        env_int_first(("TS_CONNECT_LOG_MAX_BYTES", "TS_CONNECT_UI_LOG_MAX_BYTES"), 5 * 1024 * 1024),
        1024,
    )
    log_backup_count = max(
        env_int_first(("TS_CONNECT_LOG_BACKUP_COUNT", "TS_CONNECT_UI_LOG_BACKUP_COUNT"), 5),
        1,
    )
    configure_rotating_logger(
        logger,
        update_runner_log_file,
        max_bytes=log_max_bytes,
        backup_count=log_backup_count,
        level=logging.INFO,
    )
    compose_env = None
    candidates: list[str] = []
    if args.compose_env:
        candidates.append(args.compose_env)
    candidates.append(".env")
    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if (workspace / candidate).exists():
            compose_env = candidate
            break
    manager = UpdateStateManager(data_dir / "update_state.json")
    manager.ensure()
    start_ts = _now_iso()
    logger.info(format_operation_message(f"Update-Runner gestartet um {start_ts}", operation_id=UPDATE_OPERATION_ID))
    manager.merge(
        status="running",
        update_in_progress=True,
        current_action="Update gestartet",
        last_error=None,
        job_started=start_ts,
        log_append=[f"Update-Runner gestartet um {start_ts}", f"Workspace: {workspace}"],
    )
    try:
        _ensure_workspace(workspace)
        try:
            manager.merge(log_append=["Konfiguriere Git safe.directory"], current_action="Git konfigurieren")
            _run_command(
                ["git", "config", "--global", "--add", "safe.directory", str(workspace)],
                cwd=workspace,
                manager=manager,
            )
        except CommandError:
            manager.merge(log_append=["Warnung: safe.directory konnte nicht gesetzt werden"], current_action="Git konfigurieren")
        manager.merge(log_append=["Prüfe Git-Status"], current_action="Prüfe Repository")
        _ensure_clean_repo(workspace, manager)
        _ensure_https_remote(workspace, manager)
        manager.merge(log_append=["Hole Git-Updates"], current_action="Git fetch")
        git_cmd = _github_auth_git_prefix(workspace)
        _run_command(git_cmd + ["fetch", "--all", "--tags", "--prune"], cwd=workspace, manager=manager)
        target_ref = args.ref
        if target_ref:
            manager.merge(log_append=[f"Wechsle auf {target_ref}"], current_action=f"Checkout {target_ref}")
            try:
                _run_command(["git", "checkout", target_ref], cwd=workspace, manager=manager)
            except CommandError:
                _run_command(["git", "checkout", f"tags/{target_ref}"], cwd=workspace, manager=manager)
            manager.merge(update_target=target_ref)
        else:
            branch = _detect_branch(workspace) or "main"
            manager.merge(log_append=[f"Aktualisiere Branch {branch}"], current_action=f"Pull {branch}", update_target=branch)
            _run_command(["git", "checkout", branch], cwd=workspace, manager=manager)
            _run_command(git_cmd + ["pull", "--ff-only", "origin", branch], cwd=workspace, manager=manager)
        commit = _current_commit(workspace)
        if commit:
            manager.merge(log_append=[f"Neuer Commit: {commit}"])
        prefer_local_build = _env_truthy("TS_CONNECT_UPDATE_BUILD_LOCAL")
        if prefer_local_build:
            manager.merge(log_append=["Baue neue Container"], current_action="docker compose build")
        else:
            manager.merge(log_append=["Lade Container-Images"], current_action="docker compose pull")
        compose_cmd = _compose_base(compose_env)
        _require_host_workspace_path(workspace=workspace, compose_env=compose_env)
        if not prefer_local_build:
            registry = os.getenv("TS_CONNECT_ACR_REGISTRY") or _registry_from_image(os.getenv("TS_CONNECT_UI_IMAGE"))
            username = os.getenv("TS_CONNECT_ACR_USERNAME", "").strip()
            password = os.getenv("TS_CONNECT_ACR_PASSWORD", "")
            if registry and username and password:
                try:
                    _docker_login(registry, username, password, cwd=workspace, manager=manager)
                except CommandError as exc:  # noqa: PERF203
                    raise RuntimeError(str(exc)) from exc
            _run_command(compose_cmd + ["pull"], cwd=workspace, manager=manager)
        if prefer_local_build:
            _run_command(compose_cmd + ["build", "--pull"], cwd=workspace, manager=manager)
        manager.merge(
            log_append=["Aktualisiere Haupt-Stack im laufenden Betrieb ohne globales compose down"],
            current_action="docker compose up",
        )
        _run_command(compose_cmd + ["up", "-d"], cwd=workspace, manager=manager)
        manager.merge(
            log_append=["Prüfe Healthchecks von Schema Registry, Kafka Connect, UI und MirrorMaker"],
            current_action="Container-Health prüfen",
        )
        _wait_for_container_state(
            "ts-schema-registry",
            expected={"healthy"},
            timeout_seconds=120,
            cwd=workspace,
            manager=manager,
        )
        _wait_for_container_state(
            "ts-kafka-connect",
            expected={"healthy"},
            timeout_seconds=120,
            cwd=workspace,
            manager=manager,
        )
        _wait_for_container_state(
            "ts-connect-ui",
            expected={"healthy"},
            timeout_seconds=120,
            cwd=workspace,
            manager=manager,
        )
        _stabilize_mirror_maker_after_update(
            compose_cmd,
            workspace=workspace,
            data_dir=data_dir,
            manager=manager,
        )
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        logger.exception(format_operation_message(f"Update-Runner fehlgeschlagen: {message}", operation_id=UPDATE_OPERATION_ID))
        manager.merge(
            status="error",
            update_in_progress=False,
            current_action=None,
            last_error=message,
            log_append=[f"FEHLER: {message}"],
        )
        return 1
    success_ts = _now_iso()
    logger.info(format_operation_message(f"Update-Runner erfolgreich abgeschlossen um {success_ts}", operation_id=UPDATE_OPERATION_ID))
    manager.merge(
        status="idle",
        update_in_progress=False,
        current_action=None,
        last_success=success_ts,
        log_append=[f"Update abgeschlossen um {success_ts}"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(run_update())
