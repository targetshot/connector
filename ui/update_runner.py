from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

from update_state import UpdateStateManager


class CommandError(RuntimeError):
    pass


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


def _run_command(cmd: list[str], *, cwd: Path, manager: UpdateStateManager) -> str:
    manager.merge(log_append=[f"$ {_cmd_to_str(cmd)}"])
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
    return_code = process.wait()
    if return_code != 0:
        raise CommandError(f"Befehl fehlgeschlagen ({return_code}): {_cmd_to_str(cmd)}")
    return "\n".join(output_lines)


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


def run_update() -> int:
    args = _parse_args()
    workspace = Path(args.workspace).resolve()
    data_dir = Path(args.data_dir).resolve()
    compose_env = args.compose_env
    if compose_env:
        compose_env_path = workspace / compose_env
        if not compose_env_path.exists():
            compose_env = None
    else:
        default_env = workspace / "compose.env"
        if default_env.exists():
            compose_env = "compose.env"
    manager = UpdateStateManager(data_dir / "update_state.json")
    manager.ensure()
    start_ts = _now_iso()
    manager.merge(
        status="running",
        update_in_progress=True,
        current_action="Update gestartet",
        last_error=None,
        job_started=start_ts,
        log_append=[f"Update-Runner gestartet um {start_ts}", f"Workspace: {workspace}"],
    )
    compose_down_called = False
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
        _run_command(["git", "fetch", "--all", "--tags", "--prune"], cwd=workspace, manager=manager)
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
            _run_command(["git", "pull", "--ff-only", "origin", branch], cwd=workspace, manager=manager)
        commit = _current_commit(workspace)
        if commit:
            manager.merge(log_append=[f"Neuer Commit: {commit}"])
        manager.merge(log_append=["Baue neue Container"], current_action="docker compose build")
        compose_cmd = _compose_base(compose_env)
        _run_command(compose_cmd + ["down", "--remove-orphans"], cwd=workspace, manager=manager)
        compose_down_called = True
        for name in (
            "ts-kafka-connect",
            "ts-connect-ui",
            "ts-redpanda",
            "ts-streams-transform",
            "workspace-kafka-connect",
            "workspace-ui",
            "workspace-redpanda-1",
            "workspace-streams-transform-1",
        ):
            try:
                _run_command(["docker", "rm", "-f", name], cwd=workspace, manager=manager)
            except CommandError:
                continue
        _run_command(compose_cmd + ["build", "--pull"], cwd=workspace, manager=manager)
        manager.merge(log_append=["Starte Dienste neu"], current_action="docker compose up")
        _run_command(compose_cmd + ["up", "-d"], cwd=workspace, manager=manager)
    except Exception as exc:  # noqa: BLE001
        message = str(exc)
        if compose_down_called:
            for name in (
                "ts-kafka-connect",
                "ts-connect-ui",
                "ts-redpanda",
                "ts-streams-transform",
                "workspace-kafka-connect",
                "workspace-ui",
                "workspace-redpanda-1",
                "workspace-streams-transform-1",
            ):
                try:
                    _run_command(["docker", "rm", "-f", name], cwd=workspace, manager=manager)
                except CommandError:
                    manager.merge(log_append=[f"Hinweis: Container {name} konnte nicht gelöscht werden"])
                    continue
        manager.merge(
            status="error",
            update_in_progress=False,
            current_action=None,
            last_error=message,
            log_append=[f"FEHLER: {message}"],
        )
        return 1
    success_ts = _now_iso()
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
