from __future__ import annotations

import json
import logging
import os
import secrets
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


def env_int_first(names: tuple[str, ...], default: int) -> int:
    for name in names:
        value = os.getenv(name)
        if value is None:
            continue
        trimmed = value.strip()
        if not trimmed:
            continue
        try:
            return int(trimmed)
        except ValueError:
            continue
    return default


def resolve_log_dir(*, data_dir: Path, env_name: str = "TS_CONNECT_LOG_DIR", default_subdir: str = "logs") -> Path:
    raw = os.getenv(env_name, "").strip()
    path = Path(raw) if raw else (data_dir / default_subdir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def configure_rotating_logger(
    logger: logging.Logger,
    log_file: Path,
    *,
    max_bytes: int,
    backup_count: int,
    level: int = logging.INFO,
    formatter: logging.Formatter | None = None,
) -> None:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=level)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    target_path = str(log_file.resolve())
    for handler in logger.handlers:
        if isinstance(handler, RotatingFileHandler) and getattr(handler, "baseFilename", "") == target_path:
            logger.setLevel(level)
            return
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max(max_bytes, 1024),
        backupCount=max(backup_count, 1),
        encoding="utf-8",
    )
    file_handler.setFormatter(
        formatter or logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logger.addHandler(file_handler)
    logger.setLevel(level)


def _rotate_plain_log_file(path: Path, backup_count: int) -> None:
    max_backups = max(backup_count, 1)
    oldest = path.with_name(f"{path.name}.{max_backups}")
    if oldest.exists():
        oldest.unlink()
    for index in range(max_backups - 1, 0, -1):
        source = path.with_name(f"{path.name}.{index}")
        if source.exists():
            source.replace(path.with_name(f"{path.name}.{index + 1}"))
    if path.exists():
        path.replace(path.with_name(f"{path.name}.1"))


def append_rotating_line(path: Path, line: str, *, max_bytes: int, backup_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (line.rstrip("\n") + "\n").encode("utf-8")
    if path.exists():
        current_size = path.stat().st_size
        if current_size + len(payload) > max(max_bytes, 1024):
            _rotate_plain_log_file(path, backup_count)
    with path.open("ab") as handle:
        handle.write(payload)


def append_rotating_json_line(path: Path, payload: dict[str, Any], *, max_bytes: int, backup_count: int) -> None:
    append_rotating_line(
        path,
        json.dumps(payload, ensure_ascii=False),
        max_bytes=max_bytes,
        backup_count=backup_count,
    )


def make_operation_id(prefix: str) -> str:
    trimmed = "".join(ch for ch in (prefix or "").strip().lower() if ch.isalnum()) or "op"
    return f"{trimmed}-{secrets.token_hex(4)}"


def format_operation_message(message: str, *, operation_id: str | None = None) -> str:
    text = (message or "").strip()
    if operation_id:
        return f"[op={operation_id}] {text}"
    return text
