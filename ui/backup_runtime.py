from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from pathlib import Path


def resolve_backup_dir(raw_dir: str | None, *, workspace_path: Path, default_subdir: str) -> Path:
    path = Path(raw_dir).expanduser() if raw_dir and raw_dir.strip() else (workspace_path / default_subdir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_host_display_dir(
    raw_dir: str | None,
    *,
    workspace_host: str | None,
    default_subdir: str,
) -> str | None:
    if raw_dir and raw_dir.strip():
        return str(Path(raw_dir).expanduser())
    if workspace_host and workspace_host.strip():
        return str((Path(workspace_host).expanduser() / default_subdir).resolve())
    return None


def build_backup_filename(prefix: str, now: datetime, extension: str) -> str:
    timestamp = now.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}-{timestamp}{extension}"


def next_daily_run(*, now: datetime, hour: int, minute: int, tz: tzinfo) -> datetime:
    local_now = now.astimezone(tz)
    candidate = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= local_now:
        candidate += timedelta(days=1)
    return candidate.astimezone(timezone.utc)


def current_daily_slot_id(*, now: datetime, hour: int, minute: int, tz: tzinfo) -> str | None:
    local_now = now.astimezone(tz)
    scheduled = local_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if local_now < scheduled:
        return None
    return scheduled.strftime("%Y-%m-%dT%H:%M:%S%z")


def scheduled_run_due(
    *,
    now: datetime,
    last_slot_id: str | None,
    hour: int,
    minute: int,
    tz: tzinfo,
) -> tuple[bool, str | None, datetime]:
    slot_id = current_daily_slot_id(now=now, hour=hour, minute=minute, tz=tz)
    next_run = next_daily_run(now=now, hour=hour, minute=minute, tz=tz)
    if slot_id is None:
        return False, None, next_run
    return slot_id != last_slot_id, slot_id, next_run


def prune_backup_files(
    directory: Path,
    *,
    retention_days: int,
    now: datetime | None = None,
    prefix: str = "mirror-db-backup-",
    suffix: str = ".sql.gz",
) -> list[Path]:
    if retention_days <= 0 or not directory.exists():
        return []
    reference = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    cutoff = reference - timedelta(days=retention_days)
    removed: list[Path] = []
    for path in directory.glob(f"{prefix}*{suffix}"):
        if not path.is_file():
            continue
        modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        if modified < cutoff:
            path.unlink(missing_ok=True)
            removed.append(path)
    return removed


def list_backup_files(
    directory: Path,
    *,
    limit: int = 10,
    prefix: str = "mirror-db-backup-",
    suffix: str = ".sql.gz",
) -> list[dict[str, object]]:
    if not directory.exists():
        return []
    items: list[dict[str, object]] = []
    for path in directory.glob(f"{prefix}*{suffix}"):
        if not path.is_file():
            continue
        stat = path.stat()
        modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        items.append(
            {
                "name": path.name,
                "size_bytes": stat.st_size,
                "created_at": modified,
            }
        )
    items.sort(key=lambda item: str(item["created_at"]), reverse=True)
    return items[: max(limit, 1)]
