from __future__ import annotations

import os
from pathlib import Path


def fsync_directory(path: Path) -> None:
    """Best-effort fsync to persist metadata updates inside parent directory."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        dir_fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(dir_fd)
    except OSError:
        pass
    finally:
        os.close(dir_fd)


def tmp_path_for(path: Path) -> Path:
    return path.with_name(f".{path.name}.tmp")


def atomic_write_text(path: Path, data: str, *, mode: int | None = None) -> None:
    directory = path.parent
    tmp_path = tmp_path_for(path)
    directory.mkdir(parents=True, exist_ok=True)
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
    if mode is not None:
        os.chmod(path, mode)
    fsync_directory(directory)


__all__ = ["fsync_directory", "tmp_path_for", "atomic_write_text"]
