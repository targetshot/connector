from __future__ import annotations

import os
import secrets
import stat
import time
from pathlib import Path

from file_utils import fsync_directory

UPDATE_AGENT_TOKEN_FILENAME = "update-agent.token"


def get_update_agent_token(data_dir: Path) -> str:
    """Load or create the shared token used to talk to the update agent."""
    env_value = os.getenv("TS_CONNECT_UPDATE_AGENT_TOKEN", "").strip()
    if env_value:
        return env_value
    token_path = data_dir / UPDATE_AGENT_TOKEN_FILENAME
    if token_path.exists():
        try:
            existing = token_path.read_text(encoding="utf-8").strip()
        except OSError:
            existing = ""
        if existing:
            return existing
    token = secrets.token_urlsafe(48)
    token_path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        fd = os.open(token_path, flags, stat.S_IRUSR | stat.S_IWUSR)
    except FileExistsError:
        for _ in range(5):
            try:
                existing = token_path.read_text(encoding="utf-8").strip()
            except OSError:
                existing = ""
            if existing:
                return existing
            time.sleep(0.1)
        return ""
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(token + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    fsync_directory(token_path.parent)
    return token


__all__ = ["get_update_agent_token", "UPDATE_AGENT_TOKEN_FILENAME"]
