from __future__ import annotations

import os
from pathlib import Path

HOST_AGENT_TOKEN_FILENAME = "host-agent.token"


def get_host_agent_token(data_dir: Path) -> str:
    """Return the shared host-agent token, falling back to a file inside data/."""
    env_value = os.getenv("TS_CONNECT_HOST_AGENT_TOKEN", "").strip()
    if env_value:
        return env_value
    token_path = data_dir / HOST_AGENT_TOKEN_FILENAME
    if not token_path.exists():
        return ""
    try:
        token = token_path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""
    return token


__all__ = ["get_host_agent_token", "HOST_AGENT_TOKEN_FILENAME"]
