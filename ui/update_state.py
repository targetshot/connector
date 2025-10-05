from __future__ import annotations

import json
from pathlib import Path
from typing import Any
import threading


def _default_state() -> dict[str, Any]:
    return {
        "status": "idle",
        "update_in_progress": False,
        "current_action": None,
        "last_error": None,
        "last_success": None,
        "last_check": None,
        "last_check_error": None,
        "latest_release": None,
        "update_target": None,
        "log": [],
        "job_started": None,
        "auto_update_enabled": False,
        "auto_update_hour": 1,
        "auto_update_last_run": None,
    }


class UpdateStateManager:
    def __init__(self, path: Path, *, log_limit: int = 400) -> None:
        self.path = path
        self.log_limit = log_limit
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return _default_state()
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return _default_state()
        state = _default_state()
        state.update({k: data.get(k) for k in state.keys()})
        if not isinstance(state.get("log"), list):
            state["log"] = []
        return state

    def write(self, state: dict[str, Any]) -> dict[str, Any]:
        tmp_path = self.path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp_path.replace(self.path)
        return state

    def merge(self, *, log_append: list[str] | None = None, log_reset: bool = False, **updates: Any) -> dict[str, Any]:
        with self._lock:
            state = self.read()
            if log_reset:
                state["log"] = []
            if log_append:
                log = state.get("log")
                if not isinstance(log, list):
                    log = []
                log.extend(log_append)
                if len(log) > self.log_limit:
                    log = log[-self.log_limit :]
                state["log"] = log
            for key, value in updates.items():
                state[key] = value
            return self.write(state)

    def ensure(self) -> dict[str, Any]:
        with self._lock:
            state = self.read()
            return self.write(state)


__all__ = ["UpdateStateManager"]
