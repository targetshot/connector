import ast
import pathlib
import unittest
from typing import Any, Awaitable, Callable


OPS_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "operations_runtime.py"


def _load_items(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = OPS_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in wanted:
            item_module = ast.Module(body=[node], type_ignores=[])
            exec(compile(item_module, str(OPS_SOURCE), "exec"), namespace)  # noqa: S102


class AgentFailureHandlingTest(unittest.IsolatedAsyncioTestCase):
    def test_agent_error_status_prefers_explicit_status_code(self):
        namespace = {"Any": Any, "Awaitable": Awaitable, "Callable": Callable}
        _load_items(["AgentRequestError", "agent_error_status"], namespace)

        exc = namespace["AgentRequestError"]("update-agent", "conflict", status_code=409)

        status = namespace["agent_error_status"](exc)

        self.assertEqual(status, 409)

    async def test_read_update_agent_status_returns_unavailable_payload(self):
        namespace = {"Any": Any, "Awaitable": Awaitable, "Callable": Callable}
        _load_items(["AgentRequestError", "agent_error_status", "read_update_agent_status"], namespace)

        async def failing_request(*args, **kwargs):
            raise namespace["AgentRequestError"](
                "update-agent",
                "Update-Agent nicht erreichbar: timeout",
                status_code=503,
                unavailable=True,
            )

        result = await namespace["read_update_agent_status"](
            update_agent_request_fn=failing_request,
            short_error_message=lambda text, max_len=180: text[:max_len],
        )

        self.assertFalse(result["available"])
        self.assertFalse(result["running"])
        self.assertEqual(result["status_code"], 503)
        self.assertFalse(result["auth_error"])
        self.assertIn("Update-Agent nicht erreichbar", result["error"])

    async def test_read_update_agent_status_marks_auth_error_without_offline_state(self):
        namespace = {"Any": Any, "Awaitable": Awaitable, "Callable": Callable}
        _load_items(["AgentRequestError", "agent_error_status", "read_update_agent_status"], namespace)

        async def failing_request(*args, **kwargs):
            raise namespace["AgentRequestError"](
                "update-agent",
                "Update-Agent 401: Unauthorized",
                status_code=401,
                unavailable=False,
            )

        result = await namespace["read_update_agent_status"](
            update_agent_request_fn=failing_request,
            short_error_message=lambda text, max_len=180: text[:max_len],
        )

        self.assertTrue(result["available"])
        self.assertFalse(result["running"])
        self.assertEqual(result["status_code"], 401)
        self.assertTrue(result["auth_error"])

    async def test_reconcile_stale_update_state_marks_error_when_job_missing(self):
        namespace = {"Any": Any, "Awaitable": Awaitable, "Callable": Callable}
        _load_items(["reconcile_stale_update_state"], namespace)
        state = {
            "status": "running",
            "current_action": "Update läuft…",
            "last_error": None,
            "update_in_progress": True,
            "job_started": "2026-03-20T12:00:00Z",
        }

        merged = {}

        async def ensure_update_state():
            return None

        async def get_update_state_snapshot():
            if merged:
                updated = dict(state)
                updated.update(merged)
                return updated
            return dict(state)

        async def read_update_agent_status_fn():
            return {"available": True, "running": False, "job_id": None, "error": None, "status_code": 200}

        async def merge_update_state_async(**updates):
            merged.update(updates)
            return merged

        result = await namespace["reconcile_stale_update_state"](
            ensure_update_state_fn=ensure_update_state,
            get_update_state_snapshot_fn=get_update_state_snapshot,
            read_update_agent_status_fn=read_update_agent_status_fn,
            merge_update_state_async_fn=merge_update_state_async,
        )

        self.assertEqual(result["status"], "error")
        self.assertFalse(result["update_in_progress"])
        self.assertEqual(result["last_error"], "Update-Agent meldet keinen laufenden Job mehr.")

    async def test_reconcile_stale_update_state_marks_agent_unreachable_without_reset(self):
        namespace = {"Any": Any, "Awaitable": Awaitable, "Callable": Callable}
        _load_items(["reconcile_stale_update_state"], namespace)
        state = {
            "status": "running",
            "current_action": "Update-Agent gestartet",
            "last_error": None,
            "update_in_progress": True,
            "job_started": "2026-03-20T12:00:00Z",
        }

        merged = {}

        async def ensure_update_state():
            return None

        async def get_update_state_snapshot():
            if merged:
                updated = dict(state)
                updated.update(merged)
                return updated
            return dict(state)

        async def read_update_agent_status_fn():
            return {
                "available": False,
                "running": False,
                "job_id": None,
                "error": "Update-Agent nicht erreichbar: timeout",
                "status_code": 503,
            }

        async def merge_update_state_async(**updates):
            merged.update(updates)
            return merged

        result = await namespace["reconcile_stale_update_state"](
            ensure_update_state_fn=ensure_update_state,
            get_update_state_snapshot_fn=get_update_state_snapshot,
            read_update_agent_status_fn=read_update_agent_status_fn,
            merge_update_state_async_fn=merge_update_state_async,
        )

        self.assertEqual(result["status"], "running")
        self.assertEqual(result["current_action"], "Update-Agent nicht erreichbar")
        self.assertEqual(result["last_error"], "Update-Agent nicht erreichbar: timeout")


if __name__ == "__main__":
    unittest.main()
