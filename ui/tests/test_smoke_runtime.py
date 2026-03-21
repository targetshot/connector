import ast
import asyncio
import copy
import pathlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from unittest import mock

from ui.security_bootstrap import UiSecurityBootstrap


APP_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app.py"
OPS_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "operations_runtime.py"


def _load_items(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = APP_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in wanted:
            item = copy.deepcopy(node)
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                item.decorator_list = []
            item_module = ast.Module(body=[item], type_ignores=[])
            exec(compile(item_module, str(APP_SOURCE), "exec"), namespace)  # noqa: S102


def _load_ops_items(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = OPS_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name in wanted:
            item = copy.deepcopy(node)
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                item.decorator_list = []
            item_module = ast.Module(body=[item], type_ignores=[])
            exec(compile(item_module, str(OPS_SOURCE), "exec"), namespace)  # noqa: S102


class TsConnectRuntimeSmokeTest(unittest.TestCase):
    def test_admin_password_roundtrip_verifies_successfully(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            def atomic_write_text(path, data, *, mode=None, uid=None, gid=None):
                path.write_text(data, encoding="utf-8")
                if mode is not None:
                    path.chmod(mode)

            bootstrap = UiSecurityBootstrap(
                pathlib.Path(tmp_dir),
                logger=mock.Mock(),
                atomic_write_text=atomic_write_text,
                fsync_directory=lambda path: None,
            )
            bootstrap.set_admin_password("super-secret")

            self.assertTrue(bootstrap.verify_admin_password("super-secret"))
            self.assertFalse(bootstrap.verify_admin_password("wrong-secret"))

    def test_license_is_active_for_future_activated_key(self):
        namespace = {
            "datetime": datetime,
            "timezone": timezone,
        }
        _load_items(
            [
                "_normalize_provider_status",
                "_parse_iso8601",
                "_license_is_active",
            ],
            namespace,
        )

        result = namespace["_license_is_active"](
            {
                "license_key": "ABCDE-12345",
                "license_status": "active",
                "license_activation_id": "machine-1",
                "license_valid_until": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat(),
            }
        )

        self.assertTrue(result)


class TsConnectAsyncRuntimeSmokeTest(unittest.IsolatedAsyncioTestCase):
    async def test_health_summary_returns_operator_snapshot(self):
        writes: list[dict[str, Any]] = []

        async def database_health():
            return {"status": "ok", "message": "Datenbank ok"}

        async def confluent_health():
            return {"status": "ok", "message": "Kafka ok"}

        async def connector_health():
            return {"status": "ok", "message": "Connector läuft"}

        async def backup_health():
            return {"status": "ok", "message": "Backup ok"}

        async def license_health():
            return {"status": "ok", "message": "Lizenz aktiv"}

        namespace = {
            "asyncio": asyncio,
            "_check_database_health": database_health,
            "_check_confluent_health": confluent_health,
            "_check_connector_health": connector_health,
            "_check_backup_health": backup_health,
            "_check_license_health": license_health,
            "_now_utc_iso": lambda: "2026-03-20T12:00:00+00:00",
            "_write_json_log": lambda filename, snapshot: writes.append(
                {"filename": filename, "snapshot": dict(snapshot)}
            ),
            "logger": mock.Mock(),
        }
        _load_items(["health_summary"], namespace)

        result = await namespace["health_summary"]()

        self.assertEqual(result["connector"], {"status": "ok", "message": "Connector läuft"})
        self.assertEqual(result["license"], {"status": "ok", "message": "Lizenz aktiv"})
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]["filename"], "health.log")
        self.assertEqual(writes[0]["snapshot"]["timestamp"], "2026-03-20T12:00:00+00:00")

    async def test_launch_update_job_starts_update_runner_successfully(self):
        log_calls: list[dict[str, Any]] = []
        merge_calls: list[dict[str, Any]] = []
        runner_calls: list[dict[str, Any]] = []

        async def get_update_state_snapshot():
            return {"status": "idle", "update_in_progress": False}

        async def collect_workspace_info():
            return {"workspace": "/tmp/targetshot"}

        async def detect_prerequisites(workspace_info):
            return {"ok": True}

        async def ensure_latest_release(force=False):
            return {"tag_name": "v0.4.99"}

        async def determine_repo_slug():
            return "maxhany/targetshot"

        def detect_env_file_name():
            return ".env"

        def now_utc_iso():
            return "2026-03-20T12:00:00+00:00"

        def make_operation_id(prefix):
            return f"{prefix}-abcd1234"

        def format_operation_message(message, *, operation_id=None):
            return f"[op={operation_id}] {message}" if operation_id else message

        async def append_update_log(lines, *, reset=False):
            log_calls.append({"lines": list(lines), "reset": reset})
            return {}

        async def merge_update_state_async(**updates):
            merge_calls.append(dict(updates))
            return updates

        async def start_update_runner(target_ref, repo_slug, env_file, operation_id):
            runner_calls.append(
                {
                    "target_ref": target_ref,
                    "repo_slug": repo_slug,
                    "env_file": env_file,
                    "operation_id": operation_id,
                }
            )
            return "job-123"

        namespace = {
            "Any": Any,
            "Awaitable": Awaitable,
            "Callable": Callable,
            "asyncio": asyncio,
            "AgentRequestError": RuntimeError,
            "agent_error_status": lambda exc: 503,
        }
        _load_ops_items(["launch_update_job"], namespace)

        result = await namespace["launch_update_job"](
            target_ref=None,
            initiated_by="manual",
            force_release_refresh=False,
            reset_log=True,
            update_job_lock=asyncio.Lock(),
            get_update_state_snapshot_fn=get_update_state_snapshot,
            collect_workspace_info_fn=collect_workspace_info,
            detect_prerequisites_fn=detect_prerequisites,
            ensure_latest_release_fn=ensure_latest_release,
            determine_repo_slug_fn=determine_repo_slug,
            detect_env_file_name_fn=detect_env_file_name,
            now_utc_iso_fn=now_utc_iso,
            make_operation_id_fn=make_operation_id,
            format_operation_message_fn=format_operation_message,
            append_update_log_fn=append_update_log,
            merge_update_state_async_fn=merge_update_state_async,
            start_update_runner_fn=start_update_runner,
            short_error_message=lambda text, max_len=200: text[:max_len],
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["container"], "job-123")
        self.assertEqual(result["target"], "v0.4.99")
        self.assertEqual(
            runner_calls,
            [
                {
                    "target_ref": "v0.4.99",
                    "repo_slug": "maxhany/targetshot",
                    "env_file": ".env",
                    "operation_id": "upd-abcd1234",
                }
            ],
        )
        self.assertEqual(len(log_calls), 1)
        self.assertTrue(log_calls[0]["reset"])
        self.assertTrue(log_calls[0]["lines"][0].startswith("[op=upd-abcd1234]"))
        self.assertEqual(merge_calls[0]["status"], "running")
        self.assertEqual(merge_calls[0]["operation_id"], "upd-abcd1234")
        self.assertEqual(merge_calls[-1]["current_action"], "Update-Agent gestartet")


if __name__ == "__main__":
    unittest.main()
