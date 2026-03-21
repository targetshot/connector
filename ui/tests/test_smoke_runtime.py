import ast
import asyncio
import copy
import gzip
import io
import os
import pathlib
import subprocess
import tempfile
import types
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from unittest import mock

from ui.security_bootstrap import UiSecurityBootstrap


APP_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app.py"
OPS_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "operations_runtime.py"
TEMPLATE_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "templates" / "index.html"


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
    def test_build_update_agent_urls_prefers_current_container_name_and_keeps_legacy_alias(self):
        namespace = {
            "os": types.SimpleNamespace(getenv=lambda name: None),
        }
        _load_items(["_build_update_agent_urls"], namespace)

        urls = namespace["_build_update_agent_urls"]()

        self.assertEqual(urls[0], "http://ts-connect-update-agent:9000")
        self.assertIn("http://update-agent:9000", urls)

    def test_license_template_no_longer_shows_shooter_or_plan_coverage_blocks(self):
        template = TEMPLATE_SOURCE.read_text(encoding="utf-8")

        self.assertNotIn("Vereins-Schützen", template)
        self.assertNotIn("Planabdeckung", template)
        self.assertNotIn("Planlimit", template)

    def test_build_keygen_machine_headers_prefers_bearer_token_when_present(self):
        namespace = {
            "KEYGEN_LICENSE_TOKEN": "token-123",
        }
        _load_items(["_build_keygen_machine_headers"], namespace)

        headers = namespace["_build_keygen_machine_headers"]("LIC-123", include_content_type=True)

        self.assertEqual(headers["Authorization"], "Bearer token-123")
        self.assertEqual(headers["Content-Type"], "application/vnd.api+json")

    def test_build_keygen_machine_headers_falls_back_to_license_auth(self):
        namespace = {
            "KEYGEN_LICENSE_TOKEN": "",
        }
        _load_items(["_build_keygen_machine_headers"], namespace)

        headers = namespace["_build_keygen_machine_headers"]("LIC-123")

        self.assertEqual(headers["Authorization"], "License LIC-123")
        self.assertNotIn("Content-Type", headers)

    def test_parse_trusted_networks_skips_invalid_and_keeps_loopback(self):
        namespace = {"ipaddress": __import__("ipaddress")}
        _load_items(["_parse_trusted_networks"], namespace)

        networks, invalid = namespace["_parse_trusted_networks"](
            ["192.168.0.0/16", "broken-cidr", "10.0.0.0/8"]
        )

        rendered = {str(network) for network in networks}
        self.assertIn("192.168.0.0/16", rendered)
        self.assertIn("10.0.0.0/8", rendered)
        self.assertIn("127.0.0.0/8", rendered)
        self.assertIn("::1/128", rendered)
        self.assertEqual(invalid, ["broken-cidr"])

    def test_mirror_dump_candidates_prefer_container_root_password_first(self):
        namespace = {
            "DEFAULT_MIRROR_DB_USER": "debezium_sync",
            "MIRROR_DB_PASSWORD": "db-password",
            "MIRROR_DB_CONTAINER_NAME": "ts-mariadb-mirror",
            "_workspace_env_values": lambda: {},
            "_inspect_container_env_sync": lambda name: {"MARIADB_ROOT_PASSWORD": "root-from-container"},
            "logger": mock.Mock(),
            "os": types.SimpleNamespace(getenv=lambda name: None),
        }
        _load_items(["_usable_secret", "_mirror_dump_candidates"], namespace)

        candidates = namespace["_mirror_dump_candidates"]()

        self.assertEqual(candidates[0], ("root", "root-from-container"))
        self.assertIn(("debezium_sync", "db-password"), candidates)

    def test_mirror_dump_candidates_ignore_placeholder_root_secret(self):
        env_values = {
            "TS_CONNECT_MIRROR_ROOT_PASSWORD": "change-me-root",
            "TS_CONNECT_MIRROR_DB_PASSWORD": "db-from-workspace",
        }
        namespace = {
            "DEFAULT_MIRROR_DB_USER": "debezium_sync",
            "MIRROR_DB_PASSWORD": "",
            "MIRROR_DB_CONTAINER_NAME": "ts-mariadb-mirror",
            "_workspace_env_values": lambda: env_values,
            "_inspect_container_env_sync": mock.Mock(side_effect=RuntimeError("inspect failed")),
            "logger": mock.Mock(),
            "os": types.SimpleNamespace(getenv=lambda name: None),
        }
        _load_items(["_usable_secret", "_mirror_dump_candidates"], namespace)

        candidates = namespace["_mirror_dump_candidates"]()

        self.assertEqual(candidates, [("debezium_sync", "db-from-workspace")])

    def test_create_mirror_backup_sync_uses_mysql_pwd_and_root_password(self):
        namespace = {
            "Any": Any,
            "Path": pathlib.Path,
            "gzip": gzip,
            "os": os,
            "subprocess": subprocess,
            "DEFAULT_MIRROR_DB_NAME": "SMDB",
            "MIRROR_DB_CONTAINER_NAME": "ts-mariadb-mirror",
            "_mirror_dump_candidates": lambda: [("root", "root-from-workspace")],
            "_tmp_path_for": lambda path: path.with_name(path.name + ".tmp"),
            "_fsync_directory": lambda path: None,
            "logger": mock.Mock(),
        }
        _load_items(
            ["_build_mirror_dump_command", "_mirror_dump_variants", "_write_mirror_dump_temp_sync", "_is_mirror_dump_auth_error", "_create_mirror_backup_sync"],
            namespace,
        )

        commands: list[list[str]] = []

        class FakeProcess:
            def __init__(self):
                self.stdout = io.BytesIO(b"-- dump --")
                self.stderr = io.BytesIO(b"")

            def wait(self, timeout=None):
                return 0

            def kill(self):
                return None

        def fake_popen(cmd, stdout=None, stderr=None):
            commands.append(list(cmd))
            return FakeProcess()

        namespace["subprocess"] = mock.Mock(Popen=fake_popen, PIPE=subprocess.PIPE)

        with tempfile.TemporaryDirectory() as tmp_dir:
            target = pathlib.Path(tmp_dir) / "mirror-db-backup.sql.gz"
            file_size = namespace["_create_mirror_backup_sync"](target)

        self.assertGreater(file_size, 0)
        self.assertEqual(commands[0][0:4], ["docker", "exec", "-e", "MYSQL_PWD=root-from-workspace"])
        self.assertIn("mariadb-dump", commands[0])
        self.assertIn("-uroot", commands[0])

    def test_build_mirror_dump_command_container_root_omits_tcp_password_flags(self):
        namespace = {
            "DEFAULT_MIRROR_DB_NAME": "SMDB",
            "MIRROR_DB_CONTAINER_NAME": "ts-mariadb-mirror",
        }
        _load_items(["_build_mirror_dump_command"], namespace)

        command = namespace["_build_mirror_dump_command"](
            "root",
            "",
            auth_mode="container-root",
        )

        self.assertEqual(command[:4], ["docker", "exec", "ts-mariadb-mirror", "mariadb-dump"])
        self.assertNotIn("MYSQL_PWD=", " ".join(command))
        self.assertNotIn("-h127.0.0.1", command)
        self.assertNotIn("-uroot", command)

    def test_create_mirror_backup_sync_retries_with_next_credentials(self):
        namespace = {
            "Any": Any,
            "Path": pathlib.Path,
            "gzip": gzip,
            "os": os,
            "subprocess": subprocess,
            "DEFAULT_MIRROR_DB_NAME": "SMDB",
            "MIRROR_DB_CONTAINER_NAME": "ts-mariadb-mirror",
            "_mirror_dump_candidates": lambda: [("root", "wrong-root"), ("debezium_sync", "db-password")],
            "_tmp_path_for": lambda path: path.with_name(path.name + ".tmp"),
            "_fsync_directory": lambda path: None,
            "logger": mock.Mock(),
        }
        _load_items(
            ["_build_mirror_dump_command", "_mirror_dump_variants", "_write_mirror_dump_temp_sync", "_is_mirror_dump_auth_error", "_create_mirror_backup_sync"],
            namespace,
        )

        commands: list[list[str]] = []

        class FakeProcess:
            def __init__(self, return_code: int, stdout: bytes, stderr: bytes) -> None:
                self.stdout = io.BytesIO(stdout)
                self.stderr = io.BytesIO(stderr)
                self._return_code = return_code

            def wait(self, timeout=None):
                return self._return_code

            def kill(self):
                return None

        def fake_popen(cmd, stdout=None, stderr=None):
            commands.append(list(cmd))
            if "MYSQL_PWD=wrong-root" in cmd:
                return FakeProcess(1, b"", b"Access denied")
            return FakeProcess(0, b"-- dump --", b"")

        namespace["subprocess"] = mock.Mock(Popen=fake_popen, PIPE=subprocess.PIPE)

        with tempfile.TemporaryDirectory() as tmp_dir:
            target = pathlib.Path(tmp_dir) / "mirror-db-backup.sql.gz"
            file_size = namespace["_create_mirror_backup_sync"](target)

        self.assertGreater(file_size, 0)
        self.assertEqual(len(commands), 2)
        self.assertIn("MYSQL_PWD=wrong-root", commands[0])
        self.assertIn("MYSQL_PWD=db-password", commands[1])

    def test_create_mirror_backup_sync_retries_reduced_non_root_dump_when_events_are_forbidden(self):
        namespace = {
            "Any": Any,
            "Path": pathlib.Path,
            "gzip": gzip,
            "os": os,
            "subprocess": subprocess,
            "DEFAULT_MIRROR_DB_NAME": "SSMDB2",
            "MIRROR_DB_CONTAINER_NAME": "ts-mariadb-mirror",
            "_mirror_dump_candidates": lambda: [("debezium_sync", "db-password")],
            "_tmp_path_for": lambda path: path.with_name(path.name + ".tmp"),
            "_fsync_directory": lambda path: None,
            "logger": mock.Mock(),
        }
        _load_items(
            ["_build_mirror_dump_command", "_mirror_dump_variants", "_write_mirror_dump_temp_sync", "_is_mirror_dump_auth_error", "_create_mirror_backup_sync"],
            namespace,
        )

        commands: list[list[str]] = []

        class FakeProcess:
            def __init__(self, return_code: int, stdout: bytes, stderr: bytes) -> None:
                self.stdout = io.BytesIO(stdout)
                self.stderr = io.BytesIO(stderr)
                self._return_code = return_code

            def wait(self, timeout=None):
                return self._return_code

            def kill(self):
                return None

        def fake_popen(cmd, stdout=None, stderr=None):
            commands.append(list(cmd))
            if "--events" in cmd:
                return FakeProcess(1, b"", b"Access denied for user 'debezium_sync' to database 'SSMDB2'")
            return FakeProcess(0, b"-- dump --", b"")

        namespace["subprocess"] = mock.Mock(Popen=fake_popen, PIPE=subprocess.PIPE)

        with tempfile.TemporaryDirectory() as tmp_dir:
            target = pathlib.Path(tmp_dir) / "mirror-db-backup.sql.gz"
            file_size = namespace["_create_mirror_backup_sync"](target)

        self.assertGreater(file_size, 0)
        self.assertGreaterEqual(len(commands), 2)
        self.assertIn("--events", commands[0])
        self.assertNotIn("--events", commands[1])

    def test_create_mirror_backup_sync_tries_container_root_after_explicit_credentials(self):
        namespace = {
            "Any": Any,
            "Path": pathlib.Path,
            "gzip": gzip,
            "os": os,
            "subprocess": subprocess,
            "DEFAULT_MIRROR_DB_NAME": "SSMDB2",
            "MIRROR_DB_CONTAINER_NAME": "ts-mariadb-mirror",
            "_mirror_dump_candidates": lambda: [("root", "wrong-root")],
            "_tmp_path_for": lambda path: path.with_name(path.name + ".tmp"),
            "_fsync_directory": lambda path: None,
            "logger": mock.Mock(),
        }
        _load_items(
            ["_build_mirror_dump_command", "_mirror_dump_variants", "_write_mirror_dump_temp_sync", "_is_mirror_dump_auth_error", "_create_mirror_backup_sync"],
            namespace,
        )

        commands: list[list[str]] = []

        class FakeProcess:
            def __init__(self, return_code: int, stdout: bytes, stderr: bytes) -> None:
                self.stdout = io.BytesIO(stdout)
                self.stderr = io.BytesIO(stderr)
                self._return_code = return_code

            def wait(self, timeout=None):
                return self._return_code

            def kill(self):
                return None

        def fake_popen(cmd, stdout=None, stderr=None):
            commands.append(list(cmd))
            if "MYSQL_PWD=wrong-root" in cmd:
                return FakeProcess(1, b"", b"Access denied")
            return FakeProcess(0, b"-- dump --", b"")

        namespace["subprocess"] = mock.Mock(Popen=fake_popen, PIPE=subprocess.PIPE)

        with tempfile.TemporaryDirectory() as tmp_dir:
            target = pathlib.Path(tmp_dir) / "mirror-db-backup.sql.gz"
            file_size = namespace["_create_mirror_backup_sync"](target)

        self.assertGreater(file_size, 0)
        self.assertEqual(len(commands), 2)
        self.assertIn("MYSQL_PWD=wrong-root", commands[0])
        self.assertEqual(commands[1][:4], ["docker", "exec", "ts-mariadb-mirror", "mariadb-dump"])

    def test_classify_recovery_issue_detects_kafka_connect_outage(self):
        namespace = {"Any": Any}
        _load_items(["_classify_recovery_issue"], namespace)

        result = namespace["_classify_recovery_issue"](
            "Connector REST nicht erreichbar: connection refused",
            operation_id="cfg-1234",
        )

        self.assertEqual(result["category"], "kafka-connect-unavailable")
        self.assertEqual(result["operation_id"], "cfg-1234")
        self.assertIn("Kafka Connect", result["label"])
        self.assertIn("Worker-Status", result["next_step"])

    def test_classify_recovery_issue_detects_update_agent_outage(self):
        namespace = {"Any": Any}
        _load_items(["_classify_recovery_issue"], namespace)

        result = namespace["_classify_recovery_issue"](
            "",
            operation_id="upd-1234",
            update_agent={"available": False, "error": "Update-Agent nicht erreichbar"},
        )

        self.assertEqual(result["category"], "update-agent-unavailable")
        self.assertEqual(result["operation_id"], "upd-1234")
        self.assertIn("Update-Agent", result["hint"])
        self.assertIn("Update-Agent neu starten", result["next_step"])

    def test_classify_recovery_issue_detects_update_agent_auth_error(self):
        namespace = {"Any": Any}
        _load_items(["_classify_recovery_issue"], namespace)

        result = namespace["_classify_recovery_issue"](
            "",
            operation_id="upd-401",
            update_agent={"available": True, "auth_error": True, "status_code": 401, "error": "Update-Agent 401: Unauthorized"},
        )

        self.assertEqual(result["category"], "update-agent-auth")
        self.assertEqual(result["operation_id"], "upd-401")
        self.assertIn("Token", result["hint"])
        self.assertIn("TS_CONNECT_UPDATE_AGENT_TOKEN", result["next_step"])

    def test_classify_recovery_issue_detects_keygen_validation_failure(self):
        namespace = {"Any": Any}
        _load_items(["_classify_recovery_issue"], namespace)

        result = namespace["_classify_recovery_issue"](
            "Keygen validation failed: fingerprint scope is required",
            operation_id="lic-1234",
        )

        self.assertEqual(result["category"], "keygen-validation-failed")
        self.assertEqual(result["operation_id"], "lic-1234")
        self.assertIn("Installation aktivieren", result["next_step"])

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
    async def test_ip_allowlist_allows_loopback(self):
        class FakeJsonResponse:
            def __init__(self, payload, status_code=200):
                self.payload = payload
                self.status_code = status_code

        async def call_next(request):
            return {"ok": True}

        namespace = {
            "Request": object,
            "JSONResponse": FakeJsonResponse,
            "ipaddress": __import__("ipaddress"),
            "TRUSTED_NETWORKS": [],
            "logger": mock.Mock(),
        }
        _load_items(["ip_allowlist"], namespace)

        request = types.SimpleNamespace(client=types.SimpleNamespace(host="::1"))
        result = await namespace["ip_allowlist"](request, call_next)

        self.assertEqual(result, {"ok": True})

    async def test_ip_allowlist_blocks_untrusted_ip(self):
        class FakeJsonResponse:
            def __init__(self, payload, status_code=200):
                self.payload = payload
                self.status_code = status_code

        async def call_next(request):
            return {"ok": True}

        namespace = {
            "Request": object,
            "JSONResponse": FakeJsonResponse,
            "ipaddress": __import__("ipaddress"),
            "TRUSTED_NETWORKS": [],
            "logger": mock.Mock(),
        }
        _load_items(["ip_allowlist"], namespace)

        request = types.SimpleNamespace(client=types.SimpleNamespace(host="8.8.8.8"))
        result = await namespace["ip_allowlist"](request, call_next)

        self.assertEqual(result.status_code, 403)

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

        async def apply_state():
            return {
                "pending": False,
                "last_error": "Connector REST nicht erreichbar: connection refused",
                "operation_id": "cfg-1234",
            }

        namespace = {
            "asyncio": asyncio,
            "_check_database_health": database_health,
            "_check_confluent_health": confluent_health,
            "_check_connector_health": connector_health,
            "_check_backup_health": backup_health,
            "_check_license_health": license_health,
            "get_apply_state": apply_state,
            "_classify_recovery_issue": lambda message, operation_id=None: {
                "category": "kafka-connect-unavailable" if "connection refused" in str(message) else None,
                "label": "Kafka Connect nicht erreichbar" if "connection refused" in str(message) else None,
                "hint": "Kafka Connect prüfen" if "connection refused" in str(message) else None,
                "operation_id": operation_id,
            },
            "_now_utc_iso": lambda: "2026-03-20T12:00:00+00:00",
            "_write_json_log": lambda filename, snapshot: writes.append(
                {"filename": filename, "snapshot": dict(snapshot)}
            ),
            "logger": mock.Mock(),
        }
        _load_items(["health_summary"], namespace)

        result = await namespace["health_summary"]()

        self.assertEqual(result["connector"]["status"], "warn")
        self.assertEqual(result["connector"]["message"], "Connector läuft")
        self.assertEqual(result["connector"]["recovery"]["category"], "kafka-connect-unavailable")
        self.assertEqual(result["connector"]["recovery"]["operation_id"], "cfg-1234")
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
