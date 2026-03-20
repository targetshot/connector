import ast
import pathlib
import unittest
from unittest import mock


APP_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app.py"


def _load_items(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = APP_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in wanted:
            item_module = ast.Module(body=[node], type_ignores=[])
            exec(compile(item_module, str(APP_SOURCE), "exec"), namespace)  # noqa: S102


class AppOperationsWrapperTest(unittest.IsolatedAsyncioTestCase):
    async def test_start_update_runner_delegates_to_operations_runtime(self):
        operations_runtime = mock.Mock()
        operations_runtime.start_update_runner = mock.AsyncMock(return_value="job-123")
        namespace = {
            "ops_runtime": operations_runtime,
            "PROJECT_NAME": "ts-connect",
            "_update_agent_request": mock.AsyncMock(),
        }
        _load_items(["_start_update_runner"], namespace)

        result = await namespace["_start_update_runner"]("v0.4.99", "maxhany/targetshot", ".env")

        self.assertEqual(result, "job-123")
        operations_runtime.start_update_runner.assert_awaited_once_with(
            "v0.4.99",
            "maxhany/targetshot",
            ".env",
            project_name="ts-connect",
            update_agent_request_fn=namespace["_update_agent_request"],
        )

    def test_raise_http_for_agent_exception_delegates_to_operations_runtime(self):
        operations_runtime = mock.Mock()
        namespace = {
            "ops_runtime": operations_runtime,
            "_short_error_message": mock.Mock(),
        }
        _load_items(["_raise_http_for_agent_exception"], namespace)

        exc = RuntimeError("boom")
        namespace["_raise_http_for_agent_exception"](exc, default_status=503, max_len=140)

        operations_runtime.raise_http_for_agent_exception.assert_called_once_with(
            exc,
            short_error_message=namespace["_short_error_message"],
            default_status=503,
            max_len=140,
        )

    async def test_apply_connector_config_delegates_to_operations_runtime(self):
        operations_runtime = mock.Mock()
        operations_runtime.apply_connector_config = mock.AsyncMock(return_value=None)
        namespace = {
            "ops_runtime": operations_runtime,
            "ensure_offline_buffer_ready": mock.AsyncMock(),
            "fetch_settings": mock.Mock(),
            "read_secrets_file": mock.Mock(),
            "_ensure_mirror_db_secret": mock.Mock(),
            "build_connector_config": mock.Mock(),
            "_build_backup_sink_config": mock.Mock(),
            "DEFAULT_CONNECTOR_NAME": "targetshot-debezium",
            "BACKUP_CONNECTOR_NAME": "targetshot-debezium-backup-sink",
            "CONNECT_BASE_URL": "http://kafka-connect:8083",
            "_ensure_connector": mock.AsyncMock(),
            "_delete_connector_if_exists": mock.AsyncMock(),
            "_write_mirror_maker_config": mock.Mock(),
            "restart_mirror_maker": mock.AsyncMock(),
            "MM2_CONFIG_PATH": pathlib.Path("/tmp/mm2.properties"),
            "_mark_apply_success": mock.AsyncMock(),
        }
        _load_items(["apply_connector_config"], namespace)

        await namespace["apply_connector_config"](allow_defer=False)

        operations_runtime.apply_connector_config.assert_awaited_once_with(
            allow_defer=False,
            ensure_offline_buffer_ready_fn=namespace["ensure_offline_buffer_ready"],
            fetch_settings_fn=namespace["fetch_settings"],
            read_secrets_file_fn=namespace["read_secrets_file"],
            ensure_mirror_db_secret_fn=namespace["_ensure_mirror_db_secret"],
            build_connector_config_fn=namespace["build_connector_config"],
            build_backup_sink_config_fn=namespace["_build_backup_sink_config"],
            default_connector_name="targetshot-debezium",
            backup_connector_name="targetshot-debezium-backup-sink",
            connect_base_url="http://kafka-connect:8083",
            ensure_connector_fn=namespace["_ensure_connector"],
            delete_connector_if_exists_fn=namespace["_delete_connector_if_exists"],
            write_mirror_maker_config_fn=namespace["_write_mirror_maker_config"],
            restart_mirror_maker_fn=namespace["restart_mirror_maker"],
            mm2_config_path=namespace["MM2_CONFIG_PATH"],
            mark_apply_success_fn=namespace["_mark_apply_success"],
        )

    async def test_check_connector_health_delegates_to_operations_runtime(self):
        operations_runtime = mock.Mock()
        operations_runtime.check_connector_health = mock.AsyncMock(return_value={"status": "ok", "message": "Connector läuft"})
        namespace = {
            "ops_runtime": operations_runtime,
            "CONNECT_BASE_URL": "http://kafka-connect:8083",
            "DEFAULT_CONNECTOR_NAME": "targetshot-debezium",
            "_extract_error_message": mock.Mock(),
            "_short_error_message": mock.Mock(),
        }
        _load_items(["_check_connector_health"], namespace)

        result = await namespace["_check_connector_health"]()

        self.assertEqual(result, {"status": "ok", "message": "Connector läuft"})
        operations_runtime.check_connector_health.assert_awaited_once_with(
            connect_base_url="http://kafka-connect:8083",
            default_connector_name="targetshot-debezium",
            extract_error_message_fn=namespace["_extract_error_message"],
            short_error_message=namespace["_short_error_message"],
        )


if __name__ == "__main__":
    unittest.main()
