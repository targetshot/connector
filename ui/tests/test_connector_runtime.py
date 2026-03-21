import ast
import pathlib
import types
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


class ConnectorRuntimeTest(unittest.IsolatedAsyncioTestCase):
    def test_write_mirror_maker_config_disables_internal_topics_explicitly(self):
        namespace = {"Any": Any, "Callable": Callable, "Path": pathlib.Path}
        _load_items(["write_mirror_maker_config"], namespace)

        written: dict[str, Any] = {}

        def atomic_write_text(path, text, **kwargs):
            written["path"] = path
            written["text"] = text
            written["kwargs"] = kwargs

        namespace["write_mirror_maker_config"](
            {},
            {
                "confluent_bootstrap": "pkc-test.example:9092",
                "confluent_sasl_username": "api-key",
                "confluent_sasl_password": "api-secret",
            },
            confluent_bootstrap_default="unused:9092",
            stream_target_prefix="ts.sds-test",
            mm2_internal_replication_factor=3,
            mm2_offset_storage_partitions=5,
            mm2_status_storage_partitions=3,
            mm2_state_topic_prefix="_ts_mm2_v3",
            config_path=pathlib.Path("/tmp/mm2.properties"),
            atomic_write_text_fn=atomic_write_text,
            secret_file_mode=0o600,
            secrets_file_uid=None,
            secrets_file_gid=None,
            escape_jaas_fn=lambda value: value,
        )

        config_text = written["text"]
        self.assertIn("local->remote.topics = ts.sds-test.*", config_text)
        self.assertIn("offset.storage.topic = _ts_mm2_v3_offsets", config_text)
        self.assertIn("config.storage.topic = _ts_mm2_v3_configs", config_text)
        self.assertIn("status.storage.topic = _ts_mm2_v3_status", config_text)
        self.assertIn("emit.heartbeats.enabled = false", config_text)
        self.assertIn("emit.checkpoints.enabled = false", config_text)
        self.assertIn("emit.offset-syncs.enabled = false", config_text)
        self.assertIn("heartbeats.replication.enabled = false", config_text)

    async def test_connect_request_defers_on_transient_request_error(self):
        class FakeRequestError(Exception):
            pass

        class FakeClient:
            async def request(self, method, url, json=None):
                raise FakeRequestError("connection refused")

        retries: list[str] = []
        namespace = {
            "Any": Any,
            "Awaitable": Awaitable,
            "Callable": Callable,
            "httpx": types.SimpleNamespace(AsyncClient=object, RequestError=FakeRequestError),
        }
        _load_items(["DeferredApplyError", "connect_request"], namespace)

        async def schedule_retry(msg: str, operation_id: str | None = None):
            retries.append(f"{operation_id}:{msg}")

        with self.assertRaises(namespace["DeferredApplyError"]):
            await namespace["connect_request"](
                FakeClient(),
                "GET",
                "http://connect/connectors",
                allow_defer=True,
                operation_id="op-test",
                schedule_retry_fn=schedule_retry,
                short_error_message=lambda text, max_len=180: text[:max_len],
                extract_error_message_fn=lambda resp: "ignored",
                is_transient_status_fn=lambda status, message: False,
                is_transient_request_error_fn=lambda exc: True,
            )

        self.assertEqual(retries, ["op-test:connection refused"])

    async def test_apply_source_replication_config_uses_disable_when_payload_missing(self):
        namespace = {"Any": Any, "Awaitable": Awaitable, "Callable": Callable}
        _load_items(["apply_source_replication_config"], namespace)

        calls: list[dict[str, Any]] = []

        async def update_agent_request(method: str, path: str, **kwargs):
            calls.append({"method": method, "path": path, "kwargs": kwargs})
            return {"ok": True, "mode": "disable"}

        result = await namespace["apply_source_replication_config"](
            {"source_db_host": ""},
            {},
            build_source_replication_payload_fn=lambda settings, secrets: None,
            update_agent_request_fn=update_agent_request,
        )

        self.assertEqual(result["mode"], "disable")
        self.assertEqual(
            calls,
            [
                {
                    "method": "POST",
                    "path": "/api/v1/mirror-replication/disable",
                    "kwargs": {"timeout": 25},
                }
            ],
        )

    async def test_update_remote_replication_state_starts_container_when_active_but_stopped(self):
        namespace = {"Any": Any, "Awaitable": Awaitable, "Callable": Callable}
        _load_items(["update_remote_replication_state"], namespace)

        restarted: list[str] = []
        stopped: list[str] = []
        logger = types.SimpleNamespace(warning=lambda *a, **k: None, error=lambda *a, **k: None)

        async def container_status(name: str):
            return {"exists": True, "running": False}

        async def restart():
            restarted.append("restart")

        async def stop(name: str):
            stopped.append(name)
            return {"ok": True}

        await namespace["update_remote_replication_state"](
            True,
            container_status_fn=container_status,
            restart_mirror_maker_fn=restart,
            stop_container_fn=stop,
            logger=logger,
        )

        self.assertEqual(restarted, ["restart"])
        self.assertEqual(stopped, [])


if __name__ == "__main__":
    unittest.main()
