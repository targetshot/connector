import ast
import pathlib
import tempfile
import types
import unittest
from unittest import mock


RUNNER_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "update_runner.py"


def _load_functions(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = RUNNER_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            fn_module = ast.Module(body=[node], type_ignores=[])
            exec(compile(fn_module, str(RUNNER_SOURCE), "exec"), namespace)  # noqa: S102


class _DummyManager:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def merge(self, **kwargs):
        self.calls.append(kwargs)
        return kwargs


class UpdateRunnerHelpersTest(unittest.TestCase):
    def test_mirror_maker_config_active_requires_remote_bootstrap_servers(self):
        namespace = {"Path": pathlib.Path}
        _load_functions(["_mirror_maker_config_active"], namespace)
        fn = namespace["_mirror_maker_config_active"]

        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = pathlib.Path(tmp_dir)
            self.assertFalse(fn(data_dir))

            (data_dir / "mm2.properties").write_text("# comment only\n", encoding="utf-8")
            self.assertFalse(fn(data_dir))

            (data_dir / "mm2.properties").write_text(
                "remote.bootstrap.servers = pkc-test:9092\n",
                encoding="utf-8",
            )
            self.assertTrue(fn(data_dir))

    def test_wait_for_container_running_stable_requires_healthy_when_requested(self):
        class CommandError(RuntimeError):
            pass

        class FakeClock:
            def __init__(self) -> None:
                self.now = 0.0

            def monotonic(self) -> float:
                return self.now

            def sleep(self, seconds: float) -> None:
                self.now += seconds

        fake_clock = FakeClock()
        manager = _DummyManager()
        namespace = {
            "time": types.SimpleNamespace(monotonic=fake_clock.monotonic, sleep=fake_clock.sleep),
            "CommandError": CommandError,
            "Path": pathlib.Path,
            "UpdateStateManager": object,
            "_inspect_container_details": lambda name, cwd: ("running", "starting"),
        }
        _load_functions(["_wait_for_container_running_stable"], namespace)

        with self.assertRaisesRegex(CommandError, "nicht stabil laufend"):
            namespace["_wait_for_container_running_stable"](
                "ts-mirror-maker",
                timeout_seconds=6,
                stable_seconds=4,
                require_healthy=True,
                cwd=pathlib.Path("/tmp"),
                manager=manager,
            )

    def test_stabilize_mirror_maker_after_update_retries_once_before_success(self):
        class CommandError(RuntimeError):
            pass

        manager = _DummyManager()
        run_command = mock.Mock()
        wait_for_container = mock.Mock(side_effect=[CommandError("first failure"), None])
        tail_logs = mock.Mock(return_value=["line-a", "line-b"])

        namespace = {
            "Path": pathlib.Path,
            "UpdateStateManager": object,
            "CommandError": CommandError,
            "_mirror_maker_config_active": lambda data_dir: True,
            "_run_command": run_command,
            "_wait_for_container_running_stable": wait_for_container,
            "_tail_container_logs": tail_logs,
        }
        _load_functions(["_stabilize_mirror_maker_after_update"], namespace)

        namespace["_stabilize_mirror_maker_after_update"](
            ["docker", "compose"],
            workspace=pathlib.Path("/workspace"),
            data_dir=pathlib.Path("/data"),
            manager=manager,
        )

        self.assertEqual(run_command.call_count, 2)
        self.assertEqual(wait_for_container.call_count, 2)
        tail_logs.assert_called_once()

    def test_stabilize_mirror_maker_after_update_skips_without_config(self):
        class CommandError(RuntimeError):
            pass

        manager = _DummyManager()
        run_command = mock.Mock()

        namespace = {
            "Path": pathlib.Path,
            "UpdateStateManager": object,
            "CommandError": CommandError,
            "_mirror_maker_config_active": lambda data_dir: False,
            "_run_command": run_command,
            "_wait_for_container_running_stable": mock.Mock(),
            "_tail_container_logs": mock.Mock(),
        }
        _load_functions(["_stabilize_mirror_maker_after_update"], namespace)

        namespace["_stabilize_mirror_maker_after_update"](
            ["docker", "compose"],
            workspace=pathlib.Path("/workspace"),
            data_dir=pathlib.Path("/data"),
            manager=manager,
        )

        run_command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
