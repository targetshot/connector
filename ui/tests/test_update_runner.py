import ast
import base64
import pathlib
import subprocess
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
    def test_github_auth_git_prefix_uses_token_for_github_origin(self):
        namespace = {
            "os": types.SimpleNamespace(getenv=lambda name: "ghp_test_token" if name == "TS_CONNECT_GITHUB_TOKEN" else None),
            "base64": base64,
            "Path": pathlib.Path,
            "subprocess": subprocess,
        }
        _load_functions(["_command_env", "_parse_repo_slug", "_origin_remote_url", "_github_auth_git_prefix"], namespace)

        namespace["_origin_remote_url"] = lambda workspace: "https://github.com/targetshot/connector.git"

        prefix = namespace["_github_auth_git_prefix"](pathlib.Path("/workspace"))

        self.assertEqual(prefix[0], "git")
        self.assertEqual(prefix[1:3], ["-c", prefix[2]])
        self.assertIn("http.https://github.com/.extraheader=AUTHORIZATION: basic ", prefix[2])

    def test_github_auth_git_prefix_skips_non_github_or_missing_token(self):
        namespace = {
            "os": types.SimpleNamespace(getenv=lambda name: ""),
            "base64": base64,
            "Path": pathlib.Path,
            "subprocess": subprocess,
        }
        _load_functions(["_command_env", "_parse_repo_slug", "_origin_remote_url", "_github_auth_git_prefix"], namespace)

        namespace["_origin_remote_url"] = lambda workspace: "https://gitlab.example.com/targetshot/connector.git"

        prefix = namespace["_github_auth_git_prefix"](pathlib.Path("/workspace"))

        self.assertEqual(prefix, ["git"])

    def test_run_command_includes_last_output_line_in_error(self):
        class FakeStdout:
            def __init__(self, lines: list[str]) -> None:
                self._lines = iter(lines)

            def __iter__(self):
                return self

            def __next__(self):
                return next(self._lines)

        class FakeProcess:
            def __init__(self) -> None:
                self.stdout = FakeStdout(["fatal: Authentication failed\n"])

            def wait(self):
                return 1

        class CommandError(RuntimeError):
            pass

        namespace = {
            "os": __import__("os"),
            "shlex": __import__("shlex"),
            "subprocess": mock.Mock(Popen=lambda *args, **kwargs: FakeProcess()),
            "Path": pathlib.Path,
            "UpdateStateManager": object,
            "CommandError": CommandError,
            "format_operation_message": lambda message, operation_id=None: message,
            "logger": mock.Mock(),
            "UPDATE_OPERATION_ID": None,
            "_cmd_to_str": lambda cmd: " ".join(cmd),
        }
        _load_functions(["_command_env", "_run_command"], namespace)

        with self.assertRaisesRegex(CommandError, "Authentication failed"):
            namespace["_run_command"](["git", "fetch"], cwd=pathlib.Path("/workspace"), manager=_DummyManager())

    def test_require_host_workspace_path_requires_absolute_host_override_for_container_runner(self):
        namespace = {"Path": pathlib.Path}
        _load_functions(["_parse_env_file", "_require_host_workspace_path"], namespace)

        namespace["_parse_env_file"] = lambda path: {}
        with self.assertRaisesRegex(RuntimeError, "TS_CONNECT_WORKSPACE_HOST fehlt"):
            namespace["_require_host_workspace_path"](workspace=pathlib.Path("/workspace"), compose_env=".env")

        namespace["_parse_env_file"] = lambda path: {"TS_CONNECT_WORKSPACE_HOST": "relative/path"}
        with self.assertRaisesRegex(RuntimeError, "muss absolut sein"):
            namespace["_require_host_workspace_path"](workspace=pathlib.Path("/workspace"), compose_env=".env")

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
