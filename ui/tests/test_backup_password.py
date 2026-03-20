import ast
import os
import pathlib
import types
import unittest
from unittest import mock


APP_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app.py"


def _load_function(name: str, namespace: dict):
    source = APP_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            fn_module = ast.Module(body=[node], type_ignores=[])
            exec(compile(fn_module, str(APP_SOURCE), "exec"), namespace)  # noqa: S102
            return namespace[name]
    raise AssertionError(f"Function {name} not found in {APP_SOURCE}")


class EnsureBackupPasswordTest(unittest.TestCase):
    def _make_function(self, *, rotate_side_effect=None, token_value="new-generated-password"):
        recorded_writes = []
        recorded_rotations = []
        logger = mock.Mock()

        def write_secrets_file(values):
            recorded_writes.append(dict(values))

        def rotate_backup_password(*, settings, current_password, new_password):
            recorded_rotations.append(
                {
                    "settings": dict(settings),
                    "current_password": current_password,
                    "new_password": new_password,
                }
            )
            if rotate_side_effect is not None:
                raise rotate_side_effect

        namespace = {
            "os": os,
            "secrets": types.SimpleNamespace(token_urlsafe=lambda _: token_value),
            "write_secrets_file": write_secrets_file,
            "_rotate_backup_password": rotate_backup_password,
            "logger": logger,
        }
        fn = _load_function("_ensure_backup_password", namespace)
        return fn, recorded_writes, recorded_rotations, logger

    def test_returns_existing_persisted_password_without_rotation(self):
        fn, writes, rotations, _logger = self._make_function()

        password, secrets_data = fn(
            settings={"backup_pg_user": "targetshot"},
            secrets_data={"backup_pg_password": "persisted-secret"},
        )

        self.assertEqual(password, "persisted-secret")
        self.assertEqual(secrets_data["backup_pg_password"], "persisted-secret")
        self.assertEqual(writes, [])
        self.assertEqual(rotations, [])

    def test_rotates_from_explicit_env_password_and_persists_new_secret(self):
        fn, writes, rotations, _logger = self._make_function(token_value="rotated-secret")

        with mock.patch.dict(os.environ, {"TS_CONNECT_BACKUP_PASSWORD": "explicit-old"}, clear=False):
            password, secrets_data = fn(
                settings={"backup_pg_user": "targetshot"},
                secrets_data={},
            )

        self.assertEqual(password, "rotated-secret")
        self.assertEqual(secrets_data["backup_pg_password"], "rotated-secret")
        self.assertEqual(len(rotations), 1)
        self.assertEqual(rotations[0]["current_password"], "explicit-old")
        self.assertEqual(rotations[0]["new_password"], "rotated-secret")
        self.assertEqual(writes, [{"backup_pg_password": "rotated-secret"}])

    def test_fails_closed_when_no_explicit_recovery_source_exists(self):
        fn, writes, rotations, _logger = self._make_function()

        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "Backup-Passwort fehlt"):
                fn(settings={"backup_pg_user": "targetshot"}, secrets_data={})

        self.assertEqual(writes, [])
        self.assertEqual(rotations, [])

    def test_fails_closed_when_rotation_from_explicit_source_fails(self):
        fn, writes, rotations, _logger = self._make_function(
            rotate_side_effect=RuntimeError("rotation failed"),
        )

        with mock.patch.dict(os.environ, {"TS_CONNECT_BACKUP_PASSWORD": "explicit-old"}, clear=False):
            with self.assertRaisesRegex(RuntimeError, "Backup-Passwort konnte nicht initialisiert werden"):
                fn(settings={"backup_pg_user": "targetshot"}, secrets_data={})

        self.assertEqual(len(rotations), 1)
        self.assertEqual(rotations[0]["current_password"], "explicit-old")
        self.assertEqual(writes, [])

    def test_rejects_persisted_known_insecure_password_and_reinitializes(self):
        fn, writes, rotations, logger = self._make_function(token_value="rotated-secret")

        with mock.patch.dict(os.environ, {"TS_CONNECT_BACKUP_PASSWORD": "explicit-old"}, clear=False):
            password, secrets_data = fn(
                settings={"backup_pg_user": "targetshot"},
                secrets_data={"backup_pg_password": "targetshot"},
            )

        self.assertEqual(password, "rotated-secret")
        self.assertEqual(secrets_data["backup_pg_password"], "rotated-secret")
        self.assertEqual(len(rotations), 1)
        self.assertEqual(rotations[0]["current_password"], "explicit-old")
        logger.warning.assert_called_once()
        self.assertEqual(writes, [{"backup_pg_password": "rotated-secret"}])


if __name__ == "__main__":
    unittest.main()
