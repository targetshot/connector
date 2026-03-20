import os
import pathlib
import stat
import tempfile
import unittest
from unittest import mock

from ui.security_bootstrap import (
    DEFAULT_ADMIN_PASSWORD,
    DEFAULT_SESSION_SECRET,
    PRIVATE_SECRET_FILE_MODE,
    UiSecurityBootstrap,
)


def _atomic_write_text(path, data, *, mode=None, uid=None, gid=None):
    path.write_text(data, encoding="utf-8")
    if mode is not None:
        os.chmod(path, mode)


class StartupSecretBootstrapTest(unittest.TestCase):
    def _make_bootstrap(self, tmp_dir: str, logger: mock.Mock) -> UiSecurityBootstrap:
        return UiSecurityBootstrap(
            pathlib.Path(tmp_dir),
            logger=logger,
            atomic_write_text=_atomic_write_text,
            fsync_directory=lambda path: None,
        )

    def test_resolve_env_admin_password_rejects_placeholder(self):
        logger = mock.Mock()
        with tempfile.TemporaryDirectory() as tmp_dir:
            bootstrap = self._make_bootstrap(tmp_dir, logger)

            with mock.patch.dict(os.environ, {"TS_CONNECT_UI_ADMIN_PASSWORD": DEFAULT_ADMIN_PASSWORD}, clear=False):
                result = bootstrap.resolve_env_admin_password()

        self.assertIsNone(result)
        logger.warning.assert_called_once()

    def test_resolve_session_secret_replaces_default_file_value(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            logger = mock.Mock()
            bootstrap = self._make_bootstrap(tmp_dir, logger)
            bootstrap.session_secret_file.write_text(DEFAULT_SESSION_SECRET + "\n", encoding="utf-8")
            os.chmod(bootstrap.session_secret_file, 0o644)

            with mock.patch.dict(os.environ, {}, clear=True):
                result = bootstrap.resolve_session_secret()

            self.assertEqual(result, bootstrap.session_secret_file.read_text(encoding="utf-8").strip())
            self.assertNotEqual(result, DEFAULT_SESSION_SECRET)
            self.assertEqual(stat.S_IMODE(bootstrap.session_secret_file.stat().st_mode), 0o600)

    def test_read_admin_password_record_rejects_plaintext_default(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            logger = mock.Mock()
            bootstrap = self._make_bootstrap(tmp_dir, logger)
            bootstrap.admin_password_file.write_text(DEFAULT_ADMIN_PASSWORD + "\n", encoding="utf-8")
            os.chmod(bootstrap.admin_password_file, 0o644)

            result = bootstrap.read_admin_password_record()

            self.assertEqual(result, "")
            self.assertEqual(stat.S_IMODE(bootstrap.admin_password_file.stat().st_mode), 0o600)
            logger.warning.assert_called_once()

    def test_ensure_admin_password_file_reinitializes_when_record_invalid(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            logger = mock.Mock()
            bootstrap = self._make_bootstrap(tmp_dir, logger)
            remember_generated = mock.Mock()
            set_admin_password = mock.Mock()

            bootstrap.read_admin_password_record = lambda: ""
            bootstrap.resolve_env_admin_password = lambda: None
            bootstrap.clear_generated_admin_password_file = mock.Mock()
            bootstrap.read_generated_admin_password = lambda: None
            bootstrap.generate_admin_password = lambda: "generated-admin-secret"
            bootstrap.remember_generated_admin_password = remember_generated
            bootstrap.set_admin_password = set_admin_password

            bootstrap.ensure_admin_password_file()

            remember_generated.assert_called_once_with("generated-admin-secret")
            set_admin_password.assert_called_once_with("generated-admin-secret")


if __name__ == "__main__":
    unittest.main()
