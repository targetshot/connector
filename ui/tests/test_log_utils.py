import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock

from ui.log_utils import append_rotating_json_line, resolve_log_dir


class LogUtilsTest(unittest.TestCase):
    def test_resolve_log_dir_uses_explicit_env_override(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = pathlib.Path(tmp_dir) / "data"
            explicit_dir = pathlib.Path(tmp_dir) / "host-logs"

            with mock.patch.dict(os.environ, {"TS_CONNECT_LOG_DIR": str(explicit_dir)}, clear=False):
                log_dir = resolve_log_dir(data_dir=data_dir)

            self.assertEqual(log_dir, explicit_dir)
            self.assertTrue(explicit_dir.exists())

    def test_append_rotating_json_line_rotates_when_limit_exceeded(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_file = pathlib.Path(tmp_dir) / "health.log"

            append_rotating_json_line(
                log_file,
                {"message": "first", "size": "x" * 900},
                max_bytes=1024,
                backup_count=2,
            )
            append_rotating_json_line(
                log_file,
                {"message": "second", "size": "y" * 900},
                max_bytes=1024,
                backup_count=2,
            )

            self.assertTrue(log_file.exists())
            self.assertTrue(log_file.with_name("health.log.1").exists())
            latest = json.loads(log_file.read_text(encoding="utf-8").strip())
            previous = json.loads(log_file.with_name("health.log.1").read_text(encoding="utf-8").strip())
            self.assertEqual(latest["message"], "second")
            self.assertEqual(previous["message"], "first")


if __name__ == "__main__":
    unittest.main()
