from datetime import datetime, timedelta, timezone
import os
import pathlib
import tempfile
import unittest

from ui.backup_runtime import (
    build_backup_filename,
    list_backup_files,
    prune_backup_files,
    resolve_backup_dir,
    resolve_host_display_dir,
    scheduled_run_due,
)


class BackupRuntimeTest(unittest.TestCase):
    def test_resolve_backup_dir_uses_workspace_default(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            workspace = pathlib.Path(tmp_dir)
            directory = resolve_backup_dir(None, workspace_path=workspace, default_subdir="ui/backups/mirror-db")
            self.assertEqual(directory, workspace / "ui/backups/mirror-db")
            self.assertTrue(directory.exists())

    def test_resolve_host_display_dir_uses_workspace_host(self):
        host_path = resolve_host_display_dir(
            None,
            workspace_host="/srv/connector",
            default_subdir="ui/backups/mirror-db",
        )
        self.assertEqual(host_path, "/srv/connector/ui/backups/mirror-db")

    def test_prune_backup_files_removes_entries_older_than_retention(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            directory = pathlib.Path(tmp_dir)
            old_file = directory / "mirror-db-backup-20260101T000000Z.sql.gz"
            new_file = directory / "mirror-db-backup-20260301T000000Z.sql.gz"
            old_file.write_bytes(b"old")
            new_file.write_bytes(b"new")
            old_ts = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc).timestamp()
            new_ts = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc).timestamp()
            old_file.touch()
            new_file.touch()
            os.utime(old_file, (old_ts, old_ts))
            os.utime(new_file, (new_ts, new_ts))

            removed = prune_backup_files(
                directory,
                retention_days=30,
                now=datetime(2026, 3, 21, 0, 0, tzinfo=timezone.utc),
            )

            self.assertEqual([path.name for path in removed], [old_file.name])
            self.assertFalse(old_file.exists())
            self.assertTrue(new_file.exists())

    def test_list_backup_files_returns_newest_first(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            directory = pathlib.Path(tmp_dir)
            newer = directory / "mirror-db-backup-20260321T000000Z.sql.gz"
            older = directory / "mirror-db-backup-20260320T000000Z.sql.gz"
            older.write_bytes(b"old")
            newer.write_bytes(b"new")
            older_ts = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc).timestamp()
            newer_ts = datetime(2026, 3, 21, 0, 0, tzinfo=timezone.utc).timestamp()
            os.utime(older, (older_ts, older_ts))
            os.utime(newer, (newer_ts, newer_ts))

            entries = list_backup_files(directory, limit=5)

            self.assertEqual([entry["name"] for entry in entries], [newer.name, older.name])

    def test_scheduled_run_due_only_once_per_daily_slot(self):
        now = datetime(2026, 3, 21, 2, 30, tzinfo=timezone.utc)
        due, slot_id, next_run = scheduled_run_due(
            now=now,
            last_slot_id=None,
            hour=2,
            minute=15,
            tz=timezone.utc,
        )
        self.assertTrue(due)
        self.assertEqual(slot_id, "2026-03-21T02:15:00+0000")
        self.assertEqual(next_run, datetime(2026, 3, 22, 2, 15, tzinfo=timezone.utc))

        due_again, slot_again, _ = scheduled_run_due(
            now=now + timedelta(minutes=5),
            last_slot_id=slot_id,
            hour=2,
            minute=15,
            tz=timezone.utc,
        )
        self.assertFalse(due_again)
        self.assertEqual(slot_again, slot_id)

    def test_build_backup_filename_uses_utc_timestamp(self):
        name = build_backup_filename(
            "mirror-db-backup",
            datetime(2026, 3, 21, 7, 45, tzinfo=timezone.utc),
            ".sql.gz",
        )
        self.assertEqual(name, "mirror-db-backup-20260321T074500Z.sql.gz")


if __name__ == "__main__":
    unittest.main()
