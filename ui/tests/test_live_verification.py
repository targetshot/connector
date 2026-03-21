import pathlib
import sqlite3
import tempfile
import unittest

from ui.live_verification import build_club_plus_verification_report


CURRENT_SETTINGS_SCHEMA = """
CREATE TABLE settings (
    id INTEGER PRIMARY KEY CHECK (id=1),
    db_host TEXT,
    db_port INTEGER,
    db_user TEXT,
    source_db_host TEXT,
    source_db_port INTEGER,
    source_db_repl_user TEXT,
    source_db_gtid_mode INTEGER,
    source_db_log_file TEXT,
    source_db_log_pos INTEGER,
    source_db_connect_retry INTEGER,
    confluent_bootstrap TEXT,
    confluent_sasl_username TEXT,
    topic_prefix TEXT,
    server_id INTEGER,
    server_name TEXT,
    offline_buffer_enabled INTEGER DEFAULT 0,
    license_tier TEXT,
    retention_days INTEGER,
    license_key TEXT,
    license_status TEXT,
    license_valid_until TEXT,
    license_last_checked TEXT,
    license_customer_email TEXT,
    license_activation_id TEXT,
    license_activated_at TEXT,
    backup_pg_host TEXT,
    backup_pg_port INTEGER,
    backup_pg_db TEXT,
    backup_pg_user TEXT,
    shooter_count_cached INTEGER,
    shooter_count_checked_at TEXT
)
"""


class ClubPlusLiveVerificationTest(unittest.TestCase):
    def test_report_flags_stale_schema_as_blocking(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = pathlib.Path(tmp_dir)
            conn = sqlite3.connect(data_dir / "config.db")
            conn.execute(
                "CREATE TABLE settings (id INTEGER PRIMARY KEY CHECK (id=1), db_host TEXT, db_port INTEGER, db_user TEXT)"
            )
            conn.execute("INSERT INTO settings (id, db_host, db_port, db_user) VALUES (1, 'db', 3306, 'user')")
            conn.commit()
            conn.close()

            report = build_club_plus_verification_report(data_dir)

        self.assertFalse(report["summary"]["ok"])
        self.assertFalse(report["schema_current"])
        self.assertTrue(any("Fehlende Spalten" in issue for issue in report["blocking_issues"]))

    def test_report_detects_complete_local_runtime_unlock(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = pathlib.Path(tmp_dir)
            conn = sqlite3.connect(data_dir / "config.db")
            conn.execute(CURRENT_SETTINGS_SCHEMA)
            conn.execute(
                """
                INSERT INTO settings (
                    id, db_host, db_port, db_user, source_db_host, source_db_port,
                    source_db_repl_user, source_db_gtid_mode, source_db_log_file, source_db_log_pos,
                    source_db_connect_retry, confluent_bootstrap, confluent_sasl_username,
                    topic_prefix, server_id, server_name, offline_buffer_enabled,
                    license_tier, retention_days, license_key, license_status, license_valid_until,
                    license_last_checked, license_customer_email, license_activation_id,
                    license_activated_at, backup_pg_host, backup_pg_port, backup_pg_db,
                    backup_pg_user, shooter_count_cached, shooter_count_checked_at
                ) VALUES (
                    1, 'db', 3306, 'user', 'src-db', 3306,
                    'repl', 1, '', NULL,
                    10, 'cloud:9092', 'api-key',
                    'ts.sds-test', 5401, 'ts-mariadb', 1,
                    'club_plus', 30, 'KEY-123', 'active', '2099-01-01T00:00:00+00:00',
                    '2026-03-21T10:00:00+00:00', 'admin@example.com', 'machine-1',
                    '2026-03-21T10:01:00+00:00', 'backup-db', 5432, 'targetshot_backup',
                    'targetshot', 0, NULL
                )
                """
            )
            conn.commit()
            conn.close()

            (data_dir / "license.key").write_text("KEY-123\n", encoding="utf-8")
            (data_dir / "license-meta.json").write_text('{"status":"active","plan":"club_plus"}', encoding="utf-8")
            (data_dir / "machine_fingerprint").write_text("fingerprint-1\n", encoding="utf-8")

            report = build_club_plus_verification_report(data_dir)

        self.assertTrue(report["summary"]["ok"])
        self.assertEqual(report["summary"]["blocking_failures"], 0)
        self.assertTrue(any(check["id"] == "runtime-unlock" and check["ok"] for check in report["checks"]))
