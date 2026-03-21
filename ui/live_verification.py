from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_SETTINGS_COLUMNS = {
    "license_key",
    "license_status",
    "license_tier",
    "license_valid_until",
    "license_activation_id",
    "license_activated_at",
    "retention_days",
    "topic_prefix",
}

BLOCKING_LICENSE_STATUSES = {"revoked", "cancelled", "disabled", "invalid", "inactive"}


def _parse_iso8601(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _settings_columns(db_path: Path) -> list[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        return [str(row[1]) for row in conn.execute("PRAGMA table_info(settings)")]
    finally:
        conn.close()


def _settings_row(db_path: Path, columns: list[str]) -> dict[str, Any]:
    if not columns:
        return {}
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        query = f"SELECT {', '.join(columns)} FROM settings WHERE id=1"
        row = conn.execute(query).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def _license_runtime_active(settings: dict[str, Any]) -> bool:
    license_key = str(settings.get("license_key") or "").strip()
    if not license_key:
        return False
    status = str(settings.get("license_status") or "").strip().lower()
    if status in BLOCKING_LICENSE_STATUSES:
        return False
    activation_id = str(settings.get("license_activation_id") or "").strip()
    if not activation_id:
        return False
    expires_dt = _parse_iso8601(settings.get("license_valid_until"))
    if expires_dt and expires_dt < datetime.now(timezone.utc):
        return False
    return True


def build_club_plus_verification_report(data_dir: Path) -> dict[str, Any]:
    db_path = data_dir / "config.db"
    license_key_file = data_dir / "license.key"
    license_meta_file = data_dir / "license-meta.json"
    machine_fingerprint_file = data_dir / "machine_fingerprint"

    report: dict[str, Any] = {
        "data_dir": str(data_dir),
        "db_path": str(db_path),
        "checks": [],
        "blocking_issues": [],
        "warnings": [],
    }

    if not db_path.exists():
        report["blocking_issues"].append("config.db fehlt")
        report["checks"].append({"id": "config-db", "ok": False, "detail": "config.db fehlt"})
        report["summary"] = {"ok": False, "blocking_failures": len(report["blocking_issues"]), "warnings": 0}
        return report

    columns = _settings_columns(db_path)
    missing_columns = sorted(REQUIRED_SETTINGS_COLUMNS - set(columns))
    report["settings_columns"] = columns
    report["schema_current"] = not missing_columns
    report["checks"].append(
        {
            "id": "settings-schema",
            "ok": not missing_columns,
            "detail": "Aktuelle Lizenzspalten vorhanden" if not missing_columns else f"Fehlende Spalten: {', '.join(missing_columns)}",
        }
    )

    relevant_columns = sorted(set(columns) & REQUIRED_SETTINGS_COLUMNS)
    settings = _settings_row(db_path, relevant_columns)
    report["settings"] = {
        "topic_prefix": settings.get("topic_prefix"),
        "license_tier": settings.get("license_tier"),
        "license_status": settings.get("license_status"),
        "license_valid_until": settings.get("license_valid_until"),
        "retention_days": settings.get("retention_days"),
        "has_license_key": bool(str(settings.get("license_key") or "").strip()),
        "has_activation_id": bool(str(settings.get("license_activation_id") or "").strip()),
        "license_activated_at": settings.get("license_activated_at"),
    }

    license_key_value = str(settings.get("license_key") or "").strip()
    license_file_value = license_key_file.read_text(encoding="utf-8").strip() if license_key_file.exists() else ""
    license_meta = _read_json(license_meta_file)
    runtime_active = _license_runtime_active(settings)

    report["checks"].extend(
        [
            {
                "id": "license-key-stored",
                "ok": bool(license_key_value),
                "detail": "Lizenzschlüssel in config.db vorhanden" if license_key_value else "Kein Lizenzschlüssel in config.db gespeichert",
            },
            {
                "id": "license-key-file",
                "ok": license_key_file.exists(),
                "detail": "license.key vorhanden" if license_key_file.exists() else "license.key fehlt",
            },
            {
                "id": "license-key-sync",
                "ok": bool(license_key_value) and bool(license_file_value) and license_key_value == license_file_value,
                "detail": "license.key entspricht config.db" if license_key_value and license_file_value and license_key_value == license_file_value else "license.key und config.db sind nicht synchron",
            },
            {
                "id": "license-plan",
                "ok": str(settings.get("license_tier") or "").strip() == "club_plus",
                "detail": f"Plan: {settings.get('license_tier') or 'unbekannt'}",
            },
            {
                "id": "license-status",
                "ok": str(settings.get("license_status") or "").strip().lower() not in BLOCKING_LICENSE_STATUSES and bool(str(settings.get("license_status") or "").strip()),
                "detail": f"Status: {settings.get('license_status') or 'unbekannt'}",
            },
            {
                "id": "license-activation",
                "ok": bool(str(settings.get("license_activation_id") or "").strip()),
                "detail": "Maschinenaktivierung vorhanden" if str(settings.get("license_activation_id") or "").strip() else "Maschinenaktivierung fehlt",
            },
            {
                "id": "machine-fingerprint-file",
                "ok": machine_fingerprint_file.exists(),
                "detail": "machine_fingerprint vorhanden" if machine_fingerprint_file.exists() else "machine_fingerprint fehlt",
            },
            {
                "id": "license-meta-file",
                "ok": bool(license_meta),
                "detail": "license-meta.json vorhanden" if license_meta else "license-meta.json fehlt oder ist leer",
            },
            {
                "id": "retention-days",
                "ok": settings.get("retention_days") == 30,
                "detail": f"Retention Days: {settings.get('retention_days')}",
            },
            {
                "id": "runtime-unlock",
                "ok": runtime_active,
                "detail": "Connector lokal freigeschaltet" if runtime_active else "Connector lokal noch nicht vollständig freigeschaltet",
            },
        ]
    )

    if not license_meta:
        report["warnings"].append("license-meta.json fehlt oder ist leer")
    if settings.get("retention_days") not in (None, 30):
        report["warnings"].append(f"retention_days ist {settings.get('retention_days')} statt 30")

    for check in report["checks"]:
        if not check["ok"] and check["id"] not in {"license-meta-file", "retention-days"}:
            report["blocking_issues"].append(check["detail"])

    deduped_blockers: list[str] = []
    seen_blockers: set[str] = set()
    for issue in report["blocking_issues"]:
        if issue in seen_blockers:
            continue
        deduped_blockers.append(issue)
        seen_blockers.add(issue)
    report["blocking_issues"] = deduped_blockers

    report["summary"] = {
        "ok": not report["blocking_issues"],
        "blocking_failures": len(report["blocking_issues"]),
        "warnings": len(report["warnings"]),
    }
    return report
