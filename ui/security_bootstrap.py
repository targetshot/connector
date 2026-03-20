from __future__ import annotations

import hashlib
import os
import secrets
import stat
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException, Request


DEFAULT_ADMIN_PASSWORD = "change-me"
PASSWORD_PLACEHOLDER = "********"
DEFAULT_SESSION_SECRET = "targetshot-connect-ui-secret"
PRIVATE_SECRET_FILE_MODE = stat.S_IRUSR | stat.S_IWUSR


class UiSecurityBootstrap:
    def __init__(
        self,
        data_dir: Path,
        *,
        logger: Any,
        atomic_write_text: Callable[..., Any],
        fsync_directory: Callable[[Path], Any],
    ) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logger
        self._atomic_write_text = atomic_write_text
        self._fsync_directory = fsync_directory
        self.admin_password_file = self.data_dir / "admin_password.txt"
        self.admin_password_generated_file = self.data_dir / "admin_password.generated"
        self.session_secret_file = self.data_dir / "session_secret"

    @staticmethod
    def _env_first(*names: str) -> str | None:
        for name in names:
            value = os.getenv(name)
            if value is not None:
                return value
        return None

    @staticmethod
    def _is_rejected_secret_value(value: str | None, *, rejected_values: set[str]) -> bool:
        if value is None:
            return True
        trimmed = value.strip()
        return not trimmed or trimmed in rejected_values

    def ensure_private_file_permissions(self, path: Path) -> None:
        if not path.exists():
            return
        try:
            current_mode = stat.S_IMODE(path.stat().st_mode)
        except OSError:
            return
        if current_mode == PRIVATE_SECRET_FILE_MODE:
            return
        try:
            os.chmod(path, PRIVATE_SECRET_FILE_MODE)
        except OSError as exc:
            self.logger.warning("Dateiberechtigungen für %s konnten nicht gehärtet werden: %s", path, exc)
        else:
            self.logger.info("Dateiberechtigungen für %s auf 0600 korrigiert.", path)

    @staticmethod
    def generate_admin_password(length: int = 24) -> str:
        alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        return "".join(secrets.choice(alphabet) for _ in range(length))

    def resolve_env_admin_password(self) -> str | None:
        value = self._env_first("TS_CONNECT_UI_ADMIN_PASSWORD", "UI_ADMIN_PASSWORD")
        if self._is_rejected_secret_value(value, rejected_values={DEFAULT_ADMIN_PASSWORD}):
            if value is not None:
                self.logger.warning("Unsicheres oder leeres UI-Admin-Passwort aus der Umgebung verworfen.")
            return None
        return value.strip()

    def read_generated_admin_password(self) -> str | None:
        if not self.admin_password_generated_file.exists():
            return None
        self.ensure_private_file_permissions(self.admin_password_generated_file)
        try:
            data = self.admin_password_generated_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if self._is_rejected_secret_value(data, rejected_values={DEFAULT_ADMIN_PASSWORD}):
            if data:
                self.logger.warning("Unsicheres generiertes Admin-Passwort verworfen und wird neu erzeugt.")
            return None
        return data

    def remember_generated_admin_password(self, password: str) -> None:
        self._atomic_write_text(
            self.admin_password_generated_file,
            password + "\n",
            mode=PRIVATE_SECRET_FILE_MODE,
        )

    def clear_generated_admin_password_file(self) -> None:
        if self.admin_password_generated_file.exists():
            try:
                self.admin_password_generated_file.unlink()
            except OSError:
                return
            self._fsync_directory(self.admin_password_generated_file.parent)

    def resolve_session_secret(self) -> str:
        env_secret = (self._env_first("TS_CONNECT_UI_SESSION_SECRET", "UI_SESSION_SECRET") or "").strip()
        if env_secret and env_secret != DEFAULT_SESSION_SECRET:
            return env_secret
        if env_secret:
            self.logger.warning("Unsicheres oder leeres UI-Session-Secret aus der Umgebung verworfen.")
        self.ensure_private_file_permissions(self.session_secret_file)
        try:
            stored = self.session_secret_file.read_text(encoding="utf-8").strip()
        except (FileNotFoundError, OSError):
            stored = ""
        if stored and stored != DEFAULT_SESSION_SECRET:
            return stored
        if stored:
            self.logger.warning("Unsicheres oder leeres persistiertes UI-Session-Secret verworfen und neu erzeugt.")
        generated = secrets.token_urlsafe(48)
        try:
            self._atomic_write_text(
                self.session_secret_file,
                generated + "\n",
                mode=PRIVATE_SECRET_FILE_MODE,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Session-Secret konnte nicht persistiert werden: %s", exc)
        else:
            self.logger.warning(
                "TS_CONNECT_UI_SESSION_SECRET nicht gesetzt – zufälliges Secret wurde in %s hinterlegt.",
                self.session_secret_file,
            )
        return generated

    @staticmethod
    def hash_admin_password(plain: str, salt_hex: str | None = None) -> tuple[str, str]:
        if not plain:
            raise ValueError("Admin-Passwort darf nicht leer sein")
        if salt_hex is None:
            salt_bytes = secrets.token_bytes(16)
        else:
            salt_bytes = bytes.fromhex(salt_hex)
        digest = hashlib.pbkdf2_hmac("sha256", plain.encode("utf-8"), salt_bytes, 260000)
        return salt_bytes.hex(), digest.hex()

    def set_admin_password(self, new_password: str) -> None:
        salt_hex, hash_hex = self.hash_admin_password(new_password)
        self._atomic_write_text(
            self.admin_password_file,
            f"{salt_hex}:{hash_hex}\n",
            mode=PRIVATE_SECRET_FILE_MODE,
        )

    def read_admin_password_record(self) -> str:
        if not self.admin_password_file.exists():
            return ""
        self.ensure_private_file_permissions(self.admin_password_file)
        record = self.admin_password_file.read_text(encoding="utf-8").strip()
        if not record or record == DEFAULT_ADMIN_PASSWORD:
            if record:
                self.logger.warning("Unsicherer oder leerer Admin-Passwort-Datensatz erkannt; Neuinitialisierung wird erzwungen.")
            return ""
        return record

    def ensure_admin_password_file(self) -> None:
        if self.read_admin_password_record():
            return
        env_password = self.resolve_env_admin_password()
        if env_password:
            self.set_admin_password(env_password)
            self.clear_generated_admin_password_file()
            self.logger.info("Admin-Passwort aus TS_CONNECT_UI_ADMIN_PASSWORD initialisiert.")
            return
        generated_password = self.read_generated_admin_password()
        if not generated_password:
            generated_password = self.generate_admin_password()
            self.remember_generated_admin_password(generated_password)
        self.set_admin_password(generated_password)
        self.logger.warning(
            "Es wurde ein zufälliges Admin-Passwort erzeugt und in %s abgelegt.",
            self.admin_password_generated_file,
        )

    def verify_admin_password(self, candidate: str) -> bool:
        self.ensure_admin_password_file()
        record = self.read_admin_password_record()
        if not record:
            return False
        if ":" not in record:
            return secrets.compare_digest(record, candidate)
        salt_hex, hash_hex = record.split(":", 1)
        try:
            _, candidate_hash = self.hash_admin_password(candidate, salt_hex=salt_hex)
        except ValueError:
            return False
        return secrets.compare_digest(hash_hex, candidate_hash)


def require_admin_password(verify_password: Callable[[str], bool], pw: str, *, raise_exc: bool = True) -> bool:
    if verify_password(pw):
        return True
    if raise_exc:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return False


def require_session_auth(request: Request) -> None:
    if not request.session.get("authenticated"):
        raise HTTPException(status_code=401, detail="Unauthorized")
