import ast
import pathlib
import unittest


APP_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app.py"
APP_TEXT = APP_SOURCE.read_text(encoding="utf-8")


def _load_items(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = APP_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            item_module = ast.Module(body=[node], type_ignores=[])
            exec(compile(item_module, str(APP_SOURCE), "exec"), namespace)  # noqa: S102


class KeygenValidationContractTest(unittest.TestCase):
    def test_build_keygen_validate_meta_omits_machine_scope_for_web_save(self):
        namespace = {
            "Any": object,
            "KEYGEN_POLICY_ID": "policy-123",
            "_resolve_machine_fingerprint_scope": lambda: ["fp-1", "fp-2"],
        }
        _load_items(["_build_keygen_validate_meta"], namespace)

        meta = namespace["_build_keygen_validate_meta"](
            " KEY-123 ",
            include_machine_scope=False,
        )

        self.assertEqual(meta["key"], "KEY-123")
        self.assertEqual(meta["scope"], {"policy": "policy-123"})

    def test_validate_license_key_remote_does_not_use_license_auth_lookup_for_web_save(self):
        module = ast.parse(APP_TEXT)
        validate_node = next(
            node
            for node in module.body
            if isinstance(node, ast.AsyncFunctionDef) and node.name == "validate_license_key_remote"
        )
        validate_source = ast.get_source_segment(APP_TEXT, validate_node) or ""

        self.assertNotIn("_lookup_keygen_license_by_key", validate_source)
        self.assertNotIn("licenses/me", validate_source)

    def test_parse_keygen_payload_keeps_club_plus_for_unactivated_machine_state(self):
        namespace = {
            "Any": object,
            "KEYGEN_POLICY_ID": "policy-123",
            "KEYGEN_POLICY_NAME": "Club Plus",
            "DEFAULT_LICENSE_TIER": "unlicensed",
            "KEYGEN_ACTIVATABLE_STATUSES": {
                "valid",
                "active",
                "no_machine",
                "no_machines",
                "fingerprint_scope_required",
                "fingerprint_scope_mismatch",
                "machine_scope_required",
                "machine_scope_mismatch",
            },
            "normalize_license_tier": lambda plan: "club_plus" if plan == "club_plus" else "unlicensed",
            "_normalize_license_expiry": lambda value: value,
        }
        _load_items(
            [
                "_normalize_provider_status",
                "_extract_keygen_message",
                "_parse_keygen_license_validation_payload",
            ],
            namespace,
        )

        parsed = namespace["_parse_keygen_license_validation_payload"](
            {
                "meta": {
                    "valid": False,
                    "code": "NO_MACHINE",
                    "detail": "machine activation required",
                },
                "data": {
                    "id": "lic-1",
                    "attributes": {
                        "status": "ACTIVE",
                        "expiry": "2026-04-01T00:00:00Z",
                        "metadata": {},
                    },
                    "relationships": {
                        "policy": {
                            "data": {"id": "policy-123"},
                        }
                    },
                },
            }
        )

        self.assertFalse(parsed["valid"])
        self.assertEqual(parsed["status"], "no_machine")
        self.assertEqual(parsed["plan"], "club_plus")

    def test_parse_keygen_direct_license_payload_stays_valid_without_machine_scope(self):
        namespace = {
            "Any": object,
            "KEYGEN_POLICY_ID": "policy-123",
            "KEYGEN_POLICY_NAME": "Club Plus",
            "DEFAULT_LICENSE_TIER": "unlicensed",
            "KEYGEN_ACTIVATABLE_STATUSES": {
                "valid",
                "active",
                "no_machine",
            },
            "normalize_license_tier": lambda plan: "club_plus" if plan == "club_plus" else "unlicensed",
            "_normalize_license_expiry": lambda value: value,
        }
        _load_items(
            [
                "_normalize_provider_status",
                "_extract_keygen_message",
                "_parse_keygen_license_validation_payload",
            ],
            namespace,
        )

        parsed = namespace["_parse_keygen_license_validation_payload"](
            {
                "data": {
                    "id": "lic-2",
                    "attributes": {
                        "status": "ACTIVE",
                        "expiry": "2026-04-01T00:00:00Z",
                        "metadata": {},
                    },
                    "relationships": {
                        "policy": {
                            "data": {"id": "policy-123"},
                        }
                    },
                },
            }
        )

        self.assertTrue(parsed["valid"])
        self.assertEqual(parsed["status"], "active")
        self.assertEqual(parsed["plan"], "club_plus")

    def test_keygen_license_entitled_accepts_unactivated_machine_states(self):
        namespace = {
            "Any": object,
            "KEYGEN_ACTIVATABLE_STATUSES": {
                "no_machine",
                "fingerprint_scope_required",
            },
        }
        _load_items(
            [
                "_normalize_provider_status",
                "_keygen_license_entitled",
            ],
            namespace,
        )

        entitled = namespace["_keygen_license_entitled"](
            {
                "valid": False,
                "status": "no_machine",
            }
        )

        self.assertTrue(entitled)

    def test_keygen_license_entitled_rejects_truly_invalid_states(self):
        namespace = {
            "Any": object,
            "KEYGEN_ACTIVATABLE_STATUSES": {
                "no_machine",
                "fingerprint_scope_required",
            },
        }
        _load_items(
            [
                "_normalize_provider_status",
                "_keygen_license_entitled",
            ],
            namespace,
        )

        entitled = namespace["_keygen_license_entitled"](
            {
                "valid": False,
                "status": "suspended",
            }
        )

        self.assertFalse(entitled)

    def test_license_validation_state_uses_entitlement_to_keep_club_plus(self):
        namespace = {
            "Any": object,
            "KEYGEN_ACTIVATABLE_STATUSES": {
                "no_machine",
            },
            "DEFAULT_LICENSE_TIER": "unlicensed",
            "DEFAULT_RETENTION_DAYS": 7,
            "LICENSE_RETENTION_DAYS": {
                "unlicensed": 7,
                "club_plus": 30,
            },
            "normalize_license_tier": lambda plan: "club_plus" if plan == "club_plus" else "unlicensed",
            "_normalize_iso8601": lambda value: value,
            "_now_utc_iso": lambda: "2026-03-21T10:00:00Z",
        }
        _load_items(
            [
                "_normalize_provider_status",
                "_keygen_license_entitled",
                "_license_validation_state",
            ],
            namespace,
        )

        plan, retention_days, status, expires_at_norm, last_checked = namespace["_license_validation_state"](
            {
                "valid": False,
                "status": "no_machine",
                "plan": "club_plus",
                "expires_at": "2026-04-01T00:00:00Z",
            }
        )

        self.assertEqual(plan, "club_plus")
        self.assertEqual(retention_days, 30)
        self.assertEqual(status, "no_machine")
        self.assertEqual(expires_at_norm, "2026-04-01T00:00:00Z")
        self.assertEqual(last_checked, "2026-03-21T10:00:00Z")


if __name__ == "__main__":
    unittest.main()
