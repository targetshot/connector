import ast
import pathlib
import unittest


APP_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app.py"


def _load_items(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = APP_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            item_module = ast.Module(body=[node], type_ignores=[])
            exec(compile(item_module, str(APP_SOURCE), "exec"), namespace)  # noqa: S102


class LicenseFormStateTest(unittest.TestCase):
    def test_resolve_license_form_state_prefers_pending_draft(self):
        namespace = {}
        _load_items(["_resolve_license_form_state"], namespace)

        display, draft, pending = namespace["_resolve_license_form_state"](
            "OLD-KEY-123",
            "NEW-KEY-456",
        )

        self.assertEqual(display, "NEW-KEY-456")
        self.assertEqual(draft, "NEW-KEY-456")
        self.assertTrue(pending)

    def test_resolve_license_form_state_clears_same_value_draft(self):
        namespace = {}
        _load_items(["_resolve_license_form_state"], namespace)

        display, draft, pending = namespace["_resolve_license_form_state"](
            "SAME-KEY",
            "SAME-KEY",
        )

        self.assertEqual(display, "SAME-KEY")
        self.assertEqual(draft, "")
        self.assertFalse(pending)

    def test_set_and_get_license_form_key_session_roundtrip(self):
        class FakeRequest:
            def __init__(self):
                self.session = {}

        namespace = {
            "LICENSE_FORM_KEY_SESSION": "license_form_key",
            "Request": object,
        }
        _load_items(["_set_license_form_key", "_get_license_form_key"], namespace)

        request = FakeRequest()
        namespace["_set_license_form_key"](request, " NEW-KEY ")
        self.assertEqual(namespace["_get_license_form_key"](request), "NEW-KEY")

        namespace["_set_license_form_key"](request, "")
        self.assertEqual(namespace["_get_license_form_key"](request), "")
        self.assertNotIn("license_form_key", request.session)


if __name__ == "__main__":
    unittest.main()
