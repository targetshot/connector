import ast
import pathlib
import unittest
from unittest import mock


APP_SOURCE = pathlib.Path(__file__).resolve().parents[1] / "app.py"


def _load_functions(names: list[str], namespace: dict) -> None:
    wanted = set(names)
    source = APP_SOURCE.read_text(encoding="utf-8")
    module = ast.parse(source)
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            fn_module = ast.Module(body=[node], type_ignores=[])
            exec(compile(fn_module, str(APP_SOURCE), "exec"), namespace)  # noqa: S102


class AppSecurityWrapperTest(unittest.TestCase):
    def test_verify_admin_password_delegates_to_security_bootstrap(self):
        security_bootstrap = mock.Mock()
        security_bootstrap.verify_admin_password.return_value = True
        namespace = {"security_bootstrap": security_bootstrap}
        _load_functions(["verify_admin_password"], namespace)

        result = namespace["verify_admin_password"]("secret")

        self.assertTrue(result)
        security_bootstrap.verify_admin_password.assert_called_once_with("secret")

    def test_require_admin_uses_shared_guard(self):
        namespace = {
            "require_admin_password": mock.Mock(return_value=True),
            "verify_admin_password": mock.Mock(),
        }
        _load_functions(["require_admin"], namespace)

        result = namespace["require_admin"]("secret", raise_exc=False)

        self.assertTrue(result)
        namespace["require_admin_password"].assert_called_once_with(
            namespace["verify_admin_password"],
            "secret",
            raise_exc=False,
        )


if __name__ == "__main__":
    unittest.main()
