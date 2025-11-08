import unittest

from ui import licenses


class LicenseHelpersTest(unittest.TestCase):
    def test_normalize_license_tier_default_fallback(self):
        self.assertEqual(licenses.normalize_license_tier("PLUS "), "plus")
        self.assertEqual(
            licenses.normalize_license_tier("unknown-tier"),
            licenses.DEFAULT_LICENSE_TIER,
        )

    def test_retention_for_license_returns_expected_days(self):
        self.assertEqual(
            licenses.retention_for_license("basic"),
            licenses.LICENSE_RETENTION_DAYS["basic"],
        )
        self.assertEqual(
            licenses.retention_for_license("pro"),
            licenses.LICENSE_RETENTION_DAYS["pro"],
        )

    def test_plan_limit_label_human_readable(self):
        self.assertIn("30", licenses.plan_limit_label("plus"))
        self.assertIn("90", licenses.plan_limit_label("pro"))

    def test_required_plan_for_shooter_count_thresholds(self):
        self.assertEqual(licenses.required_plan_for_shooter_count(10), "basic")
        self.assertEqual(licenses.required_plan_for_shooter_count(60), "plus")
        self.assertEqual(licenses.required_plan_for_shooter_count(120), "pro")

    def test_plan_allows_shooter_count_enforces_limits(self):
        self.assertTrue(licenses.plan_allows_shooter_count("pro", 500))
        self.assertTrue(licenses.plan_allows_shooter_count("plus", 50))
        self.assertFalse(licenses.plan_allows_shooter_count("basic", 40))


if __name__ == "__main__":
    unittest.main()
