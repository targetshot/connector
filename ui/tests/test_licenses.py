import unittest

from ui import licenses


class LicenseHelpersTest(unittest.TestCase):
    def test_normalize_license_tier_default_fallback(self):
        self.assertEqual(licenses.normalize_license_tier("PLUS "), "club_plus")
        self.assertEqual(
            licenses.normalize_license_tier("unknown-tier"),
            licenses.DEFAULT_LICENSE_TIER,
        )

    def test_retention_for_license_returns_expected_days(self):
        self.assertEqual(
            licenses.retention_for_license("unlicensed"),
            licenses.LICENSE_RETENTION_DAYS["unlicensed"],
        )
        self.assertEqual(
            licenses.retention_for_license("pro"),
            licenses.LICENSE_RETENTION_DAYS["club_plus"],
        )

    def test_plan_limit_label_human_readable(self):
        self.assertIn("Vereinslizenz", licenses.plan_limit_label("plus"))
        self.assertIn("Vereinslizenz", licenses.plan_limit_label("club_plus"))

    def test_required_plan_for_shooter_count_thresholds(self):
        self.assertEqual(licenses.required_plan_for_shooter_count(10), "club_plus")
        self.assertEqual(licenses.required_plan_for_shooter_count(60), "club_plus")
        self.assertEqual(licenses.required_plan_for_shooter_count(120), "club_plus")

    def test_plan_allows_shooter_count_enforces_limits(self):
        self.assertTrue(licenses.plan_allows_shooter_count("club_plus", 500))
        self.assertTrue(licenses.plan_allows_shooter_count("plus", 50))
        self.assertFalse(licenses.plan_allows_shooter_count("unlicensed", 40))


if __name__ == "__main__":
    unittest.main()
