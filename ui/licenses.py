from __future__ import annotations

import os
LICENSE_RETENTION_DAYS = {
    "basic": 14,
    "plus": 30,
    "pro": 90,
}

LICENSE_PLAN_RULES = {
    "basic": {"min": 0, "max": 29, "label": "Bis 29 Schützen"},
    "plus": {"min": 30, "max": 89, "label": "30 – 89 Schützen"},
    "pro": {"min": 90, "max": None, "label": "Ab 90 Schützen"},
}

LICENSE_PLAN_ORDER = ["basic", "plus", "pro"]

DEFAULT_LICENSE_TIER = os.getenv("TS_CONNECT_DEFAULT_LICENSE_TIER", "basic").strip().lower()
if DEFAULT_LICENSE_TIER not in LICENSE_RETENTION_DAYS:
    DEFAULT_LICENSE_TIER = "basic"

DEFAULT_RETENTION_DAYS = LICENSE_RETENTION_DAYS[DEFAULT_LICENSE_TIER]


def normalize_license_tier(value: str | None) -> str:
    if not value:
        return DEFAULT_LICENSE_TIER
    normalized = value.strip().lower()
    if normalized not in LICENSE_RETENTION_DAYS:
        return DEFAULT_LICENSE_TIER
    return normalized


def retention_for_license(value: str | None) -> int:
    return LICENSE_RETENTION_DAYS.get(normalize_license_tier(value), DEFAULT_RETENTION_DAYS)


def plan_display_name(plan: str | None) -> str:
    if not plan:
        return "Unbekannt"
    return normalize_license_tier(plan).capitalize()


def plan_limit_label(plan: str) -> str:
    plan = normalize_license_tier(plan)
    info = LICENSE_PLAN_RULES.get(plan, LICENSE_PLAN_RULES["basic"])
    min_value = info.get("min")
    max_value = info.get("max")
    label = info.get("label")
    if label:
        return label
    if max_value is None and min_value is not None:
        return f"Ab {min_value} Schützen"
    if min_value is None and max_value is not None:
        return f"Bis {max_value} Schützen"
    if min_value is not None and max_value is not None:
        return f"{min_value} – {max_value} Schützen"
    return "Schützenlimit unbekannt"


def required_plan_for_shooter_count(count: int | None) -> str:
    if count is None or count <= 29:
        return "basic"
    if count <= 89:
        return "plus"
    return "pro"


def plan_allows_shooter_count(plan: str, count: int | None) -> bool:
    if count is None:
        return True
    plan = normalize_license_tier(plan)
    required_plan = required_plan_for_shooter_count(count)
    required_index = LICENSE_PLAN_ORDER.index(required_plan)
    current_index = LICENSE_PLAN_ORDER.index(plan)
    return current_index >= required_index


__all__: tuple[str, ...] = (
    "DEFAULT_LICENSE_TIER",
    "DEFAULT_RETENTION_DAYS",
    "LICENSE_PLAN_RULES",
    "LICENSE_PLAN_ORDER",
    "LICENSE_RETENTION_DAYS",
    "normalize_license_tier",
    "retention_for_license",
    "plan_display_name",
    "plan_limit_label",
    "required_plan_for_shooter_count",
    "plan_allows_shooter_count",
)
