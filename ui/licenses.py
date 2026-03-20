from __future__ import annotations

import os
LICENSE_RETENTION_DAYS = {
    "unlicensed": 14,
    "club_plus": int((os.getenv("TS_CONNECT_LICENSE_RETENTION_DAYS", "30") or "30").strip() or "30"),
}

LICENSE_PLAN_RULES = {
    "unlicensed": {"min": None, "max": None, "label": "Keine aktive Lizenz"},
    "club_plus": {"min": 0, "max": None, "label": "Vereinslizenz"},
}

LICENSE_PLAN_ORDER = ["unlicensed", "club_plus"]

DEFAULT_LICENSE_TIER = os.getenv("TS_CONNECT_DEFAULT_LICENSE_TIER", "unlicensed").strip().lower()
if DEFAULT_LICENSE_TIER not in LICENSE_RETENTION_DAYS:
    DEFAULT_LICENSE_TIER = "unlicensed"

DEFAULT_RETENTION_DAYS = LICENSE_RETENTION_DAYS[DEFAULT_LICENSE_TIER]


def normalize_license_tier(value: str | None) -> str:
    if not value:
        return DEFAULT_LICENSE_TIER
    normalized = value.strip().lower()
    if normalized in {"basic", "plus", "pro", "club", "clubplus"}:
        return "club_plus"
    if normalized not in LICENSE_RETENTION_DAYS:
        return DEFAULT_LICENSE_TIER
    return normalized


def retention_for_license(value: str | None) -> int:
    return LICENSE_RETENTION_DAYS.get(normalize_license_tier(value), DEFAULT_RETENTION_DAYS)


def plan_display_name(plan: str | None) -> str:
    if not plan:
        return "Unbekannt"
    normalized = normalize_license_tier(plan)
    if normalized == "club_plus":
        return "Club Plus"
    if normalized == "unlicensed":
        return "Nicht lizenziert"
    return normalized.capitalize()


def plan_limit_label(plan: str) -> str:
    plan = normalize_license_tier(plan)
    info = LICENSE_PLAN_RULES.get(plan, LICENSE_PLAN_RULES["unlicensed"])
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
    return "club_plus"


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
