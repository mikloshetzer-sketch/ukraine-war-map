# scripts/calc_daily_stats.py

from __future__ import annotations

import json
from pathlib import Path

DATA_DIR = Path("data")


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def calculate_daily_stats() -> dict:
    daily = load_json(DATA_DIR / "summary_daily.json")
    weekly = load_json(DATA_DIR / "summary_weekly.json")
    ground = load_json(DATA_DIR / "isw_ground_index.json")
    uav = load_json(DATA_DIR / "isw_uav_index.json")
    change = load_json(DATA_DIR / "change_latest.json")

    latest_ground = ground.get("latest_stats", {}) if isinstance(ground, dict) else {}

    stats = {
        "occupied_km2": to_float(daily.get("occupied_km2"), default=None),
        "daily_delta_km2": to_float(daily.get("delta_km2"), default=None),
        "daily_interpretation": daily.get("interpretation"),
        "weekly_delta_km2": to_float(weekly.get("delta_km2"), default=None),
        "weekly_interpretation": weekly.get("interpretation"),
        "ground_raw_total": to_int(latest_ground.get("raw_total"), default=None),
        "ground_kept_points": to_int(latest_ground.get("kept_points"), default=None),
        "ground_kept_lines": to_int(latest_ground.get("kept_lines"), default=None),
        "uav_events_total": to_int(uav.get("events_total"), default=None),
        "uav_events_7d": to_int(uav.get("events_7d"), default=None),
        "gained_centroid": change.get("gained_centroid"),
        "lost_centroid": change.get("lost_centroid"),
    }

    return stats


if __name__ == "__main__":
    print(json.dumps(calculate_daily_stats(), ensure_ascii=False, indent=2))
