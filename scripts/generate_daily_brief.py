# scripts/generate_daily_brief.py

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from front_sector import detect_sector

DATA_DIR = Path("data")
OUT_TXT = DATA_DIR / "brief_daily.txt"
OUT_JSON = DATA_DIR / "brief_daily.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def fmt_num(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}".replace(",", " ")


def sign_prefix(value: float) -> str:
    return "+" if value > 0 else ""


# ---------------------------------------------------
# Activity classification
# ---------------------------------------------------

def classify_ground_activity(raw_total: int, kept_points: int, kept_lines: int) -> str:

    useful = kept_points + kept_lines

    if raw_total == 0:
        return "low"

    ratio = useful / raw_total

    if useful >= 10 or ratio >= 0.15:
        return "high"

    if useful >= 4 or ratio >= 0.05:
        return "medium"

    return "low"


def classify_uav_pressure(events_7d: int) -> str:

    if events_7d >= 1800:
        return "very high"

    if events_7d >= 1000:
        return "high"

    if events_7d >= 400:
        return "moderate"

    return "low"


# ---------------------------------------------------
# Title
# ---------------------------------------------------

def build_title(daily: dict, weekly: dict, gained_sector: str | None):

    daily_delta = float(daily.get("delta_km2", 0))
    weekly_delta = float(weekly.get("delta_km2", 0))

    if daily_delta > 0 and gained_sector:
        return f"Ukraine War Daily Brief – Russian pressure continues near {gained_sector}"

    if daily_delta > 0 and weekly_delta > 0:
        return "Ukraine War Daily Brief – Gradual Russian gains continue"

    if daily_delta < 0:
        return "Ukraine War Daily Brief – Ukrainian counter-pressure observed"

    return "Ukraine War Daily Brief – Limited territorial movement along the frontline"


# ---------------------------------------------------
# Executive summary
# ---------------------------------------------------

def build_intro(daily: dict, weekly: dict, gained_sector: str | None):

    return (
        f"The frontline situation continues to reflect a pattern of gradual positional warfare. "
        f"According to the latest DeepState estimate, Russian-occupied territory stands at "
        f"{fmt_num(daily['occupied_km2'])} km². "
        f"The daily change amounts to {sign_prefix(daily['delta_km2'])}{fmt_num(daily['delta_km2'])} km² "
        f"({daily['interpretation']}). "
        f"The weekly balance shows {sign_prefix(weekly['delta_km2'])}{fmt_num(weekly['delta_km2'],1)} km² change, "
        f"suggesting that recent developments are part of a continuing operational trend."
    )


# ---------------------------------------------------
# Operational picture
# ---------------------------------------------------

def build_operational_picture(gained_sector, ground, uav):

    latest_ground = ground.get("latest_stats", {})
    raw = latest_ground.get("raw_total", 0)
    kept = latest_ground.get("kept_points", 0) + latest_ground.get("kept_lines", 0)
    uav7 = uav.get("events_7d", 0)

    text = []

    if gained_sector:
        text.append(
            f"The latest mapped territorial activity appears concentrated in the {gained_sector} sector."
        )

    text.append(
        f"A total of {raw} ground combat mentions were identified in source reporting, "
        f"of which {kept} could be mapped with sufficient geographic precision."
    )

    text.append(
        f"Drone and UAV activity remains a central operational factor, "
        f"with {uav7} recorded events in the past seven days."
    )

    return " ".join(text)


# ---------------------------------------------------
# Events
# ---------------------------------------------------

def build_events(gained_sector, lost_sector):

    events = []

    if gained_sector:
        events.append(
            f"Recent territorial change appears to be located in the {gained_sector} sector."
        )

    if lost_sector:
        events.append(
            f"A localized Ukrainian regain was detected in the {lost_sector} sector."
        )

    events.append(
        "Many combat reports remain insufficiently geolocated, suggesting a pattern of dispersed small-unit engagements."
    )

    events.append(
        "Sustained UAV activity continues to support reconnaissance and precision strike operations."
    )

    return events


# ---------------------------------------------------
# Drivers
# ---------------------------------------------------

def build_drivers(daily, weekly, ground, uav):

    latest_ground = ground.get("latest_stats", {})

    ground_level = classify_ground_activity(
        latest_ground.get("raw_total", 0),
        latest_ground.get("kept_points", 0),
        latest_ground.get("kept_lines", 0),
    )

    uav_level = classify_uav_pressure(uav.get("events_7d", 0))

    drivers = []

    drivers.append(
        f"Ground combat activity level assessed as {ground_level}."
    )

    drivers.append(
        f"UAV operational pressure assessed as {uav_level}."
    )

    drivers.append(
        "Territorial change remains gradual rather than breakthrough-driven."
    )

    return drivers


# ---------------------------------------------------
# Assessment
# ---------------------------------------------------

def build_assessment(daily, weekly):

    daily_delta = float(daily.get("delta_km2", 0))
    weekly_delta = float(weekly.get("delta_km2", 0))

    if daily_delta > 0 and weekly_delta > 0:
        return (
            "The data indicates continued Russian positional pressure combined with attritional warfare dynamics."
        )

    if daily_delta == 0:
        return (
            "Operational activity remains high despite limited territorial movement."
        )

    return (
        "Local counter-movements continue to shape the frontline dynamics."
    )


# ---------------------------------------------------
# Outlook
# ---------------------------------------------------

def build_outlook(daily, uav):

    delta = float(daily.get("delta_km2", 0))
    uav7 = uav.get("events_7d", 0)

    if delta > 0 and uav7 > 1000:
        return (
            "If the current operational tempo continues, gradual territorial pressure is likely to persist in the coming days."
        )

    if delta == 0:
        return (
            "The next phase of operations will likely depend on localized offensives rather than large-scale maneuver."
        )

    return (
            "Short-term developments will likely remain driven by localized tactical engagements."
    )


# ---------------------------------------------------
# Build full text
# ---------------------------------------------------

def build_text(daily, weekly, change, ground, uav):

    gained = change.get("gained_centroid")
    lost = change.get("lost_centroid")

    gained_sector = detect_sector(gained[0], gained[1]) if gained else None
    lost_sector = detect_sector(lost[0], lost[1]) if lost else None

    title = build_title(daily, weekly, gained_sector)

    intro = build_intro(daily, weekly, gained_sector)

    operational_picture = build_operational_picture(
        gained_sector, ground, uav
    )

    events = build_events(gained_sector, lost_sector)

    drivers = build_drivers(daily, weekly, ground, uav)

    assessment = build_assessment(daily, weekly)

    outlook = build_outlook(daily, uav)

    numbers = [
        f"Occupied territory: {fmt_num(daily['occupied_km2'])} km²",
        f"Daily change: {sign_prefix(daily['delta_km2'])}{fmt_num(daily['delta_km2'])} km²",
        f"Weekly change: {sign_prefix(weekly['delta_km2'])}{fmt_num(weekly['delta_km2'],1)} km²",
        f"Ground combat mentions: {ground['latest_stats']['raw_total']}",
        f"Mapped combat events: {ground['latest_stats']['kept_points']}",
        f"UAV events (7d): {uav['events_7d']}",
    ]

    lines = []

    lines.append(title)
    lines.append("")
    lines.append(intro)
    lines.append("")
    lines.append("Key numbers:")
    lines.extend([f"- {x}" for x in numbers])
    lines.append("")
    lines.append("Operational picture:")
    lines.append(operational_picture)
    lines.append("")
    lines.append("Key events:")
    lines.extend([f"- {x}" for x in events])
    lines.append("")
    lines.append("Drivers of the day:")
    lines.extend([f"- {x}" for x in drivers])
    lines.append("")
    lines.append("Assessment:")
    lines.append(assessment)
    lines.append("")
    lines.append("Outlook:")
    lines.append(outlook)

    return "\n".join(lines), title, gained_sector, lost_sector


# ---------------------------------------------------
# JSON output
# ---------------------------------------------------

def build_json(text, title, gained_sector, lost_sector, daily):

    return {
        "date": daily["date"],
        "title": title,
        "sector_gain": gained_sector,
        "sector_loss": lost_sector,
        "text": text,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------
# Main
# ---------------------------------------------------

def main():

    daily = load_json(DATA_DIR / "summary_daily.json")
    weekly = load_json(DATA_DIR / "summary_weekly.json")
    change = load_json(DATA_DIR / "change_latest.json")
    ground = load_json(DATA_DIR / "isw_ground_index.json")
    uav = load_json(DATA_DIR / "isw_uav_index.json")

    text, title, gained_sector, lost_sector = build_text(
        daily, weekly, change, ground, uav
    )

    payload = build_json(text, title, gained_sector, lost_sector, daily)

    OUT_TXT.write_text(text, encoding="utf-8")
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print("Daily brief generated.")


if __name__ == "__main__":
    main()
