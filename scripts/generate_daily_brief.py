# scripts/generate_daily_brief.py

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from calc_daily_stats import calculate_daily_stats
from front_sector import detect_sector

DATA_DIR = Path("data")
OUT_TXT = DATA_DIR / "brief_daily.txt"
OUT_JSON = DATA_DIR / "brief_daily.json"


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def fmt_num(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}".replace(",", " ")


def sign_prefix(value: float | None) -> str:
    if value is None:
        return ""
    return "+" if value > 0 else ""


def classify_ground_activity(raw_total: int | None, kept_points: int | None, kept_lines: int | None) -> str:
    if raw_total is None or kept_points is None or kept_lines is None:
        return "unknown"

    useful = kept_points + kept_lines

    if raw_total == 0:
        return "low"

    ratio = useful / raw_total

    if useful >= 10 or ratio >= 0.15:
        return "high"

    if useful >= 4 or ratio >= 0.05:
        return "medium"

    return "low"


def classify_uav_pressure(events_7d: int | None) -> str:
    if events_7d is None:
        return "unknown"

    if events_7d >= 1800:
        return "very high"

    if events_7d >= 1000:
        return "high"

    if events_7d >= 400:
        return "moderate"

    return "low"


def build_title(stats: dict, gained_sector: str | None) -> str:
    daily_delta = stats.get("daily_delta_km2")
    weekly_delta = stats.get("weekly_delta_km2")

    if daily_delta is not None and daily_delta > 0 and gained_sector:
        return f"Ukraine War Daily Brief – Russian pressure continues near {gained_sector}"

    if daily_delta is not None and weekly_delta is not None and daily_delta > 0 and weekly_delta > 0:
        return "Ukraine War Daily Brief – Gradual Russian gains continue"

    if daily_delta is not None and daily_delta < 0:
        return "Ukraine War Daily Brief – Ukrainian counter-pressure observed"

    return "Ukraine War Daily Brief – Limited territorial movement along the frontline"


def build_intro(daily: dict, weekly: dict, stats: dict, gained_sector: str | None) -> str:
    occupied = fmt_num(stats.get("occupied_km2"))
    daily_delta = stats.get("daily_delta_km2")
    weekly_delta = stats.get("weekly_delta_km2")

    daily_vs = daily.get("vs_date", "the previous day")
    daily_interp = daily.get("interpretation", "no interpretation available")
    weekly_interp = weekly.get("interpretation", "no interpretation available")

    intro = (
        f"The frontline situation continues to reflect a pattern of gradual positional warfare. "
        f"According to the latest DeepState estimate, Russian-occupied territory stands at {occupied} km². "
    )

    if daily_delta is not None:
        intro += (
            f"The daily change amounts to {sign_prefix(daily_delta)}{fmt_num(daily_delta)} km² "
            f"compared with {daily_vs} ({daily_interp}). "
        )
    else:
        intro += "No confirmed daily territorial delta is currently available. "

    if weekly_delta is not None:
        intro += (
            f"The weekly balance shows {sign_prefix(weekly_delta)}{fmt_num(weekly_delta, 1)} km² change "
            f"({weekly_interp}), suggesting that recent developments are part of a continuing operational trend."
        )
    else:
        intro += "No confirmed weekly territorial delta is currently available."

    if gained_sector:
        intro += f" The latest mapped activity points toward the {gained_sector} sector as a likely focal area."

    return intro


def build_operational_picture(gained_sector: str | None, stats: dict) -> str:
    raw = stats.get("ground_raw_total")
    kept_points = stats.get("ground_kept_points")
    kept_lines = stats.get("ground_kept_lines")
    kept_total = None if kept_points is None or kept_lines is None else kept_points + kept_lines
    uav7 = stats.get("uav_events_7d")

    parts = []

    if gained_sector:
        parts.append(
            f"The latest mapped territorial activity appears concentrated in the {gained_sector} sector."
        )

    if raw is not None and kept_total is not None:
        parts.append(
            f"A total of {raw} ground combat mentions were identified in source reporting, "
            f"of which {kept_total} could be mapped with sufficient geographic precision."
        )
    else:
        parts.append(
            "Ground combat reporting remains available, but not all current event totals could be resolved into the brief."
        )

    if uav7 is not None:
        parts.append(
            f"Drone and UAV activity remains a central operational factor, with {uav7} recorded events in the past seven days."
        )
    else:
        parts.append(
            "Drone and UAV activity remains a central operational factor, although the current 7-day event total is unavailable."
        )

    return " ".join(parts)


def build_events(gained_sector: str | None, lost_sector: str | None, stats: dict) -> list[str]:
    events = []

    if gained_sector:
        events.append(
            f"Recent territorial change appears to be located in the {gained_sector} sector."
        )

    if lost_sector:
        events.append(
            f"A localized loss or regain signal was detected in the {lost_sector} sector."
        )

    raw = stats.get("ground_raw_total")
    kept_points = stats.get("ground_kept_points")
    kept_lines = stats.get("ground_kept_lines")

    if raw is not None and kept_points is not None and kept_lines is not None:
        events.append(
            f"The gap between raw reporting and mapped events remains notable: "
            f"{raw} raw mentions resulted in {kept_points} mapped points and {kept_lines} mapped lines."
        )
    else:
        events.append(
            "The gap between raw reporting and mapped events remains notable, suggesting that many reports are still only partially geolocated."
        )

    uav7 = stats.get("uav_events_7d")
    if uav7 is not None:
        events.append(
            f"Sustained UAV activity continues to support reconnaissance and strike operations, with {uav7} events recorded over the past seven days."
        )
    else:
        events.append(
            "Sustained UAV activity continues to support reconnaissance and strike operations."
        )

    return events


def build_drivers(stats: dict) -> list[str]:
    ground_level = classify_ground_activity(
        stats.get("ground_raw_total"),
        stats.get("ground_kept_points"),
        stats.get("ground_kept_lines"),
    )

    uav_level = classify_uav_pressure(stats.get("uav_events_7d"))

    drivers = [
        f"Ground combat activity level assessed as {ground_level}.",
        f"UAV operational pressure assessed as {uav_level}.",
        "Territorial change remains gradual rather than breakthrough-driven.",
    ]

    return drivers


def build_assessment(stats: dict) -> str:
    daily_delta = stats.get("daily_delta_km2")
    weekly_delta = stats.get("weekly_delta_km2")

    if daily_delta is not None and weekly_delta is not None and daily_delta > 0 and weekly_delta > 0:
        return "The data indicates continued Russian positional pressure combined with attritional warfare dynamics."

    if daily_delta == 0:
        return "Operational activity remains high despite limited territorial movement."

    if daily_delta is not None and daily_delta < 0:
        return "Local counter-movements continue to shape the frontline dynamics."

    return "The available data suggests continued pressure along the frontline, but with incomplete quantitative confirmation."


def build_outlook(stats: dict) -> str:
    delta = stats.get("daily_delta_km2")
    uav7 = stats.get("uav_events_7d")

    if delta is not None and uav7 is not None and delta > 0 and uav7 > 1000:
        return "If the current operational tempo continues, gradual territorial pressure is likely to persist in the coming days."

    if delta == 0:
        return "The next phase of operations will likely depend on localized offensives rather than large-scale maneuver."

    return "Short-term developments will likely remain driven by localized tactical engagements."


def build_metrics(stats: dict) -> list[dict]:
    kept_points = stats.get("ground_kept_points")
    kept_lines = stats.get("ground_kept_lines")
    mapped_total = None
    if kept_points is not None and kept_lines is not None:
        mapped_total = kept_points + kept_lines

    return [
        {
            "label": "Occupied territory",
            "value": f"{fmt_num(stats.get('occupied_km2'))} km²",
        },
        {
            "label": "Daily change",
            "value": f"{sign_prefix(stats.get('daily_delta_km2'))}{fmt_num(stats.get('daily_delta_km2'))} km²",
        },
        {
            "label": "Weekly change",
            "value": f"{sign_prefix(stats.get('weekly_delta_km2'))}{fmt_num(stats.get('weekly_delta_km2'), 1)} km²",
        },
        {
            "label": "Ground combat reports",
            "value": str(stats.get("ground_raw_total", "n/a")) if stats.get("ground_raw_total") is not None else "n/a",
        },
        {
            "label": "Mapped events",
            "value": str(mapped_total) if mapped_total is not None else "n/a",
        },
        {
            "label": "UAV activity (7d)",
            "value": str(stats.get("uav_events_7d", "n/a")) if stats.get("uav_events_7d") is not None else "n/a",
        },
    ]


def build_analysis(daily: dict, weekly: dict, stats: dict, gained_sector: str | None, lost_sector: str | None) -> str:
    occupied = fmt_num(stats.get("occupied_km2"))
    daily_delta = stats.get("daily_delta_km2")
    weekly_delta = stats.get("weekly_delta_km2")
    raw = stats.get("ground_raw_total")
    kept_points = stats.get("ground_kept_points")
    kept_lines = stats.get("ground_kept_lines")
    uav7 = stats.get("uav_events_7d")

    kept_total = None if kept_points is None or kept_lines is None else kept_points + kept_lines

    p1 = (
        f"Russian forces continue to apply gradual positional pressure along the frontline. "
        f"According to the latest DeepState estimate, Russian-occupied territory stands at {occupied} km²."
    )

    if daily_delta is not None:
        p1 += f" The daily change amounts to {sign_prefix(daily_delta)}{fmt_num(daily_delta)} km²"
        if daily.get("vs_date"):
            p1 += f" compared with {daily['vs_date']}"
        if daily.get("interpretation"):
            p1 += f" ({daily['interpretation']})"
        p1 += "."
    else:
        p1 += " No confirmed daily territorial delta is currently available."

    if weekly_delta is not None:
        p1 += (
            f" On a weekly basis, the balance shows {sign_prefix(weekly_delta)}"
            f"{fmt_num(weekly_delta, 1)} km² change"
        )
        if weekly.get("interpretation"):
            p1 += f" ({weekly['interpretation']})"
        p1 += ", indicating that recent developments remain part of a continuing pattern of incremental pressure rather than a sudden operational shift."
    else:
        p1 += " No confirmed weekly territorial delta is currently available."

    if gained_sector:
        p1 += f" The latest mapped activity points toward the {gained_sector} sector as the most likely focal area."

    p2 = ""
    if raw is not None and kept_total is not None:
        p2 += (
            f"Source reporting recorded {raw} mentions of ground combat activity during the reporting period, "
            f"although only {kept_total} events could be mapped with sufficient geographic precision."
        )
    else:
        p2 += (
            "Source reporting continues to indicate ground combat activity, although the currently usable mapped-event totals remain incomplete."
        )

    p2 += " This gap between raw reporting and confirmed mapped activity remains characteristic of the current phase of the war."

    if lost_sector:
        p2 += f" A localized loss or regain signal was also detected in the {lost_sector} sector."

    p3 = "Drone warfare continues to shape battlefield dynamics."
    if uav7 is not None:
        p3 += (
            f" Over the past seven days, {uav7} UAV-related events were recorded, "
            f"highlighting the scale of reconnaissance and strike operations."
        )
    else:
        p3 += " Current seven-day UAV totals are unavailable, but drone activity remains a major operational factor."

    p4 = (
        f"Overall, {build_assessment(stats)} "
        f"{build_outlook(stats)}"
    )

    return "\n\n".join([p1, p2, p3, p4])


def build_legacy_text(
    title: str,
    intro: str,
    operational_picture: str,
    events: list[str],
    drivers: list[str],
    assessment: str,
    outlook: str,
    stats: dict,
) -> str:
    numbers = [
        f"Occupied territory: {fmt_num(stats.get('occupied_km2'))} km²",
        f"Daily change: {sign_prefix(stats.get('daily_delta_km2'))}{fmt_num(stats.get('daily_delta_km2'))} km²",
        f"Weekly change: {sign_prefix(stats.get('weekly_delta_km2'))}{fmt_num(stats.get('weekly_delta_km2'), 1)} km²",
        f"Ground raw events: {stats.get('ground_raw_total', 'n/a') if stats.get('ground_raw_total') is not None else 'n/a'}",
        f"Mapped points: {stats.get('ground_kept_points', 'n/a') if stats.get('ground_kept_points') is not None else 'n/a'}",
        f"Mapped lines: {stats.get('ground_kept_lines', 'n/a') if stats.get('ground_kept_lines') is not None else 'n/a'}",
        f"UAV events (7d): {stats.get('uav_events_7d', 'n/a') if stats.get('uav_events_7d') is not None else 'n/a'}",
    ]

    lines = [
        title,
        "",
        intro,
        "",
        "Key numbers:",
    ]
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

    return "\n".join(lines)


def build_text(daily: dict, weekly: dict, stats: dict):
    gained = stats.get("gained_centroid")
    lost = stats.get("lost_centroid")

    gained_sector = detect_sector(gained[0], gained[1]) if gained and len(gained) >= 2 else None
    lost_sector = detect_sector(lost[0], lost[1]) if lost and len(lost) >= 2 else None

    title = build_title(stats, gained_sector)
    intro = build_intro(daily, weekly, stats, gained_sector)
    operational_picture = build_operational_picture(gained_sector, stats)
    events = build_events(gained_sector, lost_sector, stats)
    drivers = build_drivers(stats)
    assessment = build_assessment(stats)
    outlook = build_outlook(stats)

    legacy_text = build_legacy_text(
        title=title,
        intro=intro,
        operational_picture=operational_picture,
        events=events,
        drivers=drivers,
        assessment=assessment,
        outlook=outlook,
        stats=stats,
    )

    metrics = build_metrics(stats)
    analysis = build_analysis(daily, weekly, stats, gained_sector, lost_sector)

    return legacy_text, title, gained_sector, lost_sector, metrics, analysis


def build_json(
    text: str,
    title: str,
    gained_sector: str | None,
    lost_sector: str | None,
    daily: dict,
    weekly: dict,
    stats: dict,
    metrics: list[dict],
    analysis: str,
) -> dict:
    return {
        "date": daily.get("date"),
        "title": title,
        "summary": {
            "occupied_km2": stats.get("occupied_km2"),
            "daily_delta_km2": stats.get("daily_delta_km2"),
            "daily_interpretation": stats.get("daily_interpretation"),
            "weekly_delta_km2": stats.get("weekly_delta_km2"),
            "weekly_interpretation": stats.get("weekly_interpretation"),
            "ground_raw_total": stats.get("ground_raw_total"),
            "ground_kept_points": stats.get("ground_kept_points"),
            "ground_kept_lines": stats.get("ground_kept_lines"),
            "uav_events_total": stats.get("uav_events_total"),
            "uav_events_7d": stats.get("uav_events_7d"),
            "gained_sector": gained_sector,
            "lost_sector": lost_sector,
        },
        "metrics": metrics,
        "analysis": analysis,
        "text": text,  # legacy compatibility
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "version": 4,
    }


def main():
    daily = load_json(DATA_DIR / "summary_daily.json")
    weekly = load_json(DATA_DIR / "summary_weekly.json")
    stats = calculate_daily_stats()

    text, title, gained_sector, lost_sector, metrics, analysis = build_text(daily, weekly, stats)
    payload = build_json(
        text=text,
        title=title,
        gained_sector=gained_sector,
        lost_sector=lost_sector,
        daily=daily,
        weekly=weekly,
        stats=stats,
        metrics=metrics,
        analysis=analysis,
    )

    OUT_TXT.write_text(text, encoding="utf-8")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Daily brief generated.")
    print(f"Written: {OUT_TXT}")
    print(f"Written: {OUT_JSON}")


if __name__ == "__main__":
    main()
