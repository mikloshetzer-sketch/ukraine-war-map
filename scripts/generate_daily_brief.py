from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

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


def classify_ground_activity(raw_total: int, kept_points: int, kept_lines: int) -> str:
    useful_events = kept_points + kept_lines
    if raw_total == 0:
        return "alacsony"
    ratio = useful_events / raw_total
    if useful_events >= 10 or ratio >= 0.15:
        return "magas"
    if useful_events >= 4 or ratio >= 0.05:
        return "közepes"
    return "alacsony"


def classify_uav_pressure(events_7d: int) -> str:
    if events_7d >= 1800:
        return "nagyon magas"
    if events_7d >= 1000:
        return "magas"
    if events_7d >= 400:
        return "közepes"
    return "alacsony"


def build_driver_section(daily: dict, weekly: dict, ground: dict, uav: dict) -> list[str]:
    latest_ground = ground.get("latest_stats", {})
    daily_delta = float(daily.get("delta_km2", 0))
    weekly_delta = float(weekly.get("delta_km2", 0))
    ground_level = classify_ground_activity(
        int(latest_ground.get("raw_total", 0)),
        int(latest_ground.get("kept_points", 0)),
        int(latest_ground.get("kept_lines", 0)),
    )
    uav_level = classify_uav_pressure(int(uav.get("events_7d", 0)))

    items: list[str] = []

    if daily_delta > 0:
        items.append(
            f"A napi területi mérleg pozitív az orosz fél számára ({sign_prefix(daily_delta)}{fmt_num(daily_delta)} km²), ezért a brief fókusza a frontvonal lassú eltolódása."
        )
    elif daily_delta < 0:
        items.append(
            f"A napi területi mérleg ukrán visszaszerzést jelez ({fmt_num(daily_delta)} km²), ezért a brief fókusza a helyi ellenmozgásokon van."
        )
    else:
        items.append(
            "A napi területi mérleg stagnálást mutat, ezért a brief fókusza inkább az aktivitási mintázatokon van, mint a térképi elmozduláson."
        )

    if weekly_delta > 0:
        items.append(
            f"A heti trend továbbra is orosz előnyt mutat ({sign_prefix(weekly_delta)}{fmt_num(weekly_delta, 1)} km²), vagyis az előző nap eseményeit nem elszigetelt incidensként, hanem folytatódó nyomásként érdemes értelmezni."
        )
    elif weekly_delta < 0:
        items.append(
            f"A heti trend ukrán visszaszerzést mutat ({fmt_num(weekly_delta, 1)} km²), így az előző nap inkább ennek a korrekciós folyamatnak a része lehetett."
        )
    else:
        items.append(
            "A heti trend kiegyensúlyozott, ezért az előző nap jelentőségét inkább a lokális műveleti aktivitás adja."
        )

    items.append(
        f"A földi aktivitás ISW-alapú napi szintje {ground_level}: a {latest_ground.get('raw_total', 0)} nyers említésből {latest_ground.get('kept_points', 0)} pont és {latest_ground.get('kept_lines', 0)} vonal maradt térképezhető formában."
    )
    items.append(
        f"A drón- és UAV-nyomás {uav_level}: az index {uav.get('events_7d', 0)} eseményt jelez az elmúlt 7 napra, ami a front mögötti és mélységi nyomás fenntartását valószínűsíti."
    )

    return items


def build_text_brief(daily: dict, weekly: dict, change: dict, ground: dict, uav: dict) -> str:
    report_date = daily["date"]
    latest_ground = ground.get("latest_stats", {})
    gained_centroid = change.get("gained_centroid")
    lost_centroid = change.get("lost_centroid")

    title = f"Napi hadihelyzeti brief – {report_date}"

    intro = (
        f"Az adott napi összkép alapján a frontvonal továbbra is mozgásban van. "
        f"A DeepState összesítés szerint a megszállt terület becsült nagysága {fmt_num(daily['occupied_km2'])} km², "
        f"ami {daily['vs_date']}-hez képest {sign_prefix(daily['delta_km2'])}{fmt_num(daily['delta_km2'])} km² változást jelent "
        f"({daily['interpretation']})."
    )

    numbers = [
        f"Megszállt terület becslése: {fmt_num(daily['occupied_km2'])} km².",
        f"Napi változás: {sign_prefix(daily['delta_km2'])}{fmt_num(daily['delta_km2'])} km² ({daily['interpretation']}).",
        f"Heti változás: {sign_prefix(weekly['delta_km2'])}{fmt_num(weekly['delta_km2'], 1)} km² {weekly['vs_date']}-hez viszonyítva ({weekly['interpretation']}).",
        f"ISW ground napi nyers események: {latest_ground.get('raw_total', 0)}.",
        f"ISW ground megtartott térképezhető elemek: {latest_ground.get('kept_points', 0)} pont, {latest_ground.get('kept_lines', 0)} vonal.",
        f"ISW UAV index: összesen {uav.get('events_total', 0)} esemény, ebből {uav.get('events_7d', 0)} az elmúlt 7 napban.",
    ]

    events = [
        "A napi területi mérleg alapján a frontvonal elmozdulása továbbra is mérhető, még ha nem is nagy léptékű.",
        "A földi események száma önmagában magasabb, mint a végül térképre tehető pontoké, ami arra utal, hogy sok jelentés csak részben lokalizálható vagy túl bizonytalan a vizualizációhoz.",
        "A drónaktivitás továbbra is a napi helyzetkép egyik fő alakító tényezője, mert a 7 napos UAV-eseményszám tartósan magas terhelést jelez.",
    ]

    if gained_centroid:
        events.append(
            f"A legutóbbi nyereség centroidja a jelenlegi adatfájl szerint kb. hosszúság {gained_centroid[0]}, szélesség {gained_centroid[1]}."
        )
    if lost_centroid:
        events.append(
            f"A legutóbbi veszteség centroidja kb. hosszúság {lost_centroid[0]}, szélesség {lost_centroid[1]}."
        )

    drivers = build_driver_section(daily, weekly, ground, uav)

    caveat = (
        "Megjegyzés: ez az első verzió még szám- és mintázatalapú brief. "
        "A 'miért történt' típusú okfejtés itt még közvetett mutatókból készül, mert a jelenlegi inputfájlok nem tartalmaznak egységes, név szerinti frontszakasz- vagy helyszínmagyarázatot."
    )

    lines: list[str] = [title, "", intro, "", "Fő számok:"]
    lines.extend([f"- {item}" for item in numbers])
    lines.extend(["", "Fő események:"])
    lines.extend([f"- {item}" for item in events])
    lines.extend(["", "Mi határozta meg az előző napot:"])
    lines.extend([f"- {item}" for item in drivers])
    lines.extend(["", caveat, ""])
    return "\n".join(lines)


def build_json_payload(text: str, daily: dict, weekly: dict, ground: dict, uav: dict) -> dict:
    latest_ground = ground.get("latest_stats", {})
    return {
        "date": daily["date"],
        "title": f"Napi hadihelyzeti brief – {daily['date']}",
        "summary": {
            "occupied_km2": daily["occupied_km2"],
            "daily_delta_km2": daily["delta_km2"],
            "daily_interpretation": daily["interpretation"],
            "weekly_delta_km2": weekly["delta_km2"],
            "weekly_interpretation": weekly["interpretation"],
            "ground_raw_total": latest_ground.get("raw_total", 0),
            "ground_kept_points": latest_ground.get("kept_points", 0),
            "ground_kept_lines": latest_ground.get("kept_lines", 0),
            "uav_events_total": uav.get("events_total", 0),
            "uav_events_7d": uav.get("events_7d", 0),
        },
        "text": text,
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "version": 1,
    }


def main() -> None:
    daily = load_json(DATA_DIR / "summary_daily.json")
    weekly = load_json(DATA_DIR / "summary_weekly.json")
    change = load_json(DATA_DIR / "change_latest.json")
    ground = load_json(DATA_DIR / "isw_ground_index.json")
    uav = load_json(DATA_DIR / "isw_uav_index.json")

    text = build_text_brief(daily, weekly, change, ground, uav)
    payload = build_json_payload(text, daily, weekly, ground, uav)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text(text, encoding="utf-8")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Written: {OUT_TXT}")
    print(f"Written: {OUT_JSON}")


if __name__ == "__main__":
    main()
