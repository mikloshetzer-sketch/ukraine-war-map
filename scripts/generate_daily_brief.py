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
            f"A napi területi mérleg pozitív az orosz fél számára ({sign_prefix(daily_delta)}{fmt_num(daily_delta)} km²), "
            "ezért a fő hangsúly a lassú, folyamatos frontnyomáson van."
        )
    elif daily_delta < 0:
        items.append(
            f"A napi területi mérleg ukrán visszaszerzést jelez ({fmt_num(daily_delta)} km²), "
            "ezért a fókusz a helyi ellenmozgásokon és korrekciókon van."
        )
    else:
        items.append(
            "A napi területi mérleg stagnálást mutat, ezért a fő hangsúly az aktivitási mintázatokon és nem a térképi elmozduláson van."
        )

    if weekly_delta > 0:
        items.append(
            f"A heti trend továbbra is orosz előnyt mutat ({sign_prefix(weekly_delta)}{fmt_num(weekly_delta, 1)} km²), "
            "ami arra utal, hogy az előző nap nem elszigetelt esemény, hanem egy folytatódó nyomás része."
        )
    elif weekly_delta < 0:
        items.append(
            f"A heti trend ukrán visszaszerzést mutat ({fmt_num(weekly_delta, 1)} km²), "
            "így az előző nap inkább egy korrekciós folyamatba illeszkedhetett."
        )
    else:
        items.append(
            "A heti trend kiegyensúlyozott, ezért az előző nap jelentőségét inkább a lokális műveleti aktivitás adja."
        )

    items.append(
        f"A földi aktivitás ISW-alapon {ground_level}: a {latest_ground.get('raw_total', 0)} nyers említésből "
        f"{latest_ground.get('kept_points', 0)} pont és {latest_ground.get('kept_lines', 0)} vonal maradt térképezhető formában."
    )

    items.append(
        f"A drón- és UAV-nyomás {uav_level}: az index {uav.get('events_7d', 0)} eseményt jelez az elmúlt 7 napban, "
        "ami tartós felderítési és csapásmérő nyomást valószínűsít."
    )

    return items


def build_title(daily: dict, weekly: dict, gained_sector: str | None) -> str:
    daily_delta = float(daily.get("delta_km2", 0))
    weekly_delta = float(weekly.get("delta_km2", 0))

    if daily_delta > 0 and gained_sector:
        return f"Napi hadihelyzeti brief – Folyamatos orosz nyomás a {gained_sector} szektorban"
    if daily_delta > 0 and weekly_delta > 0:
        return "Napi hadihelyzeti brief – Fokozatos orosz előretörés folytatódik"
    if daily_delta < 0:
        return "Napi hadihelyzeti brief – Helyi ukrán korrekciók a fronton"
    return "Napi hadihelyzeti brief – Tartós frontnyomás korlátozott területi változással"


def build_intro(daily: dict, weekly: dict, gained_sector: str | None) -> str:
    daily_delta = float(daily.get("delta_km2", 0))
    weekly_delta = float(weekly.get("delta_km2", 0))

    if daily_delta > 0 and gained_sector:
        return (
            f"Az adott napi összkép alapján a harctéri dinamika továbbra is az orosz fél fokozatos nyomását tükrözi, "
            f"különösen a {gained_sector} szektor térségében. A DeepState összesítés szerint a megszállt terület becsült "
            f"nagysága {fmt_num(daily['occupied_km2'])} km², ami {daily['vs_date']}-hez képest "
            f"{sign_prefix(daily['delta_km2'])}{fmt_num(daily['delta_km2'])} km² változást jelent "
            f"({daily['interpretation']}). A heti mérleg összességében "
            f"{sign_prefix(weekly_delta)}{fmt_num(weekly_delta, 1)} km² elmozdulást mutat, ami arra utal, "
            "hogy az előző nap eseményei nem önmagukban, hanem egy szélesebb, folyamatos műveleti trend részeként értelmezhetők."
        )

    return (
        f"Az adott napi összkép alapján a frontvonal továbbra is mozgásban van, még ha a területi változások mértéke korlátozott is. "
        f"A DeepState összesítés szerint a megszállt terület becsült nagysága {fmt_num(daily['occupied_km2'])} km², "
        f"ami {daily['vs_date']}-hez képest {sign_prefix(daily['delta_km2'])}{fmt_num(daily['delta_km2'])} km² változást jelent "
        f"({daily['interpretation']}). A heti mérleg {sign_prefix(weekly_delta)}{fmt_num(weekly_delta, 1)} km², "
        "ami segít az előző napot nem elszigetelt adatpontként, hanem trendbe illesztve értelmezni."
    )


def build_events(
    gained_centroid: list[float] | None,
    lost_centroid: list[float] | None,
    gained_sector: str | None,
    lost_sector: str | None,
    ground: dict,
    uav: dict,
) -> list[str]:
    latest_ground = ground.get("latest_stats", {})
    raw_total = int(latest_ground.get("raw_total", 0))
    kept_points = int(latest_ground.get("kept_points", 0))
    kept_lines = int(latest_ground.get("kept_lines", 0))
    events_7d = int(uav.get("events_7d", 0))

    events: list[str] = []

    if gained_sector:
        events.append(
            f"A legutóbbi megerősített területi nyereség a {gained_sector} szektorhoz köthető, "
            "ami azt jelzi, hogy a napi változás nem egyenletesen oszlik el a teljes fronton, hanem meghatározott súlypontok köré koncentrálódik."
        )

    if lost_sector:
        events.append(
            f"A rendelkezésre álló adat külön veszteségi pontot is jelez a {lost_sector} szektorban, "
            "ami arra utal, hogy a frontvonal egyes részein kétirányú lokális korrekciók is előfordulhatnak."
        )

    events.append(
        f"A földi műveleti aktivitás és a ténylegesen térképezhető elemek közötti különbség jelentős: "
        f"{raw_total} nyers említésből csak {kept_points} pont és {kept_lines} vonal maradt meg. "
        "Ez inkább szétszórt, kis léptékű harctevékenységre utal, mintsem nagy, gyors gépesített áttörésre."
    )

    events.append(
        f"A UAV-aktivitás továbbra is a napi helyzetkép egyik fő alakító tényezője: "
        f"az elmúlt 7 napban {events_7d} esemény szerepel az indexben, ami tartós felderítési és csapásmérő terhelést valószínűsít."
    )

    if gained_centroid:
        events.append(
            f"A legutóbbi nyereségi centroid koordinátája a jelenlegi adatfájl szerint kb. hosszúság {gained_centroid[0]}, "
            f"szélesség {gained_centroid[1]}."
        )

    if lost_centroid:
        events.append(
            f"A legutóbbi veszteségi centroid koordinátája kb. hosszúság {lost_centroid[0]}, szélesség {lost_centroid[1]}."
        )

    return events


def build_assessment(daily: dict, weekly: dict, gained_sector: str | None, uav: dict) -> str:
    daily_delta = float(daily.get("delta_km2", 0))
    weekly_delta = float(weekly.get("delta_km2", 0))
    events_7d = int(uav.get("events_7d", 0))

    if daily_delta > 0 and weekly_delta > 0 and gained_sector and events_7d >= 1000:
        return (
            f"Összességében az előző napot a {gained_sector} szektor körüli fokozatos frontnyomás, "
            "a magas UAV-aktivitás és a korlátozott, de mérhető területi elmozdulás határozta meg. "
            "Ez a mintázat inkább felőrlő jellegű műveleti logikára, mintsem gyors manőverháborús változásra utal."
        )

    if daily_delta == 0:
        return (
            "Összességében az előző napot nem a látványos térképi elmozdulás, hanem a tartós műveleti aktivitás határozta meg. "
            "Ez olyan időszakra utal, amikor a harcok intenzitása fennmarad, de annak területi lenyomata korlátozott."
        )

    return (
        "Összességében az előző nap képe a folyamatos frontnyomás, a magas felderítési és drónterhelés, "
        "valamint a korlátozott, de értelmezhető területi változások együtteséből áll össze."
    )


def build_text_brief(daily: dict, weekly: dict, change: dict, ground: dict, uav: dict) -> tuple[str, dict]:
    gained_centroid = change.get("gained_centroid")
    lost_centroid = change.get("lost_centroid")

    gained_sector = None
    lost_sector = None

    if gained_centroid and len(gained_centroid) >= 2:
        gained_sector = detect_sector(gained_centroid[0], gained_centroid[1])

    if lost_centroid and len(lost_centroid) >= 2:
        lost_sector = detect_sector(lost_centroid[0], lost_centroid[1])

    title = build_title(daily, weekly, gained_sector)
    intro = build_intro(daily, weekly, gained_sector)
    latest_ground = ground.get("latest_stats", {})

    numbers = [
        f"Megszállt terület becslése: {fmt_num(daily['occupied_km2'])} km².",
        f"Napi változás: {sign_prefix(daily['delta_km2'])}{fmt_num(daily['delta_km2'])} km² ({daily['interpretation']}).",
        f"Heti változás: {sign_prefix(weekly['delta_km2'])}{fmt_num(weekly['delta_km2'], 1)} km² {weekly['vs_date']}-hez viszonyítva ({weekly['interpretation']}).",
        f"ISW ground napi nyers események: {latest_ground.get('raw_total', 0)}.",
        f"ISW ground megtartott térképezhető elemek: {latest_ground.get('kept_points', 0)} pont, {latest_ground.get('kept_lines', 0)} vonal.",
        f"ISW UAV index: összesen {uav.get('events_total', 0)} esemény, ebből {uav.get('events_7d', 0)} az elmúlt 7 napban.",
    ]

    events = build_events(
        gained_centroid=gained_centroid,
        lost_centroid=lost_centroid,
        gained_sector=gained_sector,
        lost_sector=lost_sector,
        ground=ground,
        uav=uav,
    )

    drivers = build_driver_section(daily, weekly, ground, uav)
    assessment = build_assessment(daily, weekly, gained_sector, uav)

    caveat = (
        "Megjegyzés: ez a brief továbbra is strukturált adatokból készül. "
        "A frontszektor-azonosítás centroid alapú közelítés, vagyis jó operatív tájékozódási pont, "
        "de nem helyettesíti a manuális térképi ellenőrzést és a név szerinti településszintű elemzést."
    )

    lines: list[str] = [
        title,
        "",
        intro,
        "",
        "Fő számok:",
    ]
    lines.extend([f"- {item}" for item in numbers])

    lines.extend(["", "Fő események:"])
    lines.extend([f"- {item}" for item in events])

    lines.extend(["", "Mi határozta meg az előző napot:"])
    lines.extend([f"- {item}" for item in drivers])

    lines.extend(["", "Értékelés:", assessment, "", caveat, ""])

    metadata = {
        "title": title,
        "gained_sector": gained_sector,
        "lost_sector": lost_sector,
    }

    return "\n".join(lines), metadata


def build_json_payload(
    text: str,
    metadata: dict,
    daily: dict,
    weekly: dict,
    ground: dict,
    uav: dict,
) -> dict:
    latest_ground = ground.get("latest_stats", {})

    return {
        "date": daily["date"],
        "title": metadata["title"],
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
            "gained_sector": metadata.get("gained_sector"),
            "lost_sector": metadata.get("lost_sector"),
        },
        "text": text,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "version": 2,
    }


def main() -> None:
    daily = load_json(DATA_DIR / "summary_daily.json")
    weekly = load_json(DATA_DIR / "summary_weekly.json")
    change = load_json(DATA_DIR / "change_latest.json")
    ground = load_json(DATA_DIR / "isw_ground_index.json")
    uav = load_json(DATA_DIR / "isw_uav_index.json")

    text, metadata = build_text_brief(daily, weekly, change, ground, uav)
    payload = build_json_payload(text, metadata, daily, weekly, ground, uav)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUT_TXT.write_text(text, encoding="utf-8")
    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Written: {OUT_TXT}")
    print(f"Written: {OUT_JSON}")


if __name__ == "__main__":
    main()
