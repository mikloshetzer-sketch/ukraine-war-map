import json
from pathlib import Path
from datetime import datetime, timezone

import requests
from shapely.geometry import shape
from shapely.ops import unary_union
from pyproj import Geod

DATA_DIR = Path("data")
DATES_JSON = DATA_DIR / "deepstate_dates.json"

OUT_DAILY = DATA_DIR / "summary_daily.json"
OUT_WEEKLY = DATA_DIR / "summary_weekly.json"
OUT_CHANGE = DATA_DIR / "change_latest.json"

geod = Geod(ellps="WGS84")


def load_json_from_ref(ref: str):
    """ref can be local path (./data/x.geojson) or https://..."""
    ref = ref.strip()
    if ref.startswith("http://") or ref.startswith("https://"):
        r = requests.get(ref, timeout=120)
        r.raise_for_status()
        return r.json()
    # local file
    p = Path(ref.lstrip("./"))
    return json.loads(p.read_text(encoding="utf-8"))


def area_km2_of_geojson(geojson_obj) -> float:
    feats = geojson_obj.get("features", [])
    geoms = []
    for f in feats:
        g = f.get("geometry")
        if not g:
            continue
        try:
            geoms.append(shape(g))
        except Exception:
            continue
    if not geoms:
        return 0.0

    merged = unary_union(geoms)

    def geom_area_m2(geom) -> float:
        if geom.is_empty:
            return 0.0
        gt = geom.geom_type
        if gt == "Polygon":
            lon, lat = geom.exterior.coords.xy
            a, _ = geod.polygon_area_perimeter(lon, lat)
            area = abs(a)
            for interior in geom.interiors:
                lonh, lath = interior.coords.xy
                ah, _ = geod.polygon_area_perimeter(lonh, lath)
                area -= abs(ah)
            return max(0.0, area)
        if gt == "MultiPolygon":
            return sum(geom_area_m2(p) for p in geom.geoms)
        return 0.0

    return geom_area_m2(merged) / 1_000_000.0


def merged_geom(geojson_obj):
    feats = geojson_obj.get("features", [])
    geoms = []
    for f in feats:
        g = f.get("geometry")
        if not g:
            continue
        try:
            geoms.append(shape(g))
        except Exception:
            continue
    if not geoms:
        return None
    return unary_union(geoms)


def interpretation_from_delta(delta: float) -> str:
    # your logic: occupied area grows => RU gain; shrinks => UA recapture
    if delta > 0:
        return "orosz területszerzés"
    if delta < 0:
        return "ukrán visszafoglalás"
    return "nincs érdemi változás"


def fmt(delta: float) -> float:
    if abs(delta) >= 100:
        return round(delta, 1)
    return round(delta, 2)


def centroid_lonlat_of_change(today_geom, prev_geom):
    if today_geom is None or prev_geom is None:
        return {"gained_centroid": None, "lost_centroid": None}

    gained = today_geom.difference(prev_geom)   # occupied grew here
    lost = prev_geom.difference(today_geom)     # occupied shrank here

    def safe_centroid(g):
        if g is None or g.is_empty:
            return None
        c = g.centroid
        if not c or c.is_empty:
            return None
        return [round(c.x, 5), round(c.y, 5)]  # lon, lat

    return {
        "gained_centroid": safe_centroid(gained),
        "lost_centroid": safe_centroid(lost),
    }


def main():
    if not DATES_JSON.exists():
        raise SystemExit("Missing data/deepstate_dates.json")

    dates = json.loads(DATES_JSON.read_text(encoding="utf-8"))
    if not isinstance(dates, list) or len(dates) < 2:
        raise SystemExit("deepstate_dates.json too short")

    i_latest = len(dates) - 1
    i_prev = len(dates) - 2
    i_week = max(0, i_latest - 7)

    latest = dates[i_latest]
    prev = dates[i_prev]
    week = dates[i_week]

    gj_latest = load_json_from_ref(latest["raw"])
    gj_prev = load_json_from_ref(prev["raw"])
    gj_week = load_json_from_ref(week["raw"])

    a_latest = area_km2_of_geojson(gj_latest)
    a_prev = area_km2_of_geojson(gj_prev)
    a_week = area_km2_of_geojson(gj_week)

    d_day = a_latest - a_prev
    d_week = a_latest - a_week

    daily = {
        "date": latest["date"],
        "occupied_km2": round(a_latest, 2),
        "delta_km2": fmt(d_day),
        "interpretation": interpretation_from_delta(d_day),
        "vs_date": prev["date"],
    }
    weekly = {
        "date": latest["date"],
        "occupied_km2": round(a_latest, 2),
        "delta_km2": fmt(d_week),
        "interpretation": interpretation_from_delta(d_week),
        "vs_date": week["date"],
    }

    g_latest = merged_geom(gj_latest)
    g_prev = merged_geom(gj_prev)
    change = centroid_lonlat_of_change(g_latest, g_prev)
    change.update({"date": latest["date"], "vs_date": prev["date"]})

    OUT_DAILY.write_text(json.dumps(daily, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_WEEKLY.write_text(json.dumps(weekly, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_CHANGE.write_text(json.dumps(change, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Wrote:", OUT_DAILY, OUT_WEEKLY, OUT_CHANGE)


if __name__ == "__main__":
    main()
