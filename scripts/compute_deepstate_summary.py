import json
import math
from pathlib import Path
from datetime import datetime

from shapely.geometry import shape
from shapely.ops import unary_union
from pyproj import Geod

DATA_DIR = Path("data")
DATES_JSON = DATA_DIR / "deepstate_dates.json"

OUT_DAILY = DATA_DIR / "summary_daily.json"
OUT_WEEKLY = DATA_DIR / "summary_weekly.json"
OUT_CHANGE = DATA_DIR / "change_latest.json"

geod = Geod(ellps="WGS84")


def read_json(p: Path):
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
    # geodesic area: pyproj.Geod can compute area from polygon coords;
    # shapely -> we iterate polygons in merged
    def geom_area_m2(geom) -> float:
        if geom.is_empty:
            return 0.0
        gt = geom.geom_type
        if gt == "Polygon":
            lon, lat = geom.exterior.coords.xy
            a, _ = geod.polygon_area_perimeter(lon, lat)
            area = abs(a)
            # subtract holes
            for interior in geom.interiors:
                lonh, lath = interior.coords.xy
                ah, _ = geod.polygon_area_perimeter(lonh, lath)
                area -= abs(ah)
            return max(0.0, area)
        if gt == "MultiPolygon":
            return sum(geom_area_m2(p) for p in geom.geoms)
        # other types (lines etc.)
        return 0.0

    area_m2 = geom_area_m2(merged)
    return area_m2 / 1_000_000.0


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


def side_from_delta(delta: float) -> str:
    if delta > 0:
        return "orosz területszerzés"
    if delta < 0:
        return "ukrán visszafoglalás"
    return "nincs érdemi változás"


def fmt(delta: float) -> float:
    # keep nice rounding but numeric
    if abs(delta) >= 100:
        return round(delta, 1)
    return round(delta, 2)


def centroid_lonlat_of_change(today_geom, prev_geom):
    if today_geom is None or prev_geom is None:
        return None

    gained = today_geom.difference(prev_geom)   # RU gained (occupied grew)
    lost = prev_geom.difference(today_geom)     # RU lost (occupied shrank)

    def safe_centroid(g):
        if g is None or g.is_empty:
            return None
        c = g.centroid
        if not c or c.is_empty:
            return None
        return [round(c.x, 5), round(c.y, 5)]  # lon, lat

    return {
        "gained_centroid": safe_centroid(gained),
        "lost_centroid": safe_centroid(lost)
    }


def main():
    if not DATES_JSON.exists():
        raise SystemExit("Missing data/deepstate_dates.json")

    dates = read_json(DATES_JSON)
    if not isinstance(dates, list) or len(dates) < 2:
        raise SystemExit("deepstate_dates.json too short")

    # dates list is chronological; last = latest
    i_latest = len(dates) - 1
    i_prev = len(dates) - 2
    i_week = max(0, i_latest - 7)

    latest = dates[i_latest]
    prev = dates[i_prev]
    week = dates[i_week]

    # Paths are stored as "raw" in your json (e.g. "./data/deepstatemap_data_YYYYMMDD.geojson")
    p_latest = Path(latest["raw"].lstrip("./"))
    p_prev = Path(prev["raw"].lstrip("./"))
    p_week = Path(week["raw"].lstrip("./"))

    gj_latest = read_json(p_latest)
    gj_prev = read_json(p_prev)
    gj_week = read_json(p_week)

    a_latest = area_km2_of_geojson(gj_latest)
    a_prev = area_km2_of_geojson(gj_prev)
    a_week = area_km2_of_geojson(gj_week)

    d_day = a_latest - a_prev
    d_week = a_latest - a_week

    daily = {
        "date": latest["date"],
        "occupied_km2": round(a_latest, 2),
        "delta_km2": fmt(d_day),
        "interpretation": side_from_delta(d_day),
        "vs_date": prev["date"]
    }
    weekly = {
        "date": latest["date"],
        "occupied_km2": round(a_latest, 2),
        "delta_km2": fmt(d_week),
        "interpretation": side_from_delta(d_week),
        "vs_date": week["date"]
    }

    # change centroids (optional helper for “hol történt”)
    g_latest = merged_geom(gj_latest)
    g_prev = merged_geom(gj_prev)
    change = centroid_lonlat_of_change(g_latest, g_prev) or {"gained_centroid": None, "lost_centroid": None}
    change.update({
        "date": latest["date"],
        "vs_date": prev["date"]
    })

    OUT_DAILY.write_text(json.dumps(daily, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_WEEKLY.write_text(json.dumps(weekly, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_CHANGE.write_text(json.dumps(change, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Wrote:", OUT_DAILY, OUT_WEEKLY, OUT_CHANGE)


if __name__ == "__main__":
    main()
