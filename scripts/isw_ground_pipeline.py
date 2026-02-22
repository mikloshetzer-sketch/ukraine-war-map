#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ISW ground operations pipeline (front-közeli pontok + vonalak).

Fő újítás:
- Ha nincs "from A to B", de van "toward/to B", akkor INFERRED vonalat rajzolunk:
  frontvonal legközelebbi pontja -> B

Szűrők:
- geokód csak UA/RU (countrycodes=ua,ru + country_code check)
- front-közeli (FRONT_NEAR_KM)
- max vonalhossz (MAX_MOVE_KM)
- külön V2 cache: data/geocode_cache_v2.json

Outputs:
- data/isw_ground_latest.geojson
- data/isw_ground_7d.geojson
- data/isw_ground_30d.geojson
- data/isw_ground_index.json
- data/geocode_cache_v2.json
"""

import re
import json
import time
import math
import datetime
from pathlib import Path
import requests


# =========================
# CONFIG
# =========================

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Ukraine-War-Map research bot; ISW ground pipeline; github actions)"
}

ROC_UPDATES_URL = "https://understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-updates-2"

ARCGIS_FRONT_GEOJSON_URL = (
    "https://services-eu1.arcgis.com/fppoCYaq7HfVFbIV/ArcGIS/rest/services/"
    "UKR_Frontline_27072025/FeatureServer/0/query?"
    "where=1%3D1&outFields=*&f=geojson"
)

# Szűrők
FRONT_NEAR_KM = 90.0        # kicsit engedékenyebb, több esemény marad
MAX_MOVE_KM = 160.0         # ha ennél hosszabb, dobjuk
MIN_INFERRED_KM = 2.0       # túl rövid vonalat ne rajzoljunk
MAX_INFERRED_KM = 70.0      # inferred vonal ne legyen túl hosszú

# Nominatim viewbox (UA + nyugat-orosz régió)
VIEW_MIN_LON, VIEW_MIN_LAT = 20.0, 42.0
VIEW_MAX_LON, VIEW_MAX_LAT = 60.0, 58.5

ALLOWED_COUNTRY_CODES = {"ua", "ru"}
NOMINATIM_SLEEP_SEC = 1.0

GEOCODE_CACHE_V2 = OUT_DIR / "geocode_cache_v2.json"


# =========================
# HTTP fetch (ISW 403 fallback)
# =========================

def fetch_url(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=25)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass

    # fallback proxy (ISW néha 403)
    try:
        proxy = "https://r.jina.ai/" + url
        r = requests.get(proxy, timeout=30)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass

    return None


# =========================
# STEP 1 — ROC cikk linkek
# =========================

def collect_recent_article_links(limit: int = 120) -> list[str]:
    html = fetch_url(ROC_UPDATES_URL)
    if not html:
        print("ISW index nem tölthető")
        return []

    links = set()
    for m in re.findall(r'href="([^"]*russian-offensive-campaign-assessment[^"]*)"', html):
        if "research" in m:
            if not m.startswith("http"):
                m = "https://understandingwar.org" + m
            links.add(m)

    links = sorted(links, reverse=True)
    return links[:limit]


# =========================
# STEP 2 — mondat + események
# =========================

GROUND_KEYWORDS = [
    "assault", "attacked", "attack", "offensive",
    "advanced", "advance", "made gains", "gains",
    "seized", "captured", "took", "recaptured",
    "pushed", "pushing", "breached", "breakthrough",
    "counterattack", "counter-attacked", "counteroffensive",
    "repelled", "repulse", "withdrew", "withdrawal",
    "cleared", "secured",
    "in the direction of",
]

def strip_html_to_text(html: str) -> str:
    text = re.sub("<[^<]+?>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def infer_date_from_url(article_url: str) -> datetime.date:
    m = re.search(r"(\w+-\d{1,2}-\d{4})", article_url)
    if m:
        s = m.group(1)
        try:
            return datetime.datetime.strptime(s, "%B-%d-%Y").date()
        except Exception:
            return datetime.date.today()
    return datetime.date.today()

# Többszavas helynevek: "Velyka Novosilka", "New York" stb.
# 1) from A to/toward B
MOVE_FROM_TO = re.compile(
    r"\bfrom\s+(?:the\s+vicinity\s+of\s+|near\s+|around\s+|in\s+)?"
    r"(.+?)\s+(?:to|toward|towards)\s+(.+?)(?:[.;]|$)",
    re.IGNORECASE
)

# 2) toward/to B (inferred)
MOVE_TOWARD = re.compile(
    r"\b(?:to|toward|towards|in the direction of)\s+([A-Z][A-Za-z0-9\-\']+(?:\s+[A-Z][A-Za-z0-9\-\']+)*)",
    re.IGNORECASE
)

# 3) single place fallback
PLACE_PATTERN = re.compile(
    r"\b(in|near|around|outside|south of|north of|east of|west of|within)\s+([A-Z][A-Za-z0-9\-\']+(?:\s+[A-Z][A-Za-z0-9\-\']+)*)",
    re.IGNORECASE
)

def clean_place(raw: str | None) -> str | None:
    if not raw:
        return None
    s = raw.strip()

    # vágások
    s = re.sub(r"\s+\(.*?\)\s*$", "", s).strip()
    s = re.sub(r"\s+(?:direction|axis|area|region|sector)\s*$", "", s, flags=re.IGNORECASE).strip()

    # túl hosszú = valószínű mondatrész
    if len(s) > 55:
        return None
    if not re.search(r"[A-Za-z]", s):
        return None
    return s

def extract_events(article_url: str) -> list[dict]:
    html = fetch_url(article_url)
    if not html:
        return []

    text = strip_html_to_text(html)
    date = infer_date_from_url(article_url)

    # egyszerű mondatbontás
    sentences = re.split(r"\.\s+", text)

    events: list[dict] = []

    for s in sentences:
        s = s.strip()
        if len(s) < 40:
            continue

        lower = s.lower()
        if not any(k in lower for k in GROUND_KEYWORDS):
            continue

        # movement: from A to B
        mm = MOVE_FROM_TO.search(s)
        if mm:
            a = clean_place(mm.group(1))
            b = clean_place(mm.group(2))
            if a and b and a != b:
                events.append({
                    "date": str(date),
                    "kind": "movement",
                    "from_place": a,
                    "to_place": b,
                    "text": s[:380],
                    "source_url": article_url
                })
                continue

        # movement: toward B (inferred)
        tm = MOVE_TOWARD.search(s)
        if tm:
            b = clean_place(tm.group(1))
            if b:
                events.append({
                    "date": str(date),
                    "kind": "toward_only",
                    "to_place": b,
                    "text": s[:380],
                    "source_url": article_url
                })
                continue

        # fallback point
        pm = PLACE_PATTERN.search(s)
        place = clean_place(pm.group(2)) if pm else None
        events.append({
            "date": str(date),
            "kind": "ground",
            "place": place,
            "text": s[:380],
            "source_url": article_url
        })

    return events


# =========================
# STEP 3 — Geocode UA/RU only (V2 cache)
# =========================

if GEOCODE_CACHE_V2.exists():
    geocache = json.loads(GEOCODE_CACHE_V2.read_text(encoding="utf-8"))
else:
    geocache = {}

def _cache_get(place: str):
    v = geocache.get(place)
    if not isinstance(v, dict):
        return None
    cc = (v.get("cc") or "").lower()
    coords = v.get("coords")
    if cc in ALLOWED_COUNTRY_CODES and isinstance(coords, list) and len(coords) == 2:
        lon, lat = coords
        if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
            return coords, cc
    return None

def _cache_set(place: str, coords, cc: str):
    geocache[place] = {"coords": coords, "cc": cc}

def geocode(place: str | None) -> tuple[list[float] | None, str | None]:
    if not place:
        return None, None
    key = place.strip()
    if not key:
        return None, None

    cached = _cache_get(key)
    if cached:
        coords, cc = cached
        return coords, cc

    try:
        viewbox = f"{VIEW_MIN_LON},{VIEW_MAX_LAT},{VIEW_MAX_LON},{VIEW_MIN_LAT}"
        url = (
            "https://nominatim.openstreetmap.org/search"
            f"?format=json"
            f"&q={requests.utils.quote(key)}"
            f"&addressdetails=1"
            f"&limit=1"
            f"&countrycodes=ua,ru"
            f"&viewbox={viewbox}&bounded=1"
        )
        r = requests.get(url, headers=HEADERS, timeout=25)
        data = r.json()
        if not data:
            return None, None

        item = data[0]
        lat = float(item["lat"])
        lon = float(item["lon"])
        addr = item.get("address") or {}
        cc = (addr.get("country_code") or "").lower()
        if cc not in ALLOWED_COUNTRY_CODES:
            return None, None

        coords = [lon, lat]
        _cache_set(key, coords, cc)
        time.sleep(NOMINATIM_SLEEP_SEC)
        return coords, cc

    except Exception:
        return None, None


# =========================
# STEP 4 — Frontline: segments + nearest point
# =========================

EARTH_R = 6371000.0  # meters

def load_frontline_segments() -> list[dict]:
    print("Frontvonal letöltés…")
    r = requests.get(ARCGIS_FRONT_GEOJSON_URL, headers=HEADERS, timeout=40)
    r.raise_for_status()
    gj = r.json()

    segs: list[dict] = []

    def add_linestring(coords):
        for i in range(len(coords) - 1):
            lon1, lat1 = coords[i]
            lon2, lat2 = coords[i + 1]
            minlat, maxlat = (lat1, lat2) if lat1 <= lat2 else (lat2, lat1)
            minlon, maxlon = (lon1, lon2) if lon1 <= lon2 else (lon2, lon1)
            segs.append({
                "lat1": lat1, "lon1": lon1,
                "lat2": lat2, "lon2": lon2,
                "minlat": minlat, "maxlat": maxlat,
                "minlon": minlon, "maxlon": maxlon
            })

    for f in (gj.get("features") or []):
        g = f.get("geometry") or {}
        t = g.get("type")
        c = g.get("coordinates")
        if not c:
            continue
        if t == "LineString":
            add_linestring(c)
        elif t == "MultiLineString":
            for part in c:
                add_linestring(part)

    print(f"Frontvonal szegmensek: {len(segs)}")
    return segs

def _to_xy_m(lat, lon, lat0):
    x = math.radians(lon) * EARTH_R * math.cos(math.radians(lat0))
    y = math.radians(lat) * EARTH_R
    return x, y

def _xy_to_latlon(x, y, lat0):
    lat = math.degrees(y / EARTH_R)
    lon = math.degrees(x / (EARTH_R * max(1e-9, math.cos(math.radians(lat0)))))
    return lat, lon

def _closest_point_on_segment_xy(px, py, ax, ay, bx, by):
    abx = bx - ax
    aby = by - ay
    apx = px - ax
    apy = py - ay
    ab2 = abx*abx + aby*aby
    if ab2 <= 1e-12:
        return ax, ay, 0.0
    t = (apx*abx + apy*aby) / ab2
    if t < 0.0:
        return ax, ay, 0.0
    if t > 1.0:
        return bx, by, 1.0
    cx = ax + t*abx
    cy = ay + t*aby
    return cx, cy, t

def min_distance_km_to_front(lat, lon, segments, threshold_km) -> float:
    deg_lat = threshold_km / 111.0
    deg_lon = threshold_km / (111.0 * max(0.2, math.cos(math.radians(lat))))

    lat0 = lat
    px, py = _to_xy_m(lat, lon, lat0)

    best_m = float("inf")
    for s in segments:
        if lat < (s["minlat"] - deg_lat) or lat > (s["maxlat"] + deg_lat):
            continue
        if lon < (s["minlon"] - deg_lon) or lon > (s["maxlon"] + deg_lon):
            continue

        ax, ay = _to_xy_m(s["lat1"], s["lon1"], lat0)
        bx, by = _to_xy_m(s["lat2"], s["lon2"], lat0)
        cx, cy, _ = _closest_point_on_segment_xy(px, py, ax, ay, bx, by)
        d = math.hypot(px - cx, py - cy)
        if d < best_m:
            best_m = d
            if best_m <= 1500:
                break

    return best_m / 1000.0

def nearest_point_on_front(lat, lon, segments, threshold_km) -> tuple[float, float, float] | None:
    """
    Returns (front_lat, front_lon, dist_km) or None if not within threshold prune window.
    """
    deg_lat = threshold_km / 111.0
    deg_lon = threshold_km / (111.0 * max(0.2, math.cos(math.radians(lat))))

    lat0 = lat
    px, py = _to_xy_m(lat, lon, lat0)

    best_m = float("inf")
    best_xy = None

    for s in segments:
        if lat < (s["minlat"] - deg_lat) or lat > (s["maxlat"] + deg_lat):
            continue
        if lon < (s["minlon"] - deg_lon) or lon > (s["maxlon"] + deg_lon):
            continue

        ax, ay = _to_xy_m(s["lat1"], s["lon1"], lat0)
        bx, by = _to_xy_m(s["lat2"], s["lon2"], lat0)
        cx, cy, _ = _closest_point_on_segment_xy(px, py, ax, ay, bx, by)
        d = math.hypot(px - cx, py - cy)
        if d < best_m:
            best_m = d
            best_xy = (cx, cy)
            if best_m <= 1500:
                break

    if best_xy is None:
        return None

    front_lat, front_lon = _xy_to_latlon(best_xy[0], best_xy[1], lat0)
    return front_lat, front_lon, best_m / 1000.0

def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    return 2*r*math.asin(min(1.0, math.sqrt(a)))


# =========================
# STEP 5 — GeoJSON (pont + vonal)
# =========================

def events_to_geojson(events: list[dict], frontline_segments: list[dict]) -> dict:
    features: list[dict] = []

    stats = {
        "raw_total": len(events),
        "raw_movement_from_to": 0,
        "raw_toward_only": 0,
        "kept_points": 0,
        "kept_lines": 0,
        "dropped_geocode": 0,
        "dropped_far": 0,
        "dropped_long_lines": 0,
        "dropped_inferred_len": 0,
    }

    for e in events:
        kind = e.get("kind")

        # 1) explicit movement: from A to B
        if kind == "movement":
            stats["raw_movement_from_to"] += 1

            a = e.get("from_place")
            b = e.get("to_place")

            ca, cca = geocode(a)
            cb, ccb = geocode(b)
            if not (ca and cb and cca and ccb):
                stats["dropped_geocode"] += 1
                continue

            lat_a, lon_a = ca[1], ca[0]
            lat_b, lon_b = cb[1], cb[0]

            length_km = haversine_km(lat_a, lon_a, lat_b, lon_b)
            if length_km > MAX_MOVE_KM:
                stats["dropped_long_lines"] += 1
                continue

            d_a = min_distance_km_to_front(lat_a, lon_a, frontline_segments, FRONT_NEAR_KM)
            d_b = min_distance_km_to_front(lat_b, lon_b, frontline_segments, FRONT_NEAR_KM)
            dmin = min(d_a, d_b)
            if dmin > FRONT_NEAR_KM:
                stats["dropped_far"] += 1
                continue

            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [ca, cb]},
                "properties": {
                    "source": "ISW",
                    "date": e["date"],
                    "title": "ISW ground movement",
                    "from": a, "to": b,
                    "from_cc": cca, "to_cc": ccb,
                    "distance_to_front_km_min": round(dmin, 1),
                    "length_km": round(length_km, 1),
                    "inferred": False,
                    "snippet": e["text"],
                    "url": e["source_url"]
                }
            })
            stats["kept_lines"] += 1
            continue

        # 2) toward only: inferred line front -> B
        if kind == "toward_only":
            stats["raw_toward_only"] += 1

            b = e.get("to_place")
            cb, ccb = geocode(b)
            if not (cb and ccb):
                stats["dropped_geocode"] += 1
                continue

            lat_b, lon_b = cb[1], cb[0]

            # célpont front-közeli legyen
            d_b = min_distance_km_to_front(lat_b, lon_b, frontline_segments, FRONT_NEAR_KM)
            if d_b > FRONT_NEAR_KM:
                stats["dropped_far"] += 1
                continue

            # frontvonal legközelebbi pontja
            np = nearest_point_on_front(lat_b, lon_b, frontline_segments, FRONT_NEAR_KM)
            if np is None:
                stats["dropped_far"] += 1
                continue
            front_lat, front_lon, dist_km = np

            # inferred vonal hossza (front -> cél)
            length_km = haversine_km(front_lat, front_lon, lat_b, lon_b)
            if length_km < MIN_INFERRED_KM or length_km > MAX_INFERRED_KM:
                stats["dropped_inferred_len"] += 1
                continue

            if length_km > MAX_MOVE_KM:
                stats["dropped_long_lines"] += 1
                continue

            # LineString coords: [lon,lat]
            c_front = [front_lon, front_lat]

            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [c_front, cb]},
                "properties": {
                    "source": "ISW",
                    "date": e["date"],
                    "title": "ISW ground movement (inferred from frontline)",
                    "from": "Frontline (nearest)",
                    "to": b,
                    "to_cc": ccb,
                    "distance_to_front_km_min": round(dist_km, 1),
                    "length_km": round(length_km, 1),
                    "inferred": True,
                    "snippet": e["text"],
                    "url": e["source_url"]
                }
            })
            stats["kept_lines"] += 1
            continue

        # 3) point event
        coords, cc = geocode(e.get("place"))
        if not (coords and cc):
            stats["dropped_geocode"] += 1
            continue

        lat, lon = coords[1], coords[0]
        d = min_distance_km_to_front(lat, lon, frontline_segments, FRONT_NEAR_KM)
        if d > FRONT_NEAR_KM:
            stats["dropped_far"] += 1
            continue

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": {
                "source": "ISW",
                "date": e["date"],
                "title": "ISW ground operation",
                "place": e.get("place"),
                "cc": cc,
                "distance_to_front_km": round(d, 1),
                "snippet": e["text"],
                "url": e["source_url"]
            }
        })
        stats["kept_points"] += 1

    return {
        "type": "FeatureCollection",
        "features": features,
        "properties": {
            "filter": {
                "allowed_country_codes": sorted(list(ALLOWED_COUNTRY_CODES)),
                "front_near_km": FRONT_NEAR_KM,
                "max_move_km": MAX_MOVE_KM,
                "min_inferred_km": MIN_INFERRED_KM,
                "max_inferred_km": MAX_INFERRED_KM,
            },
            "stats": stats
        }
    }


# =========================
# MAIN
# =========================

def main():
    print("ISW GROUND pipeline indul…")

    frontline_segments = load_frontline_segments()

    links = collect_recent_article_links(limit=120)
    print("Talált cikkek:", len(links))

    all_events: list[dict] = []
    for url in links:
        all_events.extend(extract_events(url))
    print("Talált események (nyers):", len(all_events))

    today = datetime.date.today()
    last7 = today - datetime.timedelta(days=7)
    last30 = today - datetime.timedelta(days=30)

    ev_latest = all_events[:120]
    ev_7 = [e for e in all_events if datetime.date.fromisoformat(e["date"]) >= last7]
    ev_30 = [e for e in all_events if datetime.date.fromisoformat(e["date"]) >= last30]

    gj_latest = events_to_geojson(ev_latest, frontline_segments)
    gj7 = events_to_geojson(ev_7, frontline_segments)
    gj30 = events_to_geojson(ev_30, frontline_segments)

    OUT_DIR.joinpath("isw_ground_latest.geojson").write_text(json.dumps(gj_latest, indent=2), encoding="utf-8")
    OUT_DIR.joinpath("isw_ground_7d.geojson").write_text(json.dumps(gj7, indent=2), encoding="utf-8")
    OUT_DIR.joinpath("isw_ground_30d.geojson").write_text(json.dumps(gj30, indent=2), encoding="utf-8")

    OUT_DIR.joinpath("isw_ground_index.json").write_text(
        json.dumps({
            "generated_utc": datetime.datetime.utcnow().isoformat(),
            "events_total_raw": len(all_events),
            "events_7d_raw": len(ev_7),
            "events_30d_raw": len(ev_30),
            "latest_stats": gj_latest.get("properties", {}).get("stats", {}),
            "d7_stats": gj7.get("properties", {}).get("stats", {}),
            "d30_stats": gj30.get("properties", {}).get("stats", {}),
        }, indent=2),
        encoding="utf-8"
    )

    GEOCODE_CACHE_V2.write_text(json.dumps(geocache, indent=2), encoding="utf-8")

    print("STATS latest:", gj_latest.get("properties", {}).get("stats", {}))
    print("ISW GROUND pipeline kész ✔")


if __name__ == "__main__":
    main()
