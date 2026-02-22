#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ISW ground operations pipeline (ground assaults / advances / counterattacks)
- ONLY UA/RU geocoding (Nominatim countrycodes=ua,ru + addressdetails country_code)
- FRONT-NEAR filter using ArcGIS frontline (km threshold)
- MAX movement line length filter (drops absurd long lines)
- V2 geocode cache with country_code + coords (avoids old bad cache hits)

Outputs:
- data/isw_ground_latest.geojson
- data/isw_ground_7d.geojson
- data/isw_ground_30d.geojson
- data/isw_ground_index.json
- data/geocode_cache_v2.json   (NEW cache; does NOT use old geocode_cache.json)
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

# ArcGIS frontline GeoJSON (ugyanaz, mint a HTML-ben)
ARCGIS_FRONT_GEOJSON_URL = (
    "https://services-eu1.arcgis.com/fppoCYaq7HfVFbIV/ArcGIS/rest/services/"
    "UKR_Frontline_27072025/FeatureServer/0/query?"
    "where=1%3D1&outFields=*&f=geojson"
)

# Front-közeli küszöb (km)
FRONT_NEAR_KM = 80.0

# Mozgásvonal maximális hossza (km) – ami ennél hosszabb, az szinte biztos téves geokód
MAX_MOVE_KM = 200.0

# Nominatim keresés "nézet doboz" (lon/lat) – UA + RU nyugati térség (Belgorod/Kursk/Rostov is belefér)
VIEW_MIN_LON, VIEW_MIN_LAT = 20.0, 42.0
VIEW_MAX_LON, VIEW_MAX_LAT = 60.0, 58.5

# Csak ezek az országok maradhatnak
ALLOWED_COUNTRY_CODES = {"ua", "ru"}

# Nominatim rate-limit barát
NOMINATIM_SLEEP_SEC = 1.0

# ÚJ cache (ne a régi geocode_cache.json-t használd!)
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

def collect_recent_article_links(limit: int = 40) -> list[str]:
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
# STEP 2 — mondat + kulcsszavak
# =========================

GROUND_KEYWORDS = [
    "assault", "attacked", "attack", "offensive",
    "advanced", "advance", "made gains", "gains",
    "seized", "captured", "took", "recaptured",
    "pushed", "pushing", "breached", "breakthrough",
    "counterattack", "counter-attacked", "counteroffensive",
    "repelled", "repulse", "withdrew", "withdrawal",
    "cleared", "secured",
]

PLACE_PATTERNS = [
    re.compile(r"\b(in|near|around|outside|south of|north of|east of|west of)\s+([A-Z][A-Za-z0-9\-\']+)", re.IGNORECASE),
    re.compile(r"\b(toward|towards)\s+([A-Z][A-Za-z0-9\-\']+)", re.IGNORECASE),
]

MOVE_PATTERN = re.compile(
    r"\bfrom\s+([A-Z][A-Za-z0-9\-\']+)\s+(?:to|toward|towards)\s+([A-Z][A-Za-z0-9\-\']+)",
    re.IGNORECASE
)

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

def extract_place(sentence: str) -> str | None:
    for pat in PLACE_PATTERNS:
        m = pat.search(sentence)
        if m:
            return m.group(2)
    return None

def extract_events(article_url: str) -> list[dict]:
    html = fetch_url(article_url)
    if not html:
        return []

    text = strip_html_to_text(html)
    date = infer_date_from_url(article_url)
    sentences = re.split(r"\.\s+", text)

    events: list[dict] = []

    for s in sentences:
        s = s.strip()
        if len(s) < 40:
            continue

        lower = s.lower()
        if not any(k in lower for k in GROUND_KEYWORDS):
            continue

        move = MOVE_PATTERN.search(s)
        if move:
            a = move.group(1)
            b = move.group(2)
            events.append({
                "date": str(date),
                "kind": "movement",
                "from_place": a,
                "to_place": b,
                "text": s[:350],
                "source_url": article_url
            })
            continue

        place = extract_place(s)
        events.append({
            "date": str(date),
            "kind": "ground",
            "place": place,
            "text": s[:350],
            "source_url": article_url
        })

    return events


# =========================
# STEP 3 — Geokód (Nominatim) V2 cache + countrycodes=ua,ru
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
    """
    Returns (coords [lon,lat] or None, country_code or None)
    Only UA/RU allowed.
    """
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

        # dupla biztosíték
        if cc not in ALLOWED_COUNTRY_CODES:
            return None, None

        coords = [lon, lat]
        _cache_set(key, coords, cc)
        time.sleep(NOMINATIM_SLEEP_SEC)
        return coords, cc

    except Exception:
        return None, None


# =========================
# STEP 4 — Frontvonal betöltés + távolság (km)
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

def _dist_point_to_segment_m(px, py, ax, ay, bx, by):
    abx = bx - ax
    aby = by - ay
    apx = px - ax
    apy = py - ay
    ab2 = abx*abx + aby*aby
    if ab2 <= 1e-12:
        return math.hypot(px - ax, py - ay)
    t = (apx*abx + apy*aby) / ab2
    if t < 0.0:
        return math.hypot(px - ax, py - ay)
    if t > 1.0:
        return math.hypot(px - bx, py - by)
    cx = ax + t*abx
    cy = ay + t*aby
    return math.hypot(px - cx, py - cy)

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
        d = _dist_point_to_segment_m(px, py, ax, ay, bx, by)
        if d < best_m:
            best_m = d
            if best_m <= 1500:
                break

    return best_m / 1000.0

def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    return 2*r*math.asin(min(1.0, math.sqrt(a)))


# =========================
# STEP 5 — GeoJSON (front-közeli + UA/RU + max line length)
# =========================

def events_to_geojson(events: list[dict], frontline_segments: list[dict]) -> dict:
    features: list[dict] = []
    kept = 0
    dropped_far = 0
    dropped_geocode = 0
    dropped_long = 0

    for e in events:
        kind = e.get("kind")

        if kind == "movement":
            a = e.get("from_place")
            b = e.get("to_place")

            ca, cca = geocode(a)
            cb, ccb = geocode(b)
            if not (ca and cb and cca and ccb):
                dropped_geocode += 1
                continue

            lat_a, lon_a = ca[1], ca[0]
            lat_b, lon_b = cb[1], cb[0]

            # MAX line length filter
            length_km = haversine_km(lat_a, lon_a, lat_b, lon_b)
            if length_km > MAX_MOVE_KM:
                dropped_long += 1
                continue

            # Front-közeli: mindkét végpontot mérjük, és legalább az egyik legyen közel
            d_a = min_distance_km_to_front(lat_a, lon_a, frontline_segments, FRONT_NEAR_KM)
            d_b = min_distance_km_to_front(lat_b, lon_b, frontline_segments, FRONT_NEAR_KM)
            dmin = min(d_a, d_b)
            if dmin > FRONT_NEAR_KM:
                dropped_far += 1
                continue

            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [ca, cb]},
                "properties": {
                    "source": "ISW",
                    "date": e["date"],
                    "title": "ISW ground movement (filtered)",
                    "from": a, "to": b,
                    "from_cc": cca, "to_cc": ccb,
                    "distance_to_front_km_min": round(dmin, 1),
                    "length_km": round(length_km, 1),
                    "snippet": e["text"],
                    "url": e["source_url"]
                }
            })
            kept += 1
            continue

        # ground point
        coords, cc = geocode(e.get("place"))
        if not (coords and cc):
            dropped_geocode += 1
            continue

        lat, lon = coords[1], coords[0]
        d = min_distance_km_to_front(lat, lon, frontline_segments, FRONT_NEAR_KM)
        if d > FRONT_NEAR_KM:
            dropped_far += 1
            continue

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": {
                "source": "ISW",
                "date": e["date"],
                "title": "ISW ground operation (filtered)",
                "place": e.get("place"),
                "cc": cc,
                "distance_to_front_km": round(d, 1),
                "snippet": e["text"],
                "url": e["source_url"]
            }
        })
        kept += 1

    return {
        "type": "FeatureCollection",
        "features": features,
        "properties": {
            "filter": {
                "allowed_country_codes": sorted(list(ALLOWED_COUNTRY_CODES)),
                "front_near_km": FRONT_NEAR_KM,
                "max_move_km": MAX_MOVE_KM,
                "viewbox": [VIEW_MIN_LON, VIEW_MIN_LAT, VIEW_MAX_LON, VIEW_MAX_LAT]
            },
            "stats": {
                "kept": kept,
                "dropped_far": dropped_far,
                "dropped_geocode_or_country": dropped_geocode,
                "dropped_too_long_lines": dropped_long
            }
        }
    }


# =========================
# MAIN
# =========================

def main():
    print("ISW GROUND pipeline indul…")

    frontline_segments = load_frontline_segments()

    links = collect_recent_article_links(limit=40)
    print("Talált cikkek:", len(links))

    all_events: list[dict] = []
    for url in links:
        all_events.extend(extract_events(url))

    print("Talált események (nyers):", len(all_events))

    today = datetime.date.today()
    last7 = today - datetime.timedelta(days=7)
    last30 = today - datetime.timedelta(days=30)

    ev_latest = all_events[:60]
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

    print("ISW GROUND pipeline kész ✔")


if __name__ == "__main__":
    main()
