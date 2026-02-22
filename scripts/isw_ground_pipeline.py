#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ISW ground operations pipeline (ground assaults / advances / counterattacks),
front-közeli szűréssel és csak UA/RU országokra korlátozott geokóddal.

Output:
- data/isw_ground_latest.geojson
- data/isw_ground_7d.geojson
- data/isw_ground_30d.geojson
- data/isw_ground_index.json
- data/geocode_cache.json  (közös cache az UAV pipeline-nal)

Fő elvek:
- ISW ROC cikkekből mondat-szintű események
- Geokód Nominatimmal, addressdetails=1, country_code szűréssel (UA/RU)
- Front-közeli szűrés ArcGIS frontvonal GeoJSON alapján (km küszöb)
- "from A to B" esetben LineString is készülhet, de csak ha front közelében van
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

UA = (
    "Mozilla/5.0 (Ukraine-War-Map research bot; ISW ground pipeline; "
    "github actions)"
)

HEADERS = {"User-Agent": UA}

ROC_UPDATES_URL = "https://understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-updates-2"

# ArcGIS frontvonal GeoJSON (ugyanaz, mint a HTML-ben)
ARCGIS_FRONT_GEOJSON_URL = (
    "https://services-eu1.arcgis.com/fppoCYaq7HfVFbIV/ArcGIS/rest/services/"
    "UKR_Frontline_27072025/FeatureServer/0/query?"
    "where=1%3D1&outFields=*&f=geojson"
)

# Front-közeli küszöb (km)
FRONT_NEAR_KM = 80.0

# Geokód szűkített keresési doboz (lon/lat)
# Ukrajna + nyugati/orosz harctér környéke (bőven elég)
VIEW_MIN_LON, VIEW_MIN_LAT = 20.0, 43.0
VIEW_MAX_LON, VIEW_MAX_LAT = 45.5, 56.5

# Csak ezek az országok maradhatnak
ALLOWED_COUNTRY_CODES = {"ua", "ru"}  # Ukraine, Russia

# Nominatim rate limit barát
NOMINATIM_SLEEP_SEC = 1.0


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
                "place": b,
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
# STEP 3 — Geokód (Nominatim) csak UA/RU
# =========================

GEOCODE_CACHE = OUT_DIR / "geocode_cache.json"
if GEOCODE_CACHE.exists():
    cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
else:
    cache = {}

def geocode(place: str | None) -> list[float] | None:
    """
    Return [lon, lat] ONLY if result country_code is in ALLOWED_COUNTRY_CODES.
    Uses bounded viewbox to reduce false matches.
    """
    if not place:
        return None

    key = place.strip()
    if not key:
        return None

    if key in cache:
        # cache-ban lehet régi, “rossz” találat – de ha egyszer beengedted, megmarad.
        # Ha full tisztítást akarsz, töröld a data/geocode_cache.json-t.
        return cache[key]

    try:
        viewbox = f"{VIEW_MIN_LON},{VIEW_MAX_LAT},{VIEW_MAX_LON},{VIEW_MIN_LAT}"
        url = (
            "https://nominatim.openstreetmap.org/search"
            f"?format=json"
            f"&q={requests.utils.quote(key)}"
            f"&addressdetails=1"
            f"&limit=1"
            f"&viewbox={viewbox}&bounded=1"
        )

        r = requests.get(url, headers=HEADERS, timeout=25)
        data = r.json()
        if not data:
            return None

        item = data[0]
        lat = float(item["lat"])
        lon = float(item["lon"])

        addr = item.get("address") or {}
        cc = (addr.get("country_code") or "").lower()

        if cc not in ALLOWED_COUNTRY_CODES:
            return None

        cache[key] = [lon, lat]
        time.sleep(NOMINATIM_SLEEP_SEC)
        return [lon, lat]

    except Exception:
        return None


# =========================
# STEP 4 — Frontvonal betöltés + távolság (km)
# =========================

EARTH_R = 6371000.0  # meters

def load_frontline_segments() -> list[dict]:
    """
    ArcGIS frontline GeoJSON-ból szegmensek listája:
    [{lat1, lon1, lat2, lon2, minlat, maxlat, minlon, maxlon}, ...]
    """
    print("Frontvonal letöltés…")
    r = requests.get(ARCGIS_FRONT_GEOJSON_URL, headers=HEADERS, timeout=40)
    r.raise_for_status()
    gj = r.json()

    segs: list[dict] = []

    def add_linestring(coords):
        # coords: [[lon,lat], [lon,lat], ...]
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
    # equirectangular projection around lat0
    x = math.radians(lon) * EARTH_R * math.cos(math.radians(lat0))
    y = math.radians(lat) * EARTH_R
    return x, y

def _dist_point_to_segment_m(px, py, ax, ay, bx, by):
    # 2D point-segment distance
    abx = bx - ax
    aby = by - ay
    apx = px - ax
    apy = py - ay
    ab2 = abx*abx + aby*aby
    if ab2 <= 1e-12:
        dx = px - ax
        dy = py - ay
        return math.hypot(dx, dy)
    t = (apx*abx + apy*aby) / ab2
    if t < 0.0:
        dx = px - ax
        dy = py - ay
        return math.hypot(dx, dy)
    if t > 1.0:
        dx = px - bx
        dy = py - by
        return math.hypot(dx, dy)
    cx = ax + t*abx
    cy = ay + t*aby
    return math.hypot(px - cx, py - cy)

def min_distance_km_to_front(lat, lon, segments, threshold_km) -> float:
    """
    Minimum distance from (lat,lon) to frontline segments.
    Pruning: segment bbox expanded by threshold degrees.
    """
    # ~ km to degrees for quick bbox prune
    deg_lat = threshold_km / 111.0
    # lon degrees depends on latitude
    deg_lon = threshold_km / (111.0 * max(0.2, math.cos(math.radians(lat))))

    lat0 = lat
    px, py = _to_xy_m(lat, lon, lat0)

    best_m = float("inf")

    for s in segments:
        # cheap bbox prune
        if lat < (s["minlat"] - deg_lat) or lat > (s["maxlat"] + deg_lat):
            continue
        if lon < (s["minlon"] - deg_lon) or lon > (s["maxlon"] + deg_lon):
            continue

        ax, ay = _to_xy_m(s["lat1"], s["lon1"], lat0)
        bx, by = _to_xy_m(s["lat2"], s["lon2"], lat0)
        d = _dist_point_to_segment_m(px, py, ax, ay, bx, by)
        if d < best_m:
            best_m = d
            # early stop if we are already very close
            if best_m <= 1500:  # 1.5 km
                break

    return best_m / 1000.0


# =========================
# STEP 5 — GeoJSON (front-közeli szűrés)
# =========================

def events_to_geojson(events: list[dict], frontline_segments: list[dict], near_km: float) -> dict:
    features: list[dict] = []
    kept = 0
    dropped_far = 0
    dropped_geocode = 0

    for e in events:
        kind = e.get("kind")

        if kind == "movement":
            a = e.get("from_place")
            b = e.get("to_place")
            ca = geocode(a)
            cb = geocode(b)
            if not (ca and cb):
                dropped_geocode += 1
                continue

            # front-közeli feltétel: legalább az egyik végpont legyen közel a fronthoz
            lat_a, lon_a = ca[1], ca[0]
            lat_b, lon_b = cb[1], cb[0]
            d_a = min_distance_km_to_front(lat_a, lon_a, frontline_segments, near_km)
            d_b = min_distance_km_to_front(lat_b, lon_b, frontline_segments, near_km)
            if min(d_a, d_b) > near_km:
                dropped_far += 1
                continue

            features.append({
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": [ca, cb]},
                "properties": {
                    "source": "ISW",
                    "date": e["date"],
                    "title": "ISW ground movement (front-near)",
                    "from": a,
                    "to": b,
                    "distance_to_front_km_min": round(min(d_a, d_b), 1),
                    "snippet": e["text"],
                    "url": e["source_url"]
                }
            })
            kept += 1
            continue

        # sima ground pont
        coords = geocode(e.get("place"))
        if not coords:
            dropped_geocode += 1
            continue

        lat, lon = coords[1], coords[0]
        d = min_distance_km_to_front(lat, lon, frontline_segments, near_km)
        if d > near_km:
            dropped_far += 1
            continue

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": {
                "source": "ISW",
                "date": e["date"],
                "title": "ISW ground operation (front-near)",
                "place": e.get("place"),
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
                "front_near_km": near_km
            },
            "stats": {
                "kept": kept,
                "dropped_far": dropped_far,
                "dropped_geocode_or_country": dropped_geocode
            }
        }
    }


# =========================
# MAIN
# =========================

def main():
    print("ISW GROUND pipeline indul…")

    # Frontline szegmensek betöltése (front-közeli szűréshez)
    try:
        frontline_segments = load_frontline_segments()
    except Exception as ex:
        print("HIBA: frontvonal nem tölthető, pipeline leáll:", ex)
        return

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

    latest_gj = events_to_geojson(ev_latest, frontline_segments, FRONT_NEAR_KM)
    gj7 = events_to_geojson(ev_7, frontline_segments, FRONT_NEAR_KM)
    gj30 = events_to_geojson(ev_30, frontline_segments, FRONT_NEAR_KM)

    OUT_DIR.joinpath("isw_ground_latest.geojson").write_text(json.dumps(latest_gj, indent=2), encoding="utf-8")
    OUT_DIR.joinpath("isw_ground_7d.geojson").write_text(json.dumps(gj7, indent=2), encoding="utf-8")
    OUT_DIR.joinpath("isw_ground_30d.geojson").write_text(json.dumps(gj30, indent=2), encoding="utf-8")

    OUT_DIR.joinpath("isw_ground_index.json").write_text(
        json.dumps({
            "generated_utc": datetime.datetime.utcnow().isoformat(),
            "events_total_raw": len(all_events),
            "events_7d_raw": len(ev_7),
            "events_30d_raw": len(ev_30),
            "front_near_km": FRONT_NEAR_KM,
            "allowed_country_codes": sorted(list(ALLOWED_COUNTRY_CODES)),
            "viewbox": [VIEW_MIN_LON, VIEW_MIN_LAT, VIEW_MAX_LON, VIEW_MAX_LAT],
            "latest_stats": latest_gj.get("properties", {}).get("stats", {}),
            "d7_stats": gj7.get("properties", {}).get("stats", {}),
            "d30_stats": gj30.get("properties", {}).get("stats", {})
        }, indent=2),
        encoding="utf-8"
    )

    GEOCODE_CACHE.write_text(json.dumps(cache, indent=2), encoding="utf-8")

    print("ISW GROUND pipeline kész ✔")


if __name__ == "__main__":
    main()
