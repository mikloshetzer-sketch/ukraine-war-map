#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ISW ground operations pipeline (30 napos időablak + külön dátummezők)

Outputok:
- data/isw_ground_latest.geojson
- data/isw_ground_7d.geojson
- data/isw_ground_30d.geojson
- data/isw_ground_index.json
- data/geocode_cache_v2.json
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests


# =========================================================
# CONFIG
# =========================================================

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Ukraine-War-Map research bot; ISW ground pipeline; github actions)"
}

ROC_UPDATES_URL = (
    "https://understandingwar.org/research/russia-ukraine/"
    "russian-offensive-campaign-assessment-updates-2"
)

ARCGIS_FRONT_GEOJSON_URL = (
    "https://services-eu1.arcgis.com/fppoCYaq7HfVFbIV/ArcGIS/rest/services/"
    "UKR_Frontline_27072025/FeatureServer/0/query?"
    "where=1%3D1&outFields=*&f=geojson"
)

LATEST_GEOJSON = OUT_DIR / "isw_ground_latest.geojson"
GEOJSON_7D = OUT_DIR / "isw_ground_7d.geojson"
GEOJSON_30D = OUT_DIR / "isw_ground_30d.geojson"
INDEX_JSON = OUT_DIR / "isw_ground_index.json"
GEOCODE_CACHE_V2 = OUT_DIR / "geocode_cache_v2.json"

# Időablak
LOOKBACK_DAYS = 30

# Futási korlátok
MAX_ARTICLES = 30
MAX_GEOCODE_CALLS_PER_RUN = 80
HTTP_TIMEOUT_MAIN = 20
HTTP_TIMEOUT_FALLBACK = 25
FRONT_TIMEOUT = 35

# Geokód szűkítés
VIEW_MIN_LON, VIEW_MIN_LAT = 20.0, 42.0
VIEW_MAX_LON, VIEW_MAX_LAT = 60.0, 58.5
ALLOWED_COUNTRY_CODES = {"ua", "ru"}

# Szűrők
FRONT_NEAR_KM = 90.0
MAX_MOVE_KM = 160.0
MIN_INFERRED_KM = 2.0
MAX_INFERRED_KM = 70.0

# Nominatim rate-limit
NOMINATIM_SLEEP_SEC = 1.0

EARTH_R = 6371000.0  # meters


# =========================================================
# HELPERS
# =========================================================

session = requests.Session()
session.headers.update(HEADERS)


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def today_utc_date() -> dt.date:
    return dt.datetime.utcnow().date()


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_geojson(path: Path, features: list[dict]) -> None:
    fc = {"type": "FeatureCollection", "features": features}
    write_json(path, fc)


def safe_request_text(url: str, timeout: int) -> str | None:
    try:
        r = session.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.text
        return None
    except Exception:
        return None


def safe_request_json(url: str, timeout: int) -> Any | None:
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def fetch_url(url: str) -> str | None:
    text = safe_request_text(url, HTTP_TIMEOUT_MAIN)
    if text:
        return text

    proxy = "https://r.jina.ai/http://" + url.replace("https://", "").replace("http://", "")
    text = safe_request_text(proxy, HTTP_TIMEOUT_FALLBACK)
    if text:
        return text

    return None


def parse_date_str(s: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def days_old(d: dt.date, ref: dt.date | None = None) -> int:
    if ref is None:
        ref = today_utc_date()
    return (ref - d).days


def in_last_days(d: dt.date | None, n: int, ref: dt.date | None = None) -> bool:
    if d is None:
        return False
    age = days_old(d, ref)
    return 0 <= age <= n


# =========================================================
# STEP 1 — ARTICLE LINKS
# =========================================================

ARTICLE_LINK_RE = re.compile(
    r'href="([^"]*russian-offensive-campaign-assessment[^"]*)"',
    re.IGNORECASE,
)


def infer_date_from_url(article_url: str) -> dt.date | None:
    m = re.search(r"([A-Za-z]+-\d{1,2}-\d{4})", article_url)
    if not m:
        return None
    try:
        return dt.datetime.strptime(m.group(1), "%B-%d-%Y").date()
    except Exception:
        return None


def collect_recent_article_links(limit: int = MAX_ARTICLES, lookback_days: int = LOOKBACK_DAYS) -> list[dict]:
    log("ISW index letöltése...")
    html = fetch_url(ROC_UPDATES_URL)
    if not html:
        log("ISW index nem tölthető.")
        return []

    today = today_utc_date()
    links: dict[str, dict] = {}

    for raw in ARTICLE_LINK_RE.findall(html):
        if "research" not in raw:
            continue
        if not raw.startswith("http"):
            raw = "https://understandingwar.org" + raw

        article_date = infer_date_from_url(raw)
        if article_date is None:
            continue

        if not in_last_days(article_date, lookback_days, today):
            continue

        links[raw] = {
            "url": raw,
            "article_date": str(article_date),
            "days_old": days_old(article_date, today),
        }

    out = sorted(
        links.values(),
        key=lambda x: (x["article_date"], x["url"]),
        reverse=True,
    )[:limit]

    log(f"30 napon belüli cikklinkek: {len(out)}")
    return out


# =========================================================
# STEP 2 — TEXT / EVENTS
# =========================================================

GROUND_KEYWORDS = [
    "assault",
    "attacked",
    "attack",
    "offensive",
    "advanced",
    "advance",
    "made gains",
    "gains",
    "seized",
    "captured",
    "took",
    "recaptured",
    "pushed",
    "pushing",
    "breached",
    "breakthrough",
    "counterattack",
    "counter-attacked",
    "counteroffensive",
    "repelled",
    "repulse",
    "withdrew",
    "withdrawal",
    "cleared",
    "secured",
    "in the direction of",
]

MOVE_FROM_TO = re.compile(
    r"\bfrom\s+(?:the\s+vicinity\s+of\s+|near\s+|around\s+|in\s+)?"
    r"(.+?)\s+(?:to|toward|towards)\s+(.+?)(?:[.;]|$)",
    re.IGNORECASE,
)

MOVE_TOWARD = re.compile(
    r"\b(?:to|toward|towards|in the direction of)\s+"
    r"([A-Z][A-Za-z0-9\-\']+(?:\s+[A-Z][A-Za-z0-9\-\']+)*)",
    re.IGNORECASE,
)

PLACE_PATTERN = re.compile(
    r"\b(in|near|around|outside|south of|north of|east of|west of|within)\s+"
    r"([A-Z][A-Za-z0-9\-\']+(?:\s+[A-Z][A-Za-z0-9\-\']+)*)",
    re.IGNORECASE,
)


def strip_html_to_text(html: str) -> str:
    html = re.sub(r"<script.*?>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r"<style.*?>.*?</style>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^<]+?>", " ", html)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&quot;|&#34;", '"', text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_place(raw: str | None) -> str | None:
    if not raw:
        return None

    s = raw.strip()
    s = re.sub(r"\s+\(.*?\)\s*$", "", s).strip()
    s = re.sub(r"\s+(?:direction|axis|area|region|sector)\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"^[,;:\-]+", "", s).strip()
    s = re.sub(r"[,;:\-]+$", "", s).strip()

    noise_suffixes = [
        "that",
        "which",
        "where",
        "when",
        "after",
        "before",
        "because",
        "while",
        "as",
        "and",
        "but",
    ]
    parts = s.split()
    if parts and parts[-1].lower() in noise_suffixes:
        s = " ".join(parts[:-1]).strip()

    if len(s) < 2 or len(s) > 55:
        return None
    if not re.search(r"[A-Za-z]", s):
        return None

    return s


def extract_events(article_meta: dict) -> list[dict]:
    article_url = article_meta["url"]
    article_date = parse_date_str(article_meta["article_date"])
    if article_date is None:
        return []

    html = fetch_url(article_url)
    if not html:
        return []

    text = strip_html_to_text(html)
    sentences = re.split(r"(?<=[\.\!\?])\s+", text)
    ingested_at = utc_now_iso()

    events: list[dict] = []

    for sentence in sentences:
        s = sentence.strip()
        if len(s) < 40:
            continue

        lower = s.lower()
        if not any(k in lower for k in GROUND_KEYWORDS):
            continue

        base = {
            "article_date": str(article_date),
            "event_date": str(article_date),  # fallback: ha nincs jobb, a cikk dátuma
            "ingested_at": ingested_at,
            "text": s[:420],
            "source_url": article_url,
        }

        mm = MOVE_FROM_TO.search(s)
        if mm:
            a = clean_place(mm.group(1))
            b = clean_place(mm.group(2))
            if a and b and a != b:
                events.append(
                    {
                        **base,
                        "kind": "movement",
                        "from_place": a,
                        "to_place": b,
                    }
                )
                continue

        tm = MOVE_TOWARD.search(s)
        if tm:
            b = clean_place(tm.group(1))
            if b:
                events.append(
                    {
                        **base,
                        "kind": "toward_only",
                        "to_place": b,
                    }
                )
                continue

        pm = PLACE_PATTERN.search(s)
        place = clean_place(pm.group(2)) if pm else None
        if place:
            events.append(
                {
                    **base,
                    "kind": "ground",
                    "place": place,
                }
            )

    return events


# =========================================================
# STEP 3 — GEOCODING
# =========================================================

if GEOCODE_CACHE_V2.exists():
    try:
        geocache: dict[str, Any] = json.loads(GEOCODE_CACHE_V2.read_text(encoding="utf-8"))
    except Exception:
        geocache = {}
else:
    geocache = {}

geocode_calls_this_run = 0
geocode_cache_hits = 0
geocode_cache_negative_hits = 0
geocode_new_success = 0
geocode_new_fail = 0


def save_cache() -> None:
    write_json(GEOCODE_CACHE_V2, geocache)


def _cache_get(place: str) -> tuple[list[float] | None, str | None, bool]:
    entry = geocache.get(place)
    if not isinstance(entry, dict):
        return None, None, False

    if entry.get("negative") is True:
        return None, None, True

    coords = entry.get("coords")
    cc = (entry.get("cc") or "").lower()
    if (
        cc in ALLOWED_COUNTRY_CODES
        and isinstance(coords, list)
        and len(coords) == 2
        and all(isinstance(v, (int, float)) for v in coords)
    ):
        return coords, cc, False

    return None, None, False


def _cache_set_positive(place: str, coords: list[float], cc: str) -> None:
    geocache[place] = {"coords": coords, "cc": cc, "negative": False}


def _cache_set_negative(place: str) -> None:
    geocache[place] = {"coords": None, "cc": None, "negative": True}


def geocode(place: str | None) -> tuple[list[float] | None, str | None]:
    global geocode_calls_this_run, geocode_cache_hits, geocode_cache_negative_hits
    global geocode_new_success, geocode_new_fail

    if not place:
        return None, None

    key = place.strip()
    if not key:
        return None, None

    cached_coords, cached_cc, known_negative = _cache_get(key)
    if known_negative:
        geocode_cache_negative_hits += 1
        return None, None
    if cached_coords:
        geocode_cache_hits += 1
        return cached_coords, cached_cc

    if geocode_calls_this_run >= MAX_GEOCODE_CALLS_PER_RUN:
        log(f"Geocode limit elérve ebben a futásban ({MAX_GEOCODE_CALLS_PER_RUN}).")
        return None, None

    geocode_calls_this_run += 1

    try:
        viewbox = f"{VIEW_MIN_LON},{VIEW_MAX_LAT},{VIEW_MAX_LON},{VIEW_MIN_LAT}"
        url = (
            "https://nominatim.openstreetmap.org/search"
            f"?format=json&q={requests.utils.quote(key)}"
            f"&addressdetails=1&limit=1"
            f"&countrycodes=ua,ru"
            f"&viewbox={viewbox}&bounded=1"
        )

        r = session.get(url, timeout=20)
        data = r.json()

        if not data:
            _cache_set_negative(key)
            geocode_new_fail += 1
            return None, None

        item = data[0]
        lat = float(item["lat"])
        lon = float(item["lon"])
        addr = item.get("address") or {}
        cc = (addr.get("country_code") or "").lower()

        if cc not in ALLOWED_COUNTRY_CODES:
            _cache_set_negative(key)
            geocode_new_fail += 1
            return None, None

        coords = [lon, lat]
        _cache_set_positive(key, coords, cc)
        geocode_new_success += 1

        if geocode_calls_this_run % 10 == 0:
            save_cache()

        time.sleep(NOMINATIM_SLEEP_SEC)
        return coords, cc

    except Exception:
        _cache_set_negative(key)
        geocode_new_fail += 1
        return None, None


# =========================================================
# STEP 4 — FRONTLINE
# =========================================================

def load_frontline_segments() -> list[dict]:
    log("Frontvonal letöltése...")
    gj = safe_request_json(ARCGIS_FRONT_GEOJSON_URL, FRONT_TIMEOUT)
    if not gj:
        log("Frontvonal nem tölthető, üres szegmenslista lesz.")
        return []

    segs: list[dict] = []

    def add_linestring(coords: list[list[float]]) -> None:
        for i in range(len(coords) - 1):
            lon1, lat1 = coords[i]
            lon2, lat2 = coords[i + 1]
            segs.append(
                {
                    "lat1": lat1,
                    "lon1": lon1,
                    "lat2": lat2,
                    "lon2": lon2,
                    "minlat": min(lat1, lat2),
                    "maxlat": max(lat1, lat2),
                    "minlon": min(lon1, lon2),
                    "maxlon": max(lon1, lon2),
                }
            )

    for f in gj.get("features", []):
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

    log(f"Frontvonal szegmensek: {len(segs)}")
    return segs


def _to_xy_m(lat: float, lon: float, lat0: float) -> tuple[float, float]:
    x = math.radians(lon) * EARTH_R * math.cos(math.radians(lat0))
    y = math.radians(lat) * EARTH_R
    return x, y


def _xy_to_latlon(x: float, y: float, lat0: float) -> tuple[float, float]:
    lat = math.degrees(y / EARTH_R)
    lon = math.degrees(x / (EARTH_R * max(1e-9, math.cos(math.radians(lat0)))))
    return lat, lon


def _closest_point_on_segment_xy(
    px: float, py: float, ax: float, ay: float, bx: float, by: float
) -> tuple[float, float, float]:
    abx = bx - ax
    aby = by - ay
    apx = px - ax
    apy = py - ay
    ab2 = abx * abx + aby * aby

    if ab2 <= 1e-12:
        return ax, ay, 0.0

    t = (apx * abx + apy * aby) / ab2
    if t < 0.0:
        return ax, ay, 0.0
    if t > 1.0:
        return bx, by, 1.0

    cx = ax + t * abx
    cy = ay + t * aby
    return cx, cy, t


def min_distance_km_to_front(lat: float, lon: float, segments: list[dict], threshold_km: float) -> float:
    if not segments:
        return float("inf")

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


def nearest_point_on_front(
    lat: float, lon: float, segments: list[dict], threshold_km: float
) -> tuple[float, float, float] | None:
    if not segments:
        return None

    deg_lat = threshold_km / 111.0
    deg_lon = threshold_km / (111.0 * max(0.2, math.cos(math.radians(lat))))
    lat0 = lat
    px, py = _to_xy_m(lat, lon, lat0)
    best_m = float("inf")
    best_xy: tuple[float, float] | None = None

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

    if best_xy is None:
        return None

    dist_km = best_m / 1000.0
    if dist_km > threshold_km:
        return None

    front_lat, front_lon = _xy_to_latlon(best_xy[0], best_xy[1], lat0)
    return front_lat, front_lon, dist_km


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(min(1.0, math.sqrt(a)))


# =========================================================
# STEP 5 — FEATURES
# =========================================================

def make_point_feature(
    coords: list[float],
    event: dict,
    place: str,
    country_code: str | None,
    front_dist_km: float | None,
) -> dict:
    props = {
        "source": "isw_ground",
        "kind": event.get("kind"),
        "article_date": event.get("article_date"),
        "event_date": event.get("event_date"),
        "ingested_at": event.get("ingested_at"),
        "place": place,
        "country_code": country_code,
        "front_dist_km": None if front_dist_km is None or math.isinf(front_dist_km) else round(front_dist_km, 2),
        "text": event.get("text"),
        "source_url": event.get("source_url"),
    }
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": coords},
        "properties": props,
    }


def make_line_feature(
    from_coords: list[float],
    to_coords: list[float],
    event: dict,
    line_kind: str,
    length_km: float,
) -> dict:
    props = {
        "source": "isw_ground",
        "kind": line_kind,
        "article_date": event.get("article_date"),
        "event_date": event.get("event_date"),
        "ingested_at": event.get("ingested_at"),
        "from_place": event.get("from_place"),
        "to_place": event.get("to_place"),
        "length_km": round(length_km, 2),
        "text": event.get("text"),
        "source_url": event.get("source_url"),
    }
    return {
        "type": "Feature",
        "geometry": {"type": "LineString", "coordinates": [from_coords, to_coords]},
        "properties": props,
    }


def feature_event_date(feature: dict) -> dt.date | None:
    props = feature.get("properties") or {}
    event_date = parse_date_str(props.get("event_date") or "")
    if event_date is not None:
        return event_date

    article_date = parse_date_str(props.get("article_date") or "")
    return article_date


def feature_in_last_days(feature: dict, days: int, ref: dt.date) -> bool:
    d = feature_event_date(feature)
    return in_last_days(d, days, ref)


def build_features(events: list[dict], segments: list[dict]) -> tuple[list[dict], dict]:
    features: list[dict] = []

    stats = {
        "events_total": len(events),
        "events_with_any_geo": 0,
        "point_features": 0,
        "line_features": 0,
        "movement_lines": 0,
        "inferred_lines": 0,
        "dropped_not_front_near": 0,
        "dropped_geocode": 0,
        "dropped_too_long": 0,
        "frontline_available": bool(segments),
    }

    for idx, event in enumerate(events, start=1):
        if idx % 25 == 0:
            log(f"Feature build progress: {idx}/{len(events)}")

        kind = event.get("kind")

        if kind == "movement":
            from_coords, from_cc = geocode(event.get("from_place"))
            to_coords, to_cc = geocode(event.get("to_place"))

            if not from_coords or not to_coords:
                stats["dropped_geocode"] += 1
                continue

            stats["events_with_any_geo"] += 1

            from_lon, from_lat = from_coords
            to_lon, to_lat = to_coords

            from_front = min_distance_km_to_front(from_lat, from_lon, segments, FRONT_NEAR_KM)
            to_front = min_distance_km_to_front(to_lat, to_lon, segments, FRONT_NEAR_KM)

            if segments and from_front > FRONT_NEAR_KM and to_front > FRONT_NEAR_KM:
                stats["dropped_not_front_near"] += 1
                continue

            length_km = haversine_km(from_lat, from_lon, to_lat, to_lon)
            if length_km > MAX_MOVE_KM:
                stats["dropped_too_long"] += 1
                continue

            features.append(make_line_feature(from_coords, to_coords, event, "movement_line", length_km))
            stats["line_features"] += 1
            stats["movement_lines"] += 1

            features.append(
                make_point_feature(
                    to_coords,
                    event,
                    event.get("to_place") or "",
                    to_cc,
                    to_front if not math.isinf(to_front) else None,
                )
            )
            stats["point_features"] += 1
            continue

        if kind == "toward_only":
            to_coords, to_cc = geocode(event.get("to_place"))
            if not to_coords:
                stats["dropped_geocode"] += 1
                continue

            stats["events_with_any_geo"] += 1

            to_lon, to_lat = to_coords
            to_front = min_distance_km_to_front(to_lat, to_lon, segments, FRONT_NEAR_KM)

            if segments and to_front > FRONT_NEAR_KM:
                stats["dropped_not_front_near"] += 1
                continue

            features.append(
                make_point_feature(
                    to_coords,
                    event,
                    event.get("to_place") or "",
                    to_cc,
                    to_front if not math.isinf(to_front) else None,
                )
            )
            stats["point_features"] += 1

            nearest = nearest_point_on_front(to_lat, to_lon, segments, FRONT_NEAR_KM)
            if nearest:
                front_lat, front_lon, _ = nearest
                inferred_km = haversine_km(front_lat, front_lon, to_lat, to_lon)
                if MIN_INFERRED_KM <= inferred_km <= MAX_INFERRED_KM:
                    inferred_event = dict(event)
                    inferred_event["from_place"] = "frontline"
                    features.append(
                        make_line_feature(
                            [front_lon, front_lat],
                            to_coords,
                            inferred_event,
                            "inferred_line",
                            inferred_km,
                        )
                    )
                    stats["line_features"] += 1
                    stats["inferred_lines"] += 1
            continue

        place = event.get("place")
        coords, cc = geocode(place)
        if not coords:
            stats["dropped_geocode"] += 1
            continue

        stats["events_with_any_geo"] += 1

        lon, lat = coords
        front_dist = min_distance_km_to_front(lat, lon, segments, FRONT_NEAR_KM)
        if segments and front_dist > FRONT_NEAR_KM:
            stats["dropped_not_front_near"] += 1
            continue

        features.append(
            make_point_feature(
                coords,
                event,
                place or "",
                cc,
                front_dist if not math.isinf(front_dist) else None,
            )
        )
        stats["point_features"] += 1

    return features, stats


# =========================================================
# STEP 6 — MAIN
# =========================================================

def main() -> int:
    started = utc_now_iso()
    today = today_utc_date()
    log("ISW ground pipeline start")

    article_metas = collect_recent_article_links(MAX_ARTICLES, LOOKBACK_DAYS)

    all_events: list[dict] = []
    articles_ok = 0

    for i, meta in enumerate(article_metas, start=1):
        log(f"Cikk feldolgozás {i}/{len(article_metas)}: {meta['url']} | article_date={meta['article_date']}")
        try:
            ev = extract_events(meta)
            if ev:
                articles_ok += 1
            all_events.extend(ev)
            log(f"  -> események ebből a cikkből: {len(ev)} | összesen: {len(all_events)}")
        except Exception as e:
            log(f"  -> hiba cikk feldolgozás közben: {e}")

    dedup_seen = set()
    dedup_events: list[dict] = []
    for e in all_events:
        key = (
            e.get("article_date"),
            e.get("event_date"),
            e.get("kind"),
            e.get("from_place"),
            e.get("to_place"),
            e.get("place"),
            e.get("text"),
        )
        if key in dedup_seen:
            continue
        dedup_seen.add(key)
        dedup_events.append(e)

    log(f"Nyers események: {len(all_events)} | deduplikált: {len(dedup_events)}")

    segments = load_frontline_segments()
    features_all, stats = build_features(dedup_events, segments)

    features_latest = [
        f for f in features_all
        if feature_event_date(f) == today
    ]
    features_7d = [
        f for f in features_all
        if feature_in_last_days(f, 7, today)
    ]
    features_30d = [
        f for f in features_all
        if feature_in_last_days(f, 30, today)
    ]

    write_geojson(LATEST_GEOJSON, features_latest)
    write_geojson(GEOJSON_7D, features_7d)
    write_geojson(GEOJSON_30D, features_30d)
    save_cache()

    index = {
        "source": "isw_ground",
        "generated_at_utc": utc_now_iso(),
        "started_at_utc": started,
        "config": {
            "lookback_days": LOOKBACK_DAYS,
            "max_articles": MAX_ARTICLES,
            "max_geocode_calls_per_run": MAX_GEOCODE_CALLS_PER_RUN,
            "front_near_km": FRONT_NEAR_KM,
            "max_move_km": MAX_MOVE_KM,
            "min_inferred_km": MIN_INFERRED_KM,
            "max_inferred_km": MAX_INFERRED_KM,
        },
        "stats": {
            "article_links_found_in_window": len(article_metas),
            "articles_with_events": articles_ok,
            "raw_events_total": len(all_events),
            "dedup_events_total": len(dedup_events),
            "features_latest": len(features_latest),
            "features_7d": len(features_7d),
            "features_30d": len(features_30d),
            **stats,
            "geocode_cache_hits": geocode_cache_hits,
            "geocode_cache_negative_hits": geocode_cache_negative_hits,
            "geocode_new_success": geocode_new_success,
            "geocode_new_fail": geocode_new_fail,
            "geocode_calls_this_run": geocode_calls_this_run,
            "cache_entries_total": len(geocache),
        },
        "files": {
            "latest": str(LATEST_GEOJSON),
            "7d": str(GEOJSON_7D),
            "30d": str(GEOJSON_30D),
            "cache": str(GEOCODE_CACHE_V2),
        },
    }

    write_json(INDEX_JSON, index)

    log("Pipeline kész.")
    log(f"Latest features: {len(features_latest)}")
    log(f"7d features: {len(features_7d)}")
    log(f"30d features: {len(features_30d)}")
    log(f"Geocode cache hits: {geocode_cache_hits}")
    log(f"New geocode success/fail: {geocode_new_success}/{geocode_new_fail}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        fallback = {
            "source": "isw_ground",
            "generated_at_utc": utc_now_iso(),
            "error": str(e),
        }
        try:
            write_json(INDEX_JSON, fallback)
            save_cache()
        except Exception:
            pass

        log(f"Végzetes hiba: {e}")
        raise
