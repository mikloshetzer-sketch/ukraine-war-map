#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GDELT ground events pipeline
Második publikus forrás az ISW mellé.

Cél:
- frontközeli, földi harccselekményekről szóló friss hírek begyűjtése GDELT-ből
- helyszínkinyerés cím/szöveg alapján
- geokódolás
- 7 napos és 30 napos GeoJSON rétegek előállítása

Outputok:
- data/gdelt_ground_latest.geojson
- data/gdelt_ground_7d.geojson
- data/gdelt_ground_30d.geojson
- data/gdelt_ground_index.json
- data/geocode_cache_v2.json

Megjegyzés:
- ez nem DeepState API, hanem nyilvános híralapú második forrás.
- így jogilag és technikailag is kisebb a kockázat.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import requests


# =========================================================
# CONFIG
# =========================================================

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Ukraine-War-Map research bot; GDELT ground pipeline; github actions)"
}

LATEST_GEOJSON = OUT_DIR / "gdelt_ground_latest.geojson"
GEOJSON_7D = OUT_DIR / "gdelt_ground_7d.geojson"
GEOJSON_30D = OUT_DIR / "gdelt_ground_30d.geojson"
INDEX_JSON = OUT_DIR / "gdelt_ground_index.json"
GEOCODE_CACHE_V2 = OUT_DIR / "geocode_cache_v2.json"

# Front line source - ugyanaz, mint az ISW ground pipeline-ban
ARCGIS_FRONT_GEOJSON_URL = (
    "https://services-eu1.arcgis.com/fppoCYaq7HfVFbIV/ArcGIS/rest/services/"
    "UKR_Frontline_27072025/FeatureServer/0/query?"
    "where=1%3D1&outFields=*&f=geojson"
)

# Időablakok
LOOKBACK_DAYS = 30
LATEST_WINDOW_DAYS = 1

# Feldolgozási korlátok
MAX_ARTICLES_TOTAL = 120
MAX_GEOCODE_CALLS_PER_RUN = 80

# Geokódolás / front
VIEW_MIN_LON, VIEW_MIN_LAT = 20.0, 42.0
VIEW_MAX_LON, VIEW_MAX_LAT = 60.0, 58.5
ALLOWED_COUNTRY_CODES = {"ua", "ru"}

FRONT_NEAR_KM = 90.0
NOMINATIM_SLEEP_SEC = 1.0

HTTP_TIMEOUT = 30
FRONT_TIMEOUT = 35
EARTH_R = 6371000.0

# Keresések:
# szándékosan több, rövidebb query, hogy ne egyetlen túl általános találathalmazt kapjunk
QUERIES = [
    '("Pokrovsk" OR "Toretsk" OR "Chasiv Yar" OR "Kupiansk" OR "Lyman") AND (attack OR assault OR advance OR fighting)',
    '("Kurakhove" OR "Vovchansk" OR "Robotyne" OR "Orikhiv" OR "Siversk") AND (attack OR assault OR advance OR fighting)',
    '("Donetsk Oblast" OR "Zaporizhzhia Oblast" OR "Kharkiv Oblast" OR "Kherson Oblast") AND (frontline OR fighting OR assault)',
]

# Csak általános, megbízhatóbb ukrajnai háborús hírforrások
# domain-lista szűk, hogy ne ömöljön be minden zaj
SOURCE_DOMAINS = [
    "reuters.com",
    "kyivindependent.com",
    "euromaidanpress.com",
    "pravda.com.ua",
    "interfax.com.ua",
    "ukrinform.net",
    "unian.net",
]

GROUND_KEYWORDS = [
    "attack",
    "attacked",
    "assault",
    "assaulted",
    "advance",
    "advanced",
    "fighting",
    "battle",
    "battles",
    "clashes",
    "offensive",
    "counterattack",
    "repelled",
    "captured",
    "seized",
    "gains",
    "frontline",
]

ACTION_RE = re.compile(
    r"\b("
    r"attack(?:ed)?|assault(?:ed)?|advance(?:d)?|fighting|battle|clashes|"
    r"offensive|counterattack|repelled|captured|seized|gains?"
    r")\b",
    re.IGNORECASE,
)

PLACE_PATTERN = re.compile(
    r"\b(?:in|near|around|outside|south of|north of|east of|west of|toward|towards)\s+"
    r"([A-Z][A-Za-z0-9\-\']+(?:\s+[A-Z][A-Za-z0-9\-\']+)*)",
    re.IGNORECASE,
)

TITLE_PLACE_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9\-\']+(?:\s+[A-Z][A-Za-z0-9\-\']+)*)\b"
)

NOISE_EXACT = {
    "russian forces",
    "ukrainian forces",
    "russian troops",
    "ukrainian troops",
    "ukraine",
    "russia",
    "frontline",
    "the frontline",
    "donetsk oblast",
    "luhansk oblast",
    "kharkiv oblast",
    "zaporizhzhia oblast",
    "kherson oblast",
    "sumy oblast",
    "kursk oblast",
    "the area",
    "this area",
    "the direction",
}

NOISE_CONTAINS = [
    "forces",
    "troops",
    "military",
    "units",
    "unit",
    "brigade",
    "command",
    "ministry",
    "staff",
    "frontline",
    "direction",
    "axis",
    "operation",
    "operations",
    "campaign",
    "defense",
    "defence",
]

BAD_TOKENS = {
    "the", "a", "an", "and", "or", "of", "for", "to", "toward", "towards",
    "near", "around", "within", "outside", "north", "south", "east", "west",
    "from", "by", "on", "at", "in"
}


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


def parse_iso_dt(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_date_str(s: str | None) -> dt.date | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%a, %d %b %Y %H:%M:%S %Z"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except Exception:
            continue
    dt_obj = parse_iso_dt(s)
    if dt_obj is not None:
        return dt_obj.date()
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


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_geojson(path: Path, features: list[dict]) -> None:
    fc = {"type": "FeatureCollection", "features": features}
    write_json(path, fc)


def safe_get_text(url: str, timeout: int = HTTP_TIMEOUT) -> str | None:
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.text
    except Exception:
        return None


def safe_get_json(url: str, timeout: int = HTTP_TIMEOUT) -> Any | None:
    try:
        r = session.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


def xml_text(elem: ET.Element | None, tag: str) -> str:
    if elem is None:
        return ""
    found = elem.find(tag)
    if found is None or found.text is None:
        return ""
    return found.text.strip()


# =========================================================
# GDELT FETCH
# =========================================================

def make_gdelt_rss_url(query: str, max_records: int = 50) -> str:
    encoded_query = requests.utils.quote(query)
    return (
        "https://api.gdeltproject.org/api/v2/doc/doc?"
        f"query={encoded_query}"
        "&mode=ArtList"
        "&format=rss"
        "&sort=datedesc"
        f"&maxrecords={max_records}"
    )


def parse_gdelt_rss(xml_text_data: str) -> list[dict]:
    items: list[dict] = []

    try:
        root = ET.fromstring(xml_text_data)
    except Exception:
        return items

    channel = root.find("channel")
    if channel is None:
        return items

    for item in channel.findall("item"):
        title = xml_text(item, "title")
        link = xml_text(item, "link")
        pub_date = xml_text(item, "pubDate")
        desc = xml_text(item, "description")

        if not title or not link:
            continue

        items.append(
            {
                "title": title,
                "url": link,
                "published_raw": pub_date,
                "published_date": str(parse_date_str(pub_date)) if parse_date_str(pub_date) else None,
                "description": desc,
            }
        )

    return items


def collect_articles() -> list[dict]:
    today = today_utc_date()
    all_items: list[dict] = []

    for q in QUERIES:
        rss_url = make_gdelt_rss_url(q, max_records=50)
        log(f"GDELT query: {q}")
        xml_data = safe_get_text(rss_url, timeout=HTTP_TIMEOUT)
        if not xml_data:
            continue

        items = parse_gdelt_rss(xml_data)

        for item in items:
            d = parse_date_str(item.get("published_raw"))
            if d is None:
                continue
            if not in_last_days(d, LOOKBACK_DAYS, today):
                continue

            url_low = (item.get("url") or "").lower()
            if not any(domain in url_low for domain in SOURCE_DOMAINS):
                continue

            text_blob = f"{item.get('title', '')} {item.get('description', '')}".lower()
            if not any(k in text_blob for k in GROUND_KEYWORDS):
                continue
            if not ACTION_RE.search(text_blob):
                continue

            all_items.append(item)

    # dedupe URL alapján
    dedup: dict[str, dict] = {}
    for item in all_items:
        dedup[item["url"]] = item

    out = list(dedup.values())
    out.sort(key=lambda x: (x.get("published_date") or "", x.get("url") or ""), reverse=True)

    if len(out) > MAX_ARTICLES_TOTAL:
        out = out[:MAX_ARTICLES_TOTAL]

    log(f"GDELT cikkek összesen szűrés után: {len(out)}")
    return out


# =========================================================
# PLACE EXTRACTION
# =========================================================

def clean_place(raw: str | None) -> str | None:
    if not raw:
        return None

    s = raw.strip()
    s = re.sub(r"\s+\(.*?\)\s*$", "", s).strip()
    s = re.sub(r"\s+(?:direction|axis|area|region|sector)\s*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"^[,;:\-]+", "", s).strip()
    s = re.sub(r"[,;:\-]+$", "", s).strip()
    s = re.split(r"\b(?:that|which|where|when|after|before|because|while|although|but|and)\b", s, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    s = re.sub(r"\s+", " ", s).strip()

    if len(s) < 3 or len(s) > 45:
        return None
    if not re.search(r"[A-Za-z]", s):
        return None

    return s


def looks_like_reasonable_place(place: str | None) -> bool:
    if not place:
        return False

    p = place.strip()
    if not p:
        return False

    low = p.lower()

    if low in NOISE_EXACT:
        return False

    for frag in NOISE_CONTAINS:
        if frag in low:
            return False

    tokens = re.findall(r"[A-Za-z][A-Za-z'\-]*", p)
    if not tokens:
        return False

    if len(tokens) > 4:
        return False

    useful = [t for t in tokens if t.lower() not in BAD_TOKENS]
    if not useful:
        return False

    if not any(t[:1].isupper() for t in useful):
        return False

    if len(useful) == 1 and len(useful[0]) < 4:
        return False

    return True


def best_place_from_text(title: str, description: str) -> str | None:
    candidates: list[str] = []

    # 1) strukturáltabb minták
    for blob in (title, description):
        for m in PLACE_PATTERN.findall(blob):
            c = clean_place(m)
            if looks_like_reasonable_place(c):
                candidates.append(c)

    # 2) title-ból nagybetűs szekvenciák
    if not candidates:
        for m in TITLE_PLACE_PATTERN.findall(title):
            c = clean_place(m)
            if looks_like_reasonable_place(c):
                candidates.append(c)

    # 3) description-ből utolsó fallback
    if not candidates:
        for m in TITLE_PLACE_PATTERN.findall(description):
            c = clean_place(m)
            if looks_like_reasonable_place(c):
                candidates.append(c)

    if not candidates:
        return None

    # gyakoriság + rövidebb forma előny
    scored: dict[str, int] = {}
    for c in candidates:
        scored[c] = scored.get(c, 0) + 1

    ranked = sorted(scored.items(), key=lambda x: (-x[1], len(x[0])))
    return ranked[0][0]


def article_to_event(article: dict) -> dict | None:
    title = article.get("title", "") or ""
    description = article.get("description", "") or ""
    published_date = article.get("published_date")

    event_date = parse_date_str(published_date)
    if event_date is None:
        return None

    text_blob = f"{title}. {description}".strip()
    if not ACTION_RE.search(text_blob):
        return None

    place = best_place_from_text(title, description)
    if not place:
        return None

    return {
        "source": "gdelt_ground",
        "kind": "ground_news",
        "article_date": str(event_date),
        "event_date": str(event_date),
        "ingested_at": utc_now_iso(),
        "place": place,
        "title": title[:260],
        "text": text_blob[:420],
        "source_url": article.get("url"),
        "publisher_hint": article.get("url", ""),
    }


# =========================================================
# GEOCODING
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
rejected_before_geocode = 0


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
    global geocode_new_success, geocode_new_fail, rejected_before_geocode

    if not looks_like_reasonable_place(place):
        rejected_before_geocode += 1
        return None, None

    key = place.strip()

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
# FRONTLINE
# =========================================================

def load_frontline_segments() -> list[dict]:
    log("Frontvonal letöltése...")
    gj = safe_get_json(ARCGIS_FRONT_GEOJSON_URL, timeout=FRONT_TIMEOUT)
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


# =========================================================
# FEATURES
# =========================================================

def make_point_feature(
    coords: list[float],
    event: dict,
    country_code: str | None,
    front_dist_km: float | None,
) -> dict:
    props = {
        "source": event.get("source"),
        "kind": event.get("kind"),
        "article_date": event.get("article_date"),
        "event_date": event.get("event_date"),
        "ingested_at": event.get("ingested_at"),
        "place": event.get("place"),
        "country_code": country_code,
        "front_dist_km": None if front_dist_km is None or math.isinf(front_dist_km) else round(front_dist_km, 2),
        "title": event.get("title"),
        "text": event.get("text"),
        "source_url": event.get("source_url"),
        "publisher_hint": event.get("publisher_hint"),
    }
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": coords},
        "properties": props,
    }


def feature_event_date(feature: dict) -> dt.date | None:
    props = feature.get("properties") or {}
    event_date = parse_date_str(props.get("event_date") or "")
    if event_date is not None:
        return event_date
    return parse_date_str(props.get("article_date") or "")


def feature_in_last_days(feature: dict, days: int, ref: dt.date) -> bool:
    d = feature_event_date(feature)
    return in_last_days(d, days, ref)


def build_features(events: list[dict], segments: list[dict]) -> tuple[list[dict], dict]:
    features: list[dict] = []

    stats = {
        "events_total": len(events),
        "events_with_any_geo": 0,
        "point_features": 0,
        "dropped_not_front_near": 0,
        "dropped_geocode": 0,
        "frontline_available": bool(segments),
    }

    for idx, event in enumerate(events, start=1):
        if idx % 20 == 0:
            log(f"Feature build progress: {idx}/{len(events)}")

        coords, cc = geocode(event.get("place"))
        if not coords:
            stats["dropped_geocode"] += 1
            continue

        stats["events_with_any_geo"] += 1

        lon, lat = coords
        front_dist = min_distance_km_to_front(lat, lon, segments, FRONT_NEAR_KM)
        if segments and front_dist > FRONT_NEAR_KM:
            stats["dropped_not_front_near"] += 1
            continue

        features.append(make_point_feature(coords, event, cc, front_dist))
        stats["point_features"] += 1

    return features, stats


# =========================================================
# MAIN
# =========================================================

def main() -> int:
    started = utc_now_iso()
    today = today_utc_date()
    log("GDELT ground pipeline start")

    articles = collect_articles()

    raw_events: list[dict] = []
    for idx, article in enumerate(articles, start=1):
        if idx % 20 == 0:
            log(f"Article parse progress: {idx}/{len(articles)}")

        event = article_to_event(article)
        if event:
            raw_events.append(event)

    # dedupe: azonos URL + place + date ne ismétlődjön
    dedup_map: dict[tuple, dict] = {}
    for e in raw_events:
        key = (
            e.get("source_url"),
            e.get("place"),
            e.get("event_date"),
        )
        dedup_map[key] = e

    events = list(dedup_map.values())

    log(f"Nyers események: {len(raw_events)} | deduplikált események: {len(events)}")

    segments = load_frontline_segments()
    features_all, stats = build_features(events, segments)

    features_latest = [f for f in features_all if feature_in_last_days(f, LATEST_WINDOW_DAYS, today)]
    features_7d = [f for f in features_all if feature_in_last_days(f, 7, today)]
    features_30d = [f for f in features_all if feature_in_last_days(f, 30, today)]

    write_geojson(LATEST_GEOJSON, features_latest)
    write_geojson(GEOJSON_7D, features_7d)
    write_geojson(GEOJSON_30D, features_30d)
    save_cache()

    index = {
        "source": "gdelt_ground",
        "generated_at_utc": utc_now_iso(),
        "started_at_utc": started,
        "config": {
            "lookback_days": LOOKBACK_DAYS,
            "latest_window_days": LATEST_WINDOW_DAYS,
            "max_articles_total": MAX_ARTICLES_TOTAL,
            "max_geocode_calls_per_run": MAX_GEOCODE_CALLS_PER_RUN,
            "front_near_km": FRONT_NEAR_KM,
            "source_domains": SOURCE_DOMAINS,
            "queries": QUERIES,
        },
        "stats": {
            "articles_after_filters": len(articles),
            "raw_events_total": len(raw_events),
            "dedup_events_total": len(events),
            "features_latest": len(features_latest),
            "features_7d": len(features_7d),
            "features_30d": len(features_30d),
            **stats,
            "geocode_cache_hits": geocode_cache_hits,
            "geocode_cache_negative_hits": geocode_cache_negative_hits,
            "geocode_new_success": geocode_new_success,
            "geocode_new_fail": geocode_new_fail,
            "geocode_calls_this_run": geocode_calls_this_run,
            "rejected_before_geocode": rejected_before_geocode,
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
            "source": "gdelt_ground",
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
