#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GDELT ground events pipeline
Második publikus forrás az ISW mellé.

Cél:
- frontközeli, földi harccselekményekről szóló friss hírek begyűjtése GDELT-ből
- több lehetséges helyszín kinyerése cím/szöveg alapján
- geokódolás
- legjobb frontközeli jelölt kiválasztása
- 7 napos és 30 napos GeoJSON rétegek előállítása
- rejection debug export
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
import sys
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
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
REJECTIONS_JSON = OUT_DIR / "gdelt_ground_rejections.json"
GEOCODE_CACHE_V2 = OUT_DIR / "geocode_cache_v2.json"

ARCGIS_FRONT_GEOJSON_URL = (
    "https://services-eu1.arcgis.com/fppoCYaq7HfVFbIV/ArcGIS/rest/services/"
    "UKR_Frontline_27072025/FeatureServer/0/query?"
    "where=1%3D1&outFields=*&f=geojson"
)

LOOKBACK_DAYS = 30
LATEST_WINDOW_DAYS = 1
MAX_ARTICLES_TOTAL = 150
MAX_GEOCODE_CALLS_PER_RUN = 80
MAX_REJECTIONS_SAVED = 200
MAX_CANDIDATES_PER_ARTICLE = 8

VIEW_MIN_LON, VIEW_MIN_LAT = 20.0, 42.0
VIEW_MAX_LON, VIEW_MAX_LAT = 60.0, 58.5
ALLOWED_COUNTRY_CODES = {"ua", "ru"}

# GDELT-nél lazább frontközeli szűrés kell, mert a hírek gyakran tágabb helyet neveznek meg
FRONT_NEAR_KM = 180.0
KNOWN_PLACE_FALLBACK_FRONT_KM = 240.0

NOMINATIM_SLEEP_SEC = 1.0

HTTP_TIMEOUT = 30
FRONT_TIMEOUT = 35
EARTH_R = 6371000.0

QUERIES = [
    '"Ukraine" AND (attack OR assault OR advance OR fighting OR clashes)',
    '("Donetsk" OR "Luhansk" OR "Kharkiv" OR "Zaporizhzhia" OR "Kherson") AND (attack OR fighting OR assault OR advance)',
    '("Pokrovsk" OR "Toretsk" OR "Chasiv Yar" OR "Kupiansk" OR "Lyman" OR "Vovchansk" OR "Orikhiv") AND (attack OR fighting OR assault)',
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

# Konkrétabb, frontközeli/taktikai helyek
KNOWN_FRONT_PLACES = {
    "Pokrovsk", "Toretsk", "Chasiv Yar", "Kupiansk", "Kupyansk", "Lyman", "Siversk",
    "Orikhiv", "Robotyne", "Vovchansk", "Kurakhove", "Kurakhivka", "Kramatorsk",
    "Sloviansk", "Avdiivka", "Bakhmut", "Velyka Novosilka", "Selydove",
    "Kostiantynivka", "Novopavlivka", "Myrnohrad", "Borova", "Terny", "Yampil",
    "Bilohorivka", "New York", "Huliaipole", "Hulyaipole", "Mala Tokmachka",
    "Stepove", "Novodanylivka", "Ocheretyne", "Marinka", "Vuhledar", "Shevchenko"
}

# Túl tág / regionális helyek, amiket nem akarunk végső eseménypontnak
BROAD_PLACES = {
    "Ukraine", "Russia", "Donetsk", "Luhansk", "Kharkiv", "Kherson",
    "Zaporizhzhia", "Sumy", "Dnipro", "Kyiv", "Moscow",
    "Donetsk Oblast", "Luhansk Oblast", "Kharkiv Oblast",
    "Kherson Oblast", "Zaporizhzhia Oblast", "Sumy Oblast", "Kursk Oblast"
}

KNOWN_PLACES = KNOWN_FRONT_PLACES | BROAD_PLACES

NOISE_EXACT = {
    "russian forces",
    "ukrainian forces",
    "russian troops",
    "ukrainian troops",
    "ukraine",
    "russia",
    "frontline",
    "the frontline",
    "the area",
    "this area",
    "the direction",
    "donetsk oblast",
    "luhansk oblast",
    "kharkiv oblast",
    "zaporizhzhia oblast",
    "kherson oblast",
    "sumy oblast",
    "kursk oblast",
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

rejections_log: list[dict[str, Any]] = []


def log(msg: str) -> None:
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts} UTC] {msg}", flush=True)


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def today_utc_date() -> dt.date:
    return dt.datetime.utcnow().date()


def parse_date_str(s: str | None) -> dt.date | None:
    if not s:
        return None

    s = s.strip()
    if not s:
        return None

    try:
        d = parsedate_to_datetime(s)
        if d is not None:
            if d.tzinfo is not None:
                d = d.astimezone(dt.timezone.utc)
            return d.date()
    except Exception:
        pass

    iso_candidate = s.replace("Z", "+00:00")
    try:
        d = dt.datetime.fromisoformat(iso_candidate)
        return d.date()
    except Exception:
        pass

    patterns = [
        (r"(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
        (r"(\d{8})", "%Y%m%d"),
        (r"([A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4})", "%a, %d %b %Y"),
    ]
    for pat, fmt in patterns:
        m = re.search(pat, s)
        if not m:
            continue
        try:
            return dt.datetime.strptime(m.group(1), fmt).date()
        except Exception:
            continue

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


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def child_text_any(item: ET.Element, names: list[str]) -> str:
    wanted = {n.lower() for n in names}
    for child in list(item):
        if local_name(child.tag).lower() in wanted:
            return (child.text or "").strip()
    return ""


def add_rejection(reason: str, payload: dict[str, Any]) -> None:
    if len(rejections_log) >= MAX_REJECTIONS_SAVED:
        return
    rejections_log.append({"reason": reason, **payload})


# =========================================================
# GDELT FETCH
# =========================================================

def make_gdelt_rss_url(query: str, max_records: int = 80) -> str:
    encoded_query = requests.utils.quote(query)
    return (
        "https://api.gdeltproject.org/api/v2/doc/doc?"
        f"query={encoded_query}"
        "&mode=ArtList"
        "&format=rss"
        "&sort=datedesc"
        f"&maxrecords={max_records}"
    )


def parse_gdelt_rss(xml_text_data: str) -> tuple[list[dict], dict]:
    items: list[dict] = []
    debug = {
        "parse_ok": False,
        "root_tag": "",
        "channel_found": False,
        "item_count_raw": 0,
        "sample_date_fields": [],
    }

    try:
        root = ET.fromstring(xml_text_data)
    except Exception:
        return items, debug

    debug["parse_ok"] = True
    debug["root_tag"] = local_name(root.tag)

    channel = None
    if local_name(root.tag).lower() == "rss":
        for child in list(root):
            if local_name(child.tag).lower() == "channel":
                channel = child
                break
    elif local_name(root.tag).lower() == "feed":
        channel = root

    if channel is None:
        return items, debug

    debug["channel_found"] = True

    raw_items = [child for child in list(channel) if local_name(child.tag).lower() in {"item", "entry"}]
    debug["item_count_raw"] = len(raw_items)

    for idx, item in enumerate(raw_items):
        title = child_text_any(item, ["title"])
        link = child_text_any(item, ["link", "url"])
        pub_date_raw = child_text_any(item, ["pubDate", "published", "updated", "seendate"])
        description = child_text_any(item, ["description", "summary"])

        if not link:
            for child in list(item):
                if local_name(child.tag).lower() == "link":
                    href = child.attrib.get("href", "").strip()
                    if href:
                        link = href
                        break

        if not pub_date_raw:
            for child in list(item):
                lname = local_name(child.tag).lower()
                if lname in {"pubdate", "published", "updated", "seendate", "date"}:
                    pub_date_raw = (child.text or "").strip()
                    if pub_date_raw:
                        break

        if idx < 5:
            debug["sample_date_fields"].append(pub_date_raw)

        published_date = parse_date_str(pub_date_raw)

        if not title and not link:
            continue

        items.append(
            {
                "title": title,
                "url": link,
                "published_raw": pub_date_raw,
                "published_date": str(published_date) if published_date else None,
                "description": description,
            }
        )

    return items, debug


def collect_articles() -> tuple[list[dict], dict]:
    today = today_utc_date()
    all_items: list[dict] = []

    debug = {
        "queries_total": len(QUERIES),
        "queries_with_response": 0,
        "items_before_filters": 0,
        "items_with_date": 0,
        "items_in_lookback_window": 0,
        "items_keyword_matched": 0,
        "items_action_matched": 0,
        "items_after_dedup": 0,
        "parser_debug": [],
    }

    for q in QUERIES:
        rss_url = make_gdelt_rss_url(q, max_records=80)
        log(f"GDELT query: {q}")
        xml_data = safe_get_text(rss_url, timeout=HTTP_TIMEOUT)
        if not xml_data:
            continue

        debug["queries_with_response"] += 1
        items, parse_debug = parse_gdelt_rss(xml_data)
        debug["parser_debug"].append({
            "query": q,
            **parse_debug,
        })

        debug["items_before_filters"] += len(items)

        for item in items:
            d = parse_date_str(item.get("published_raw"))
            if d is None and item.get("published_date"):
                d = parse_date_str(item.get("published_date"))

            if d is None:
                continue
            debug["items_with_date"] += 1

            if not in_last_days(d, LOOKBACK_DAYS, today):
                continue
            debug["items_in_lookback_window"] += 1

            text_blob = f"{item.get('title', '')} {item.get('description', '')}".lower()
            if not any(k in text_blob for k in GROUND_KEYWORDS):
                continue
            debug["items_keyword_matched"] += 1

            if not ACTION_RE.search(text_blob):
                continue
            debug["items_action_matched"] += 1

            item["published_date"] = str(d)
            all_items.append(item)

    dedup: dict[str, dict] = {}
    for item in all_items:
        key = item.get("url") or f"{item.get('title')}|{item.get('published_date')}"
        dedup[key] = item

    out = list(dedup.values())
    out.sort(key=lambda x: (x.get("published_date") or "", x.get("url") or ""), reverse=True)

    if len(out) > MAX_ARTICLES_TOTAL:
        out = out[:MAX_ARTICLES_TOTAL]

    debug["items_after_dedup"] = len(out)
    log(f"GDELT cikkek összesen szűrés után: {len(out)}")
    return out, debug


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
    s = re.split(
        r"\b(?:that|which|where|when|after|before|because|while|although|but|and)\b",
        s,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip()
    s = re.sub(r"\s+", " ", s).strip()

    if len(s) < 3 or len(s) > 45:
        return None
    if not re.search(r"[A-Za-z]", s):
        return None

    return s


def normalize_known_place(place: str) -> str:
    for kp in KNOWN_PLACES:
        if kp.lower() == place.lower():
            return kp
    return place


def looks_like_reasonable_place(place: str | None) -> bool:
    if not place:
        return False

    p = place.strip()
    if not p:
        return False

    if normalize_known_place(p) in KNOWN_PLACES:
        return True

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


def place_priority(place: str) -> tuple[int, int]:
    p = normalize_known_place(place)
    if p in KNOWN_FRONT_PLACES:
        return (0, len(p))
    if p in BROAD_PLACES:
        return (2, len(p))
    return (1, len(p))


def extract_candidate_places(title: str, description: str) -> list[tuple[str, int, str]]:
    candidates: list[tuple[str, int, str]] = []

    # 1) explicit known front places - highest priority
    for place in KNOWN_FRONT_PLACES:
        if re.search(rf"\b{re.escape(place)}\b", title, re.IGNORECASE):
            candidates.append((place, 100, "known_front_title"))
        elif re.search(rf"\b{re.escape(place)}\b", description, re.IGNORECASE):
            candidates.append((place, 80, "known_front_description"))

    # 2) explicit broad places - low priority
    for place in BROAD_PLACES:
        if re.search(rf"\b{re.escape(place)}\b", title, re.IGNORECASE):
            candidates.append((place, 15, "broad_title"))
        elif re.search(rf"\b{re.escape(place)}\b", description, re.IGNORECASE):
            candidates.append((place, 5, "broad_description"))

    # 3) preposition-based extraction
    for blob_name, blob, base_score in (
        ("title", title, 60),
        ("description", description, 40),
    ):
        for m in PLACE_PATTERN.findall(blob):
            c = clean_place(m)
            if looks_like_reasonable_place(c):
                candidates.append((normalize_known_place(c), base_score, f"pattern_{blob_name}"))

    # 4) title chunks
    for m in TITLE_PLACE_PATTERN.findall(title):
        c = clean_place(m)
        if looks_like_reasonable_place(c):
            candidates.append((normalize_known_place(c), 25, "title_chunk"))

    # 5) description chunks
    for m in TITLE_PLACE_PATTERN.findall(description):
        c = clean_place(m)
        if looks_like_reasonable_place(c):
            candidates.append((normalize_known_place(c), 10, "description_chunk"))

    # dedupe by best score
    unique: dict[str, tuple[int, str]] = {}
    for p, score, src in candidates:
        if p not in unique or unique[p][0] < score:
            unique[p] = (score, src)

    result = [(p, sc, src) for p, (sc, src) in unique.items()]

    # sort by score desc, then priority, then shorter length
    result.sort(key=lambda x: (-x[1], place_priority(x[0])[0], len(x[0])))

    return result[:MAX_CANDIDATES_PER_ARTICLE]


def classify_location_confidence(place: str) -> str:
    p = normalize_known_place(place)
    if p in KNOWN_FRONT_PLACES:
        return "known_front_place"
    if p in BROAD_PLACES:
        return "broad_place"
    return "inferred_place"


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

    candidates = extract_candidate_places(title, description)

    # broad place only = gyenge találat, ne engedjük át
    candidates = [c for c in candidates if c[0] not in BROAD_PLACES]

    if not candidates:
        add_rejection(
            "no_specific_place_selected",
            {
                "title": title[:220],
                "source_url": article.get("url"),
                "published_date": published_date,
            },
        )
        return None

    return {
        "source": "gdelt_ground",
        "kind": "ground_news",
        "article_date": str(event_date),
        "event_date": str(event_date),
        "ingested_at": utc_now_iso(),
        "candidates": candidates,
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

    key = normalize_known_place(place.strip())

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


def place_front_threshold_km(place: str | None) -> float:
    if place and normalize_known_place(place) in KNOWN_FRONT_PLACES:
        return KNOWN_PLACE_FALLBACK_FRONT_KM
    return FRONT_NEAR_KM


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
        "location_confidence": event.get("location_confidence"),
        "candidate_score": event.get("candidate_score"),
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

        candidates = event.get("candidates") or []
        best_feature = None
        best_rank: tuple[float, float, float] | None = None
        had_geocoded_candidate = False
        candidate_debug: list[dict[str, Any]] = []

        for place, score, source in candidates:
            coords, cc = geocode(place)
            if not coords:
                candidate_debug.append({
                    "place": place,
                    "score": score,
                    "source": source,
                    "status": "geocode_failed",
                })
                continue

            had_geocoded_candidate = True
            stats["events_with_any_geo"] += 1

            lon, lat = coords
            allowed_dist = place_front_threshold_km(place)
            front_dist = min_distance_km_to_front(lat, lon, segments, allowed_dist)

            candidate_debug.append({
                "place": place,
                "score": score,
                "source": source,
                "status": "ok" if front_dist <= allowed_dist else "too_far",
                "front_dist_km": round(front_dist, 2) if not math.isinf(front_dist) else None,
                "allowed_front_dist_km": allowed_dist,
            })

            if segments and front_dist > allowed_dist:
                continue

            # ranking: higher score first, then smaller dist, then known-front preferred
            rank = (
                float(score),
                -float(front_dist),
                -float(place_priority(place)[0]),
            )

            feature = make_point_feature(
                coords,
                {
                    **event,
                    "place": place,
                    "location_confidence": source,
                    "candidate_score": score,
                },
                cc,
                front_dist,
            )

            if best_feature is None or rank > best_rank:
                best_feature = feature
                best_rank = rank

        if best_feature is not None:
            features.append(best_feature)
            stats["point_features"] += 1
            continue

        if had_geocoded_candidate:
            stats["dropped_not_front_near"] += 1
            add_rejection(
                "all_candidates_too_far_from_front",
                {
                    "title": event.get("title"),
                    "source_url": event.get("source_url"),
                    "candidate_debug": candidate_debug,
                },
            )
        else:
            stats["dropped_geocode"] += 1
            add_rejection(
                "all_candidates_failed_geocode",
                {
                    "title": event.get("title"),
                    "source_url": event.get("source_url"),
                    "candidate_debug": candidate_debug,
                },
            )

    return features, stats


# =========================================================
# MAIN
# =========================================================

def main() -> int:
    started = utc_now_iso()
    today = today_utc_date()
    log("GDELT ground pipeline start")

    articles, collect_debug = collect_articles()

    raw_events: list[dict] = []
    for idx, article in enumerate(articles, start=1):
        if idx % 20 == 0:
            log(f"Article parse progress: {idx}/{len(articles)}")

        event = article_to_event(article)
        if event:
            raw_events.append(event)

    dedup_map: dict[tuple, dict] = {}
    for e in raw_events:
        key = (
            e.get("source_url"),
            e.get("event_date"),
            tuple((p, s, src) for p, s, src in e.get("candidates", [])),
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
    write_json(REJECTIONS_JSON, {"generated_at_utc": utc_now_iso(), "rejections": rejections_log})

    index = {
        "source": "gdelt_ground",
        "generated_at_utc": utc_now_iso(),
        "started_at_utc": started,
        "config": {
            "lookback_days": LOOKBACK_DAYS,
            "latest_window_days": LATEST_WINDOW_DAYS,
            "max_articles_total": MAX_ARTICLES_TOTAL,
            "max_geocode_calls_per_run": MAX_GEOCODE_CALLS_PER_RUN,
            "max_candidates_per_article": MAX_CANDIDATES_PER_ARTICLE,
            "front_near_km": FRONT_NEAR_KM,
            "known_place_fallback_front_km": KNOWN_PLACE_FALLBACK_FRONT_KM,
            "queries": QUERIES,
            "known_places_count": len(KNOWN_PLACES),
            "known_front_places_count": len(KNOWN_FRONT_PLACES),
            "broad_places_count": len(BROAD_PLACES),
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
            "rejections_saved": len(rejections_log),
            "collect_debug": collect_debug,
        },
        "files": {
            "latest": str(LATEST_GEOJSON),
            "7d": str(GEOJSON_7D),
            "30d": str(GEOJSON_30D),
            "cache": str(GEOCODE_CACHE_V2),
            "rejections": str(REJECTIONS_JSON),
        },
    }

    write_json(INDEX_JSON, index)

    log("Pipeline kész.")
    log(f"Latest features: {len(features_latest)}")
    log(f"7d features: {len(features_7d)}")
    log(f"30d features: {len(features_30d)}")
    log(f"Articles after filters: {len(articles)}")
    log(f"New geocode success/fail: {geocode_new_success}/{geocode_new_fail}")
    log(f"Rejections saved: {len(rejections_log)}")

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
            write_json(REJECTIONS_JSON, {"generated_at_utc": utc_now_iso(), "rejections": rejections_log})
        except Exception:
            pass

        log(f"Végzetes hiba: {e}")
        raise
