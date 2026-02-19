#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

CACHE_PATH = DATA_DIR / "geocode_cache.json"
OUT_LATEST = DATA_DIR / "isw_uav_latest.geojson"
OUT_7D = DATA_DIR / "isw_uav_7d.geojson"
OUT_30D = DATA_DIR / "isw_uav_30d.geojson"
OUT_INDEX = DATA_DIR / "isw_uav_index.json"

# ---------------------------
# Config: ISW source
# ---------------------------
# Legjobb, ha RSS-ből dolgozunk, mert stabil: ha később akarsz RSS-t, ezt bővítjük.
# Addig: kézzel megadjuk az "offensive campaign assessment" oldalát, és a script kikeresi a legfrissebbet.
ISW_ROOT = "https://www.understandingwar.org"
ISW_RUS_OFFENSIVE_PAGE = "https://www.understandingwar.org/backgrounder/russian-offensive-campaign-assessment"

# ---------------------------
# Config: Nominatim
# ---------------------------
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = os.getenv("NOMINATIM_USER_AGENT", "ukraine-war-map/1.0 (contact: you@example.com)")
NOMINATIM_EMAIL = os.getenv("NOMINATIM_EMAIL", "")  # opcionális
NOMINATIM_MIN_DELAY_SEC = 1.05  # 1 req/s + kis puffer

# Geokód keresést érdemes régióra szűkíteni
PREFERRED_COUNTRIES = ["ua", "ru", "by", "md", "pl", "ro", "sk", "hu"]


UAV_KEYWORDS = [
    r"\bdrone(s)?\b",
    r"\bUAV(s)?\b",
    r"\bShahed\b",
    r"\bGeran\b",
    r"\bmissile(s)?\b",
    r"\bcruise missile(s)?\b",
    r"\bballistic missile(s)?\b",
    r"\bair defense\b",
    r"\bintercept(ed|ion)\b",
]
UAV_RE = re.compile("|".join(UAV_KEYWORDS), re.IGNORECASE)

# Nagyon egyszerű helynév-kivonás:
# - "in/near/around/over/at <Place>" vagy "<Place> Oblast/Region/City"
PLACE_PATTERNS = [
    re.compile(r"\b(in|near|around|over|at)\s+([A-Z][A-Za-z\-\’' ]{2,60})", re.IGNORECASE),
    re.compile(r"\b([A-Z][A-Za-z\-\’' ]{2,60})\s+(Oblast|Region|Raion|City)\b", re.IGNORECASE),
]

# Kizárás: tipikus szavak, amik nem helyek (bővíthető)
STOP_WORDS = set([
    "Ukrainian", "Russian", "Russians", "Ukrainians", "February", "January", "March", "April", "May",
    "ISW", "MoD", "Ministry", "Defense", "General", "Staff", "Kremlin", "Black Sea", "Azov Sea",
])

@dataclass
class ExtractedEvent:
    date: str               # YYYY-MM-DD (ISW cikk dátuma)
    source_url: str
    sentence: str
    place: str
    tag: str                # e.g. "UAV/drone/missile"
    confidence: str         # low/med/high


def _load_cache() -> Dict[str, dict]:
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def _save_cache(cache: Dict[str, dict]) -> None:
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

_last_nom_call = 0.0

def _nominatim_geocode(place: str, cache: Dict[str, dict]) -> Optional[Tuple[float, float, dict]]:
    """
    Returns (lon, lat, raw) or None.
    Uses cache, rate-limits, and a conservative query.
    """
    key = place.strip().lower()
    if key in cache:
        entry = cache[key]
        if entry.get("ok") and "lon" in entry and "lat" in entry:
            return float(entry["lon"]), float(entry["lat"]), entry.get("raw", {})
        return None

    global _last_nom_call
    now = time.time()
    wait = NOMINATIM_MIN_DELAY_SEC - (now - _last_nom_call)
    if wait > 0:
        time.sleep(wait)

    params = {
        "q": place,
        "format": "jsonv2",
        "limit": 1,
        "addressdetails": 1,
    }
    # ország preferencia: nem mindig támogatja így, de segíthet
    # Nominatimnál a "countrycodes" működik
    params["countrycodes"] = ",".join(PREFERRED_COUNTRIES)

    headers = {"User-Agent": USER_AGENT}
    try:
        r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=25)
        _last_nom_call = time.time()
        if r.status_code != 200:
            cache[key] = {"ok": False, "status": r.status_code}
            return None

        data = r.json()
        if not data:
            cache[key] = {"ok": False, "status": 200, "empty": True}
            return None

        top = data[0]
        lon = float(top["lon"])
        lat = float(top["lat"])
        cache[key] = {"ok": True, "lon": lon, "lat": lat, "raw": top}
        return lon, lat, top
    except Exception as e:
        cache[key] = {"ok": False, "error": str(e)}
        return None


def _html_to_text(html: str) -> str:
    # minimál “HTML strip” – elég jó ISW-hez, később cserélhetjük BeautifulSoup-ra
    txt = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", html)
    txt = re.sub(r"(?is)<br\s*/?>", "\n", txt)
    txt = re.sub(r"(?is)</p\s*>", "\n", txt)
    txt = re.sub(r"(?is)<.*?>", " ", txt)
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{2,}", "\n", txt)
    return txt.strip()


def _split_sentences(text: str) -> List[str]:
    # egyszerű mondat-szétválasztó
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if len(p.strip()) > 20]


def _extract_places_from_sentence(sentence: str) -> List[str]:
    places = []
    for pat in PLACE_PATTERNS:
        for m in pat.finditer(sentence):
            # group 2 vagy 1
            cand = m.group(2) if m.lastindex and m.lastindex >= 2 else m.group(1)
            cand = cand.strip(" ,.;:()[]")
            cand = re.sub(r"\s{2,}", " ", cand)
            if len(cand) < 3:
                continue
            # stop words
            if cand in STOP_WORDS:
                continue
            # túl sok szó: valószínű zaj
            if len(cand.split()) > 6:
                continue
            places.append(cand)
    # uniq, sorrend megtartás
    out = []
    seen = set()
    for p in places:
        k = p.lower()
        if k not in seen:
            seen.add(k)
            out.append(p)
    return out


def _confidence(sentence: str, place: str) -> str:
    s = sentence.lower()
    # ha konkrét “in X” szerkezet van és drone/missile, akkor közepes/magas
    if re.search(rf"\bin\s+{re.escape(place.lower())}\b", s):
        return "high"
    if "near" in s or "around" in s or "over" in s:
        return "med"
    return "low"


def _find_latest_isw_article_url() -> Tuple[str, str]:
    """
    Returns (url, date_yyyy_mm_dd)
    Heurisztika: a "Russian Offensive Campaign Assessment" oldalról kikeressük az első backgrounder linket,
    és a cím/dátum alapján próbálunk dátumot olvasni.
    """
    r = requests.get(ISW_RUS_OFFENSIVE_PAGE, headers={"User-Agent": USER_AGENT}, timeout=25)
    r.raise_for_status()
    html = r.text

    # első olyan link, ami backgrounder és tartalmazza "russian-offensive-campaign-assessment"
    m = re.search(r'href="([^"]+russian-offensive-campaign-assessment[^"]+)"', html, re.IGNORECASE)
    if not m:
        # fallback: maga az oldal
        return ISW_RUS_OFFENSIVE_PAGE, datetime.now(timezone.utc).date().isoformat()

    href = m.group(1)
    url = href if href.startswith("http") else (ISW_ROOT + href)

    # dátum kinyerés: gyakran szerepel a címben "February 19, 2026" jelleggel
    # kicsit agresszív regex:
    # Month DD, YYYY
    m2 = re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b", html)
    if m2:
        month, day, year = m2.group(1), int(m2.group(2)), int(m2.group(3))
        dt = datetime.strptime(f"{year}-{month}-{day}", "%Y-%B-%d").date()
        return url, dt.isoformat()

    # ha nincs, akkor majd a cikkből próbáljuk
    return url, datetime.now(timezone.utc).date().isoformat()


def _try_extract_date_from_article(html: str) -> Optional[str]:
    # keresünk Month DD, YYYY formátumot
    m = re.search(r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),\s+(\d{4})\b", html)
    if not m:
        return None
    month, day, year = m.group(1), int(m.group(2)), int(m.group(3))
    dt = datetime.strptime(f"{year}-{month}-{day}", "%Y-%B-%d").date()
    return dt.isoformat()


def build_events_from_isw(url: str, date_hint: str) -> Tuple[str, List[ExtractedEvent]]:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=25)
    r.raise_for_status()
    html = r.text

    date_real = _try_extract_date_from_article(html) or date_hint
    text = _html_to_text(html)
    sentences = _split_sentences(text)

    events: List[ExtractedEvent] = []
    for s in sentences:
        if not UAV_RE.search(s):
            continue
        places = _extract_places_from_sentence(s)
        if not places:
            continue
        for p in places:
            events.append(ExtractedEvent(
                date=date_real,
                source_url=url,
                sentence=s[:600],
                place=p,
                tag="UAV/drone/missile",
                confidence=_confidence(s, p)
            ))

    # limitálás: ne robbanjon fel
    if len(events) > 200:
        events = events[:200]

    return date_real, events


def to_feature(lon: float, lat: float, ev: ExtractedEvent, geocode_raw: dict) -> dict:
    props = {
        "source": "ISW",
        "source_url": ev.source_url,
        "date": ev.date,
        "place": ev.place,
        "tag": ev.tag,
        "confidence": ev.confidence,
        "title": f"ISW: {ev.tag} – {ev.place}",
        "snippet": ev.sentence,
        "geocode": {
            "display_name": geocode_raw.get("display_name"),
            "type": geocode_raw.get("type"),
            "class": geocode_raw.get("class"),
        }
    }
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": props
    }


def write_geojson(path: Path, features: List[dict]) -> None:
    fc = {"type": "FeatureCollection", "features": features}
    path.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")


def main():
    cache = _load_cache()

    latest_url, date_hint = _find_latest_isw_article_url()
    print(f"[ISW] Latest candidate: {latest_url} (hint: {date_hint})")

    date_real, events = build_events_from_isw(latest_url, date_hint)
    print(f"[ISW] Extracted events: {len(events)} (date: {date_real})")

    features_latest = []
    for ev in events:
        geo = _nominatim_geocode(ev.place, cache)
        if not geo:
            continue
        lon, lat, raw = geo
        features_latest.append(to_feature(lon, lat, ev, raw))

    # latest
    write_geojson(OUT_LATEST, features_latest)

    # 7d / 30d: itt egyszerűen “latest cikkből” csináljuk
    # Később: több napi ISW cikket is be lehet húzni (RSS alapján).
    # Addig is legyen hasznos: a workflow napi fut, így a “latest” mindig friss.
    write_geojson(OUT_7D, features_latest)
    write_geojson(OUT_30D, features_latest)

    # meta
    meta = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source_latest_url": latest_url,
        "source_date": date_real,
        "points_latest": len(features_latest),
        "note": "Heuristic extraction from ISW daily assessment; not a full reproduction of the report.",
    }
    OUT_INDEX.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    _save_cache(cache)
    print(f"[OK] Wrote: {OUT_LATEST}, {OUT_7D}, {OUT_30D}, {OUT_INDEX}, cache={CACHE_PATH}")


if __name__ == "__main__":
    main()
