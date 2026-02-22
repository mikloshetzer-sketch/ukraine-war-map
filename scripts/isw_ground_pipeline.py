#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ISW ground operations pipeline (ground assaults / advances / counterattacks).

Output:
- data/isw_ground_latest.geojson
- data/isw_ground_7d.geojson
- data/isw_ground_30d.geojson
- data/isw_ground_index.json

Megjegyzés:
- Alapból pontokat rak (place alapján).
- Ha felismer "from <A> to/toward <B>" mintát, akkor vonalat (LineString) is készít.
"""

import re
import json
import time
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
    "contact: github actions)"
)

HEADERS = {
    "User-Agent": UA
}

ROC_UPDATES_URL = "https://understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-updates-2"

# ISW néha 403 — proxy fallback
def fetch_url(url: str) -> str | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass

    try:
        proxy = "https://r.jina.ai/" + url
        r = requests.get(proxy, timeout=25)
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

    # ROC oldal általában dátum szerinti URL-eket ad — reverse sort jó közelítés
    links = sorted(links, reverse=True)
    return links[:limit]


# =========================
# STEP 2 — kulcsszó + esemény kinyerés
# =========================

# Földi műveletekhez tipikus igék/kifejezések (angol, mert ISW cikk)
GROUND_KEYWORDS = [
    "assault", "attacked", "attack", "offensive",
    "advanced", "advance", "made gains", "gains",
    "seized", "captured", "took", "recaptured",
    "pushed", "pushing", "breached", "breakthrough",
    "counterattack", "counter-attacked", "counteroffensive",
    "repelled", "repulse", "withdrew", "withdrawal",
    "cleared", "secured",
]

# egyszerű “hely” kinyerés (in/near/around/toward)
PLACE_PATTERNS = [
    re.compile(r"\b(in|near|around|outside|south of|north of|east of|west of)\s+([A-Z][A-Za-z0-9\-\']+)", re.IGNORECASE),
    re.compile(r"\b(toward|towards)\s+([A-Z][A-Za-z0-9\-\']+)", re.IGNORECASE),
]

# mozgásvonal: "from A to/toward B"
MOVE_PATTERN = re.compile(
    r"\bfrom\s+([A-Z][A-Za-z0-9\-\']+)\s+(?:to|toward|towards)\s+([A-Z][A-Za-z0-9\-\']+)",
    re.IGNORECASE
)

def strip_html_to_text(html: str) -> str:
    text = re.sub("<[^<]+?>", " ", html)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def infer_date_from_url(article_url: str) -> datetime.date:
    # ISW URL-ben: Month-dd-YYYY
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

    # mondatok (durva, de elég)
    sentences = re.split(r"\.\s+", text)

    events: list[dict] = []

    for s in sentences:
        s = s.strip()
        if len(s) < 40:
            continue

        lower = s.lower()
        if not any(k in lower for k in GROUND_KEYWORDS):
            continue

        # mozgás: from A to B
        move = MOVE_PATTERN.search(s)
        if move:
            a = move.group(1)
            b = move.group(2)
            events.append({
                "date": str(date),
                "kind": "movement",
                "from_place": a,
                "to_place": b,
                "place": b,  # fallback: célpontra is rá lehet tenni pontként
                "text": s[:350],
                "source_url": article_url
            })
            continue

        # sima esemény ponttal
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
# STEP 3 — geokód (Nominatim)
# =========================

GEOCODE_CACHE = OUT_DIR / "geocode_cache.json"
if GEOCODE_CACHE.exists():
    cache = json.loads(GEOCODE_CACHE.read_text(encoding="utf-8"))
else:
    cache = {}

def geocode(place: str | None) -> list[float] | None:
    if not place:
        return None

    key = place.strip()
    if not key:
        return None

    if key in cache:
        return cache[key]

    # opcionális szűkítés: Ukraine / Russia — de ez néha árt (pl. “Belgorod” ok, “Donetsk” több)
    # ezért most simán q=place
    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&q={requests.utils.quote(key)}"
        r = requests.get(url, headers=HEADERS, timeout=20)
        data = r.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            cache[key] = [lon, lat]
            time.sleep(1)  # Nominatim rate limit barát
            return [lon, lat]
    except Exception:
        pass

    return None


# =========================
# STEP 4 — GeoJSON
# =========================

def events_to_geojson(events: list[dict]) -> dict:
    features: list[dict] = []

    for e in events:
        kind = e.get("kind")

        if kind == "movement":
            a = e.get("from_place")
            b = e.get("to_place")
            ca = geocode(a)
            cb = geocode(b)
            if ca and cb:
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "LineString",
                        "coordinates": [ca, cb]
                    },
                    "properties": {
                        "source": "ISW",
                        "date": e["date"],
                        "title": "ISW ground movement",
                        "from": a,
                        "to": b,
                        "snippet": e["text"],
                        "url": e["source_url"]
                    }
                })
            else:
                # fallback: pont a cél településre, ha az megvan
                if cb:
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": cb},
                        "properties": {
                            "source": "ISW",
                            "date": e["date"],
                            "title": "ISW ground event (fallback)",
                            "place": b,
                            "snippet": e["text"],
                            "url": e["source_url"]
                        }
                    })
            continue

        # sima ground pont
        coords = geocode(e.get("place"))
        if not coords:
            continue

        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": coords},
            "properties": {
                "source": "ISW",
                "date": e["date"],
                "title": "ISW ground operation",
                "place": e.get("place"),
                "snippet": e["text"],
                "url": e["source_url"]
            }
        })

    return {"type": "FeatureCollection", "features": features}


# =========================
# MAIN
# =========================

def main():
    print("ISW GROUND pipeline indul…")

    links = collect_recent_article_links(limit=40)
    print("Talált cikkek:", len(links))

    all_events: list[dict] = []
    for url in links:
        ev = extract_events(url)
        all_events.extend(ev)

    print("Talált események (nyers):", len(all_events))

    # időszűrés
    today = datetime.date.today()
    last7 = today - datetime.timedelta(days=7)
    last30 = today - datetime.timedelta(days=30)

    # “latest” = legfrissebb 60 mondat-alapú event
    ev_latest = all_events[:60]
    ev_7 = [e for e in all_events if datetime.date.fromisoformat(e["date"]) >= last7]
    ev_30 = [e for e in all_events if datetime.date.fromisoformat(e["date"]) >= last30]

    OUT_DIR.joinpath("isw_ground_latest.geojson").write_text(
        json.dumps(events_to_geojson(ev_latest), indent=2),
        encoding="utf-8"
    )
    OUT_DIR.joinpath("isw_ground_7d.geojson").write_text(
        json.dumps(events_to_geojson(ev_7), indent=2),
        encoding="utf-8"
    )
    OUT_DIR.joinpath("isw_ground_30d.geojson").write_text(
        json.dumps(events_to_geojson(ev_30), indent=2),
        encoding="utf-8"
    )

    OUT_DIR.joinpath("isw_ground_index.json").write_text(
        json.dumps({
            "generated_utc": datetime.datetime.utcnow().isoformat(),
            "events_total_raw": len(all_events),
            "events_7d_raw": len(ev_7),
            "events_30d_raw": len(ev_30)
        }, indent=2),
        encoding="utf-8"
    )

    GEOCODE_CACHE.write_text(json.dumps(cache, indent=2), encoding="utf-8")

    print("ISW GROUND pipeline kész ✔")


if __name__ == "__main__":
    main()
