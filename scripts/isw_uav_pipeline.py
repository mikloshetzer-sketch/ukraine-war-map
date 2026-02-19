#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ISW UAV / missile pipeline
Valódi napi ISW ROC cikkekből szed eseményeket.

Output:
- data/isw_uav_latest.geojson
- data/isw_uav_7d.geojson
- data/isw_uav_30d.geojson
- data/isw_uav_index.json
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Ukraine-War-Map research bot)"
}

# proxy fallback (ISW néha 403)
def fetch_url(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return r.text
    except:
        pass

    # fallback proxy
    try:
        proxy = "https://r.jina.ai/" + url
        r = requests.get(proxy, timeout=25)
        if r.status_code == 200:
            return r.text
    except:
        pass

    return None


# =========================
# STEP 1 — listázd ROC cikkeket
# =========================

ROC_UPDATES_URL = "https://understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-updates-2"

def collect_recent_article_links(limit=40):
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
# STEP 2 — kulcsszó keresés
# =========================

KEYWORDS = [
    "drone", "uav", "missile", "shahed", "strike",
    "loitering munition", "airstrike", "rocket"
]

def extract_events(article_url):

    html = fetch_url(article_url)
    if not html:
        return []

    text = re.sub("<[^<]+?>", " ", html)
    text = re.sub(r"\s+", " ", text)

    # dátum URL-ből
    date_match = re.search(r'(\w+-\d{1,2}-\d{4})', article_url)
    if date_match:
        try:
            date = datetime.datetime.strptime(date_match.group(1), "%B-%d-%Y").date()
        except:
            date = datetime.date.today()
    else:
        date = datetime.date.today()

    events = []

    for sentence in re.split(r'\. ', text):
        lower = sentence.lower()
        if any(k in lower for k in KEYWORDS):

            place = None

            # próbáljunk települést kivenni
            m = re.search(r'(in|near|around)\s+([A-Z][a-zA-Z\-]+)', sentence)
            if m:
                place = m.group(2)

            events.append({
                "date": str(date),
                "text": sentence[:300],
                "place": place,
                "source_url": article_url
            })

    return events


# =========================
# STEP 3 — geokód (Nominatim)
# =========================

GEOCODE_CACHE = OUT_DIR / "geocode_cache.json"

if GEOCODE_CACHE.exists():
    cache = json.loads(GEOCODE_CACHE.read_text())
else:
    cache = {}

def geocode(place):

    if not place:
        return None

    if place in cache:
        return cache[place]

    try:
        url = f"https://nominatim.openstreetmap.org/search?format=json&q={place}"
        r = requests.get(url, headers=HEADERS, timeout=20)
        data = r.json()
        if data:
            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])
            cache[place] = [lon, lat]
            time.sleep(1)
            return [lon, lat]
    except:
        pass

    return None


# =========================
# STEP 4 — GeoJSON
# =========================

def events_to_geojson(events):

    features = []

    for e in events:
        coords = geocode(e["place"])
        if not coords:
            continue

        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": coords
            },
            "properties": {
                "source": "ISW",
                "date": e["date"],
                "title": "ISW UAV/missile",
                "place": e["place"],
                "snippet": e["text"],
                "url": e["source_url"]
            }
        })

    return {
        "type": "FeatureCollection",
        "features": features
    }


# =========================
# MAIN
# =========================

def main():

    print("ISW UAV pipeline indul…")

    links = collect_recent_article_links()

    print("Talált cikkek:", len(links))

    all_events = []

    for url in links:
        ev = extract_events(url)
        all_events.extend(ev)

    print("Talált események:", len(all_events))

    # dátum szerint
    today = datetime.date.today()
    last7 = today - datetime.timedelta(days=7)
    last30 = today - datetime.timedelta(days=30)

    ev_latest = all_events[:40]
    ev_7 = [e for e in all_events if datetime.date.fromisoformat(e["date"]) >= last7]
    ev_30 = [e for e in all_events if datetime.date.fromisoformat(e["date"]) >= last30]

    OUT_DIR.joinpath("isw_uav_latest.geojson").write_text(
        json.dumps(events_to_geojson(ev_latest), indent=2)
    )

    OUT_DIR.joinpath("isw_uav_7d.geojson").write_text(
        json.dumps(events_to_geojson(ev_7), indent=2)
    )

    OUT_DIR.joinpath("isw_uav_30d.geojson").write_text(
        json.dumps(events_to_geojson(ev_30), indent=2)
    )

    OUT_DIR.joinpath("isw_uav_index.json").write_text(
        json.dumps({
            "generated_utc": datetime.datetime.utcnow().isoformat(),
            "events_total": len(all_events),
            "events_7d": len(ev_7),
            "events_30d": len(ev_30)
        }, indent=2)
    )

    GEOCODE_CACHE.write_text(json.dumps(cache, indent=2))

    print("ISW UAV pipeline kész ✔")


if __name__ == "__main__":
    main()
