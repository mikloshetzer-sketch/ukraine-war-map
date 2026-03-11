#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional

import requests

IN_FILE = Path("data/ua_war_events_filtered.json")
OUT_FILE = Path("data/ua_war_events_latest.geojson")
CACHE_FILE = Path("data/geocode_cache_ua.json")

HEADERS = {
    "User-Agent": "ukraine-war-map/1.0 (research use)"
}

UKRAINE_BOUNDS = "22.0,44.0,41.0,53.0"  # west,south,east,north approx


def load_cache() -> dict:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict) -> None:
    CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def geocode_place(place: str, cache: dict) -> Optional[dict]:
    key = place.strip().lower()
    if key in cache:
        return cache[key]

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{place}, Ukraine",
        "format": "jsonv2",
        "limit": 1,
        "bounded": 1,
        "viewbox": UKRAINE_BOUNDS,
    }

    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception:
        cache[key] = None
        return None

    if not data:
        cache[key] = None
        return None

    first = data[0]
    result = {
        "lat": float(first["lat"]),
        "lon": float(first["lon"]),
        "display_name": first.get("display_name"),
    }
    cache[key] = result
    time.sleep(1.1)  # Nominatim rate limit
    return result


def event_to_feature(event: dict, coords: dict, matched_place: str) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [coords["lon"], coords["lat"]],
        },
        "properties": {
            "id": event["id"],
            "source": event["source"],
            "channel": event["channel"],
            "kind": event["kind"],
            "title": event["title"],
            "url": event["url"],
            "published_at": event["published_at"],
            "event_type": event["event_type"],
            "confidence": event["confidence"],
            "sector_hints": event["sector_hints"],
            "place_candidates": event["place_candidates"],
            "matched_place": matched_place,
            "display_name": coords.get("display_name"),
            "tags": event["tags"],
            "text": event["text"],
        },
    }


def main() -> int:
    if not IN_FILE.exists():
        raise FileNotFoundError(f"Missing input: {IN_FILE}")

    raw = json.loads(IN_FILE.read_text(encoding="utf-8"))
    events = raw.get("items", [])
    cache = load_cache()

    features = []

    for event in events:
        coords = None
        matched_place = None

        for place in event.get("place_candidates", []):
            result = geocode_place(place, cache)
            if result:
                coords = result
                matched_place = place
                break

        if coords:
            features.append(event_to_feature(event, coords, matched_place))

    geojson = {
        "type": "FeatureCollection",
        "features": features,
    }

    OUT_FILE.write_text(json.dumps(geojson, ensure_ascii=False, indent=2), encoding="utf-8")
    save_cache(cache)

    print(f"[OK] wrote {OUT_FILE} ({len(features)} features)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
