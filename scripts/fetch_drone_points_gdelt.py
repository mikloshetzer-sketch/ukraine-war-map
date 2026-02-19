#!/usr/bin/env python3
"""
Fetch drone/UAV-related items from GDELT and write GeoJSON points.

Fontos célok:
- SOHA ne dobjon JSONDecodeError miatt piros workflow-t
- Ha nincs geokódolt koordináta, akkor is írjon érvényes (akár üres) GeoJSON-t
- Minimális, biztonságos mezők (title, url, date)

Kimenet:
  data/drones_latest.geojson
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests


OUT_PATH = Path("data/drones_latest.geojson")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def ymdhms(dt: datetime) -> str:
    # GDELT sok endpointnál ezt szereti: YYYYMMDDHHMMSS
    return dt.strftime("%Y%m%d%H%M%S")


def safe_json(resp: requests.Response) -> Optional[Any]:
    """
    JSON parse: ha nem JSON, visszaad None-t (és nem dob).
    """
    try:
        return resp.json()
    except Exception:
        return None


def ensure_out_dir() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)


def write_feature_collection(features: List[Dict[str, Any]]) -> None:
    ensure_out_dir()
    fc = {"type": "FeatureCollection", "features": features}
    OUT_PATH.write_text(json.dumps(fc, ensure_ascii=False, indent=2), encoding="utf-8")


def build_doc_query_url(query: str, start: datetime, end: datetime, maxrecords: int) -> str:
    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(maxrecords),
        "sort": "HybridRel",
        "startdatetime": ymdhms(start),
        "enddatetime": ymdhms(end),
    }
    # kézi querystring (requests úgyis megcsinálná, de itt átlátható)
    from urllib.parse import urlencode
    return base + "?" + urlencode(params)


def extract_point(obj: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    """
    Próbál koordinátát kinyerni több lehetséges mezőből.
    Ha nincs, None.
    """
    # gyakori variációk
    # 1) direct
    for lat_k, lon_k in [("lat", "lon"), ("latitude", "longitude"), ("Lat", "Lon")]:
        if lat_k in obj and lon_k in obj:
            try:
                lat = float(obj[lat_k])
                lon = float(obj[lon_k])
                return (lon, lat)
            except Exception:
                pass

    # 2) nested location / locations
    loc = obj.get("location") or obj.get("Location")
    if isinstance(loc, dict):
        for lat_k, lon_k in [("lat", "lon"), ("latitude", "longitude")]:
            if lat_k in loc and lon_k in loc:
                try:
                    lat = float(loc[lat_k])
                    lon = float(loc[lon_k])
                    return (lon, lat)
                except Exception:
                    pass

    locs = obj.get("locations") or obj.get("Locations")
    if isinstance(locs, list) and locs:
        for cand in locs:
            if isinstance(cand, dict):
                p = extract_point(cand)
                if p:
                    return p

    # 3) geo / geocode style
    geo = obj.get("geo") or obj.get("Geo") or obj.get("geocode") or obj.get("Geocode")
    if isinstance(geo, dict):
        for lat_k, lon_k in [("lat", "lon"), ("latitude", "longitude")]:
            if lat_k in geo and lon_k in geo:
                try:
                    lat = float(geo[lat_k])
                    lon = float(geo[lon_k])
                    return (lon, lat)
                except Exception:
                    pass

    return None


def to_feature(lon: float, lat: float, props: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": props,
    }


def main() -> int:
    lookback_days = int(os.environ.get("DRONE_LOOKBACK_DAYS", "7"))
    maxrecords = int(os.environ.get("DRONE_MAXRECORDS", "250"))

    # Query: szándékosan egyszerű, mert a túl bonyolult query-k gyakrabban hoznak nem várt választ
    query = os.environ.get(
        "DRONE_QUERY",
        '(drone OR drones OR UAV OR "unmanned aerial" OR "Shahed") (Ukraine OR Ukrainian OR Russia OR Russian)',
    )

    end = now_utc()
    start = end - timedelta(days=lookback_days)

    url = build_doc_query_url(query=query, start=start, end=end, maxrecords=maxrecords)

    try:
        r = requests.get(
            url,
            timeout=45,
            headers={
                "User-Agent": "ukraine-war-map-bot/1.0",
                "Accept": "application/json,text/plain,*/*",
            },
        )
    except Exception as e:
        # hálózati hiba: NE bukjon a workflow
        write_feature_collection([])
        print(f"[drone] network error -> wrote empty geojson: {e}")
        return 0

    if r.status_code != 200:
        write_feature_collection([])
        print(f"[drone] HTTP {r.status_code} -> wrote empty geojson")
        return 0

    data = safe_json(r)
    if data is None:
        # ez volt nálad a JSONDecodeError oka: HTML / üres válasz / rate-limit text
        write_feature_collection([])
        snippet = (r.text or "")[:200].replace("\n", " ")
        print(f"[drone] non-JSON response -> wrote empty geojson. Snippet: {snippet}")
        return 0

    # GDELT DOC 2.1 tipikusan: {"articles":[...], ...}
    articles = []
    if isinstance(data, dict):
        if isinstance(data.get("articles"), list):
            articles = data["articles"]
        elif isinstance(data.get("data"), list):
            articles = data["data"]

    features: List[Dict[str, Any]] = []

    for a in articles:
        if not isinstance(a, dict):
            continue

        pt = extract_point(a)
        if not pt:
            # nincs koordináta -> kihagyjuk (különben hibás pont lenne)
            continue

        lon, lat = pt

        title = a.get("title") or a.get("Title") or "Drone/UAV (GDELT)"
        link = a.get("url") or a.get("URL") or a.get("shareImage") or ""
        date = a.get("seendate") or a.get("seenDate") or a.get("date") or ""

        props = {
            "title": str(title)[:280],
            "url": str(link),
            "date": str(date),
            "source": "GDELT DOC 2.1",
            "query": query,
        }
        features.append(to_feature(lon, lat, props))

    write_feature_collection(features)
    print(f"[drone] wrote {len(features)} features to {OUT_PATH.as_posix()} (lookback_days={lookback_days})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
