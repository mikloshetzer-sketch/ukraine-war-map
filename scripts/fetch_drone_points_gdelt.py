import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

OUT = Path("data/drones_latest.geojson")

GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

QUERY = (
    '(drone OR drones OR uav OR uavs OR "fpv drone" OR quadcopter OR shahed OR "loitering munition") '
    'AND (ukraine OR ukrainian OR russia OR russian)'
)

# Fallback: ha nincs koordináta, tegyük UA közepére (Kijev környéke)
UA_FALLBACK_LON = 30.5234
UA_FALLBACK_LAT = 50.4501


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    days = int(os.getenv("DRONE_LOOKBACK_DAYS", "7"))
    max_records = int(os.getenv("DRONE_MAXRECORDS", "250"))

    start_dt = datetime.now(timezone.utc) - timedelta(days=days)

    params = {
        "query": QUERY,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max_records),
        "sort": "HybridRel",
        "startdatetime": start_dt.strftime("%Y%m%d%H%M%S"),
    }

    r = requests.get(GDELT_DOC, params=params, timeout=120)
    r.raise_for_status()
    data = r.json()

    arts = data.get("articles", []) or []

    features = []
    used = set()

    with_geo = 0
    fallback = 0

    for a in arts:
        title = (a.get("title") or "")[:300]
        url = a.get("url") or ""
        seendate = a.get("seendate") or a.get("datetime") or ""

        lat = safe_float(a.get("lat"))
        lon = safe_float(a.get("lon"))

        if lat is None or lon is None:
            # próbáljuk locations-ből
            locs = a.get("locations") or []
            got = False
            for loc in locs:
                lat2 = safe_float(loc.get("lat"))
                lon2 = safe_float(loc.get("lon"))
                if lat2 is not None and lon2 is not None:
                    lat, lon = lat2, lon2
                    got = True
                    break
            if got:
                with_geo += 1
            else:
                # fallback UA közép
                lat, lon = UA_FALLBACK_LAT, UA_FALLBACK_LON
                fallback += 1
        else:
            with_geo += 1

        key = (round(lat, 4), round(lon, 4), url[:120])
        if key in used:
            continue
        used.add(key)

        props = {
            "type": "drone_news_point",
            "title": title,
            "url": url,
            "seendate": seendate,
            "has_exact_geo": (lat != UA_FALLBACK_LAT or lon != UA_FALLBACK_LON),
        }

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        })

    OUT.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"Wrote {OUT} with {len(features)} features (exact_geo={with_geo}, fallback={fallback})")


if __name__ == "__main__":
    main()
