import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

OUT = Path("data/drones_latest.geojson")

# Egyszerű, "drón témájú" GDELT 2.1 DOC API lekérdezés.
# Nem csapatmozgás, csak híralapú pontok (cikkek alapján).
# Megjegyzés: a GDELT nem ad mindig pontos koordinátát; ahol nincs, az kimarad.

GDELT_DOC = "https://api.gdeltproject.org/api/v2/doc/doc"

# Kulcsszavak: ezekkel indulunk (finomítható később)
QUERY = (
    '(ukraine OR ukrainian OR russia OR russian) AND '
    '(drone OR drones OR uav OR uavs OR "fpv drone" OR quadcopter OR "loitering munition" OR shahed)'
)

# Földrajzi szűkítés: Ukraine környéke (nagy doboz)
# GDELT doc API-nál nincs klasszik bbox filter mindenhol stabilan,
# ezért csak "ukraine" kulcsszóval + nyelvi/tematikus szűréssel dolgozunk.
# Később lehet GKG/Events API-ra váltani, ott jobb a geo.

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)

    # utolsó N nap (alap: 7) - workflowból lehet env-vel állítani
    days = int(os.getenv("DRONE_LOOKBACK_DAYS", "7"))
    start_dt = datetime.now(timezone.utc) - timedelta(days=days)

    params = {
        "query": QUERY,
        "mode": "ArtList",
        "format": "json",
        "maxrecords": "250",
        "sort": "HybridRel",
        # startdatetime: YYYYMMDDHHMMSS
        "startdatetime": start_dt.strftime("%Y%m%d%H%M%S"),
    }

    r = requests.get(GDELT_DOC, params=params, timeout=120)
    r.raise_for_status()
    data = r.json()

    arts = data.get("articles", []) or []

    features = []
    used = set()

    for a in arts:
        # GDELT doc-ban előfordul: "sourceCountry", "locations", "location" stb.
        # Leggyakrabban a "sourceCountry" nem geo. Nekünk lat/lon kell.
        lat = safe_float(a.get("lat"))
        lon = safe_float(a.get("lon"))

        if lat is None or lon is None:
            # próbáljuk locations listából
            locs = a.get("locations") or []
            got = False
            for loc in locs:
                lat2 = safe_float(loc.get("lat"))
                lon2 = safe_float(loc.get("lon"))
                if lat2 is not None and lon2 is not None:
                    lat, lon = lat2, lon2
                    got = True
                    break
            if not got:
                continue

        # duplikációk ellen
        key = (round(lat, 4), round(lon, 4), (a.get("url") or "")[:120])
        if key in used:
            continue
        used.add(key)

        title = a.get("title") or ""
        url = a.get("url") or ""
        seendate = a.get("seendate") or a.get("datetime") or ""

        props = {
            "title": title[:300],
            "url": url,
            "seendate": seendate,
            "source": a.get("sourceCountry") or a.get("source") or "",
            "type": "drone_news_point",
        }

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        })

    out = {"type": "FeatureCollection", "features": features}
    OUT.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {OUT} with {len(features)} features (lookback_days={days})")

if __name__ == "__main__":
    main()
