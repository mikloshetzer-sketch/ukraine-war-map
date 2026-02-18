import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

OUT = Path("data/drones_latest.geojson")

# GDELT 2.1 EVENTS API (CSV output) - sokkal jobb geo (ActionGeo_*)
EVENTS = "https://api.gdeltproject.org/api/v2/events/events"

# Drónos kulcsszavak a Events "query" szűrőjéhez
# (Events-ben "query" a cikkekben/említésekben szereplő szöveg alapján szűr)
QUERY = (
    '(drone OR drones OR uav OR uavs OR "fpv drone" OR quadcopter OR shahed OR "loitering munition") '
    'AND (ukraine OR ukrainian OR russia OR russian)'
)

# Ukrán országkód szűrés (FIPS-10-4): UP = Ukraine
# (GDELT-ben a COUNTRY mező sokszor így jelenik meg az actiongeo oldalon)
UA_FIPS = "UP"

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
        "mode": "EventList",
        "format": "csv",          # CSV könnyen parse-olható
        "maxrecords": str(max_records),
        "sort": "HybridRel",
        "startdatetime": start_dt.strftime("%Y%m%d%H%M%S"),
    }

    r = requests.get(EVENTS, params=params, timeout=120)
    r.raise_for_status()
    text = r.text

    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        # nincs adat / API nem adott vissza semmit
        OUT.write_text(json.dumps({"type": "FeatureCollection", "features": []}, indent=2), encoding="utf-8")
        print("No rows returned from GDELT Events.")
        return

    header = lines[0].split(",")
    idx = {name: i for i, name in enumerate(header)}

    # szükséges oszlopok (Events export standard mezők)
    need = [
        "GLOBALEVENTID",
        "SQLDATE",
        "Actor1Name",
        "Actor2Name",
        "EventCode",
        "EventBaseCode",
        "EventRootCode",
        "GoldsteinScale",
        "NumMentions",
        "NumSources",
        "NumArticles",
        "ActionGeo_FullName",
        "ActionGeo_CountryCode",
        "ActionGeo_Lat",
        "ActionGeo_Long",
    ]

    # ha eltér a header (ritka), akkor csak azt használjuk ami megvan
    def col(row, name):
        i = idx.get(name)
        if i is None or i >= len(row):
            return ""
        return row[i].strip()

    features = []
    used = set()

    # egyszerű CSV parse (GDELT CSV nem mindig idézőjelez stabilan, de az EventList általában oké)
    for ln in lines[1:]:
        row = ln.split(",")
        # minimál védelem
        if len(row) < len(header) * 0.6:
            continue

        lat = safe_float(col(row, "ActionGeo_Lat"))
        lon = safe_float(col(row, "ActionGeo_Long"))
        if lat is None or lon is None:
            continue

        # Ukrán szűrés (ha csak Ukrajnát akarod)
        ccode = col(row, "ActionGeo_CountryCode")
        if ccode and ccode != UA_FIPS:
            continue

        evid = col(row, "GLOBALEVENTID")
        if not evid:
            evid = f"{lat:.4f},{lon:.4f}"

        # duplikáció csökkentése
        key = (evid, round(lat, 4), round(lon, 4))
        if key in used:
            continue
        used.add(key)

        # dátum (SQLDATE: YYYYMMDD)
        sqld = col(row, "SQLDATE")
        if len(sqld) == 8:
            seendate = f"{sqld[0:4]}-{sqld[4:6]}-{sqld[6:8]}"
        else:
            seendate = sqld

        props = {
            "type": "drone_event",
            "gdelt_event_id": evid,
            "date": seendate,
            "place": col(row, "ActionGeo_FullName"),
            "actor1": col(row, "Actor1Name"),
            "actor2": col(row, "Actor2Name"),
            "event_code": col(row, "EventCode"),
            "event_base": col(row, "EventBaseCode"),
            "event_root": col(row, "EventRootCode"),
            "goldstein": col(row, "GoldsteinScale"),
            "mentions": col(row, "NumMentions"),
            "sources": col(row, "NumSources"),
            "articles": col(row, "NumArticles"),
        }

        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
        })

    OUT.write_text(json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False, indent=2),
                   encoding="utf-8")

    print(f"Wrote {OUT} with {len(features)} features (lookback_days={days}, maxrecords={max_records})")

if __name__ == "__main__":
    main()
