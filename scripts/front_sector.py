# scripts/front_sector.py

from __future__ import annotations

SECTORS = [
    ("Kupiansk", 49.710, 37.615),
    ("Kreminna", 49.049, 38.217),
    ("Bakhmut", 48.595, 37.999),
    ("Avdiivka", 48.139, 37.742),
    ("Pokrovsk", 48.282, 37.181),
    ("Velyka Novosilka", 47.846, 36.835),
    ("Robotyne", 47.443, 35.841),
    ("Kherson", 46.635, 32.617),
]


def distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return ((lat1 - lat2) ** 2 + (lon1 - lon2) ** 2) ** 0.5


def detect_sector(lon: float, lat: float) -> str:
    closest_name = "Unknown sector"
    closest_dist = float("inf")

    for name, sector_lat, sector_lon in SECTORS:
        d = distance(lat, lon, sector_lat, sector_lon)
        if d < closest_dist:
            closest_name = name
            closest_dist = d

    return closest_name
