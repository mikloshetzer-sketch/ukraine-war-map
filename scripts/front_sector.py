# simple sector detection based on coordinates

SECTORS = [
    ("Kupiansk", 49.7, 37.6),
    ("Kreminna", 49.0, 38.2),
    ("Bakhmut", 48.6, 38.0),
    ("Avdiivka", 48.1, 37.7),
    ("Pokrovsk", 48.3, 37.2),
    ("Velyka Novosilka", 47.8, 36.8),
    ("Robotyne", 47.4, 35.8),
    ("Kherson", 46.7, 32.6)
]


def distance(lat1, lon1, lat2, lon2):
    return ((lat1-lat2)**2 + (lon1-lon2)**2) ** 0.5


def detect_sector(lon, lat):

    closest = None
    closest_dist = 999

    for name, s_lat, s_lon in SECTORS:

        d = distance(lat, lon, s_lat, s_lon)

        if d < closest_dist:
            closest = name
            closest_dist = d

    return closest
