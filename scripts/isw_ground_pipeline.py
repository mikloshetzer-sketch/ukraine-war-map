#!/usr/bin/env python3
# -*- kódolás: utf-8 -*-

"""
ISW földi rohamvezeték
Földi támadások / előretörések ISW ROC cikkekből
"""

import újra
json importálása
importálási idő
dátum/idő importálása
a pathlib import Path-ból
importálási kérelmek

OUT_DIR = Path("adatok")
OUT_DIR.mkdir(létezik_ok=Igaz)

FEJLÉKEK = {
„User-Agent”: „Mozilla/5.0 (Ukrajna-Háborús-Térkép kutatóbot)”
}

ROC_UPDATES_URL = " https://understandingwar.org/research/russia-ukraine/russian-offensive-campaign-assessment-updates-2 "

KULCSSZAVAK = [
"támadás",
"gépesített támadás",
"ellentámadás",
"áttörés",
"fejlett",
„tolta”,
„lefoglalták”,
„elfogott”,
„eltaszított”,
"támadó hadművelet"
]

# =============================
# FETCH
# =============================

def fetch_url(url):
megpróbál:
r = requests.get(url, fejlécek=FEJLÉKEK, időtúllépés=20)
ha r.állapotkód == 200:
return r.text
kivéve:
átmegy

# proxy tartalék
megpróbál:
proxy = " https://r.jina.ai/ " + url
r = kérések.get(proxy, időtúllépés=25)
ha r.állapotkód == 200:
return r.text
kivéve:
átmegy

vissza Nincs


def gyűjtés_legutóbbi_cikk_linkek(korlát=40):
html = fetch_url(ROC_FRISSÍTÉSEK_URL)
ha nem html:
vissza []

linkek = set()

m karakterlánc a re.findall(r'href="([^"]*russian-offensive-campaign-assessment[^"]*)"', html fájlban):
ha a „kutatás” az „m” betűben van:
ha nem m.startswith("http"):
m = " https://understandingwar.org " + m
linkek.hozzáadás(m)

return rendezve(linkek, fordított=Igaz)[:korlát]


# =============================
# ESEMÉNYEK KIVONÁSA
# =============================

def extract_events(cikk_url):

html = fetch_url(cikk_url)
ha nem html:
vissza []

szöveg = re.sub("<[^<]+?>", " ", html)
szöveg = re.sub(r"\s+", " ", szöveg)

date_match = re.search(r'(\w+-\d{1,2}-\d{4})', cikk_url)
ha dátum_egyezés:
megpróbál:
dátum = datetime.datetime.strptime(date_match.group(1), "%B-%d-%Y").date()
kivéve:
dátum = dátum/idő.dátum.ma()
más:
dátum = dátum/idő.dátum.ma()

események = []

a re.split(r'\. ', text) függvényben található mondathoz:
alsó = mondat.alsó()
ha van ilyen (k az alsó értékben, ha k a KULCSSZAVAKBAN van):

hely = Nincs
m = re.search(r'(bent|közelben|körül)\s+([AZ][a-zA-Z\-]+)', mondat)
ha m:
hely = m.csoport(2)

események.append({
"dátum": str(dátum),
"szöveg": mondat[:300],
"hely": hely,
"forrás_url": cikk_url
})

visszatérési események


# =============================
# GEOKÓD
# =============================

CACHE_FILE = KIMENETI_KÖNYVTÁR / "geocode_cache.json"

ha CACHE_FILE.exists():
gyorsítótár = json.loads(CACHE_FILE.read_text())
más:
gyorsítótár = {}

def geocode(hely):

ha nem hely:
vissza Nincs

ha a gyorsítótárban van:
return cache[hely]

megpróbál:
url = f" https://nominatim.openstreetmap.org/search?format=json&q={place} "
r = requests.get(url, fejlécek=FEJLÉKEK, időtúllépés=20)
adatok = r.json()
ha adatok:
lat = float(adatok[0]["lat"])
lon = float(adatok[0]["lon"])
gyorsítótár[hely] = [hosszúság, szélesség]
idő.alvás(1)
visszatérés [hosszúság, szélesség]
kivéve:
átmegy

vissza Nincs


# =============================
# GEOJSON ÉPÍTÉSE
# =============================

def events_to_geojson(events):

jellemzők = []

e esetén eseményekben:
koordináták = geokód(e["hely"])
ha nem koordináták:
folytatás

jellemzők.append({
"típus": "Jellemző",
"geometria": {
"típus": "Pont",
"koordináták": koordináták
},
"tulajdonságok": {
"forrás": "ISW",
"kategória": "talaj",
"dátum": e["dátum"],
"title": "ISW földi támadás",
"hely": e["hely"],
"részlet": e["szöveg"],
"url": e["forrás_url"]
}
})

visszatérés {
"type": "Jellemzőgyűjtemény",
„jellemzők”: jellemzők
}


# =============================
# FŐ
# =============================

def main():

print("ISW földi csővezeték indul…")

linkek = legutolsó_cikk_linkek_gyűjtése()
minden_esemény = []

linkekben található URL-hez:
ev = események_kivonása(url)
minden_esemény.kiterjesztés(esemény)

ma = dátum/idő.dátum.ma()
utolsó7 = ma - datetime.timedelta(napok=7)
utolsó30 = ma - dátum/idő.idődelta(napok=30)

ev_latest = összes_esemény[:40]
ev_7 = [e az összes_eseményben szereplő e értékhez, ha datetime.date.fromisoformat(e["date"]) >= last7]
ev_30 = [e az összes_eseményben szereplő e értékhez, ha datetime.date.fromisoformat(e["date"]) >= last30]

OUT_DIR.joinpath("isw_ground_latest.geojson").write_text(
json.dumps(events_to_geojson(ev_latest), behúzás=2)
)

OUT_DIR.joinpath("isw_ground_7d.geojson").write_text(
json.dumps(events_to_geojson(ev_7), behúzás=2)
)

OUT_DIR.joinpath("isw_ground_30d.geojson").write_text(
json.dumps(events_to_geojson(ev_30), behúzás=2)
)

OUT_DIR.joinpath("isw_ground_index.json").write_text(
json.dumps({
"generated_utc": datetime.datetime.utcnow().isoformat(),
"események_összesen": len(összes_esemény),
"events_7d": len(ev_7),
"events_30d": len(ev_30)
}, behúzás=2)
)

CACHE_FILE.write_text(json.dumps(gyorsítótár, behúzás=2))

print("ISW Ground pipeline kész ")


ha __név__ == "__main__":
fő()
