#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Fetch war-related items from:
  Web:
    - General Staff of Ukraine (Telegram public mirror)
    - Ukrinform
    - RBC-Ukraine
  Telegram public channels:
    - GeneralStaffZSU
    - DeepStateUA
    - UkraineNow

Output:
  - data/ua_war_sources_latest.json
  - data/ua_war_sources_latest.geo_candidates.json

Notes:
  - No Telegram API key required; uses public /s/ pages.
  - This script collects normalized source items, not final verified battle events.
  - Geocoding is intentionally left for a downstream step.
"""

from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable, Optional

import requests
from bs4 import BeautifulSoup

OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_JSON = OUT_DIR / "ua_war_sources_latest.json"
OUT_GEO_CANDIDATES = OUT_DIR / "ua_war_sources_latest.geo_candidates.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    )
}

WEB_SOURCES = [
    {
        "name": "ukrinform",
        "kind": "web",
        "url": "https://www.ukrinform.net/rubric-ato/",
    },
    {
        "name": "rbc_ukraine",
        "kind": "web",
        "url": "https://newsukraine.rbc.ua/tags/war-in-ukraine",
    },
    # General Staff handled via Telegram mirror because it is more scrapeable
]

TELEGRAM_CHANNELS = [
    {
        "name": "general_staff_tg",
        "kind": "telegram",
        "channel": "GeneralStaffZSU",
        "url": "https://t.me/s/GeneralStaffZSU",
    },
    {
        "name": "deepstate_tg",
        "kind": "telegram",
        "channel": "DeepStateUA",
        "url": "https://t.me/s/DeepStateUA",
    },
    {
        "name": "ukrainenow_tg",
        "kind": "telegram",
        "channel": "UkraineNow",
        "url": "https://t.me/s/UkraineNow",
    },
]

WAR_KEYWORDS = [
    "frontline", "combat clashes", "battle", "battles", "attack", "attacks",
    "drone", "uav", "missile", "shelling", "airstrike", "front", "sector",
    "покровськ", "покровск", "покровськ", "kupiansk", "lyman", "bakhmut",
    "avdiivka", "pokrovsk", "kherson", "zaporizhzhia", "huliaipole",
    "гш", "генштаб", "оперативна інформація", "боєзіткнення", "фронт",
    "обстріл", "удар", "дрон", "шахед", "ракет", "авіаудар", "напрямок",
    "покровський", "куп’янський", "лиманський", "бахмутський",
]

SECTOR_HINTS = [
    "Kupiansk", "Kreminna", "Lyman", "Siversk", "Bakhmut", "Toretsk",
    "Kostiantynivka", "Avdiivka", "Pokrovsk", "Kurakhove", "Velyka Novosilka",
    "Huliaipole", "Robotyne", "Orikhiv", "Kherson", "Dnipro", "Zaporizhzhia",
    "Куп'янськ", "Кремінна", "Лиман", "Сіверськ", "Бахмут", "Торецьк",
    "Костянтинівка", "Авдіївка", "Покровськ", "Курахове", "Велика Новосілка",
    "Гуляйполе", "Роботине", "Оріхів", "Херсон", "Запоріжжя",
]

STOPWORDS = {
    "Ukraine", "Ukrainian", "Russia", "Russian", "Kyiv", "Moscow",
    "Україна", "Росія", "Київ", "Москва", "ЗСУ", "РФ"
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


@dataclass
class Item:
    source: str
    kind: str  # web | telegram
    channel: Optional[str]
    title: str
    url: str
    published_at: Optional[str]
    text: str
    tags: list[str]


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text


def parse_date_maybe(value: str) -> Optional[str]:
    if not value:
        return None

    value = value.strip()

    # ISO-ish first
    for fmt in (
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass

    # RFC2822 fallback
    try:
        dt = parsedate_to_datetime(value)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def fetch(url: str, timeout: int = 60) -> str:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def has_war_keywords(text: str) -> bool:
    low = text.lower()
    return any(k.lower() in low for k in WAR_KEYWORDS)


def extract_tags(text: str) -> list[str]:
    found = []
    low = text.lower()
    for kw in WAR_KEYWORDS:
        if kw.lower() in low:
            found.append(kw)
    # dedupe preserve order
    deduped = []
    seen = set()
    for x in found:
        xl = x.lower()
        if xl not in seen:
            deduped.append(x)
            seen.add(xl)
    return deduped[:20]


def guess_places(text: str) -> list[str]:
    """
    Very lightweight place candidate extractor.
    Keep this conservative; real geocoding should happen downstream.
    """
    candidates = set(SECTOR_HINTS)

    # Latin title case tokens / token pairs
    latin = re.findall(r"\b[A-Z][a-z'’-]{2,}(?:\s+[A-Z][a-z'’-]{2,})?\b", text)
    # Cyrillic title case tokens / token pairs
    cyr = re.findall(r"\b[А-ЯІЇЄҐ][а-яіїєґ'’-]{2,}(?:\s+[А-ЯІЇЄҐ][а-яіїєґ'’-]{2,})?\b", text)

    out: list[str] = []
    seen = set()

    for token in list(candidates) + latin + cyr:
        token = token.strip(" ,.;:()[]{}<>\"'“”")
        if len(token) < 3:
            continue
        if token in STOPWORDS:
            continue
        if token.lower() in {s.lower() for s in seen}:
            continue
        seen.add(token)
        out.append(token)

    return out[:30]


def parse_ukrinform() -> list[Item]:
    html = fetch("https://www.ukrinform.net/rubric-ato/")
    soup = BeautifulSoup(html, "html.parser")

    items: list[Item] = []
    links = soup.select("a[href]")

    seen = set()

    for a in links:
        href = a.get("href", "")
        text = clean_text(a.get_text(" ", strip=True))

        if not href or "/rubric-ato/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)

        if not href.startswith("http"):
            href = "https://www.ukrinform.net" + href

        title = text
        if not title:
            continue

        if not has_war_keywords(title):
            continue

        items.append(
            Item(
                source="ukrinform",
                kind="web",
                channel=None,
                title=title,
                url=href,
                published_at=None,
                text=title,
                tags=extract_tags(title),
            )
        )

    return items[:40]


def parse_rbc() -> list[Item]:
    html = fetch("https://newsukraine.rbc.ua/tags/war-in-ukraine")
    soup = BeautifulSoup(html, "html.parser")

    items: list[Item] = []
    seen = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        text = clean_text(a.get_text(" ", strip=True))

        if not href:
            continue

        if href.startswith("/"):
            href = "https://newsukraine.rbc.ua" + href

        if "newsukraine.rbc.ua/news/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)

        if not text:
            continue
        if not has_war_keywords(text):
            continue

        items.append(
            Item(
                source="rbc_ukraine",
                kind="web",
                channel=None,
                title=text,
                url=href,
                published_at=None,
                text=text,
                tags=extract_tags(text),
            )
        )

    return items[:40]


def parse_telegram_channel(channel: str) -> list[Item]:
    url = f"https://t.me/s/{channel}"
    html = fetch(url)
    soup = BeautifulSoup(html, "html.parser")

    items: list[Item] = []

    for msg in soup.select("div.tgme_widget_message_wrap"):
        text_el = msg.select_one("div.tgme_widget_message_text")
        date_el = msg.select_one("a.tgme_widget_message_date time")
        link_el = msg.select_one("a.tgme_widget_message_date")

        text = clean_text(text_el.get_text(" ", strip=True) if text_el else "")
        if not text:
            continue

        if not has_war_keywords(text):
            continue

        published_at = None
        if date_el and date_el.has_attr("datetime"):
            published_at = parse_date_maybe(date_el["datetime"])

        link = link_el.get("href", url) if link_el else url

        # title = first sentence or first 120 chars
        title = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)[0]
        title = title[:120].strip() or text[:120]

        items.append(
            Item(
                source=channel.lower(),
                kind="telegram",
                channel=channel,
                title=title,
                url=link,
                published_at=published_at,
                text=text,
                tags=extract_tags(text),
            )
        )

    return items[:80]


def dedupe_items(items: Iterable[Item]) -> list[Item]:
    out = []
    seen = set()

    for item in items:
        key = (
            item.source,
            item.channel or "",
            item.url,
            clean_text(item.title).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def sort_items(items: list[Item]) -> list[Item]:
    def key(x: Item):
        return x.published_at or "0000-00-00T00:00:00+00:00"
    return sorted(items, key=key, reverse=True)


def build_geo_candidates(items: list[Item]) -> list[dict]:
    candidates: list[dict] = []

    for item in items:
        places = guess_places(f"{item.title}. {item.text}")
        if not places:
            continue

        candidates.append(
            {
                "source": item.source,
                "channel": item.channel,
                "kind": item.kind,
                "title": item.title,
                "url": item.url,
                "published_at": item.published_at,
                "text": item.text,
                "place_candidates": places,
                "tags": item.tags,
            }
        )

    return candidates


def main() -> int:
    collected: list[Item] = []

    # Web
    try:
        collected.extend(parse_ukrinform())
    except Exception as e:
        print(f"[WARN] ukrinform failed: {e}", file=sys.stderr)

    try:
        collected.extend(parse_rbc())
    except Exception as e:
        print(f"[WARN] rbc failed: {e}", file=sys.stderr)

    # Telegram mirrors
    for ch in ("GeneralStaffZSU", "DeepStateUA", "UkraineNow"):
        try:
            time.sleep(1.5)
            collected.extend(parse_telegram_channel(ch))
        except Exception as e:
            print(f"[WARN] telegram {ch} failed: {e}", file=sys.stderr)

    collected = dedupe_items(collected)
    collected = sort_items(collected)

    payload = {
        "generated_at": now_iso(),
        "count": len(collected),
        "items": [asdict(x) for x in collected],
    }

    geo_payload = {
        "generated_at": now_iso(),
        "count": 0,
        "items": build_geo_candidates(collected),
    }
    geo_payload["count"] = len(geo_payload["items"])

    OUT_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_GEO_CANDIDATES.write_text(
        json.dumps(geo_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[OK] wrote {OUT_JSON} ({payload['count']} items)")
    print(f"[OK] wrote {OUT_GEO_CANDIDATES} ({geo_payload['count']} items)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
