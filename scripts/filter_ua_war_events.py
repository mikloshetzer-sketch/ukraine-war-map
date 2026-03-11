#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

IN_FILE = Path("data/ua_war_sources_latest.json")
OUT_FILE = Path("data/ua_war_events_filtered.json")
SUMMARY_FILE = Path("data/ua_war_events_summary.json")

LOOKBACK_HOURS = 72

SECTOR_KEYWORDS = {
    "kupiansk": ["kupiansk", "куп'янськ", "купянськ"],
    "kreminna": ["kreminna", "кремінна"],
    "lyman": ["lyman", "лиман"],
    "siversk": ["siversk", "сіверськ"],
    "bakhmut": ["bakhmut", "бахмут"],
    "toretsk": ["toretsk", "торецьк"],
    "kostiantynivka": ["kostiantynivka", "костянтинівка"],
    "avdiivka": ["avdiivka", "авдіївка"],
    "pokrovsk": ["pokrovsk", "покровськ"],
    "kurakhove": ["kurakhove", "курахове"],
    "velyka_novosilka": ["velyka novosilka", "велика новосілка"],
    "huliaipole": ["huliaipole", "гуляйполе"],
    "robotyne": ["robotyne", "роботине"],
    "orikhiv": ["orikhiv", "оріхів"],
    "kherson": ["kherson", "херсон"],
    "zaporizhzhia": ["zaporizhzhia", "запоріжжя"],
}

HIGH_CONF_SOURCES = {"generalstaffzsu", "deepstateua", "general_staff_tg", "deepstate_tg"}
MEDIUM_CONF_SOURCES = {"ukrinform", "rbc_ukraine", "ukrainenow", "ukrainenow_tg"}

GROUND_PATTERNS = [
    r"\bfrontline clashes?\b",
    r"\bbattle[s]?\b",
    r"\bcombat clashes?\b",
    r"\battacks?\b",
    r"\bassault[s]?\b",
    r"\bбоєзіткнен",
    r"\bштурм",
    r"\bата[кк]",
    r"\bнаступ",
]
DRONE_PATTERNS = [
    r"\bdrone[s]?\b",
    r"\buav\b",
    r"\bshahed\b",
    r"\bшахед",
    r"\bдрон",
]
MISSILE_PATTERNS = [
    r"\bmissile[s]?\b",
    r"\braket",
    r"\brocket[s]?\b",
    r"\bракет",
]
ARTY_PATTERNS = [
    r"\bartillery\b",
    r"\bshelling\b",
    r"\bобстр",
    r"\bартилер",
]
AIR_PATTERNS = [
    r"\bairstrike[s]?\b",
    r"\baviation strike[s]?\b",
    r"\bавіаудар",
    r"\bair attack\b",
]
BRIEF_PATTERNS = [
    r"\boperational information\b",
    r"\bwar update\b",
    r"\bfrontline update\b",
    r"\bоперативна інформація\b",
]

PLACE_PATTERNS_LATIN = re.compile(r"\b[A-Z][a-z'’-]{2,}(?:\s+[A-Z][a-z'’-]{2,})?\b")
PLACE_PATTERNS_CYR = re.compile(r"\b[А-ЯІЇЄҐ][а-яіїєґ'’-]{2,}(?:\s+[А-ЯІЇЄҐ][а-яіїєґ'’-]{2,})?\b")


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def classify_event_type(text: str) -> str:
    t = text.lower()

    def any_match(patterns: list[str]) -> bool:
        return any(re.search(p, t, flags=re.IGNORECASE) for p in patterns)

    if any_match(DRONE_PATTERNS):
        return "drone_attack"
    if any_match(MISSILE_PATTERNS):
        return "missile_strike"
    if any_match(ARTY_PATTERNS):
        return "artillery"
    if any_match(AIR_PATTERNS):
        return "airstrike"
    if any_match(GROUND_PATTERNS):
        return "ground_clash"
    if any_match(BRIEF_PATTERNS):
        return "frontline_update"
    return "other"


def confidence_for_source(source: str) -> str:
    s = (source or "").lower()
    if s in HIGH_CONF_SOURCES:
        return "high"
    if s in MEDIUM_CONF_SOURCES:
        return "medium"
    return "low"


def extract_sector_hints(text: str) -> list[str]:
    t = text.lower()
    out = []
    for sector, keywords in SECTOR_KEYWORDS.items():
        if any(k.lower() in t for k in keywords):
            out.append(sector)
    return out


def extract_place_candidates(text: str) -> list[str]:
    raw = list(PLACE_PATTERNS_LATIN.findall(text)) + list(PLACE_PATTERNS_CYR.findall(text))
    out = []
    seen = set()

    blacklist = {
        "Ukraine", "Ukrainian", "Russia", "Russian", "Kyiv", "Moscow",
        "Україна", "Росія", "Київ", "Москва", "General Staff",
        "War Update", "Frontline Update", "Operational Information"
    }

    for token in raw:
        token = token.strip(" ,.;:()[]{}<>\"'“”")
        if len(token) < 3:
            continue
        if token in blacklist:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)

    return out[:20]


def fingerprint(item: dict) -> str:
    source = (item.get("source") or "").lower().strip()
    title = clean_text(item.get("title") or "").lower()
    text = clean_text(item.get("text") or "").lower()

    # rövidebb, erősebb kulcs
    text_core = re.sub(r"[^a-zA-Zа-яА-ЯіїєґІЇЄҐ0-9 ]+", "", text)[:180]
    title_core = re.sub(r"[^a-zA-Zа-яА-ЯіїєґІЇЄҐ0-9 ]+", "", title)[:120]

    return f"{source}|{title_core}|{text_core}"


def should_keep_by_time(item: dict, now: datetime) -> bool:
    published_at = parse_dt(item.get("published_at"))
    if published_at is None:
        # ha nincs idő, tartsuk meg, de később lehet szigorítani
        return True
    return published_at >= now - timedelta(hours=LOOKBACK_HOURS)


def normalize_item(item: dict) -> dict:
    source = item.get("source") or ""
    channel = item.get("channel")
    title = clean_text(item.get("title") or "")
    text = clean_text(item.get("text") or "")
    full_text = f"{title}. {text}".strip()

    event_type = classify_event_type(full_text)
    sector_hints = extract_sector_hints(full_text)
    place_candidates = extract_place_candidates(full_text)

    return {
        "id": fingerprint(item),
        "source": source,
        "channel": channel,
        "kind": item.get("kind"),
        "title": title,
        "url": item.get("url"),
        "published_at": item.get("published_at"),
        "event_type": event_type,
        "confidence": confidence_for_source(source),
        "sector_hints": sector_hints,
        "place_candidates": place_candidates,
        "tags": item.get("tags", []),
        "text": text,
    }


def dedupe_events(items: list[dict]) -> list[dict]:
    seen = set()
    out = []
    for item in items:
        key = item["id"]
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def build_summary(events: list[dict]) -> dict:
    event_type_counts = Counter(e["event_type"] for e in events)
    source_counts = Counter((e["source"] or "").lower() for e in events)
    confidence_counts = Counter(e["confidence"] for e in events)

    sector_counts = Counter()
    for e in events:
        for s in e["sector_hints"]:
            sector_counts[s] += 1

    return {
        "generated_at": now_utc().replace(microsecond=0).isoformat(),
        "count": len(events),
        "by_event_type": dict(event_type_counts),
        "by_source": dict(source_counts),
        "by_confidence": dict(confidence_counts),
        "top_sectors": sector_counts.most_common(10),
    }


def main() -> int:
    if not IN_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {IN_FILE}")

    raw = json.loads(IN_FILE.read_text(encoding="utf-8"))
    items = raw.get("items", [])

    current = now_utc()

    filtered = []
    for item in items:
        if not should_keep_by_time(item, current):
            continue

        normalized = normalize_item(item)

        # csak releváns eventek maradjanak
        if normalized["event_type"] == "other":
            continue

        filtered.append(normalized)

    filtered = dedupe_events(filtered)
    filtered.sort(key=lambda x: x.get("published_at") or "", reverse=True)

    payload = {
        "generated_at": current.replace(microsecond=0).isoformat(),
        "count": len(filtered),
        "items": filtered,
    }

    summary = build_summary(filtered)

    OUT_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    SUMMARY_FILE.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OK] wrote {OUT_FILE} ({payload['count']} events)")
    print(f"[OK] wrote {SUMMARY_FILE}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
