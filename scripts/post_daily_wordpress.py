import html
import json
import os
import re
from pathlib import Path
from datetime import datetime, timezone

import requests

BRIEF_JSON = Path("data/brief_daily.json")

WP_API_BASE = "https://public-api.wordpress.com/rest/v1.1"


def load_brief(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"Missing input file: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def sanitize_text(value: str) -> str:
    return html.escape(str(value), quote=True)


def split_sections(text: str):
    lines = text.splitlines()
    sections = []
    current_heading = None
    current_lines = []

    for line in lines:
        stripped = line.strip()

        if not stripped:
            current_lines.append("")
            continue

        if stripped.endswith(":") and not stripped.startswith("- "):
            if current_heading is not None or current_lines:
                sections.append((current_heading, "\n".join(current_lines).strip()))
            current_heading = stripped[:-1].strip()
            current_lines = []
        else:
            current_lines.append(line)

    if current_heading is not None or current_lines:
        sections.append((current_heading, "\n".join(current_lines).strip()))

    return sections


def convert_block_to_html(block: str) -> str:
    if not block.strip():
        return ""

    parts = []
    lines = block.splitlines()
    i = 0

    while i < len(lines):
        line = lines[i].rstrip()
        stripped = line.strip()

        if not stripped:
            i += 1
            continue

        if stripped.startswith("- "):
            items = []
            while i < len(lines):
                candidate = lines[i].strip()
                if candidate.startswith("- "):
                    items.append(candidate[2:].strip())
                    i += 1
                else:
                    break

            list_items = "".join(
                f'<li style="margin:0 0 10px 0;text-align:justify;">{sanitize_text(item)}</li>'
                for item in items
            )
            parts.append(
                f'<ul style="margin:0 0 18px 22px;padding:0;color:#f1f5f9;line-height:1.8;">{list_items}</ul>'
            )
            continue

        paragraph_lines = [stripped]
        i += 1

        while i < len(lines):
            nxt = lines[i].strip()
            if not nxt:
                i += 1
                break
            if nxt.startswith("- "):
                break
            paragraph_lines.append(nxt)
            i += 1

        paragraph = " ".join(paragraph_lines).strip()
        parts.append(
            f'<p style="margin:0 0 16px 0;font-size:16px;line-height:1.8;color:#f1f5f9;text-align:justify;">'
            f'{sanitize_text(paragraph)}</p>'
        )

    return "\n".join(parts)


def build_metric_cards(brief: dict) -> str:
    summary = brief.get("summary", {})

    cards = [
        ("Occupied territory", f"{summary.get('occupied_km2', 'n/a')} km²"),
        ("Daily change", f"{summary.get('daily_delta_km2', 'n/a')} km²"),
        ("Weekly change", f"{summary.get('weekly_delta_km2', 'n/a')} km²"),
        ("Ground raw events", str(summary.get("ground_raw_total", "n/a"))),
        ("Mapped points", str(summary.get("ground_kept_points", "n/a"))),
        ("UAV events (7d)", str(summary.get("uav_events_7d", "n/a"))),
    ]

    card_html = []
    for label, value in cards:
        card_html.append(
            f"""
            <div style="
                background:#f8fafc;
                border-radius:16px;
                padding:18px 18px;
                box-shadow:0 8px 20px rgba(0,0,0,0.12);
                min-height:94px;
            ">
              <div style="font-size:12px;text-transform:uppercase;letter-spacing:1px;color:#64748b;margin-bottom:8px;">
                {sanitize_text(label)}
              </div>
              <div style="font-size:22px;font-weight:700;line-height:1.25;color:#0f172a;">
                {sanitize_text(value)}
              </div>
            </div>
            """
        )

    return f"""
    <section style="margin:0 0 26px 0;">
      <div style="
          background:#e5e7eb;
          color:#0f172a;
          padding:18px 22px;
          border-radius:16px;
          box-shadow:0 6px 18px rgba(0,0,0,0.16);
          margin:0 0 14px 0;
      ">
        <div style="font-size:22px;font-weight:700;line-height:1.3;">
          Key metrics
        </div>
      </div>

      <div style="
          display:grid;
          grid-template-columns:repeat(auto-fit,minmax(210px,1fr));
          gap:14px;
          padding:2px 8px 0 8px;
      ">
        {''.join(card_html)}
      </div>
    </section>
    """


def build_meta_band(brief: dict) -> str:
    date_value = brief.get("date", "")
    summary = brief.get("summary", {})
    gained_sector = summary.get("gained_sector") or "No clear gain sector"
    lost_sector = summary.get("lost_sector") or "No clear loss sector"

    items = [
        ("Date", date_value or "n/a"),
        ("Main gain sector", gained_sector),
        ("Loss / regain sector", lost_sector),
        ("Version", str(brief.get("version", "n/a"))),
    ]

    cells = []
    for label, value in items:
        cells.append(
            f"""
            <div style="
                background:rgba(255,255,255,0.08);
                border:1px solid rgba(255,255,255,0.12);
                border-radius:14px;
                padding:14px 16px;
            ">
              <div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;color:#cbd5e1;margin-bottom:6px;">
                {sanitize_text(label)}
              </div>
              <div style="font-size:15px;font-weight:600;line-height:1.4;color:#f8fafc;">
                {sanitize_text(value)}
              </div>
            </div>
            """
        )

    return f"""
    <div style="
        display:grid;
        grid-template-columns:repeat(auto-fit,minmax(220px,1fr));
        gap:12px;
        margin-top:18px;
    ">
      {''.join(cells)}
    </div>
    """


def build_section_html(title: str, body: str) -> str:
    body_html = convert_block_to_html(body)

    return f"""
    <section style="margin:0 0 26px 0;">
      <div style="
          background:#e5e7eb;
          color:#0f172a;
          padding:18px 22px;
          border-radius:16px;
          box-shadow:0 6px 18px rgba(0,0,0,0.16);
          margin:0 0 14px 0;
      ">
        <div style="font-size:22px;font-weight:700;line-height:1.3;">
          {sanitize_text(title)}
        </div>
      </div>
      <div style="padding:2px 8px 0 8px;">
        {body_html}
      </div>
    </section>
    """


def build_intro_block(intro_text: str) -> str:
    intro_html = convert_block_to_html(intro_text)

    return f"""
    <section style="margin:0 0 26px 0;">
      <div style="
          background:#cbd5e1;
          color:#0f172a;
          padding:18px 22px;
          border-radius:16px;
          box-shadow:0 6px 18px rgba(0,0,0,0.16);
          margin:0 0 14px 0;
      ">
        <div style="font-size:22px;font-weight:700;line-height:1.3;">
          Executive summary
        </div>
      </div>
      <div style="padding:2px 8px 0 8px;">
        {intro_html}
      </div>
    </section>
    """


def build_styled_content(brief: dict) -> str:
    title = brief.get("title", "Ukraine War Daily Brief")
    full_text = (brief.get("text") or "").strip()

    if not full_text:
        raise SystemExit("brief_daily.json does not contain 'text'.")

    sections = split_sections(full_text)

    if not sections:
        raise SystemExit("Could not parse sections from brief text.")

    first_heading, first_body = sections[0]
    hero_intro = first_body if first_heading is None else ""
    remaining_sections = sections[1:] if first_heading is None else sections

    blocks = []

    if hero_intro:
        blocks.append(build_intro_block(hero_intro))

    blocks.append(build_metric_cards(brief))

    for section_title, section_body in remaining_sections:
        if not section_body.strip():
            continue
        heading = section_title or "Brief"
        blocks.append(build_section_html(heading, section_body))

    meta_band = build_meta_band(brief)

    return f"""
    <div style="background:#4b5563;padding:40px 20px;">
      <div style="max-width:1000px;margin:0 auto;display:flex;flex-direction:column;gap:18px;">

        <div style="
            background:linear-gradient(135deg,#1e293b,#334155);
            padding:26px 28px;
            border-radius:22px;
            color:#f8fafc;
            box-shadow:0 12px 30px rgba(0,0,0,0.22);
            margin-bottom:8px;
        ">
          <div style="font-size:12px;text-transform:uppercase;letter-spacing:1.4px;color:#cbd5e1;">
            Ukraine War Daily Brief
          </div>
          <div style="font-size:30px;font-weight:700;line-height:1.2;margin-top:8px;">
            {sanitize_text(title)}
          </div>
          {meta_band}
        </div>

        {''.join(blocks)}

      </div>
    </div>
    """


def build_excerpt(brief: dict) -> str:
    title = brief.get("title", "Ukraine War Daily Brief")
    text = (brief.get("text") or "").replace("\n", " ").strip()
    excerpt = f"{title}. {text}"
    excerpt = " ".join(excerpt.split())

    if len(excerpt) > 280:
        excerpt = excerpt[:277].rstrip() + "..."
    return excerpt


def build_slug(brief: dict) -> str:
    date_value = brief.get("date") or datetime.now(timezone.utc).date().isoformat()
    title = brief.get("title", "ukraine-war-daily-brief").lower()
    title = re.sub(r"[^a-z0-9]+", "-", title)
    title = re.sub(r"-{2,}", "-", title).strip("-")
    return f"{date_value}-{title}"[:190]


def main():
    token = os.environ.get("WPCOM_ACCESS_TOKEN", "").strip()
    site = os.environ.get("WPCOM_SITE", "").strip()
    status = os.environ.get("WPCOM_STATUS", "draft").strip().lower()

    if not token:
        raise SystemExit("Missing WPCOM_ACCESS_TOKEN secret.")
    if not site:
        raise SystemExit("Missing WPCOM_SITE secret.")

    brief = load_brief(BRIEF_JSON)

    title = brief.get("title", "Ukraine War Daily Brief")
    content = build_styled_content(brief)
    excerpt = build_excerpt(brief)
    slug = build_slug(brief)

    url = f"{WP_API_BASE}/sites/{site}/posts/new"
    headers = {"Authorization": f"Bearer {token}"}

    payload = {
        "title": title,
        "content": content,
        "excerpt": excerpt,
        "status": status,
        "format": "standard",
        "slug": slug,
    }

    resp = requests.post(url, headers=headers, data=payload, timeout=60)
    if resp.status_code >= 300:
        raise SystemExit(f"WP post failed ({resp.status_code}): {resp.text}")

    data = resp.json()
    post_url = data.get("URL") or data.get("url") or ""
    post_id = data.get("ID") or data.get("id") or ""

    print(f"Posted daily brief from: {BRIEF_JSON}")
    print(f"Status: {status}, Post ID: {post_id}")
    if post_url:
        print(f"URL: {post_url}")


if __name__ == "__main__":
    main()
