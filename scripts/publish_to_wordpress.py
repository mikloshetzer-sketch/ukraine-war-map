# scripts/publish_to_wordpress.py

from __future__ import annotations

import base64
import json
import os
import sys
from pathlib import Path
from urllib import error, request

DATA_DIR = Path("data")
BRIEF_JSON = DATA_DIR / "brief_daily.json"


def load_brief(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Brief file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def build_basic_auth(username: str, app_password: str) -> str:
    token = f"{username}:{app_password}".encode("utf-8")
    encoded = base64.b64encode(token).decode("utf-8")
    return f"Basic {encoded}"


def build_wp_content(brief: dict) -> str:
    title = brief.get("title", "Daily Brief")
    text = brief.get("text", "").strip()
    date_value = brief.get("date", "")
    gained_sector = brief.get("sector_gain")
    lost_sector = brief.get("sector_loss")

    intro_lines: list[str] = []
    intro_lines.append(f"<p><strong>{title}</strong></p>")

    if date_value:
        intro_lines.append(f"<p><strong>Date:</strong> {date_value}</p>")

    if gained_sector:
        intro_lines.append(f"<p><strong>Main gain sector:</strong> {gained_sector}</p>")

    if lost_sector:
        intro_lines.append(f"<p><strong>Loss / regain sector:</strong> {lost_sector}</p>")

    paragraphs = []
    for block in text.split("\n\n"):
        cleaned = block.strip()
        if not cleaned:
            continue

        if cleaned.endswith(":"):
            paragraphs.append(f"<h2>{cleaned[:-1]}</h2>")
            continue

        if cleaned.startswith("- "):
            lines = [line.strip()[2:] for line in cleaned.splitlines() if line.strip().startswith("- ")]
            if lines:
                items = "".join(f"<li>{line}</li>" for line in lines)
                paragraphs.append(f"<ul>{items}</ul>")
            continue

        if "\n- " in cleaned:
            lines = cleaned.splitlines()
            first = lines[0].strip()
            bullet_lines = [line.strip()[2:] for line in lines[1:] if line.strip().startswith("- ")]
            if first:
                paragraphs.append(f"<p>{first}</p>")
            if bullet_lines:
                items = "".join(f"<li>{line}</li>" for line in bullet_lines)
                paragraphs.append(f"<ul>{items}</ul>")
            continue

        paragraphs.append(f"<p>{cleaned}</p>")

    return "\n".join(intro_lines + paragraphs)


def build_excerpt(brief: dict) -> str:
    title = brief.get("title", "Daily Brief")
    text = brief.get("text", "").replace("\n", " ").strip()

    excerpt = f"{title}. {text}"
    excerpt = " ".join(excerpt.split())

    if len(excerpt) > 250:
        excerpt = excerpt[:247].rstrip() + "..."
    return excerpt


def post_to_wordpress(site_url: str, username: str, app_password: str, brief: dict) -> dict:
    api_url = site_url.rstrip("/") + "/wp-json/wp/v2/posts"

    payload = {
        "title": brief.get("title", "Daily Brief"),
        "status": "draft",
        "content": build_wp_content(brief),
        "excerpt": build_excerpt(brief),
    }

    body = json.dumps(payload).encode("utf-8")

    req = request.Request(
        api_url,
        data=body,
        method="POST",
        headers={
            "Authorization": build_basic_auth(username, app_password),
            "Content-Type": "application/json",
        },
    )

    with request.urlopen(req, timeout=60) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw)


def main() -> None:
    try:
        site_url = get_env("WP_SITE_URL")
        username = get_env("WP_USERNAME")
        app_password = get_env("WP_APP_PASSWORD")

        brief = load_brief(BRIEF_JSON)
        result = post_to_wordpress(site_url, username, app_password, brief)

        print("WordPress draft created successfully.")
        print(f"Post ID: {result.get('id')}")
        print(f"Post URL: {result.get('link')}")

    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
    except error.HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP error: {exc.code}", file=sys.stderr)
        print(details, file=sys.stderr)
        sys.exit(1)
    except error.URLError as exc:
        print(f"Connection error: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"Unexpected error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
