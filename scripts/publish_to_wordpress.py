# scripts/publish_to_wordpress.py

from __future__ import annotations

import base64
import html
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


def escape_text(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def get_summary_sector(brief: dict, key: str) -> str | None:
    summary = brief.get("summary", {})
    value = summary.get(key)
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def build_metrics_from_summary(brief: dict) -> list[dict]:
    summary = brief.get("summary", {})

    def fmt_num(value, digits=2):
        if value is None:
            return "n/a"
        return f"{value:,.{digits}f}".replace(",", " ")

    def sign_prefix(value):
        if value is None:
            return ""
        return "+" if value > 0 else ""

    kept_points = summary.get("ground_kept_points")
    kept_lines = summary.get("ground_kept_lines")
    mapped_total = None
    if kept_points is not None and kept_lines is not None:
        mapped_total = kept_points + kept_lines

    return [
        {
            "label": "Occupied territory",
            "value": f"{fmt_num(summary.get('occupied_km2'))} km²",
        },
        {
            "label": "Daily change",
            "value": f"{sign_prefix(summary.get('daily_delta_km2'))}{fmt_num(summary.get('daily_delta_km2'))} km²",
        },
        {
            "label": "Weekly change",
            "value": f"{sign_prefix(summary.get('weekly_delta_km2'))}{fmt_num(summary.get('weekly_delta_km2'), 1)} km²",
        },
        {
            "label": "Ground combat reports",
            "value": str(summary.get("ground_raw_total", "n/a")) if summary.get("ground_raw_total") is not None else "n/a",
        },
        {
            "label": "Mapped events",
            "value": str(mapped_total) if mapped_total is not None else "n/a",
        },
        {
            "label": "UAV activity (7d)",
            "value": str(summary.get("uav_events_7d", "n/a")) if summary.get("uav_events_7d") is not None else "n/a",
        },
    ]


def build_legacy_html(brief: dict) -> str:
    title = brief.get("title", "Daily Brief")
    text = brief.get("text", "").strip()
    date_value = brief.get("date", "")
    gained_sector = get_summary_sector(brief, "gained_sector")
    lost_sector = get_summary_sector(brief, "lost_sector")

    intro_lines: list[str] = []
    intro_lines.append(f"<p><strong>{escape_text(title)}</strong></p>")

    if date_value:
        intro_lines.append(f"<p><strong>Date:</strong> {escape_text(date_value)}</p>")

    if gained_sector:
        intro_lines.append(f"<p><strong>Main gain sector:</strong> {escape_text(gained_sector)}</p>")

    if lost_sector:
        intro_lines.append(f"<p><strong>Loss / regain sector:</strong> {escape_text(lost_sector)}</p>")

    paragraphs = []
    for block in text.split("\n\n"):
        cleaned = block.strip()
        if not cleaned:
            continue

        if cleaned.endswith(":"):
            paragraphs.append(f"<h2>{escape_text(cleaned[:-1])}</h2>")
            continue

        if cleaned.startswith("- "):
            lines = [line.strip()[2:] for line in cleaned.splitlines() if line.strip().startswith("- ")]
            if lines:
                items = "".join(f"<li>{escape_text(line)}</li>" for line in lines)
                paragraphs.append(f"<ul>{items}</ul>")
            continue

        if "\n- " in cleaned:
            lines = cleaned.splitlines()
            first = lines[0].strip()
            bullet_lines = [line.strip()[2:] for line in lines[1:] if line.strip().startswith("- ")]
            if first:
                paragraphs.append(f"<p>{escape_text(first)}</p>")
            if bullet_lines:
                items = "".join(f"<li>{escape_text(line)}</li>" for line in bullet_lines)
                paragraphs.append(f"<ul>{items}</ul>")
            continue

        paragraphs.append(f"<p>{escape_text(cleaned)}</p>")

    return "\n".join(intro_lines + paragraphs)


def build_styles() -> str:
    return """
<style>
.brief-shell{
  max-width: 860px;
  margin: 0 auto;
}

.brief-meta{
  margin: 0 0 18px 0;
  color: #d9e2ec;
  font-size: 0.95rem;
}

.brief-panel{
  background: #e9edf2;
  border-radius: 12px;
  padding: 20px;
  margin: 0 0 20px 0;
  box-shadow: 0 2px 10px rgba(0,0,0,0.10);
}

.brief-panel h3{
  margin: 0 0 14px 0;
  font-size: 1.05rem;
  color: #1f2937;
}

.metrics-grid{
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
}

.metric-card{
  background: #f8fafc;
  border-radius: 10px;
  padding: 14px;
  border: 1px solid rgba(15, 23, 42, 0.06);
}

.metric-label{
  font-size: 0.75rem;
  line-height: 1.3;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: #64748b;
  margin-bottom: 6px;
}

.metric-value{
  font-size: 1.08rem;
  line-height: 1.35;
  font-weight: 700;
  color: #0f172a;
}

.analysis-body p{
  margin: 0 0 1em 0;
  color: #1f2937;
  line-height: 1.7;
}

.analysis-body p:last-child{
  margin-bottom: 0;
}

@media (max-width: 640px){
  .metrics-grid{
    grid-template-columns: 1fr;
  }

  .brief-panel{
    padding: 16px;
  }
}
</style>
""".strip()


def build_metrics_panel(metrics: list[dict]) -> str:
    cards = []
    for item in metrics:
        label = escape_text(item.get("label", "Metric"))
        value = escape_text(item.get("value", "n/a"))
        cards.append(
            f"""
            <div class="metric-card">
              <div class="metric-label">{label}</div>
              <div class="metric-value">{value}</div>
            </div>
            """.strip()
        )

    cards_html = "\n".join(cards)

    return f"""
    <section class="brief-panel brief-metrics">
      <h3>Key metrics</h3>
      <div class="metrics-grid">
        {cards_html}
      </div>
    </section>
    """.strip()


def build_analysis_panel(analysis: str) -> str:
    paragraphs = []
    for block in analysis.split("\n\n"):
        cleaned = block.strip()
        if cleaned:
            paragraphs.append(f"<p>{escape_text(cleaned)}</p>")

    paragraphs_html = "\n".join(paragraphs) if paragraphs else "<p>No analysis available.</p>"

    return f"""
    <section class="brief-panel brief-analysis">
      <h3>Analysis</h3>
      <div class="analysis-body">
        {paragraphs_html}
      </div>
    </section>
    """.strip()


def build_wp_content(brief: dict) -> str:
    title = brief.get("title", "Daily Brief")
    date_value = brief.get("date", "")
    gained_sector = get_summary_sector(brief, "gained_sector")
    lost_sector = get_summary_sector(brief, "lost_sector")

    metrics = brief.get("metrics")
    analysis = brief.get("analysis")

    if not isinstance(metrics, list) or not metrics:
        metrics = build_metrics_from_summary(brief)

    if not isinstance(analysis, str) or not analysis.strip():
        return build_legacy_html(brief)

    meta_parts = [f"<strong>{escape_text(title)}</strong>"]
    if date_value:
        meta_parts.append(f"Date: {escape_text(date_value)}")
    if gained_sector:
        meta_parts.append(f"Main gain sector: {escape_text(gained_sector)}")
    if lost_sector:
        meta_parts.append(f"Loss / regain sector: {escape_text(lost_sector)}")

    meta_html = " &nbsp;|&nbsp; ".join(meta_parts)

    return f"""
    {build_styles()}
    <div class="brief-shell">
      <div class="brief-meta">{meta_html}</div>
      {build_metrics_panel(metrics)}
      {build_analysis_panel(analysis)}
    </div>
    """.strip()


def build_excerpt(brief: dict) -> str:
    title = brief.get("title", "Daily Brief")
    analysis = brief.get("analysis", "")
    text = analysis if isinstance(analysis, str) and analysis.strip() else brief.get("text", "")
    text = text.replace("\n", " ").strip()

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
