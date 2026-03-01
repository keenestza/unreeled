"""
Build script for Unreeled static site v2.

Reads ALL release JSON files from public/data/ and builds:
- Latest day's releases for the main feed
- Historical data for date picker browsing
- Trending titles (appearing across multiple days)
- Weekly/monthly archive stats

Injects everything into public/template.html -> public/index.html

Usage:
    python scripts/build_site.py
"""

import json
import glob
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def load_release_file(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Warning: Could not load {filepath}: {e}")
        return None


def process_releases(releases, max_per_type=15, max_total=200):
    """Select top releases ensuring all media types are represented."""
    for r in releases:
        syn = r.get("synopsis", "")
        if len(syn) > 500:
            r["synopsis"] = syn[:497] + "..."

    by_type = {}
    for r in releases:
        by_type.setdefault(r.get("media_type", "other"), []).append(r)

    def sort_key(r):
        score = r.get("metadata", {}).get("popularity", 0) or 0
        if r.get("synopsis"):
            score += 50
        if r.get("poster_url"):
            score += 30
        return score

    for items in by_type.values():
        items.sort(key=sort_key, reverse=True)

    selected = []
    remaining = []
    for media_type, items in by_type.items():
        guaranteed = items[:max_per_type]
        overflow = items[max_per_type:]
        selected.extend(guaranteed)
        remaining.extend(overflow)

    remaining.sort(key=sort_key, reverse=True)
    slots_left = max_total - len(selected)
    if slots_left > 0:
        selected.extend(remaining[:slots_left])

    selected.sort(key=sort_key, reverse=True)
    return selected


def compute_trending(all_data):
    """Find titles appearing across multiple days or with high engagement."""
    title_appearances = Counter()
    title_info = {}

    for date_str, data in all_data.items():
        for r in data.get("releases", []):
            key = (r.get("title", "").lower().strip(), r.get("media_type", ""))
            title_appearances[key] += 1

            if key not in title_info or date_str > title_info[key]["first_seen"]:
                title_info[key] = {
                    "title": r.get("title", ""),
                    "media_type": r.get("media_type", ""),
                    "genres": r.get("genres", [])[:3],
                    "synopsis": (r.get("synopsis", "") or "")[:200],
                    "poster_url": r.get("poster_url", ""),
                    "comment_count": r.get("comment_count", 0),
                    "spoiler_counts": r.get("spoiler_counts", {}),
                    "metadata": {
                        k: v for k, v in (r.get("metadata", {}) or {}).items()
                        if k in ("artists", "authors", "studios", "networks",
                                 "runtime_minutes", "score", "platforms",
                                 "publisher", "formats", "labels")
                    },
                    "first_seen": date_str,
                    "days_seen": title_appearances[key],
                }

    trending = sorted(
        title_info.values(),
        key=lambda t: (t["days_seen"], t.get("comment_count", 0)),
        reverse=True,
    )
    return trending[:20]


def compute_archive(all_data):
    """Build per-date, weekly, and monthly summaries."""
    dates = []
    weekly = {}
    monthly = {}

    for date_str in sorted(all_data.keys()):
        data = all_data[date_str]
        releases = data.get("releases", [])

        type_counts = {}
        for r in releases:
            mt = r.get("media_type", "other")
            type_counts[mt] = type_counts.get(mt, 0) + 1

        dates.append({"date": date_str, "total": len(releases), "types": type_counts})

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            week_key = dt.strftime("%Y-W%W")
            month_key = dt.strftime("%Y-%m")

            if week_key not in weekly:
                weekly[week_key] = {"week": week_key, "days": 0, "total": 0, "types": {}}
            weekly[week_key]["days"] += 1
            weekly[week_key]["total"] += len(releases)
            for mt, c in type_counts.items():
                weekly[week_key]["types"][mt] = weekly[week_key]["types"].get(mt, 0) + c

            if month_key not in monthly:
                monthly[month_key] = {"month": month_key, "days": 0, "total": 0, "types": {}}
            monthly[month_key]["days"] += 1
            monthly[month_key]["total"] += len(releases)
            for mt, c in type_counts.items():
                monthly[month_key]["types"][mt] = monthly[month_key]["types"].get(mt, 0) + c
        except ValueError:
            pass

    return {
        "dates": dates,
        "weekly": list(weekly.values()),
        "monthly": list(monthly.values()),
    }


def build():
    project_root = Path(__file__).parent.parent
    data_dir = project_root / "docs" / "data"
    template_file = project_root / "docs" / "template.html"
    output_file = project_root / "docs" / "index.html"

    # Load all release files
    all_data = {}
    for filepath in sorted(glob.glob(str(data_dir / "releases_*.json"))):
        data = load_release_file(filepath)
        if data and data.get("date"):
            all_data[data["date"]] = data

    print(f"Found {len(all_data)} days of release data")

    if not all_data:
        print("No data files found - building with empty data")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        all_data[today] = {"date": today, "total_releases": 0, "source_stats": {}, "releases": []}

    latest_date = max(all_data.keys())
    latest_data = all_data[latest_date]
    print(f"Latest: {latest_date} ({latest_data.get('total_releases', 0)} releases)")

    # Process releases for each date
    historical = {}
    for date_str, data in all_data.items():
        historical[date_str] = process_releases(
            data.get("releases", []),
            max_per_type=25 if date_str != latest_date else 30,
            max_total=250 if date_str != latest_date else 400,
        )

    latest_releases = historical[latest_date]
    trending = compute_trending(all_data)
    archive = compute_archive(all_data)

    print(f"Trending: {len(trending)} titles")
    print(f"Archive: {len(archive['dates'])} days, {len(archive['weekly'])} weeks, {len(archive['monthly'])} months")

    inject_data = {
        "latest_date": latest_date,
        "built_at": utcnow_iso(),
        "dates_available": sorted(all_data.keys(), reverse=True),
        "latest": {
            "date": latest_date,
            "total_releases": len(latest_releases),
            "source_stats": latest_data.get("source_stats", {}),
            "releases": latest_releases,
        },
        "historical": historical,
        "trending": trending,
        "archive": archive,
    }

    json_str = json.dumps(inject_data, ensure_ascii=False, separators=(",", ":"))

    if not template_file.exists():
        print(f"ERROR: Template not found at {template_file}")
        return False

    with open(template_file, "r", encoding="utf-8") as f:
        template = f.read()

    html = template.replace("__RELEASE_DATA_PLACEHOLDER__", json_str)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    file_size = output_file.stat().st_size / 1024
    print(f"Built {output_file} ({file_size:.0f} KB)")

    # Generate RSS feed
    rss_file = project_root / "docs" / "feed.xml"
    build_rss(latest_releases, latest_date, rss_file)

    return True


def build_rss(releases, date, output_path):
    """Generate RSS 2.0 feed from latest releases."""
    from xml.sax.saxutils import escape

    site_url = "https://keenestza.github.io/unreeled/"
    items = []
    for r in releases[:50]:
        title = escape(r.get("title", "Unknown"))
        media_type = r.get("media_type", "")
        mc = {"movie": "Film", "tv": "TV", "book": "Book", "game": "Game",
              "anime": "Anime", "music": "Music", "podcast": "Podcast", "news": "News"}
        category = mc.get(media_type, media_type)
        synopsis = escape((r.get("synopsis", "") or "")[:300])
        genres = ", ".join(r.get("genres", [])[:3])
        poster = r.get("poster_url", "")

        desc = f"[{category}] {synopsis}"
        if genres:
            desc += f" — Genres: {genres}"

        items.append(f"""    <item>
      <title>{title}</title>
      <description>{escape(desc)}</description>
      <category>{category}</category>
      <pubDate>{date}</pubDate>
      <guid>{site_url}#{media_type}-{escape(title).replace(' ', '-').lower()}</guid>
      <link>{site_url}</link>
    </item>""")

    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>UNREELED — Daily Media Releases</title>
    <link>{site_url}</link>
    <description>Daily releases across movies, TV, books, games, anime, music and podcasts.</description>
    <language>en</language>
    <lastBuildDate>{date}</lastBuildDate>
    <atom:link href="{site_url}feed.xml" rel="self" type="application/rss+xml"/>
{chr(10).join(items)}
  </channel>
</rss>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rss)
    print(f"RSS feed: {len(items)} items → {output_path}")


if __name__ == "__main__":
    success = build()
    exit(0 if success else 1)
