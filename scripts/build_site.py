"""
Build script for Unreeled static site.

Pipeline:
1. unreeled_ingest.py writes JSON files into scripts/output/
2. GitHub Actions copies those files into docs/data/
3. This script reads docs/data/ and builds docs/index.html
4. It also generates SEO pages in docs/r/

Reads all release JSON files from docs/data/ and builds:
- Date index (all available dates)
- Latest day's releases
- Trending data (titles appearing across multiple days)
- Weekly/monthly archive stats

Injects everything into docs/template.html → docs/index.html

Usage:
    python scripts/build_site.py
"""

import json
import os
import glob
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import Counter


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def load_release_file(filepath):
    """Load and return a release JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Warning: Could not load {filepath}: {e}")
        return None


def process_releases(releases, max_per_type=30, max_total=200):
    """
    Select top releases ensuring all media types are represented.
    Truncate long synopses.
    """
    for r in releases:
        syn = r.get("synopsis") or ""
        if len(syn) > 500:
            r["synopsis"] = syn[:497] + "..."

    by_type = {}
    for r in releases:
        by_type.setdefault(r.get("media_type", "other"),[]).append(r)

    def sort_key(r):
        score = r.get("metadata", {}).get("popularity", 0) or 0
        if r.get("synopsis"):
            score += 50
        if r.get("poster_url"):
            score += 30
        return score

    for items in by_type.values():
        items.sort(key=sort_key, reverse=True)

    guaranteed_per_type = min(max_per_type, 15)
    selected =[]
    remaining =[]

    for media_type, items in by_type.items():
        guaranteed = items[:guaranteed_per_type]
        overflow = items[guaranteed_per_type:]
        selected.extend(guaranteed)
        remaining.extend(overflow)

    remaining.sort(key=sort_key, reverse=True)
    slots_left = max_total - len(selected)
    if slots_left > 0:
        selected.extend(remaining[:slots_left])

    selected.sort(key=sort_key, reverse=True)
    return selected


def compute_trending(all_data):
    """
    Compute trending titles — titles that appear across multiple days
    or have high comment counts.
    """
    title_appearances = Counter()
    title_info = {}

    for date_str, data in all_data.items():
        for r in data.get("releases",[]):
            key = (r.get("title", "").lower().strip(), r.get("media_type", ""))
            title_appearances[key] += 1

            # Keep the most recent version of the info
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

    # Sort by days_seen (multi-day = trending), then by comment count
    trending = sorted(
        title_info.values(),
        key=lambda t: (t["days_seen"], t.get("comment_count", 0)),
        reverse=True,
    )

    # Return top 20 trending
    return trending[:20]


def compute_archive_stats(all_data):
    """
    Build archive metadata: per-date stats for the date picker,
    weekly and monthly summaries.
    """
    dates =[]
    weekly = {}
    monthly = {}

    for date_str in sorted(all_data.keys()):
        data = all_data[date_str]
        releases = data.get("releases",[])
        stats = data.get("source_stats", {})

        # Per-type counts
        type_counts = {}
        for r in releases:
            mt = r.get("media_type", "other")
            type_counts[mt] = type_counts.get(mt, 0) + 1

        date_info = {
            "date": date_str,
            "total": len(releases),
            "types": type_counts,
        }
        dates.append(date_info)

        # Weekly grouping (ISO week)
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            week_key = dt.strftime("%Y-W%W")
            month_key = dt.strftime("%Y-%m")

            if week_key not in weekly:
                weekly[week_key] = {"week": week_key, "days": 0, "total": 0, "types": {}}
            weekly[week_key]["days"] += 1
            weekly[week_key]["total"] += len(releases)
            for mt, count in type_counts.items():
                weekly[week_key]["types"][mt] = weekly[week_key]["types"].get(mt, 0) + count

            if month_key not in monthly:
                monthly[month_key] = {"month": month_key, "days": 0, "total": 0, "types": {}}
            monthly[month_key]["days"] += 1
            monthly[month_key]["total"] += len(releases)
            for mt, count in type_counts.items():
                monthly[month_key]["types"][mt] = monthly[month_key]["types"].get(mt, 0) + count
        except ValueError:
            pass

    return {
        "dates": dates,
        "weekly": list(weekly.values()),
        "monthly": list(monthly.values()),
    }


def build():
    project_root = Path(__file__).parent.parent
    docs_dir = project_root / "docs"
    
    # Build only from docs/, which is the published GitHub Pages output tree.
    data_dir = docs_dir / "data"
    template_file = docs_dir / "template.html"
    output_file = docs_dir / "index.html"

    # Load all release files already copied into docs/data by the workflow.
    all_data = {}
    json_files = sorted(glob.glob(str(data_dir / "releases_*.json")))

    for filepath in json_files:
        data = load_release_file(filepath)
        if data and data.get("date"):
            all_data[data["date"]] = data

    print(f"Found {len(all_data)} days of release data")

    if not all_data:
        print("No data files found — building with empty data")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        all_data[today] = {
            "date": today,
            "total_releases": 0,
            "source_stats": {},
            "releases":[],
        }

    # ── Find the latest date ──
    latest_date = max(all_data.keys())
    latest_data = all_data[latest_date]
    print(f"Latest date: {latest_date} ({latest_data.get('total_releases', 0)} releases)")

    # ── Process latest releases for display ──
    latest_releases = process_releases(latest_data.get("releases",[]))

    # ── Process each historical date (smaller set for archive browsing) ──
    historical = {}
    for date_str, data in all_data.items():
        processed = process_releases(data.get("releases",[]), max_per_type=20, max_total=150)
        historical[date_str] = processed

    # ── Compute trending ──
    trending = compute_trending(all_data)
    print(f"Trending titles: {len(trending)}")

    # ── Compute archive stats ──
    archive = compute_archive_stats(all_data)
    print(f"Archive: {len(archive['dates'])} days, {len(archive['weekly'])} weeks, {len(archive['monthly'])} months")

    # ── Build injection payload ──
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

    # Read the template and inject a single JSON blob for the frontend app.
    if not template_file.exists():
        print(f"ERROR: Template not found at {template_file}")
        return False

    with open(template_file, "r", encoding="utf-8") as f:
        template = f.read()

    # The frontend reads this injected payload from the placeholder at build time.
    html = template.replace("__RELEASE_DATA_PLACEHOLDER__", json_str)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    file_size = output_file.stat().st_size / 1024
    print(f"Built {output_file} ({file_size:.0f} KB)")
    print(f"  Latest: {len(latest_releases)} releases from {latest_date}")
    print(f"  Historical dates: {len(all_data)}")
    print(f"  Trending: {len(trending)} titles")

    # Generate individual SEO release pages
    generate_release_pages(all_data, docs_dir)

    return True


def generate_release_pages(all_data, docs_dir):
    """Generate individual SEO-friendly HTML pages for each release."""
    releases_dir = docs_dir / "r"
    releases_dir.mkdir(exist_ok=True)
    
    count = 0
    sitemap_entries =[]
    
    for date_str, data in all_data.items():
        for r in data.get("releases",[]):
            title = r.get("title", "")
            if not title:
                continue
            
            # Generate URL-safe slug
            slug = title.lower().strip()
            slug = "".join(c if c.isalnum() or c == " " else "" for c in slug)
            slug = "-".join(slug.split())[:80]
            if not slug:
                continue
            
            media_type = r.get("media_type", "movie")
            page_slug = f"{date_str}-{media_type}-{slug}"
            
            synopsis = r.get("synopsis", "")
            genres = ", ".join(r.get("genres", [])[:5])
            poster = r.get("poster_url", "")
            meta = r.get("metadata", {})
            
            mc_labels = {"movie": "Film", "tv": "TV", "book": "Book", "game": "Game", 
                        "anime": "Anime", "music": "Music", "podcast": "Podcast",
                        "boardgame": "Board Game", "disc": "Physical", "news": "News"}
            type_label = mc_labels.get(media_type, media_type.title())
            
            desc = synopsis[:160] if synopsis else f"New {type_label.lower()} release: {title}"
            
            # Build minimal SEO page that redirects to main app
            html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} — {type_label} | UNREELED</title>
<meta name="description" content="{desc}">
<meta property="og:title" content="{title} — {type_label} Release">
<meta property="og:description" content="{desc}">
<meta property="og:type" content="website">
<meta property="og:url" content="https://unreeled.co.za/r/{page_slug}">
<meta property="og:site_name" content="UNREELED">
{f'<meta property="og:image" content="{poster}">' if poster else ''}
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{title} — {type_label} | UNREELED">
<meta name="twitter:description" content="{desc}">
{f'<meta name="twitter:image" content="{poster}">' if poster else ''}
<link rel="canonical" href="https://unreeled.co.za/r/{page_slug}">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#08090c;color:#e8e8ec;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;padding:20px}}
.card{{max-width:500px;width:100%;background:#0f1014;border:1px solid #1e1f28;border-radius:16px;overflow:hidden}}
.poster{{width:100%;height:280px;object-fit:cover;display:block}}
.info{{padding:20px}}
.type{{display:inline-block;padding:3px 10px;border-radius:4px;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.7px;background:rgba(255,107,53,0.15);color:#ff6b35;margin-bottom:10px}}
h1{{font-size:24px;font-weight:700;letter-spacing:-0.5px;margin-bottom:8px}}
.date{{font-size:13px;color:#5e5e6e;margin-bottom:12px}}
.desc{{font-size:14px;color:#a0a0ae;line-height:1.6;margin-bottom:16px}}
.genres{{font-size:12px;color:#5e5e6e;margin-bottom:16px}}
.cta{{display:inline-block;padding:12px 28px;background:#ff6b35;color:#fff;border-radius:8px;font-size:14px;font-weight:600;text-decoration:none}}
.cta:hover{{filter:brightness(1.1)}}
.footer{{text-align:center;padding:16px;font-size:12px;color:#5e5e6e}}
</style>
</head>
<body>
<div class="card">
{f'<img class="poster" src="{poster}" alt="{title}">' if poster else ''}
<div class="info">
<span class="type">{type_label}</span>
<h1>{title}</h1>
<div class="date">Released {date_str}</div>
{f'<p class="desc">{synopsis[:300]}</p>' if synopsis else ''}
{f'<div class="genres">{genres}</div>' if genres else ''}
<a class="cta" href="https://unreeled.co.za">View on UNREELED →</a>
</div>
<div class="footer">Track daily releases across movies, TV, books, games, anime &amp; more at <a href="https://unreeled.co.za" style="color:#ff6b35">unreeled.co.za</a></div>
</div>
</body>
</html>'''
            
            page_file = releases_dir / f"{page_slug}.html"
            with open(page_file, "w", encoding="utf-8") as f:
                f.write(html)
            
            sitemap_entries.append(f"https://unreeled.co.za/r/{page_slug}")
            count += 1
    
    # Generate sitemap
    sitemap_xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    sitemap_xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    sitemap_xml += '  <url><loc>https://unreeled.co.za</loc><changefreq>daily</changefreq><priority>1.0</priority></url>\n'
    for url in sitemap_entries[:5000]:  # Sitemap limit
        sitemap_xml += f'  <url><loc>{url}</loc><changefreq>weekly</changefreq></url>\n'
    sitemap_xml += '</urlset>'
    
    with open(docs_dir / "sitemap.xml", "w", encoding="utf-8") as f:
        f.write(sitemap_xml)
    
    # robots.txt
    robots = "User-agent: *\nAllow: /\nSitemap: https://unreeled.co.za/sitemap.xml\n"
    with open(docs_dir / "robots.txt", "w", encoding="utf-8") as f:
        f.write(robots)
    
    print(f"Generated {count} release pages + sitemap.xml + robots.txt")
    return count

# Executable block moved to the very bottom
if __name__ == "__main__":
    success = build()
    exit(0 if success else 1)
