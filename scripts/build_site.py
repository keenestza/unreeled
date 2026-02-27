"""
Build script for Unreeled static site.

Reads the latest release JSON from public/data/latest.json
and injects it into the HTML template to produce public/index.html.

This runs both locally and in GitHub Actions.

Usage:
    python scripts/build_site.py
"""

import json
import os
from pathlib import Path
from datetime import datetime, timezone


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def build():
    project_root = Path(__file__).parent.parent
    data_file = project_root / "public" / "data" / "latest.json"
    template_file = project_root / "public" / "template.html"
    output_file = project_root / "public" / "index.html"

    # Load release data
    if data_file.exists():
        with open(data_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"Loaded {data['total_releases']} releases from {data['date']}")
    else:
        print("No data file found — building with empty data")
        data = {
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "total_releases": 0,
            "source_stats": {},
            "releases": [],
        }

    # Prepare releases for injection — strip very long synopses
    # and limit to top 200 by popularity to keep file size reasonable
    releases = data.get("releases", [])
    for r in releases:
        syn = r.get("synopsis", "")
        if len(syn) > 500:
            r["synopsis"] = syn[:497] + "..."

    # Ensure all media types are represented in the cap
    # Take top items per type, then fill remaining slots by score
    MAX_TOTAL = 200
    by_type = {}
    for r in releases:
        by_type.setdefault(r.get("media_type", "other"), []).append(r)

    # Sort each type by quality score
    def sort_key(r):
        score = r.get("metadata", {}).get("popularity", 0) or 0
        if r.get("synopsis"):
            score += 50
        if r.get("poster_url"):
            score += 30
        return score

    for items in by_type.values():
        items.sort(key=sort_key, reverse=True)

    # Guarantee at least 15 per type (or all if fewer), then fill the rest
    guaranteed_per_type = 15
    selected = []
    remaining = []

    for media_type, items in by_type.items():
        guaranteed = items[:guaranteed_per_type]
        overflow = items[guaranteed_per_type:]
        selected.extend(guaranteed)
        remaining.extend(overflow)

    # Fill remaining slots with highest-scoring items across all types
    remaining.sort(key=sort_key, reverse=True)
    slots_left = MAX_TOTAL - len(selected)
    if slots_left > 0:
        selected.extend(remaining[:slots_left])

    # Final sort by score for display order
    selected.sort(key=sort_key, reverse=True)
    releases = selected

    # Build the JSON string to inject
    inject_data = {
        "date": data.get("date", ""),
        "total_releases": len(releases),
        "source_stats": data.get("source_stats", {}),
        "built_at": utcnow_iso(),
        "releases": releases,
    }

    json_str = json.dumps(inject_data, ensure_ascii=False, separators=(",", ":"))

    # Read template
    if not template_file.exists():
        print(f"ERROR: Template not found at {template_file}")
        return False

    with open(template_file, "r", encoding="utf-8") as f:
        template = f.read()

    # Inject data — replace the placeholder
    html = template.replace("__RELEASE_DATA_PLACEHOLDER__", json_str)

    # Write output
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    file_size = output_file.stat().st_size / 1024
    print(f"Built {output_file} ({file_size:.0f} KB)")
    print(f"  {len(releases)} releases from {data.get('date', 'unknown')}")
    print(f"  Source stats: {data.get('source_stats', {})}")
    return True


if __name__ == "__main__":
    success = build()
    exit(0 if success else 1)
