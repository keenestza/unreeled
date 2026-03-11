import json
import os
import re
from pathlib import Path
from datetime import datetime, timezone

import requests


BLUESKY_PDS = "https://bsky.social"


def load_latest_data() -> dict:
    root = Path(__file__).resolve().parent.parent
    latest_file = root / "docs" / "data" / "latest.json"
    with open(latest_file, "r", encoding="utf-8") as f:
        return json.load(f)


def score_release(r: dict) -> float:
    meta = r.get("metadata", {}) or {}
    score = float(meta.get("popularity", 0) or 0)

    if r.get("poster_url"):
        score += 40
    if r.get("synopsis"):
        score += 30

    media_type = (r.get("media_type") or "").lower()
    if media_type == "movie":
        score += 18
    elif media_type == "game":
        score += 16
    elif media_type == "anime":
        score += 12
    elif media_type == "music":
        score += 10
    elif media_type == "tv":
        score += 8
    elif media_type == "book":
        score += 7
    elif media_type == "podcast":
        score += 6
    elif media_type == "news":
        score += 5

    tv_kind = (meta.get("tv_release_kind") or "").lower()
    if tv_kind == "series_premiere":
        score += 35
    elif tv_kind == "season_premiere":
        score += 24
    elif tv_kind == "new_episode":
        score += 10

    return score


def tv_suffix(r: dict) -> str:
    tv_kind = (((r.get("metadata") or {}).get("tv_release_kind")) or "").lower()
    if tv_kind == "series_premiere":
        return " — Brand New Series"
    if tv_kind == "season_premiere":
        return " — New Season"
    if tv_kind == "new_episode":
        return " — New Episode"
    return ""


def media_emoji(r: dict) -> str:
    media_type = (r.get("media_type") or "").lower()
    return {
        "movie": "🎬",
        "tv": "📺",
        "game": "🎮",
        "book": "📚",
        "anime": "🎌",
        "music": "🎵",
        "podcast": "🎙️",
        "news": "📰",
        "disc": "💿",
        "boardgame": "🎲",
    }.get(media_type, "✨")


def normalize_group(r: dict) -> str:
    media_type = (r.get("media_type") or "").lower()
    if media_type == "disc":
        return "movie"
    return media_type or "other"


def pick_highlights(releases: list[dict], limit: int = 4) -> list[dict]:
    """
    Prefer one different highlight per category where possible.
    Falls back to strongest remaining releases if needed.
    """
    candidates = [r for r in releases if r.get("title")]
    candidates.sort(key=score_release, reverse=True)

    preferred_order = [
        "tv",
        "movie",
        "game",
        "music",
        "anime",
        "book",
        "podcast",
        "news",
        "boardgame",
    ]

    picked: list[dict] = []
    used_titles: set[str] = set()

    # First pass: one per category
    for group in preferred_order:
        for r in candidates:
            title_key = (r.get("title") or "").strip().lower()
            norm_group = normalize_group(r)
            if not title_key or title_key in used_titles:
                continue
            if norm_group != group:
                continue
            picked.append(r)
            used_titles.add(title_key)
            break
        if len(picked) >= limit:
            return picked[:limit]

    # Second pass: strongest remaining, still avoiding duplicate titles
    for r in candidates:
        if len(picked) >= limit:
            break
        title_key = (r.get("title") or "").strip().lower()
        if not title_key or title_key in used_titles:
            continue
        picked.append(r)
        used_titles.add(title_key)

    return picked[:limit]


def build_post(data: dict) -> str:
    releases = data.get("releases", [])
    highlights = pick_highlights(releases, limit=4)

    if not highlights:
        return "Today on Unreeled:\n✨ See all today's releases:\nhttps://unreeled.co.za/"

    lines = ["Today on Unreeled:"]
    for r in highlights:
        title = (r.get("title") or "").strip()
        suffix = tv_suffix(r)
        emoji = media_emoji(r)
        lines.append(f"{emoji} {title}{suffix}")

    lines.append("")
    lines.append("🔗 See all today's releases:")
    lines.append("https://unreeled.co.za/")

    post = "\n".join(lines)

    if len(post) <= 300:
        return post

    # If too long, trim gracefully and keep category variety
    trimmed = ["Today on Unreeled:"]
    for r in highlights:
        title = (r.get("title") or "").strip()
        suffix = tv_suffix(r)
        emoji = media_emoji(r)
        line = f"{emoji} {title}{suffix}"
        candidate = "\n".join(trimmed + [line, "", "🔗 https://unreeled.co.za/"])
        if len(candidate) > 300:
            break
        trimmed.append(line)

    if len(trimmed) == 1:
        top = highlights[0]
        short = f"{media_emoji(top)} {(top.get('title') or '').strip()}"
        return f"Today on Unreeled:\n{short}\n\n🔗 https://unreeled.co.za/"

    trimmed.append("")
    trimmed.append("🔗 https://unreeled.co.za/")
    return "\n".join(trimmed)


def parse_url_facets(text: str) -> list[dict]:
    """
    Build Bluesky rich-text link facets so URLs become clickable.
    Facet indexes must use UTF-8 byte offsets.
    """
    facets = []
    text_bytes = text.encode("UTF-8")
    url_regex = rb"(https?:\/\/[^\s]+)"

    for m in re.finditer(url_regex, text_bytes):
        url = m.group(1).decode("UTF-8")
        facets.append({
            "index": {
                "byteStart": m.start(1),
                "byteEnd": m.end(1),
            },
            "features": [
                {
                    "$type": "app.bsky.richtext.facet#link",
                    "uri": url,
                }
            ],
        })

    return facets


def build_post_record(text: str) -> dict:
    return {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "facets": parse_url_facets(text),
    }


def create_session(handle: str, app_password: str) -> dict:
    url = f"{BLUESKY_PDS}/xrpc/com.atproto.server.createSession"
    resp = requests.post(
        url,
        json={"identifier": handle, "password": app_password},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def create_post(handle: str, access_jwt: str, text: str) -> dict:
    url = f"{BLUESKY_PDS}/xrpc/com.atproto.repo.createRecord"

    payload = {
        "repo": handle,
        "collection": "app.bsky.feed.post",
        "record": build_post_record(text),
    }

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {access_jwt}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    handle = os.environ.get("BLUESKY_HANDLE", "").strip()
    app_password = os.environ.get("BLUESKY_APP_PASSWORD", "").strip()

    if not handle or not app_password:
        print("Bluesky credentials missing; skipping post.")
        return 0

    data = load_latest_data()
    post_text = build_post(data)

    print("Posting to Bluesky:")
    print(post_text)

    session = create_session(handle, app_password)
    access_jwt = session["accessJwt"]
    result = create_post(handle, access_jwt, post_text)

    print("Bluesky post created.")
    print(result.get("uri", ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
