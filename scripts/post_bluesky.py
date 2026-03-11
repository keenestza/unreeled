import json
import os
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

    media_type = r.get("media_type", "")
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


def pick_highlights(releases: list[dict], limit: int = 4) -> list[dict]:
    by_type_limit = {
        "tv": 2,
        "movie": 1,
        "game": 1,
        "anime": 1,
        "music": 1,
        "book": 1,
        "podcast": 1,
        "news": 1,
        "disc": 1,
    }

    picked: list[dict] = []
    type_count: dict[str, int] = {}

    candidates = [r for r in releases if r.get("title")]
    candidates.sort(key=score_release, reverse=True)

    for r in candidates:
        if len(picked) >= limit:
            break
        media_type = r.get("media_type", "other")
        allowed = by_type_limit.get(media_type, 1)
        if type_count.get(media_type, 0) >= allowed:
            continue
        type_count[media_type] = type_count.get(media_type, 0) + 1
        picked.append(r)

    return picked


def build_post(data: dict) -> str:
    releases = data.get("releases", [])
    date_str = data.get("date", "")
    highlights = pick_highlights(releases, limit=4)

    if not highlights:
        return "Today on Unreeled:\nSee all today's releases:\nhttps://unreeled.co.za/"

    lines = ["Today on Unreeled:"]
    for r in highlights:
        title = (r.get("title") or "").strip()
        suffix = tv_suffix(r)
        lines.append(f"• {title}{suffix}")

    lines.append("")
    lines.append("See all today's releases:")
    lines.append("https://unreeled.co.za/")

    post = "\n".join(lines)

    if len(post) <= 300:
        return post

    # Trim gracefully for Bluesky character limits
    trimmed = ["Today on Unreeled:"]
    for r in highlights:
        title = (r.get("title") or "").strip()
        suffix = tv_suffix(r)
        line = f"• {title}{suffix}"
        candidate = "\n".join(trimmed + [line, "", "https://unreeled.co.za/"])
        if len(candidate) > 300:
            break
        trimmed.append(line)

    trimmed.append("")
    trimmed.append("https://unreeled.co.za/")
    return "\n".join(trimmed)


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
        "record": {
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
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
