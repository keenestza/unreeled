"""
Email Digest — sends daily release summaries to subscribers.

Uses:
- Supabase to fetch subscribers and their preferences
- Resend API (free 100 emails/day) to send HTML emails
- Release data from the latest JSON

Usage:
    python scripts/send_digest.py

Env vars needed:
    RESEND_API_KEY — from resend.com/api-keys
    SUPABASE_URL — your Supabase project URL
    SUPABASE_SERVICE_KEY — service role key (NOT anon key, needed to read all subscriptions)
"""

import json
import os
import sys
import requests
from pathlib import Path
from datetime import datetime, timezone


RESEND_KEY = os.environ.get("RESEND_API_KEY", "")
SB_URL = os.environ.get("SUPABASE_URL", "")
SB_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
FROM_EMAIL = "Unreeled <digest@unreeled.app>"
SITE_URL = "https://keenestza.github.io/unreeled/"

MC = {
    "movie": {"icon": "🎬", "label": "Film", "color": "#6366f1"},
    "tv": {"icon": "📺", "label": "TV", "color": "#a855f7"},
    "book": {"icon": "📖", "label": "Book", "color": "#14b8a6"},
    "game": {"icon": "🎮", "label": "Game", "color": "#f43f5e"},
    "anime": {"icon": "🎌", "label": "Anime", "color": "#ec4899"},
    "music": {"icon": "🎵", "label": "Music", "color": "#eab308"},
    "podcast": {"icon": "🎙", "label": "Podcast", "color": "#8b5cf6"},
    "news": {"icon": "📰", "label": "News", "color": "#64748b"},
}


def load_latest_releases():
    """Load latest release data."""
    data_dir = Path(__file__).parent.parent / "docs" / "data"
    latest = data_dir / "latest.json"
    if not latest.exists():
        print("No latest.json found")
        return [], ""

    with open(latest, "r", encoding="utf-8") as f:
        data = json.load(f)

    releases = data.get("releases", [])
    date = data.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    return releases, date


def get_subscribers():
    """Fetch all subscribers and their subscription preferences from Supabase."""
    if not SB_URL or not SB_KEY:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY")
        return []

    headers = {
        "apikey": SB_KEY,
        "Authorization": f"Bearer {SB_KEY}",
    }

    # Get all subscriptions joined with profile info
    resp = requests.get(
        f"{SB_URL}/rest/v1/subscriptions?select=*,profiles(username)",
        headers=headers,
        timeout=15,
    )
    if resp.status_code != 200:
        print(f"Failed to fetch subscriptions: {resp.status_code} {resp.text}")
        return []

    subs = resp.json()

    # Get user emails (need auth admin API)
    # Group subs by user_id
    user_subs = {}
    for s in subs:
        uid = s["user_id"]
        if uid not in user_subs:
            user_subs[uid] = {
                "user_id": uid,
                "username": (s.get("profiles") or {}).get("username", "User"),
                "types": [],
            }
        if s["subscription_type"] == "media_type":
            user_subs[uid]["types"].append(s["subscription_value"])

    # Get emails via auth admin
    for uid in user_subs:
        resp = requests.get(
            f"{SB_URL}/auth/v1/admin/users/{uid}",
            headers={**headers, "Authorization": f"Bearer {SB_KEY}"},
            timeout=15,
        )
        if resp.status_code == 200:
            user_data = resp.json()
            user_subs[uid]["email"] = user_data.get("email", "")

    return [v for v in user_subs.values() if v.get("email") and v.get("types")]


def build_email_html(username, releases_by_type, date):
    """Build a nice HTML email for the digest."""
    total = sum(len(r) for r in releases_by_type.values())
    formatted_date = datetime.strptime(date, "%Y-%m-%d").strftime("%B %d, %Y")

    sections = ""
    for mtype, releases in releases_by_type.items():
        mc = MC.get(mtype, {"icon": "📦", "label": mtype, "color": "#666"})
        items = ""
        for r in releases[:8]:  # Max 8 per type
            title = r.get("title", "Unknown")
            synopsis = (r.get("synopsis", "") or "")[:120]
            genres = ", ".join(r.get("genres", [])[:3])
            meta_parts = []
            m = r.get("metadata", {})
            if mtype == "tv" and m.get("networks"):
                meta_parts.append(m["networks"][0])
            if mtype == "movie" and m.get("runtime_minutes"):
                meta_parts.append(f"{m['runtime_minutes']} min")
            if mtype == "music" and m.get("artists"):
                meta_parts.append(m["artists"][0])
            if mtype == "book" and m.get("authors"):
                meta_parts.append(m["authors"][0])
            meta_str = " · ".join(meta_parts)

            items += f"""
            <tr>
              <td style="padding:8px 0;border-bottom:1px solid #1e1f28">
                <div style="font-size:14px;font-weight:600;color:#e8e8ec">{title}</div>
                {f'<div style="font-size:11px;color:#a0a0ae;margin-top:2px">{meta_str}</div>' if meta_str else ''}
                {f'<div style="font-size:12px;color:#5e5e6e;margin-top:4px">{synopsis}{"..." if len(r.get("synopsis","")or"") > 120 else ""}</div>' if synopsis else ''}
                {f'<div style="font-size:10px;color:#5e5e6e;margin-top:3px">{genres}</div>' if genres else ''}
              </td>
            </tr>"""

        more = len(releases) - 8
        if more > 0:
            items += f'<tr><td style="padding:8px 0;font-size:12px;color:#5e5e6e">+ {more} more {mc["label"].lower()} releases</td></tr>'

        sections += f"""
        <div style="margin-bottom:24px">
          <div style="font-size:16px;font-weight:700;color:{mc['color']};margin-bottom:8px">{mc['icon']} {mc['label']} ({len(releases)})</div>
          <table style="width:100%">{items}</table>
        </div>"""

    return f"""
    <div style="max-width:560px;margin:0 auto;background:#08090c;color:#e8e8ec;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;border-radius:12px;overflow:hidden">
      <div style="background:#ff6b35;padding:20px 24px">
        <div style="font-size:22px;font-weight:700;color:#fff">UNREELED</div>
        <div style="font-size:13px;color:rgba(255,255,255,0.8);margin-top:4px">Daily Digest — {formatted_date}</div>
      </div>
      <div style="padding:24px">
        <div style="font-size:15px;margin-bottom:20px">Hey {username} 👋</div>
        <div style="font-size:14px;color:#a0a0ae;margin-bottom:20px">
          Here are today's <strong style="color:#e8e8ec">{total} releases</strong> in your subscribed categories:
        </div>
        {sections}
        <div style="text-align:center;margin-top:24px;padding-top:20px;border-top:1px solid #1e1f28">
          <a href="{SITE_URL}" style="display:inline-block;padding:10px 24px;background:#ff6b35;color:#fff;text-decoration:none;border-radius:8px;font-weight:600;font-size:14px">View All Releases →</a>
        </div>
        <div style="text-align:center;margin-top:20px;font-size:11px;color:#5e5e6e">
          You're receiving this because you subscribed on Unreeled.<br>
          Manage your subscriptions at {SITE_URL}
        </div>
      </div>
    </div>"""


def send_email(to_email, subject, html_body):
    """Send email via Resend API."""
    if not RESEND_KEY:
        print(f"  SKIP (no RESEND_API_KEY): {to_email}")
        return False

    resp = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "from": FROM_EMAIL,
            "to": [to_email],
            "subject": subject,
            "html": html_body,
        },
        timeout=15,
    )

    if resp.status_code in (200, 201):
        print(f"  ✓ Sent to {to_email}")
        return True
    else:
        print(f"  ✗ Failed for {to_email}: {resp.status_code} {resp.text}")
        return False


def main():
    print("📧 Unreeled Email Digest")
    print("=" * 40)

    releases, date = load_latest_releases()
    if not releases:
        print("No releases found — skipping digest")
        return

    print(f"Date: {date} | Releases: {len(releases)}")

    # Group releases by media type
    by_type = {}
    for r in releases:
        mt = r.get("media_type", "")
        if mt not in by_type:
            by_type[mt] = []
        by_type[mt].append(r)

    subscribers = get_subscribers()
    if not subscribers:
        print("No subscribers found — skipping digest")
        return

    print(f"Subscribers: {len(subscribers)}")
    sent = 0
    failed = 0

    for sub in subscribers:
        # Filter releases to subscriber's chosen types
        sub_releases = {}
        for mt in sub["types"]:
            if mt in by_type and by_type[mt]:
                sub_releases[mt] = by_type[mt]

        if not sub_releases:
            print(f"  SKIP {sub['email']}: no matching releases")
            continue

        total = sum(len(r) for r in sub_releases.values())
        subject = f"📬 {total} new releases today — {date}"
        html = build_email_html(sub["username"], sub_releases, date)

        if send_email(sub["email"], subject, html):
            sent += 1
        else:
            failed += 1

    print(f"\nDone: {sent} sent, {failed} failed")


if __name__ == "__main__":
    main()
