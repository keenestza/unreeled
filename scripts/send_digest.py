"""
Unreeled - Daily Email Digest
Sends personalized emails to users about new releases matching their
watchlist items and subscriptions.

Requires:
  RESEND_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_KEY (service role)

Usage: python scripts/send_digest.py
"""

import os, json, logging, requests
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("unreeled_digest")

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SITE_URL = "https://unreeled.co.za"
FROM_EMAIL = "Unreeled <digest@unreeled.co.za>"

MC = {
    "movie": {"icon": "\U0001f3ac", "label": "Film", "color": "#6366f1"},
    "tv": {"icon": "\U0001f4fa", "label": "TV", "color": "#a855f7"},
    "book": {"icon": "\U0001f4d6", "label": "Book", "color": "#14b8a6"},
    "game": {"icon": "\U0001f3ae", "label": "Game", "color": "#f43f5e"},
    "anime": {"icon": "\U0001f38c", "label": "Anime", "color": "#ec4899"},
    "music": {"icon": "\U0001f3b5", "label": "Music", "color": "#eab308"},
    "podcast": {"icon": "\U0001f399", "label": "Podcast", "color": "#8b5cf6"},
    "boardgame": {"icon": "\U0001f3b2", "label": "Board Game", "color": "#06b6d4"},
    "disc": {"icon": "\U0001f4bf", "label": "Physical", "color": "#0ea5e9"},
    "news": {"icon": "\U0001f4f0", "label": "News", "color": "#64748b"},
}

def sb_get(endpoint, params=None):
    headers = {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}", "Content-Type": "application/json"}
    resp = requests.get(f"{SUPABASE_URL}/rest/v1/{endpoint}", headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def load_todays_releases():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = os.path.join("docs", "data", f"releases_{today}.json")
    if not os.path.exists(path):
        logger.warning(f"No release file: {path}")
        return [], today
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    releases = data.get("releases", [])
    logger.info(f"Loaded {len(releases)} releases for {today}")
    return releases, today

def get_user_emails():
    headers = {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}
    resp = requests.get(f"{SUPABASE_URL}/auth/v1/admin/users", headers=headers, params={"per_page": 1000}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    users = data.get("users", data) if isinstance(data, dict) else data
    return {u["id"]: u.get("email", "") for u in users if u.get("email")}

def match_releases(releases, subs, watchlist):
    matched = {"watchlist": [], "subscription": []}
    sub_types = {s["subscription_value"] for s in subs if s["subscription_type"] == "media_type"}
    sub_genres = {s["subscription_value"].lower() for s in subs if s["subscription_type"] == "genre"}
    wl_titles = {w["release_title"].lower() for w in watchlist}

    for r in releases:
        title_lower = (r.get("title") or "").lower()
        if title_lower in wl_titles:
            matched["watchlist"].append(r)
            continue
        if r.get("media_type") in sub_types:
            matched["subscription"].append(r)
            continue
        release_genres = {g.lower() for g in (r.get("genres") or [])}
        if release_genres & sub_genres:
            matched["subscription"].append(r)
    return matched



def tv_badge_html(r):
    meta = r.get("metadata") or {}
    if r.get("media_type") != "tv":
        return ""
    kind = meta.get("tv_release_kind", "")
    base = "display:inline-block;padding:2px 6px;border-radius:4px;font-size:10px;font-weight:700;margin-left:8px;color:#fff;vertical-align:middle"
    if kind == "series_premiere":
        return f'<span style="{base};background:#16a34a">Brand New Series</span>'
    if kind == "season_premiere":
        return f'<span style="{base};background:#2563eb">New Season</span>'
    if kind == "new_episode":
        return f'<span style="{base};background:#7c3aed">New Episode</span>'
    return ""

def build_email_html(username, matched, date):
    wl = matched["watchlist"]
    sub = matched["subscription"][:20]
    seen = set()
    unique_wl, unique_sub = [], []
    for r in wl:
        k = (r.get("title",""), r.get("media_type",""))
        if k not in seen: seen.add(k); unique_wl.append(r)
    for r in sub:
        k = (r.get("title",""), r.get("media_type",""))
        if k not in seen: seen.add(k); unique_sub.append(r)
    if not unique_wl and not unique_sub:
        return None

    fdate = datetime.strptime(date, "%Y-%m-%d").strftime("%B %d, %Y")
    html = f'''<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#08090c;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
<div style="max-width:600px;margin:0 auto;padding:20px">
<div style="text-align:center;padding:24px 0;border-bottom:1px solid #1e1f28">
<div style="display:inline-block;background:#ff6b35;border-radius:8px;padding:4px 12px;margin-bottom:8px"><span style="color:#fff;font-size:18px;font-weight:800">U</span></div>
<h1 style="color:#e8e8ec;font-size:24px;font-weight:700;margin:8px 0 4px;letter-spacing:-1px">Your Daily Digest</h1>
<p style="color:#5e5e6e;font-size:13px;margin:0">{fdate}</p></div>
<p style="color:#a0a0ae;font-size:14px;padding:20px 0 0">Hey {username} \U0001f44b</p>'''

    if unique_wl:
        html += '<div style="padding:16px 0"><h2 style="color:#ff6b35;font-size:16px;font-weight:700;margin:0 0 12px">\u2b50 From Your Watchlist</h2>'
        for r in unique_wl:
            mc = MC.get(r.get("media_type","movie"), MC["movie"])
            title = r.get("title","Unknown")
            syn = (r.get("synopsis") or "")[:120]
            if len(r.get("synopsis") or "") > 120: syn += "..."
            genres = ", ".join((r.get("genres") or [])[:3])
            html += f'<div style="background:#0f1014;border:1px solid #1e1f28;border-left:3px solid {mc["color"]};border-radius:8px;padding:14px;margin-bottom:8px"><span style="font-size:15px;font-weight:600;color:#e8e8ec">{title}</span>{tv_badge_html(r)}<span style="background:rgba(255,255,255,0.08);padding:2px 6px;border-radius:3px;font-size:10px;color:{mc["color"]};margin-left:8px">{mc["label"]}</span>'
            if syn: html += f'<p style="color:#a0a0ae;font-size:12px;margin:6px 0 0;line-height:1.5">{syn}</p>'
            if genres: html += f'<p style="color:#5e5e6e;font-size:11px;margin:4px 0 0">{genres}</p>'
            html += '</div>'
        html += '</div>'

    if unique_sub:
        html += '<div style="padding:16px 0"><h2 style="color:#a0a0ae;font-size:16px;font-weight:700;margin:0 0 12px">\U0001f4e1 From Your Subscriptions</h2>'
        for r in unique_sub:
            mc = MC.get(r.get("media_type","movie"), MC["movie"])
            title = r.get("title","Unknown")
            genres = ", ".join((r.get("genres") or [])[:3])
            html += f'<div style="background:#0f1014;border:1px solid #1e1f28;border-left:3px solid {mc["color"]};border-radius:8px;padding:12px;margin-bottom:6px"><span style="font-size:14px;font-weight:600;color:#e8e8ec">{mc["icon"]} {title}</span>{tv_badge_html(r)}'
            if genres: html += f'<span style="color:#5e5e6e;font-size:11px;margin-left:8px">{genres}</span>'
            html += '</div>'
        html += '</div>'

    total = len(unique_wl) + len(unique_sub)
    html += f'''<div style="text-align:center;padding:24px 0"><a href="{SITE_URL}" style="display:inline-block;background:#ff6b35;color:#fff;padding:12px 32px;border-radius:8px;font-size:14px;font-weight:600;text-decoration:none">View All Releases \u2192</a></div>
<div style="border-top:1px solid #1e1f28;padding:20px 0;text-align:center"><p style="color:#5e5e6e;font-size:11px;margin:0">You have {total} matching release{"s" if total!=1 else ""} today.<br><a href="{SITE_URL}" style="color:#ff6b35;text-decoration:none">Manage your subscriptions</a> to change what you receive.<br><br><a href="{SITE_URL}" style="color:#5e5e6e;text-decoration:underline">Unsubscribe from daily emails</a> — go to My Account and toggle off the Daily Email Digest.</p></div>
</div></body></html>'''
    return html

def send_email(to, subject, html):
    resp = requests.post("https://api.resend.com/emails", headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
        json={"from": FROM_EMAIL, "to": [to], "subject": subject, "html": html}, timeout=30)
    if resp.status_code in (200, 201): return True
    logger.error(f"Resend error {to}: {resp.status_code} {resp.text}")
    return False

def main():
    if not RESEND_API_KEY: logger.error("RESEND_API_KEY not set"); return
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY: logger.error("Supabase creds not set"); return

    releases, today = load_todays_releases()
    if not releases: logger.info("No releases — skipping"); return

    subs = sb_get("subscriptions", {"select": "user_id,subscription_type,subscription_value", "email_digest": "eq.true"})
    watchlist = sb_get("watchlist", {"select": "user_id,release_title,media_type"})
    profiles = sb_get("profiles", {"select": "id,username"})
    profile_map = {p["id"]: p["username"] for p in profiles}
    emails = get_user_emails()

    # Group by user — only users with digest-enabled subs get emails
    users = {}
    digest_users = set()
    for s in subs:
        users.setdefault(s["user_id"], {"subs": [], "wl": []})["subs"].append(s)
        digest_users.add(s["user_id"])
    for w in watchlist:
        # Only include watchlist for users who opted into digest
        if w["user_id"] in digest_users:
            users.setdefault(w["user_id"], {"subs": [], "wl": []})["wl"].append(w)

    logger.info(f"{len(emails)} users with emails, {len(users)} with subs/watchlist")

    sent = skipped = errors = 0
    fdate = datetime.strptime(today, "%Y-%m-%d").strftime("%b %d")

    for uid, data in users.items():
        email = emails.get(uid)
        if not email: skipped += 1; continue

        matched = match_releases(releases, data["subs"], data["wl"])
        if not matched["watchlist"] and not matched["subscription"]: skipped += 1; continue

        username = profile_map.get(uid, "there")
        html = build_email_html(username, matched, today)
        if not html: skipped += 1; continue

        wc, sc = len(matched["watchlist"]), len(matched["subscription"])
        parts = []
        if wc: parts.append(f"{wc} watchlisted")
        if sc: parts.append(f"{sc} new")
        subject = f"\U0001f3ac {' + '.join(parts)} release{'s' if (wc+sc)>1 else ''} — {fdate}"

        if send_email(email, subject, html):
            sent += 1; logger.info(f"Sent to {email} ({wc} wl, {sc} sub)")
        else: errors += 1

    logger.info(f"Done: {sent} sent, {skipped} skipped, {errors} errors")

    # Weekly recap on Fridays
    try:
        send_weekly_recap()
    except Exception as e:
        logger.error(f"Weekly recap failed: {e}")

if __name__ == "__main__":
    main()


def send_weekly_recap():
    """Send Friday weekly recap email with the week's top releases."""
    today = datetime.now(timezone.utc)
    if today.weekday() != 4:  # Only on Fridays
        logger.info("Not Friday — skipping weekly recap")
        return

    # Load all releases from this week
    week_releases = []
    for i in range(7):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        path = os.path.join("docs", "data", f"releases_{d}.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            week_releases.extend(data.get("releases", []))

    if not week_releases:
        logger.info("No releases this week")
        return

    # Get top 10 by popularity
    week_releases.sort(key=lambda r: (r.get("metadata") or {}).get("popularity", 0), reverse=True)
    top10 = week_releases[:10]

    # Count by type
    type_counts = {}
    for r in week_releases:
        t = r.get("media_type", "other")
        type_counts[t] = type_counts.get(t, 0) + 1

    week_start = (today - timedelta(days=6)).strftime("%b %d")
    week_end = today.strftime("%b %d, %Y")

    # Build email
    html = f'''<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#08090c;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
<div style="max-width:600px;margin:0 auto;padding:20px">
<div style="text-align:center;padding:24px 0;border-bottom:1px solid #1e1f28">
<div style="display:inline-block;background:#ff6b35;border-radius:8px;padding:4px 12px;margin-bottom:8px"><span style="color:#fff;font-size:18px;font-weight:800">U</span></div>
<h1 style="color:#e8e8ec;font-size:24px;font-weight:700;margin:8px 0 4px;letter-spacing:-1px">\U0001f3ac Weekly Recap</h1>
<p style="color:#5e5e6e;font-size:13px;margin:0">{week_start} — {week_end}</p></div>

<div style="padding:20px 0">
<p style="color:#a0a0ae;font-size:14px;margin-bottom:16px">This week on Unreeled: <b style="color:#e8e8ec">{len(week_releases)} releases</b> across {len(type_counts)} media types.</p>

<div style="display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px">'''

    for t, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        mc = MC.get(t, MC.get("movie"))
        html += f'<span style="padding:4px 10px;background:#16171d;border:1px solid #1e1f28;border-radius:6px;font-size:12px;color:#a0a0ae">{mc["icon"]} {mc["label"]}: {count}</span>'

    html += '''</div>
<h2 style="color:#ff6b35;font-size:16px;font-weight:700;margin:0 0 12px">\U0001f525 Top 10 This Week</h2>'''

    for i, r in enumerate(top10):
        mc = MC.get(r.get("media_type", "movie"), MC["movie"])
        title = r.get("title", "Unknown")
        syn = (r.get("synopsis") or "")[:100]
        if len(r.get("synopsis") or "") > 100:
            syn += "..."
        poster = r.get("poster_url", "")

        html += f'''<div style="display:flex;gap:12px;padding:12px;background:#0f1014;border:1px solid #1e1f28;border-left:3px solid {mc["color"]};border-radius:8px;margin-bottom:8px">
<div style="width:28px;height:28px;border-radius:6px;background:rgba(255,107,53,0.12);display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;color:#ff6b35;flex-shrink:0">{i+1}</div>
<div style="flex:1;min-width:0"><div style="font-size:14px;font-weight:600;color:#e8e8ec;margin-bottom:2px">{title} <span style="font-size:10px;color:{mc["color"]}">{mc["label"]}</span></div>
{f'<div style="font-size:12px;color:#5e5e6e;line-height:1.4">{syn}</div>' if syn else ''}</div></div>'''

    html += f'''</div>
<div style="text-align:center;padding:24px 0"><a href="{SITE_URL}" style="display:inline-block;background:#ff6b35;color:#fff;padding:12px 32px;border-radius:8px;font-size:14px;font-weight:600;text-decoration:none">Explore All Releases \u2192</a></div>
<div style="border-top:1px solid #1e1f28;padding:20px 0;text-align:center"><p style="color:#5e5e6e;font-size:11px;margin:0">Weekly recap from <a href="{SITE_URL}" style="color:#ff6b35;text-decoration:none">unreeled.co.za</a><br><a href="{SITE_URL}" style="color:#5e5e6e;text-decoration:underline">Unsubscribe</a> — toggle off in My Account.</p></div>
</div></body></html>'''

    # Send to all users with at least one digest-enabled subscription
    subs = sb_get("subscriptions", {"select": "user_id", "email_digest": "eq.true"})
    recap_users = list({s["user_id"] for s in subs})
    emails_map = get_user_emails()
    profiles = sb_get("profiles", {"select": "id,username"})
    profile_map = {p["id"]: p["username"] for p in profiles}

    sent = 0
    subject = f"\U0001f525 Unreeled Weekly Recap — {week_start} to {week_end}"
    for uid in recap_users:
        email = emails_map.get(uid)
        if not email:
            continue
        if send_email(email, subject, html):
            sent += 1

    logger.info(f"Weekly recap: sent to {sent} users")
