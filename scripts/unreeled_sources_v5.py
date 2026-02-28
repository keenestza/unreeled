"""
UNREELED — Additional Data Sources v5
======================================

New content sources:
  - Podcast Index — New podcast episodes
  - Board Game Geek — Board game releases (no key needed)
  - Comic Vine — Comics and graphic novel releases
  - RAWG — Additional game data and screenshots
  - NewsData.io — Entertainment news headlines

Enrichment sources (enhance existing releases):
  - OMDb — Rotten Tomatoes / Metacritic scores
  - TasteDive — "If you liked X" recommendations
  - Watchmode — Where to stream/buy each title

All sources are optional — if an API key is missing, the source is skipped.
"""

import os
import json
import time
import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Optional

try:
    import requests
except ImportError:
    print("Please install requests: pip install requests")
    exit(1)

logger = logging.getLogger("unreeled")

REQUEST_DELAY = 0.3


def rate_limit(delay=REQUEST_DELAY):
    time.sleep(delay)


def safe_str(val):
    if val is None:
        return ""
    return str(val)


def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()


def make_release(source, media_type, title, release_date, synopsis="",
                 genres=None, metadata=None, poster_url="", external_ids=None):
    return {
        "source": source,
        "media_type": media_type,
        "title": title,
        "release_date": release_date,
        "synopsis": safe_str(synopsis),
        "genres": genres or [],
        "metadata": metadata or {},
        "poster_url": safe_str(poster_url),
        "external_ids": external_ids or {},
        "ingested_at": utcnow_iso(),
        "comment_count": 0,
        "spoiler_counts": {"light": 0, "medium": 0, "heavy": 0},
    }


# ═══════════════════════════════════════════════════════════════
# SOURCE: PODCAST INDEX (New podcast episodes)
# ═══════════════════════════════════════════════════════════════

class PodcastIndexSource:
    """
    Podcast Index API — fetches trending and recent podcast episodes.
    Free, requires key + secret from https://api.podcastindex.org/
    """

    BASE = "https://api.podcastindex.org/api/1.0"

    def __init__(self, api_key: str = "", api_secret: str = ""):
        self.api_key = api_key
        self.api_secret = api_secret

    def _headers(self):
        """Generate auth headers with epoch time hash."""
        epoch = str(int(time.time()))
        data = self.api_key + self.api_secret + epoch
        sha1 = hashlib.sha1(data.encode("utf-8")).hexdigest()
        return {
            "X-Auth-Date": epoch,
            "X-Auth-Key": self.api_key,
            "Authorization": sha1,
            "User-Agent": "Unreeled/1.0",
        }

    def fetch_podcasts(self, date: str) -> list[dict]:
        if not self.api_key or not self.api_secret:
            logger.info("Podcast Index: No API key configured, skipping")
            return []

        releases = []

        try:
            # Fetch trending podcasts
            resp = requests.get(
                f"{self.BASE}/podcasts/trending",
                headers=self._headers(),
                params={"max": 30, "lang": "en", "pretty": "false"},
                timeout=15,
            )
            rate_limit()

            if resp.status_code == 200:
                data = resp.json()
                feeds = data.get("feeds", [])

                for feed in feeds[:25]:
                    title = safe_str(feed.get("title"))
                    if not title:
                        continue

                    releases.append(make_release(
                        source="podcast_index",
                        media_type="podcast",
                        title=title,
                        release_date=date,
                        synopsis=safe_str(feed.get("description", ""))[:500],
                        genres=[safe_str(c) for c in (feed.get("categories") or {}).values()][:3],
                        metadata={
                            "author": safe_str(feed.get("author")),
                            "language": safe_str(feed.get("language")),
                            "episode_count": feed.get("episodeCount", 0),
                            "popularity": feed.get("trendScore", 0),
                        },
                        poster_url=safe_str(feed.get("image")),
                        external_ids={"podcast_index_id": feed.get("id")},
                    ))

            # Also fetch recent episodes
            resp2 = requests.get(
                f"{self.BASE}/recent/episodes",
                headers=self._headers(),
                params={"max": 20, "lang": "en", "pretty": "false"},
                timeout=15,
            )
            rate_limit()

            if resp2.status_code == 200:
                data2 = resp2.json()
                items = data2.get("items", [])

                for ep in items[:15]:
                    feed_title = safe_str(ep.get("feedTitle"))
                    ep_title = safe_str(ep.get("title"))
                    if not feed_title:
                        continue

                    full_title = f"{feed_title}: {ep_title}" if ep_title else feed_title

                    releases.append(make_release(
                        source="podcast_index",
                        media_type="podcast",
                        title=full_title,
                        release_date=date,
                        synopsis=safe_str(ep.get("description", ""))[:500],
                        genres=[],
                        metadata={
                            "author": safe_str(ep.get("feedAuthor")),
                            "duration_seconds": ep.get("duration", 0),
                            "episode_type": safe_str(ep.get("episodeType")),
                            "popularity": 0,
                        },
                        poster_url=safe_str(ep.get("feedImage") or ep.get("image")),
                        external_ids={"podcast_index_id": ep.get("feedId")},
                    ))

        except Exception as e:
            logger.error(f"Podcast Index failed: {e}")

        # Deduplicate by feed title
        seen = set()
        unique = []
        for r in releases:
            key = r["title"].lower().split(":")[0].strip()
            if key not in seen:
                seen.add(key)
                unique.append(r)

        logger.info(f"Podcast Index: {len(unique)} podcast releases for {date}")
        return unique


# ═══════════════════════════════════════════════════════════════
# SOURCE: BOARD GAME GEEK (Board games — no API key needed)
# ═══════════════════════════════════════════════════════════════

class BoardGameGeekSource:
    """
    BoardGameGeek XML API — fetches hot/trending board games.
    Completely free, no key needed.
    """

    HOT_URL = "https://boardgamegeek.com/xmlapi2/hot?type=boardgame"
    THING_URL = "https://boardgamegeek.com/xmlapi2/thing"

    def fetch_boardgames(self, date: str) -> list[dict]:
        # BGG now requires auth tokens as of July 2025
        # Disabled until we register for an application token
        logger.info("BoardGameGeek: Disabled (requires auth token since July 2025)")
        return []


# ═══════════════════════════════════════════════════════════════
# SOURCE: COMIC VINE (Comics & graphic novels)
# ═══════════════════════════════════════════════════════════════

class ComicVineSource:
    """
    Comic Vine API — fetches new comic issues.
    Free with API key from https://comicvine.gamespot.com/api/
    """

    BASE = "https://comicvine.gamespot.com/api"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def fetch_comics(self, date: str) -> list[dict]:
        if not self.api_key:
            logger.info("Comic Vine: No API key configured, skipping")
            return []

        releases = []

        try:
            # Fetch issues by store date
            resp = requests.get(
                f"{self.BASE}/issues/",
                params={
                    "api_key": self.api_key,
                    "format": "json",
                    "filter": f"store_date:{date}",
                    "sort": "store_date:desc",
                    "limit": 50,
                    "field_list": "id,name,issue_number,volume,cover_date,store_date,description,image",
                },
                headers={"User-Agent": "Unreeled/1.0"},
                timeout=15,
            )
            rate_limit(1.0)  # Comic Vine rate limits

            if resp.status_code != 200:
                logger.warning(f"Comic Vine: HTTP {resp.status_code}")
                return []

            data = resp.json()
            results = data.get("results", [])

            for issue in results:
                vol = issue.get("volume") or {}
                vol_name = safe_str(vol.get("name"))
                issue_num = safe_str(issue.get("issue_number"))
                title = f"{vol_name} #{issue_num}" if vol_name and issue_num else vol_name or safe_str(issue.get("name"))

                if not title:
                    continue

                desc = safe_str(issue.get("description", ""))
                # Strip HTML tags
                import re
                desc = re.sub(r"<[^>]+>", " ", desc).strip()
                if len(desc) > 500:
                    desc = desc[:497] + "..."

                img = issue.get("image") or {}
                poster = safe_str(img.get("medium_url") or img.get("small_url"))

                releases.append(make_release(
                    source="comic_vine",
                    media_type="comic",
                    title=title,
                    release_date=date,
                    synopsis=desc,
                    genres=["Comics"],
                    metadata={
                        "issue_number": issue_num,
                        "volume": vol_name,
                        "publisher": "",
                        "popularity": 50,
                    },
                    poster_url=poster,
                    external_ids={"comic_vine_id": issue.get("id")},
                ))

        except Exception as e:
            logger.error(f"Comic Vine failed: {e}")

        logger.info(f"Comic Vine: {len(releases)} comics for {date}")
        return releases


# ═══════════════════════════════════════════════════════════════
# SOURCE: RAWG (Additional game data)
# ═══════════════════════════════════════════════════════════════

class RawgSource:
    """
    RAWG API — additional game releases and screenshots.
    Free with key from https://rawg.io/apidocs (20,000/month)
    """

    BASE = "https://api.rawg.io/api"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def fetch_games(self, date: str) -> list[dict]:
        if not self.api_key:
            logger.info("RAWG: No API key configured, skipping")
            return []

        releases = []

        try:
            resp = requests.get(
                f"{self.BASE}/games",
                params={
                    "key": self.api_key,
                    "dates": f"{date},{date}",
                    "ordering": "-rating",
                    "page_size": 30,
                },
                timeout=15,
            )
            rate_limit()

            if resp.status_code != 200:
                logger.warning(f"RAWG: HTTP {resp.status_code}")
                return []

            data = resp.json()
            results = data.get("results", [])

            for game in results:
                title = safe_str(game.get("name"))
                if not title:
                    continue

                genres = [safe_str(g.get("name")) for g in (game.get("genres") or [])][:3]
                platforms = [safe_str(p.get("platform", {}).get("name")) for p in (game.get("platforms") or [])][:4]

                releases.append(make_release(
                    source="rawg",
                    media_type="game",
                    title=title,
                    release_date=date,
                    synopsis="",
                    genres=genres,
                    metadata={
                        "platforms": platforms,
                        "rating": game.get("rating", 0),
                        "ratings_count": game.get("ratings_count", 0),
                        "metacritic": game.get("metacritic"),
                        "popularity": game.get("added", 0),
                    },
                    poster_url=safe_str(game.get("background_image")),
                    external_ids={"rawg_id": game.get("id"), "rawg_slug": game.get("slug")},
                ))

        except Exception as e:
            logger.error(f"RAWG failed: {e}")

        logger.info(f"RAWG: {len(releases)} games for {date}")
        return releases


# ═══════════════════════════════════════════════════════════════
# SOURCE: NEWSDATA.IO (Entertainment news)
# ═══════════════════════════════════════════════════════════════

class NewsDataSource:
    """
    NewsData.io — entertainment news headlines.
    Free: 200 requests/day from https://newsdata.io/
    """

    BASE = "https://newsdata.io/api/1/latest"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def fetch_news(self, date: str) -> list[dict]:
        if not self.api_key:
            logger.info("NewsData: No API key configured, skipping")
            return []

        releases = []

        try:
            resp = requests.get(
                self.BASE,
                params={
                    "apikey": self.api_key,
                    "category": "entertainment",
                    "language": "en",
                    "size": 20,
                },
                timeout=15,
            )
            rate_limit()

            if resp.status_code != 200:
                logger.warning(f"NewsData: HTTP {resp.status_code}")
                return []

            data = resp.json()
            articles = data.get("results", [])

            for article in articles:
                title = safe_str(article.get("title"))
                if not title:
                    continue

                releases.append(make_release(
                    source="newsdata",
                    media_type="news",
                    title=title,
                    release_date=date,
                    synopsis=safe_str(article.get("description", ""))[:500],
                    genres=["Entertainment News"],
                    metadata={
                        "source_name": safe_str(article.get("source_name")),
                        "source_url": safe_str(article.get("source_url")),
                        "link": safe_str(article.get("link")),
                        "creator": (article.get("creator") or [""])[0] if article.get("creator") else "",
                        "popularity": 80,
                    },
                    poster_url=safe_str(article.get("image_url")),
                    external_ids={"newsdata_id": article.get("article_id")},
                ))

        except Exception as e:
            logger.error(f"NewsData failed: {e}")

        logger.info(f"NewsData: {len(releases)} news articles for {date}")
        return releases


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT: OMDb (Rotten Tomatoes / Metacritic scores)
# ═══════════════════════════════════════════════════════════════

class OMDbEnricher:
    """
    Adds Rotten Tomatoes and Metacritic scores to movie/TV releases.
    Free: 1,000 requests/day from https://www.omdbapi.com/
    """

    BASE = "https://www.omdbapi.com/"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def enrich(self, releases: list[dict], max_lookups: int = 40) -> int:
        if not self.api_key:
            logger.info("OMDb: No API key configured, skipping enrichment")
            return 0

        enriched = 0
        lookups = 0

        for r in releases:
            if lookups >= max_lookups:
                break
            if r["media_type"] not in ("movie", "tv"):
                continue

            title = r["title"]
            imdb_id = r.get("external_ids", {}).get("imdb_id")

            try:
                params = {"apikey": self.api_key, "type": "movie" if r["media_type"] == "movie" else "series"}
                if imdb_id:
                    params["i"] = imdb_id
                else:
                    params["t"] = title

                resp = requests.get(self.BASE, params=params, timeout=10)
                lookups += 1
                rate_limit(0.15)

                if resp.status_code != 200:
                    continue

                data = resp.json()
                if data.get("Response") != "True":
                    continue

                meta = r.get("metadata", {})

                # Add ratings
                ratings = {}
                for rating in data.get("Ratings", []):
                    src = rating.get("Source", "")
                    val = rating.get("Value", "")
                    if "Rotten Tomatoes" in src:
                        ratings["rotten_tomatoes"] = val
                    elif "Metacritic" in src:
                        ratings["metacritic"] = val
                    elif "Internet Movie Database" in src:
                        ratings["imdb"] = val

                if ratings:
                    meta["ratings"] = ratings
                    r["metadata"] = meta
                    enriched += 1

                # Add IMDb ID if we didn't have it
                if data.get("imdbID") and not imdb_id:
                    r.setdefault("external_ids", {})["imdb_id"] = data["imdbID"]

            except Exception:
                continue

        logger.info(f"OMDb: Enriched {enriched} releases with ratings ({lookups} lookups)")
        return enriched


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT: TASTEDIVE (Recommendations)
# ═══════════════════════════════════════════════════════════════

class TasteDiveEnricher:
    """
    Adds "if you liked this" recommendations to releases.
    Free: 300 requests/hour from https://tastedive.com/
    """

    BASE = "https://tastedive.com/api/similar"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def enrich(self, releases: list[dict], max_lookups: int = 30) -> int:
        if not self.api_key:
            logger.info("TasteDive: No API key configured, skipping enrichment")
            return 0

        type_map = {"movie": "movie", "tv": "show", "book": "book", "music": "music", "game": "game"}
        enriched = 0
        lookups = 0

        for r in releases:
            if lookups >= max_lookups:
                break
            td_type = type_map.get(r["media_type"])
            if not td_type:
                continue

            try:
                resp = requests.get(
                    self.BASE,
                    params={
                        "q": f"{td_type}:{r['title']}",
                        "k": self.api_key,
                        "type": td_type,
                        "limit": 5,
                        "info": 0,
                    },
                    timeout=10,
                )
                lookups += 1
                rate_limit(0.5)

                if resp.status_code != 200:
                    continue

                data = resp.json()
                results = data.get("Similar", {}).get("Results", [])

                if results:
                    recs = [safe_str(item.get("Name")) for item in results[:5]]
                    r.setdefault("metadata", {})["recommendations"] = recs
                    enriched += 1

            except Exception:
                continue

        logger.info(f"TasteDive: Added recommendations to {enriched} releases ({lookups} lookups)")
        return enriched


# ═══════════════════════════════════════════════════════════════
# ENRICHMENT: WATCHMODE (Streaming availability)
# ═══════════════════════════════════════════════════════════════

class WatchmodeEnricher:
    """
    Adds streaming availability (Netflix, Disney+, etc.) to movie/TV releases.
    Free: 1,000 requests/month from https://api.watchmode.com/
    """

    BASE = "https://api.watchmode.com/v1"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key

    def enrich(self, releases: list[dict], max_lookups: int = 20) -> int:
        if not self.api_key:
            logger.info("Watchmode: No API key configured, skipping enrichment")
            return 0

        enriched = 0
        lookups = 0

        for r in releases:
            if lookups >= max_lookups:
                break
            if r["media_type"] not in ("movie", "tv"):
                continue

            try:
                # Search for the title
                search_resp = requests.get(
                    f"{self.BASE}/search/",
                    params={
                        "apiKey": self.api_key,
                        "search_field": "name",
                        "search_value": r["title"],
                        "types": "movie" if r["media_type"] == "movie" else "tv_series",
                    },
                    timeout=10,
                )
                lookups += 1
                rate_limit(0.3)

                if search_resp.status_code != 200:
                    continue

                search_data = search_resp.json()
                results = search_data.get("title_results", [])

                if not results:
                    continue

                wm_id = results[0].get("id")
                if not wm_id:
                    continue

                # Get streaming sources
                source_resp = requests.get(
                    f"{self.BASE}/title/{wm_id}/sources/",
                    params={"apiKey": self.api_key},
                    timeout=10,
                )
                lookups += 1
                rate_limit(0.3)

                if source_resp.status_code != 200:
                    continue

                sources = source_resp.json()
                if not isinstance(sources, list):
                    continue

                # Extract unique streaming services
                streaming = {}
                for src in sources:
                    name = safe_str(src.get("name"))
                    stype = safe_str(src.get("type"))
                    url = safe_str(src.get("web_url"))
                    if name and name not in streaming:
                        streaming[name] = {"type": stype, "url": url}

                if streaming:
                    r.setdefault("metadata", {})["streaming"] = streaming
                    r.setdefault("external_ids", {})["watchmode_id"] = wm_id
                    enriched += 1

            except Exception:
                continue

        logger.info(f"Watchmode: Added streaming info to {enriched} releases ({lookups} lookups)")
        return enriched
