"""
UNREELED — Media Release Ingestion Pipeline v4
=================================================

Pulls daily new releases from multiple public APIs and normalizes
them into a unified schema for the Unreeled platform.

Data Sources:
  - TMDB (The Movie Database) — Movies & TV shows
  - Open Library API — New book releases
  - IGDB (via Twitch/Amazon) — Video games
  - Jikan (MyAnimeList unofficial API) — Anime episodes
  - MusicBrainz — Music releases (CD, vinyl, digital)

Changes in v4:
  - Added: MusicBrainz source for music releases (CD, vinyl, digital)
  - Cover art fetched from Cover Art Archive (coverartarchive.org)
  - Tracks physical format types (CD, vinyl, cassette, digital)
  - No API key required — just a polite User-Agent header

Setup:
  1. pip install requests python-dotenv schedule
  2. Create .env file:
     TMDB_API_KEY=your_key
     IGDB_CLIENT_ID=your_twitch_client_id
     IGDB_CLIENT_SECRET=your_twitch_client_secret
  3. Run:
     python unreeled_ingest.py              # Today's releases
     python unreeled_ingest.py --schedule   # Daily at 6 AM UTC
     python unreeled_ingest.py --date 2026-02-20
"""

import os
import json
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

try:
    import requests
except ImportError:
    print("Please install requests: pip install requests")
    exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

TMDB_API_KEY = os.getenv("TMDB_API_KEY", "")
IGDB_CLIENT_ID = os.getenv("IGDB_CLIENT_ID", "")
IGDB_CLIENT_SECRET = os.getenv("IGDB_CLIENT_SECRET", "")

# New API keys (v5)
PODCAST_INDEX_KEY = os.getenv("PODCAST_INDEX_KEY", "")
PODCAST_INDEX_SECRET = os.getenv("PODCAST_INDEX_SECRET", "")
RAWG_KEY = os.getenv("RAWG_KEY", "")
OMDB_KEY = os.getenv("OMDB_KEY", "")
WATCHMODE_KEY = os.getenv("WATCHMODE_KEY", "")
NEWSDATA_KEY = os.getenv("NEWSDATA_KEY", "")

OUTPUT_DIR = Path("./output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Filters (adjust these to your preferences) ──
MIN_MOVIE_RUNTIME = 40          # Minutes — filters out short films
LANGUAGE_FILTER = None           # Set to "en" to only show English content, None for all
INCLUDE_TALK_SHOWS = False       # Set True to include talk/late night shows
INCLUDE_REALITY = False          # Set True to include reality TV
INCLUDE_NEWS = False             # Set True to include news programs
INCLUDE_SINGLES = False          # Set True to include single releases in music
MUSIC_COVER_ART_LIMIT = 80      # Max cover art lookups per run (~1 sec each)

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("unreeled")

REQUEST_DELAY = 0.25


def rate_limit():
    time.sleep(REQUEST_DELAY)


def utcnow() -> datetime:
    """Timezone-aware UTC now (avoids deprecation warning)."""
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    """ISO formatted UTC timestamp."""
    return utcnow().isoformat()


# ═══════════════════════════════════════════════════════════════
# UNIFIED RELEASE SCHEMA
# ═══════════════════════════════════════════════════════════════

def make_release(
    source: str,
    media_type: str,
    title: str,
    release_date: str,
    synopsis: str = "",
    genres: list[str] = None,
    metadata: dict = None,
    poster_url: str = "",
    external_ids: dict = None,
) -> dict:
    return {
        "source": source,
        "media_type": media_type,
        "title": title,
        "release_date": release_date,
        "synopsis": synopsis,
        "genres": genres or [],
        "metadata": metadata or {},
        "poster_url": poster_url,
        "external_ids": external_ids or {},
        "ingested_at": utcnow_iso(),
        "comment_count": 0,
        "spoiler_counts": {"light": 0, "medium": 0, "heavy": 0},
    }


# ═══════════════════════════════════════════════════════════════
# SOURCE: TMDB (Movies & TV)
# ═══════════════════════════════════════════════════════════════

# TV genres to filter out
TMDB_EXCLUDED_TV_GENRE_NAMES = set()
if not INCLUDE_TALK_SHOWS:
    TMDB_EXCLUDED_TV_GENRE_NAMES.add("Talk")
if not INCLUDE_REALITY:
    TMDB_EXCLUDED_TV_GENRE_NAMES.add("Reality")
if not INCLUDE_NEWS:
    TMDB_EXCLUDED_TV_GENRE_NAMES.add("News")


class TMDBSource:
    """
    TMDB API v3 — Movies & TV.
    Docs: https://developer.themoviedb.org/docs
    Rate Limit: ~40 req/10s | Cost: Free with attribution
    """

    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.params = {"api_key": self.api_key}
        self._movie_genres = {}
        self._tv_genres = {}

    def _get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        try:
            resp = self.session.get(
                f"{self.BASE_URL}{endpoint}", params=params or {}, timeout=15
            )
            resp.raise_for_status()
            rate_limit()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"TMDB {endpoint}: {e}")
            return None

    def _load_genres(self):
        movie_data = self._get("/genre/movie/list")
        if movie_data:
            self._movie_genres = {g["id"]: g["name"] for g in movie_data.get("genres", [])}
        tv_data = self._get("/genre/tv/list")
        if tv_data:
            self._tv_genres = {g["id"]: g["name"] for g in tv_data.get("genres", [])}

    def _resolve_genres(self, genre_ids: list, media_type: str) -> list[str]:
        genre_map = self._movie_genres if media_type == "movie" else self._tv_genres
        return [genre_map.get(gid, "Unknown") for gid in genre_ids]

    def fetch_movies(self, date: str) -> list[dict]:
        if not self.api_key:
            logger.warning("TMDB_API_KEY not set — skipping movies")
            return []

        if not self._movie_genres:
            self._load_genres()

        releases = []
        page = 1
        total_pages = 1

        while page <= min(total_pages, 5):
            params = {
                "primary_release_date.gte": date,
                "primary_release_date.lte": date,
                "sort_by": "popularity.desc",
                "page": page,
            }
            if LANGUAGE_FILTER:
                params["with_original_language"] = LANGUAGE_FILTER

            data = self._get("/discover/movie", params)
            if not data:
                break

            total_pages = data.get("total_pages", 1)

            for movie in data.get("results", []):
                details = self._get(f"/movie/{movie['id']}")
                runtime = details.get("runtime", 0) if details else 0

                # Filter: skip short films
                if runtime and runtime < MIN_MOVIE_RUNTIME:
                    continue

                # Filter: skip entries with no synopsis AND no poster
                if not movie.get("overview") and not movie.get("poster_path"):
                    continue

                release = make_release(
                    source="tmdb",
                    media_type="movie",
                    title=movie.get("title", "Unknown"),
                    release_date=movie.get("release_date", date),
                    synopsis=movie.get("overview", ""),
                    genres=self._resolve_genres(movie.get("genre_ids", []), "movie"),
                    metadata={
                        "runtime_minutes": runtime,
                        "original_language": movie.get("original_language", ""),
                        "popularity": movie.get("popularity", 0),
                        "vote_average": movie.get("vote_average", 0),
                        "adult": movie.get("adult", False),
                    },
                    poster_url=(
                        f"{self.IMAGE_BASE}{movie['poster_path']}"
                        if movie.get("poster_path")
                        else ""
                    ),
                    external_ids={"tmdb_id": movie["id"]},
                )
                releases.append(release)

            page += 1

        logger.info(f"TMDB: {len(releases)} movies for {date} (after filtering)")
        return releases

    def fetch_tv(self, date: str) -> list[dict]:
        if not self.api_key:
            logger.warning("TMDB_API_KEY not set — skipping TV")
            return []

        if not self._tv_genres:
            self._load_genres()

        releases = []
        page = 1
        total_pages = 1

        while page <= min(total_pages, 5):
            params = {
                "air_date.gte": date,
                "air_date.lte": date,
                "sort_by": "popularity.desc",
                "page": page,
            }
            if LANGUAGE_FILTER:
                params["with_original_language"] = LANGUAGE_FILTER

            data = self._get("/discover/tv", params)
            if not data:
                break

            total_pages = data.get("total_pages", 1)

            for show in data.get("results", []):
                genres = self._resolve_genres(show.get("genre_ids", []), "tv")

                # Filter: skip excluded genres (talk shows, reality, news)
                if TMDB_EXCLUDED_TV_GENRE_NAMES and any(
                    g in TMDB_EXCLUDED_TV_GENRE_NAMES for g in genres
                ):
                    continue

                # Filter: skip entries with no synopsis AND no poster
                if not show.get("overview") and not show.get("poster_path"):
                    continue

                details = self._get(f"/tv/{show['id']}")
                networks = []
                if details:
                    networks = [n["name"] for n in details.get("networks", [])]

                release = make_release(
                    source="tmdb",
                    media_type="tv",
                    title=show.get("name", "Unknown"),
                    release_date=show.get("first_air_date", date),
                    synopsis=show.get("overview", ""),
                    genres=genres,
                    metadata={
                        "networks": networks,
                        "original_language": show.get("original_language", ""),
                        "popularity": show.get("popularity", 0),
                        "vote_average": show.get("vote_average", 0),
                        "episode_air_date": date,
                    },
                    poster_url=(
                        f"{self.IMAGE_BASE}{show['poster_path']}"
                        if show.get("poster_path")
                        else ""
                    ),
                    external_ids={"tmdb_id": show["id"]},
                )
                releases.append(release)

            page += 1

        logger.info(f"TMDB: {len(releases)} TV shows for {date} (after filtering)")
        return releases


# ═══════════════════════════════════════════════════════════════
# SOURCE: Open Library
# ═══════════════════════════════════════════════════════════════

class OpenLibrarySource:
    """
    Open Library API — new book releases.
    Docs: https://openlibrary.org/developers/api
    Rate Limit: ~100 req/5min (be respectful, no key needed)
    Cost: Free, fully open, no key required

    Strategy:
      - Search by subject + publish year for recent books
      - Use the /search.json endpoint which supports date filtering
      - Fetch cover images from covers.openlibrary.org
    """

    SEARCH_URL = "https://openlibrary.org/search.json"
    WORKS_URL = "https://openlibrary.org"
    COVERS_BASE = "https://covers.openlibrary.org/b"

    # Only return books in these languages (ISO 639-2 codes)
    ALLOWED_LANGUAGES = {"eng", "en"}

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "UnreeledBot/1.0 (media release tracker)"
        })

    def _search(self, params: dict) -> Optional[dict]:
        try:
            resp = self.session.get(self.SEARCH_URL, params=params, timeout=20)
            resp.raise_for_status()
            rate_limit()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"Open Library search failed: {e}")
            return None

    def _fetch_synopsis(self, work_key: str) -> str:
        """
        Fetch full description from a work's detail page.
        work_key looks like '/works/OL12345W'
        """
        if not work_key:
            return ""
        try:
            url = f"{self.WORKS_URL}{work_key}.json"
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            rate_limit()

            # Description can be a string or a dict with "value" key
            desc = data.get("description", "")
            if isinstance(desc, dict):
                desc = desc.get("value", "")
            if isinstance(desc, str) and desc:
                return desc

            # Fall back to first_sentence
            first = data.get("first_sentence", "")
            if isinstance(first, dict):
                first = first.get("value", "")
            return first if isinstance(first, str) else ""

        except requests.RequestException:
            return ""

    def _get_cover_url(self, cover_id: int = None, isbn: str = None) -> str:
        """Build cover image URL. Prefer cover ID, fall back to ISBN."""
        if cover_id and cover_id > 0:
            return f"{self.COVERS_BASE}/id/{cover_id}-L.jpg"
        if isbn:
            return f"{self.COVERS_BASE}/isbn/{isbn}-L.jpg"
        return ""

    def fetch_books(self, date: str) -> list[dict]:
        target_dt = datetime.strptime(date, "%Y-%m-%d")
        target_year = target_dt.year
        target_month = target_dt.month

        # Subjects to search across — casting a wide net
        subjects = [
            "fiction",
            "thriller",
            "science_fiction",
            "fantasy",
            "mystery",
            "romance",
            "biography",
            "history",
            "science",
            "horror",
            "literary_fiction",
            "young_adult",
        ]

        releases = []

        for subject in subjects:
            # Open Library search supports filtering by publish year
            # and sorting by new. We search per subject to get variety.
            data = self._search({
                "subject": subject,
                "first_publish_year": target_year,
                "sort": "new",
                "limit": 20,
                "fields": (
                    "key,title,author_name,first_publish_year,"
                    "publish_date,subject,isbn,number_of_pages_median,"
                    "cover_i,publisher,language,ratings_average,"
                    "ratings_count,edition_count,first_sentence"
                ),
            })

            if not data:
                continue

            for doc in data.get("docs", []):
                # Try to match more precisely by checking publish_date
                # Open Library publish_date can contain multiple dates
                # from different editions — we check if any match our month
                publish_dates = doc.get("publish_date", [])
                if isinstance(publish_dates, str):
                    publish_dates = [publish_dates]

                # Check if any edition was published in our target month/year
                month_match = False
                target_month_str = target_dt.strftime("%B %Y")  # e.g. "February 2026"
                target_month_short = target_dt.strftime("%b %Y")  # e.g. "Feb 2026"
                target_ym = f"{target_year}"

                for pd in publish_dates:
                    pd_lower = pd.lower()
                    if (target_month_str.lower() in pd_lower or
                        target_month_short.lower() in pd_lower or
                        str(target_year) in pd):
                        month_match = True
                        break

                # Accept if publish year matches (Open Library dates are imprecise)
                year_match = doc.get("first_publish_year") == target_year

                if not month_match and not year_match:
                    continue

                title = doc.get("title", "")
                if not title:
                    continue

                # ── Filter: English only ──
                languages = doc.get("language", [])
                if isinstance(languages, list) and languages:
                    if not any(lang in self.ALLOWED_LANGUAGES for lang in languages):
                        continue
                elif isinstance(languages, str):
                    if languages not in self.ALLOWED_LANGUAGES:
                        continue

                # Build synopsis from first_sentence if available
                first_sentence = doc.get("first_sentence", [])
                if isinstance(first_sentence, list) and first_sentence:
                    synopsis = first_sentence[0] if isinstance(first_sentence[0], str) else ""
                elif isinstance(first_sentence, str):
                    synopsis = first_sentence
                else:
                    synopsis = ""

                # Get the work key for fetching full synopsis later
                work_key = doc.get("key", "")

                # Get ISBN (prefer ISBN-13)
                isbns = doc.get("isbn", [])
                isbn_13 = ""
                isbn_10 = ""
                for isbn in isbns:
                    if len(isbn) == 13 and not isbn_13:
                        isbn_13 = isbn
                    elif len(isbn) == 10 and not isbn_10:
                        isbn_10 = isbn

                best_isbn = isbn_13 or isbn_10

                # Cover image
                cover_id = doc.get("cover_i")
                cover_url = self._get_cover_url(cover_id=cover_id, isbn=best_isbn)

                # Genres/subjects — take first few relevant ones
                subjects_list = doc.get("subject", [])
                if isinstance(subjects_list, list):
                    # Filter out overly generic or long subjects
                    genres = [
                        s for s in subjects_list[:20]
                        if len(s) < 40 and s.lower() not in (
                            "fiction", "accessible book", "protected daisy",
                            "in library", "large type books", "lending library",
                        )
                    ][:5]
                else:
                    genres = []

                # Authors
                authors = doc.get("author_name", [])

                # Publishers (can be a list of all edition publishers)
                publishers = doc.get("publisher", [])
                publisher = publishers[0] if publishers else ""

                # Determine best release date string
                if month_match:
                    release_date = date  # Use the target date
                else:
                    release_date = f"{target_year}"

                release = make_release(
                    source="open_library",
                    media_type="book",
                    title=title,
                    release_date=release_date,
                    synopsis=synopsis,
                    genres=genres,
                    metadata={
                        "authors": authors,
                        "publisher": publisher,
                        "page_count": doc.get("number_of_pages_median", 0),
                        "isbn": best_isbn,
                        "language": doc.get("language", ["eng"])[0] if doc.get("language") else "eng",
                        "average_rating": doc.get("ratings_average", 0),
                        "ratings_count": doc.get("ratings_count", 0),
                        "edition_count": doc.get("edition_count", 0),
                        "open_library_key": doc.get("key", ""),
                    },
                    poster_url=cover_url,
                    external_ids={
                        "open_library_key": doc.get("key", ""),
                        "isbn": best_isbn,
                    },
                )
                releases.append(release)

        # Deduplicate by title + authors
        seen = set()
        unique = []
        for r in releases:
            authors = tuple(sorted(r.get("metadata", {}).get("authors", [])))
            key = (r["title"].lower().strip(), authors)
            if key not in seen:
                seen.add(key)
                unique.append(r)

        # Sort by ratings count (proxy for popularity/relevance)
        unique.sort(
            key=lambda r: r.get("metadata", {}).get("ratings_count", 0),
            reverse=True,
        )

        # ── Enrich: fetch full synopses from work detail pages ──
        # Only fetch for books missing a synopsis (limit API calls)
        enriched_count = 0
        max_enrichments = 60  # Cap to avoid excessive API calls
        for r in unique:
            if enriched_count >= max_enrichments:
                break
            if not r.get("synopsis"):
                work_key = r.get("external_ids", {}).get("open_library_key", "")
                if work_key:
                    synopsis = self._fetch_synopsis(work_key)
                    if synopsis:
                        r["synopsis"] = synopsis
                        enriched_count += 1

        if enriched_count:
            logger.info(f"Open Library: Enriched {enriched_count} books with synopses")

        logger.info(f"Open Library: {len(unique)} books for {date}")
        return unique


# ═══════════════════════════════════════════════════════════════
# SOURCE: IGDB (Games) via Twitch API
# ═══════════════════════════════════════════════════════════════

class IGDBSource:
    """
    IGDB API v4 — Video games.
    Docs: https://api-docs.igdb.com/
    Auth: Twitch OAuth2 client credentials
    Rate Limit: 4 req/s | Cost: Free
    """

    AUTH_URL = "https://id.twitch.tv/oauth2/token"
    BASE_URL = "https://api.igdb.com/v4"
    IMAGE_BASE = "https://images.igdb.com/igdb/image/upload/t_cover_big"

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None
        self.session = requests.Session()

    def _authenticate(self) -> bool:
        if not self.client_id or not self.client_secret:
            logger.warning("IGDB credentials not set — skipping games")
            return False

        try:
            resp = requests.post(
                self.AUTH_URL,
                params={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "grant_type": "client_credentials",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            self.access_token = data["access_token"]

            self.session.headers.update({
                "Client-ID": self.client_id,
                "Authorization": f"Bearer {self.access_token}",
            })
            logger.info("IGDB: Authentication successful")
            return True

        except requests.RequestException as e:
            logger.error(f"IGDB authentication failed: {e}")
            return False

    def _query(self, endpoint: str, body: str) -> Optional[list]:
        try:
            resp = self.session.post(
                f"{self.BASE_URL}/{endpoint}",
                data=body,
                headers={"Content-Type": "text/plain"},
                timeout=15,
            )
            rate_limit()

            if resp.status_code != 200:
                logger.error(f"IGDB {endpoint} HTTP {resp.status_code}: {resp.text[:500]}")
                return None

            result = resp.json()
            logger.info(f"IGDB {endpoint}: returned {len(result)} results")
            return result

        except requests.RequestException as e:
            logger.error(f"IGDB query failed for {endpoint}: {e}")
            return None

    def fetch_games(self, date: str) -> list[dict]:
        if not self._authenticate():
            return []

        dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_ts = int(dt.timestamp())
        end_ts = int((dt + timedelta(days=1)).timestamp())

        # ── Step 1: Get games with basic fields (no nested expansions) ──
        query = (
            f"fields name, summary, first_release_date, rating, "
            f"cover, genres, platforms; "
            f"where first_release_date >= {start_ts} "
            f"& first_release_date < {end_ts}; "
            f"sort rating desc; "
            f"limit 50;"
        )

        logger.info(f"IGDB: Querying games for {date} (unix {start_ts} to {end_ts})")
        results = self._query("games", query)

        # If no results, try a wider window (+/- 1 day)
        if not results:
            wider_start = int((dt - timedelta(days=1)).timestamp())
            wider_end = int((dt + timedelta(days=2)).timestamp())
            query_wide = (
                f"fields name, summary, first_release_date, rating, "
                f"cover, genres, platforms; "
                f"where first_release_date >= {wider_start} "
                f"& first_release_date < {wider_end}; "
                f"sort rating desc; "
                f"limit 50;"
            )
            logger.info("IGDB: No exact matches, trying +/- 1 day window")
            results = self._query("games", query_wide)

        if not results:
            logger.info("IGDB: No games found")
            return []

        # ── Step 2: Resolve covers ──
        cover_ids = [g["cover"] for g in results if g.get("cover")]
        covers = {}
        if cover_ids:
            cover_query = (
                f"fields game, image_id; "
                f"where id = ({','.join(str(c) for c in cover_ids)}); "
                f"limit 50;"
            )
            cover_results = self._query("covers", cover_query)
            if cover_results:
                covers = {
                    c["game"]: c.get("image_id", "")
                    for c in cover_results
                    if c.get("game")
                }

        # ── Step 3: Resolve genre names ──
        genre_ids = set()
        for g in results:
            if g.get("genres"):
                genre_ids.update(g["genres"])
        genre_names = {}
        if genre_ids:
            genre_query = (
                f"fields id, name; "
                f"where id = ({','.join(str(gid) for gid in genre_ids)}); "
                f"limit 50;"
            )
            genre_results = self._query("genres", genre_query)
            if genre_results:
                genre_names = {g["id"]: g["name"] for g in genre_results}

        # ── Step 4: Resolve platform names ──
        platform_ids = set()
        for g in results:
            if g.get("platforms"):
                platform_ids.update(g["platforms"])
        platform_names = {}
        if platform_ids:
            plat_query = (
                f"fields id, name; "
                f"where id = ({','.join(str(pid) for pid in platform_ids)}); "
                f"limit 200;"
            )
            plat_results = self._query("platforms", plat_query)
            if plat_results:
                platform_names = {p["id"]: p["name"] for p in plat_results}

        # ── Step 5: Build release objects ──
        releases = []
        for game in results:
            game_id = game.get("id", 0)

            game_genres = [
                genre_names[gid]
                for gid in game.get("genres", [])
                if gid in genre_names
            ]

            game_platforms = [
                platform_names[pid]
                for pid in game.get("platforms", [])
                if pid in platform_names
            ]

            cover_image_id = covers.get(game_id, "")
            cover_url = f"{self.IMAGE_BASE}/{cover_image_id}.jpg" if cover_image_id else ""

            release = make_release(
                source="igdb",
                media_type="game",
                title=game.get("name", "Unknown"),
                release_date=date,
                synopsis=game.get("summary", ""),
                genres=game_genres,
                metadata={
                    "platforms": game_platforms,
                    "rating": game.get("rating", 0),
                },
                poster_url=cover_url,
                external_ids={"igdb_id": game_id},
            )
            releases.append(release)

        logger.info(f"IGDB: {len(releases)} games for {date}")
        return releases


# ═══════════════════════════════════════════════════════════════
# SOURCE: Jikan (Anime via MyAnimeList)
# ═══════════════════════════════════════════════════════════════

class JikanSource:
    """
    Jikan API v4 — Anime schedules.
    Docs: https://docs.api.jikan.moe/
    Rate Limit: 3 req/s, 60/min | Cost: Free, no key
    """

    BASE_URL = "https://api.jikan.moe/v4"

    def __init__(self):
        self.session = requests.Session()

    def fetch_anime(self, date: str) -> list[dict]:
        dt = datetime.strptime(date, "%Y-%m-%d")
        day_name = dt.strftime("%A").lower()

        releases = []
        page = 1

        while page <= 3:
            try:
                resp = self.session.get(
                    f"{self.BASE_URL}/schedules",
                    params={"filter": day_name, "page": page, "limit": 25},
                    timeout=15,
                )
                resp.raise_for_status()
                data = resp.json()
                rate_limit()

                items = data.get("data", [])
                if not items:
                    break

                for anime in items:
                    images = anime.get("images", {})
                    jpg = images.get("jpg", {})
                    image_url = jpg.get("large_image_url", jpg.get("image_url", ""))

                    genres = [g["name"] for g in anime.get("genres", [])]
                    genres += [g["name"] for g in anime.get("themes", [])]
                    studios = [s["name"] for s in anime.get("studios", [])]
                    streaming = [s["name"] for s in anime.get("streaming", [])]

                    release = make_release(
                        source="jikan",
                        media_type="anime",
                        title=anime.get("title", "Unknown"),
                        release_date=date,
                        synopsis=anime.get("synopsis", ""),
                        genres=genres,
                        metadata={
                            "title_japanese": anime.get("title_japanese", ""),
                            "episodes_total": anime.get("episodes"),
                            "status": anime.get("status", ""),
                            "rating": anime.get("rating", ""),
                            "score": anime.get("score", 0),
                            "studios": studios,
                            "streaming_on": streaming,
                            "type": anime.get("type", ""),
                        },
                        poster_url=image_url,
                        external_ids={"mal_id": anime.get("mal_id", 0)},
                    )
                    releases.append(release)

                pagination = data.get("pagination", {})
                if not pagination.get("has_next_page", False):
                    break
                page += 1

            except requests.RequestException as e:
                logger.error(f"Jikan request failed: {e}")
                break

        # Deduplicate by MAL ID
        seen_ids = set()
        unique = []
        for r in releases:
            mal_id = r["external_ids"].get("mal_id", 0)
            if mal_id not in seen_ids:
                seen_ids.add(mal_id)
                unique.append(r)

        dupes_removed = len(releases) - len(unique)
        if dupes_removed:
            logger.info(f"Jikan: Removed {dupes_removed} duplicate anime entries")

        logger.info(f"Jikan: {len(unique)} anime airing on {day_name} ({date})")
        return unique


# ═══════════════════════════════════════════════════════════════
# SOURCE: MusicBrainz (Music — CD, Vinyl, Digital)
# ═══════════════════════════════════════════════════════════════

class MusicBrainzSource:
    """
    MusicBrainz API — Music releases.
    Docs: https://musicbrainz.org/doc/MusicBrainz_API
    Cover Art: https://coverartarchive.org
    Rate Limit: 1 req/second (strict — requires polite User-Agent)
    Cost: Free, no key required

    Strategy:
      - Query /release endpoint filtered by date range
      - Fetch cover art from Cover Art Archive by release MBID
      - Distinguish physical formats (CD, vinyl) from digital
    """

    BASE_URL = "https://musicbrainz.org/ws/2"
    COVER_ART_URL = "https://coverartarchive.org/release"
    APP_CONTACT = "unreeled-bot/1.0 (https://unreeled.netlify.app)"

    # Format IDs we care about — maps MusicBrainz format names to our labels
    FORMAT_MAP = {
        "CD": "CD",
        "Enhanced CD": "CD",
        "CD-R": "CD",
        "12\" Vinyl": "Vinyl",
        "7\" Vinyl": "Vinyl",
        "10\" Vinyl": "Vinyl",
        "Vinyl": "Vinyl",
        "Cassette": "Cassette",
        "Digital Media": "Digital",
    }

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.APP_CONTACT,
            "Accept": "application/json",
        })

    def _get(self, endpoint: str, params: dict) -> Optional[dict]:
        try:
            resp = self.session.get(
                f"{self.BASE_URL}{endpoint}",
                params=params,
                timeout=20,
            )
            # MusicBrainz requires strict 1 req/sec rate limiting
            time.sleep(1.1)

            if resp.status_code == 503:
                logger.warning("MusicBrainz: Rate limited (503), waiting 5s...")
                time.sleep(5)
                resp = self.session.get(
                    f"{self.BASE_URL}{endpoint}",
                    params=params,
                    timeout=20,
                )

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            logger.error(f"MusicBrainz {endpoint}: {e}")
            return None

    def _get_cover_art(self, mbid: str) -> str:
        """
        Fetch cover art URL from Cover Art Archive.
        Returns the front image URL or empty string.
        """
        try:
            resp = self.session.get(
                f"{self.COVER_ART_URL}/{mbid}",
                timeout=10,
                allow_redirects=True,
            )
            time.sleep(0.5)  # Be polite to CAA too

            if resp.status_code == 404:
                return ""

            resp.raise_for_status()
            data = resp.json()

            # Find the front image
            for image in data.get("images", []):
                if image.get("front", False):
                    # Use the 500px thumbnail for consistency
                    thumbs = image.get("thumbnails", {})
                    return (
                        thumbs.get("500", "")
                        or thumbs.get("large", "")
                        or thumbs.get("small", "")
                        or image.get("image", "")
                    )

            # Fall back to first image if no front
            if data.get("images"):
                return data["images"][0].get("image", "")

            return ""

        except requests.RequestException:
            return ""

    def _extract_formats(self, media_list: list) -> list[str]:
        """Extract human-readable format names from MusicBrainz media array."""
        formats = set()
        for medium in media_list:
            fmt = medium.get("format", "")
            mapped = self.FORMAT_MAP.get(fmt, "")
            if mapped:
                formats.add(mapped)
        return sorted(formats)

    def fetch_music(self, date: str) -> list[dict]:
        """
        Fetch music releases for a specific date.

        Uses the MusicBrainz /release endpoint with date filtering.
        Paginates through results (100 per page, up to 3 pages).
        """
        releases = []
        offset = 0
        max_results = 300  # Cap at 3 pages of 100

        while offset < max_results:
            # Query releases by date with artist info included
            data = self._get("/release", {
                "query": f'date:{date}',
                "fmt": "json",
                "limit": 100,
                "offset": offset,
            })

            if not data:
                break

            release_list = data.get("releases", [])
            if not release_list:
                break

            total = data.get("count", 0)

            for rel in release_list:
                # Extract artist info
                artists = []
                artist_credit = rel.get("artist-credit", [])
                for ac in artist_credit:
                    artist = ac.get("artist", {}) if isinstance(ac, dict) else {}
                    name = ac.get("name", "") or artist.get("name", "")
                    if name:
                        artists.append(name)

                title = rel.get("title", "")
                if not title:
                    continue

                # Extract formats from media
                media_list = rel.get("media", [])
                formats = self._extract_formats(media_list)

                # Calculate total tracks
                track_count = sum(
                    m.get("track-count", 0) for m in media_list
                )

                # Get release date (prefer the exact date from the query)
                rel_date = rel.get("date", date)

                # Get label info
                label_info = rel.get("label-info", [])
                labels = []
                catalog_numbers = []
                for li in label_info:
                    label = li.get("label", {})
                    if label and label.get("name"):
                        labels.append(label["name"])
                    if li.get("catalog-number"):
                        catalog_numbers.append(li["catalog-number"])

                # Get country
                country = rel.get("country", "")

                # Get release group type (album, single, EP, etc.)
                release_group = rel.get("release-group", {})
                primary_type = release_group.get("primary-type", "")

                # Get barcode
                barcode = rel.get("barcode", "")

                # MBID for cover art lookup
                mbid = rel.get("id", "")

                # Build genre list from primary type + formats
                genres = []
                if primary_type:
                    genres.append(primary_type)
                if formats:
                    genres.extend(formats)

                release = make_release(
                    source="musicbrainz",
                    media_type="music",
                    title=title,
                    release_date=rel_date,
                    synopsis="",  # MusicBrainz doesn't have descriptions
                    genres=genres,
                    metadata={
                        "artists": artists,
                        "formats": formats,
                        "track_count": track_count,
                        "labels": labels,
                        "catalog_numbers": catalog_numbers,
                        "country": country,
                        "release_type": primary_type,
                        "barcode": barcode,
                        "mbid": mbid,
                    },
                    poster_url="",  # Will be enriched below
                    external_ids={
                        "musicbrainz_id": mbid,
                        "barcode": barcode,
                    },
                )
                releases.append(release)

            offset += 100
            if offset >= total:
                break

        # Deduplicate by title + artist (same album can have multiple
        # regional releases with different MBIDs)
        seen = set()
        unique = []
        for r in releases:
            artists = tuple(sorted(r.get("metadata", {}).get("artists", [])))
            key = (r["title"].lower().strip(), artists)
            if key not in seen:
                seen.add(key)
                unique.append(r)

        dupes_removed = len(releases) - len(unique)
        if dupes_removed:
            logger.info(f"MusicBrainz: Removed {dupes_removed} duplicate releases")

        # ── Filter: skip singles unless configured to include them ──
        if not INCLUDE_SINGLES:
            before_filter = len(unique)
            unique = [
                r for r in unique
                if r.get("metadata", {}).get("release_type", "").lower() != "single"
            ]
            singles_removed = before_filter - len(unique)
            if singles_removed:
                logger.info(f"MusicBrainz: Filtered out {singles_removed} singles (keeping Albums, EPs, etc.)")

        # ── Enrich: fetch cover art for top releases ──
        # Uses configurable limit (~1 second per lookup due to rate limiting)
        max_covers = MUSIC_COVER_ART_LIMIT
        covers_found = 0
        for r in unique[:max_covers]:
            mbid = r.get("metadata", {}).get("mbid", "")
            if mbid:
                cover_url = self._get_cover_art(mbid)
                if cover_url:
                    r["poster_url"] = cover_url
                    covers_found += 1

        if covers_found:
            logger.info(f"MusicBrainz: Found cover art for {covers_found}/{min(len(unique), max_covers)} releases")

        logger.info(f"MusicBrainz: {len(unique)} music releases for {date}")
        return unique


# ═══════════════════════════════════════════════════════════════
# INGESTION PIPELINE
# ═══════════════════════════════════════════════════════════════

class UnreeledPipeline:
    def __init__(self):
        self.sources = {
            "tmdb": TMDBSource(TMDB_API_KEY),
            "open_library": OpenLibrarySource(),
            "igdb": IGDBSource(IGDB_CLIENT_ID, IGDB_CLIENT_SECRET),
            "jikan": JikanSource(),
            "musicbrainz": MusicBrainzSource(),
        }

        # Import v5 sources
        try:
            from unreeled_sources_v5 import (
                PodcastIndexSource, BoardGameGeekSource,
                RawgSource, NewsDataSource,
                OMDbEnricher, WatchmodeEnricher,
            )
            self.v5_sources = {
                "podcast_index": PodcastIndexSource(PODCAST_INDEX_KEY, PODCAST_INDEX_SECRET),
                "bgg": BoardGameGeekSource(),
                "rawg": RawgSource(RAWG_KEY),
                "newsdata": NewsDataSource(NEWSDATA_KEY),
            }
            self.enrichers = {
                "omdb": OMDbEnricher(OMDB_KEY),
                "watchmode": WatchmodeEnricher(WATCHMODE_KEY),
            }
        except ImportError as e:
            logger.warning(f"V5 sources not available: {e}")
            self.v5_sources = {}
            self.enrichers = {}

    def ingest_date(self, date: str) -> dict:
        logger.info(f"{'=' * 60}")
        logger.info(f"Starting ingestion for {date}")
        logger.info(f"{'=' * 60}")

        all_releases = []
        source_stats = {}
        errors = {}

        # TMDB: Movies
        try:
            movies = self.sources["tmdb"].fetch_movies(date)
            all_releases.extend(movies)
            source_stats["tmdb_movies"] = len(movies)
        except Exception as e:
            logger.error(f"TMDB movies failed: {e}")
            source_stats["tmdb_movies"] = 0
            errors["tmdb_movies"] = str(e)

        # TMDB: TV
        try:
            tv_shows = self.sources["tmdb"].fetch_tv(date)
            all_releases.extend(tv_shows)
            source_stats["tmdb_tv"] = len(tv_shows)
        except Exception as e:
            logger.error(f"TMDB TV failed: {e}")
            source_stats["tmdb_tv"] = 0
            errors["tmdb_tv"] = str(e)

        # Open Library: Books
        try:
            books = self.sources["open_library"].fetch_books(date)
            all_releases.extend(books)
            source_stats["open_library"] = len(books)
        except Exception as e:
            logger.error(f"Open Library failed: {e}")
            source_stats["open_library"] = 0
            errors["open_library"] = str(e)

        # IGDB: Games
        try:
            games = self.sources["igdb"].fetch_games(date)
            all_releases.extend(games)
            source_stats["igdb_games"] = len(games)
        except Exception as e:
            logger.error(f"IGDB games failed: {e}")
            source_stats["igdb_games"] = 0
            errors["igdb_games"] = str(e)

        # Jikan: Anime
        try:
            anime = self.sources["jikan"].fetch_anime(date)
            all_releases.extend(anime)
            source_stats["jikan_anime"] = len(anime)
        except Exception as e:
            logger.error(f"Jikan anime failed: {e}")
            source_stats["jikan_anime"] = 0
            errors["jikan_anime"] = str(e)

        # MusicBrainz: Music
        try:
            music = self.sources["musicbrainz"].fetch_music(date)
            all_releases.extend(music)
            source_stats["musicbrainz_music"] = len(music)
        except Exception as e:
            logger.error(f"MusicBrainz music failed: {e}")
            source_stats["musicbrainz_music"] = 0
            errors["musicbrainz_music"] = str(e)

        # ═══════ V5 NEW SOURCES ═══════

        # Podcast Index: Podcasts
        if "podcast_index" in self.v5_sources:
            try:
                podcasts = self.v5_sources["podcast_index"].fetch_podcasts(date)
                all_releases.extend(podcasts)
                source_stats["podcast_index"] = len(podcasts)
            except Exception as e:
                logger.error(f"Podcast Index failed: {e}")
                source_stats["podcast_index"] = 0
                errors["podcast_index"] = str(e)

        # Board Game Geek: Board Games
        if "bgg" in self.v5_sources:
            try:
                boardgames = self.v5_sources["bgg"].fetch_boardgames(date)
                all_releases.extend(boardgames)
                source_stats["bgg_boardgames"] = len(boardgames)
            except Exception as e:
                logger.error(f"BoardGameGeek failed: {e}")
                source_stats["bgg_boardgames"] = 0
                errors["bgg_boardgames"] = str(e)

        # RAWG: Additional Games
        if "rawg" in self.v5_sources:
            try:
                rawg_games = self.v5_sources["rawg"].fetch_games(date)
                # Only add games not already from IGDB
                existing_titles = {r["title"].lower() for r in all_releases if r["media_type"] == "game"}
                new_games = [g for g in rawg_games if g["title"].lower() not in existing_titles]
                all_releases.extend(new_games)
                source_stats["rawg_games"] = len(new_games)
            except Exception as e:
                logger.error(f"RAWG failed: {e}")
                source_stats["rawg_games"] = 0
                errors["rawg_games"] = str(e)

        # NewsData: Entertainment News
        if "newsdata" in self.v5_sources:
            try:
                news = self.v5_sources["newsdata"].fetch_news(date)
                all_releases.extend(news)
                source_stats["newsdata"] = len(news)
            except Exception as e:
                logger.error(f"NewsData failed: {e}")
                source_stats["newsdata"] = 0
                errors["newsdata"] = str(e)

        # ═══════ ENRICHMENT ═══════
        logger.info(f"{'─' * 40}")
        logger.info("Running enrichment passes...")

        # OMDb: Add Rotten Tomatoes / Metacritic scores
        if "omdb" in self.enrichers:
            try:
                self.enrichers["omdb"].enrich(all_releases, max_lookups=40)
            except Exception as e:
                logger.error(f"OMDb enrichment failed: {e}")

        # Watchmode: Add streaming availability
        if "watchmode" in self.enrichers:
            try:
                self.enrichers["watchmode"].enrich(all_releases, max_lookups=20)
            except Exception as e:
                logger.error(f"Watchmode enrichment failed: {e}")

        # Sort by popularity
        all_releases.sort(
            key=lambda r: r.get("metadata", {}).get("popularity", 0),
            reverse=True,
        )

        # Build output
        output = {
            "date": date,
            "ingested_at": utcnow_iso(),
            "total_releases": len(all_releases),
            "source_stats": source_stats,
            "filters_applied": {
                "min_movie_runtime": MIN_MOVIE_RUNTIME,
                "language_filter": LANGUAGE_FILTER,
                "include_talk_shows": INCLUDE_TALK_SHOWS,
                "include_reality": INCLUDE_REALITY,
                "include_news": INCLUDE_NEWS,
                "include_singles": INCLUDE_SINGLES,
                "music_cover_art_limit": MUSIC_COVER_ART_LIMIT,
            },
            "releases": all_releases,
        }

        if errors:
            output["errors"] = errors

        # Save
        output_file = OUTPUT_DIR / f"releases_{date}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        logger.info("")
        logger.info(f"Ingestion complete for {date}")
        logger.info(f"  Total releases: {len(all_releases)}")
        for source, count in source_stats.items():
            logger.info(f"  {source}: {count}")
        if errors:
            logger.warning(f"  Errors: {errors}")
        logger.info(f"  Output: {output_file}")
        logger.info("")

        return output


# ═══════════════════════════════════════════════════════════════
# SCHEDULING
# ═══════════════════════════════════════════════════════════════

def run_scheduled():
    try:
        import schedule
    except ImportError:
        print("Install schedule for daily runs: pip install schedule")
        print("Running once instead...")
        run_once(0)
        return

    pipeline = UnreeledPipeline()

    def daily_job():
        today = utcnow().strftime("%Y-%m-%d")
        try:
            pipeline.ingest_date(today)
        except Exception as e:
            logger.error(f"Daily ingestion failed: {e}")

    daily_job()
    schedule.every().day.at("06:00").do(daily_job)

    logger.info("Scheduler running — daily ingestion at 06:00 UTC")
    logger.info("Press Ctrl+C to stop")

    while True:
        schedule.run_pending()
        time.sleep(60)


def run_once(days_back: int = 0):
    pipeline = UnreeledPipeline()
    target_date = utcnow() - timedelta(days=days_back)
    date_str = target_date.strftime("%Y-%m-%d")
    result = pipeline.ingest_date(date_str)

    print(f"\n{'━' * 60}")
    print(f"  UNREELED — Release Summary for {date_str}")
    print(f"{'━' * 60}")
    print(f"  Total: {result['total_releases']} releases\n")

    for release in result["releases"][:12]:
        media_icons = {
            "movie": "🎬", "tv": "📺", "book": "📖",
            "game": "🎮", "anime": "🎌", "play": "🎭",
            "music": "🎵",
        }
        icon = media_icons.get(release["media_type"], "📦")
        title = release["title"][:50]
        genres = ", ".join(release["genres"][:3]) if release["genres"] else "—"
        print(f"  {icon} {title}")
        print(f"     {release['media_type'].upper()} · {genres}")
        if release["synopsis"]:
            synopsis = release["synopsis"][:100]
            if len(release["synopsis"]) > 100:
                synopsis += "..."
            print(f"     {synopsis}")
        print()

    remaining = result["total_releases"] - 12
    if remaining > 0:
        print(f"  ... and {remaining} more\n")

    print(f"  Full output: output/releases_{date_str}.json")
    print(f"{'━' * 60}\n")


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="UNREELED — Media Release Ingestion Pipeline v4",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python unreeled_ingest.py                    # Today's releases
  python unreeled_ingest.py --days-back 3      # 3 days ago
  python unreeled_ingest.py --date 2026-02-20  # Specific date
  python unreeled_ingest.py --schedule         # Daily at 6 AM UTC
        """,
    )

    parser.add_argument("--schedule", action="store_true", help="Run daily at 6 AM UTC")
    parser.add_argument("--days-back", type=int, default=0, help="Days back from today (default: 0)")
    parser.add_argument("--date", type=str, default=None, help="Specific date (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.schedule:
        run_scheduled()
    elif args.date:
        pipeline = UnreeledPipeline()
        pipeline.ingest_date(args.date)
    else:
        run_once(args.days_back)
