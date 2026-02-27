# UNREELED — Daily Media Release Tracker

Track daily releases across movies, TV, books, games, anime, and music with community spoiler-tagged discussions.

🌐 **Live site**: [unreeled.netlify.app](https://unreeled.netlify.app)

## How it works

1. **GitHub Actions** runs the ingestion script daily at 6 AM UTC
2. The script pulls from 5 free APIs (TMDB, Open Library, IGDB, Jikan, MusicBrainz)
3. A build script bakes the JSON data into a static HTML file
4. The commit triggers **Netlify** to auto-deploy the updated site

## Project Structure

```
unreeled/
├── .github/workflows/
│   └── daily-ingest.yml    # GitHub Actions workflow (daily cron)
├── scripts/
│   ├── unreeled_ingest.py  # Data ingestion pipeline
│   └── build_site.py       # Builds static HTML from data + template
├── public/
│   ├── template.html       # Site template (don't edit index.html directly)
│   ├── index.html           # Built output (auto-generated)
│   └── data/
│       └── latest.json     # Latest release data (auto-generated)
└── README.md
```

## Setup

### 1. Create a GitHub repo

Push this entire folder to a new GitHub repository.

### 2. Add your API secrets

Go to your repo → **Settings → Secrets and variables → Actions** and add:

- `TMDB_API_KEY` — your TMDB API key
- `IGDB_CLIENT_ID` — your Twitch/IGDB client ID
- `IGDB_CLIENT_SECRET` — your Twitch/IGDB client secret

(Open Library, Jikan, and MusicBrainz don't need keys.)

### 3. Connect Netlify

- In Netlify, connect your GitHub repo
- Set **publish directory** to `public`
- No build command needed (the HTML is pre-built by GitHub Actions)

### 4. Enable GitHub Actions

Actions should run automatically. You can also trigger manually:
- Go to **Actions** tab → **Daily Media Ingestion** → **Run workflow**

## Running locally

```bash
# Install dependencies
pip install requests python-dotenv

# Create .env file with your API keys
echo "TMDB_API_KEY=your_key" > scripts/.env
echo "IGDB_CLIENT_ID=your_id" >> scripts/.env
echo "IGDB_CLIENT_SECRET=your_secret" >> scripts/.env

# Run ingestion
cd scripts
python unreeled_ingest.py --date 2026-02-20

# Build the site
cd ..
mkdir -p public/data
cp scripts/output/releases_*.json public/data/latest.json
python scripts/build_site.py

# Open public/index.html in your browser
```

## Data Sources

| Source | Media Type | API Key Required |
|--------|-----------|-----------------|
| [TMDB](https://www.themoviedb.org/) | Movies, TV | Yes (free) |
| [Open Library](https://openlibrary.org/) | Books | No |
| [IGDB](https://api-docs.igdb.com/) | Games | Yes (free, via Twitch) |
| [Jikan](https://jikan.moe/) | Anime | No |
| [MusicBrainz](https://musicbrainz.org/) | Music (CD, Vinyl, Digital) | No |

## Configuration

Edit the filter settings at the top of `scripts/unreeled_ingest.py`:

```python
MIN_MOVIE_RUNTIME = 40        # Filter out short films
INCLUDE_TALK_SHOWS = False    # Skip talk shows
INCLUDE_REALITY = False       # Skip reality TV
INCLUDE_NEWS = False          # Skip news programs
INCLUDE_SINGLES = False       # Skip music singles
MUSIC_COVER_ART_LIMIT = 80   # Cover art lookups per run
```
