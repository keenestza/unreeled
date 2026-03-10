# Docs site directory

This folder is the source of truth for the deployed UNREELED site.

## What to edit
- `docs/template.html` — main site template.
- `docs/data/` — generated release JSON files.
- `docs/index.html` — generated output from `scripts/build_site.py` (auto-built by Daily Ingest workflow).

## What to ignore
- Do **not** treat `public/` as the primary site source.
- If `public/` exists, consider it non-authoritative for deployment.

## Local preview
Open `docs/index.html` in your browser after running the build pipeline.

## Generated files
- Do not manually edit `docs/index.html` in routine changes; it is generated from `docs/template.html` by the build pipeline.
