# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project scrapes LDS General Conference talks and scriptures from churchofjesuschrist.org, processes them into JSON, enriches them with AI-generated summaries via the xAI SDK (Grok), and generates Obsidian-compatible Markdown files with cross-referenced wiki-links.

## Key Commands

```bash
# === Unified conference pipeline (conference.py) ===

# Scrape a conference (structure + talk content + IDs + thumbnails → JSON)
py conference.py scrape 2026-april
py conference.py scrape 2020-2025              # year range
py conference.py scrape 2026-april --replace   # re-scrape existing

# Add resource URLs (Gospel Library, Saints AI, BYU, YouTube via Gemini CLI)
py conference.py resources 2026-april
py conference.py resources 2026-april --only youtube   # just one type
py conference.py resources 2026-april --replace        # overwrite existing

# Generate Obsidian markdown from JSON
py conference.py markdown 2026-april
py conference.py markdown 2026-april --replace  # full overwrite (default preserves "# Notes")

# Full pipeline: scrape + resources + markdown
py conference.py pipeline 2026-april

# === Legacy scripts (still work, used for bulk/AI operations) ===

# AI summaries, topics, and mindmaps (conference_ai.py — uses xAI Grok)
python3 conference_ai.py --update 2025-10
python3 conference_ai.py --update 2023-2025

# Scripture pipeline
python3 scripture_ai.py --update "Matthew 5" --debug
python3 scripture_ai.py --update new_testament.json
python3 scripture_md_updater.py
```

No build system, test framework, or linter is configured. Scripts are run directly with Python.
Dependencies: `requests`, `beautifulsoup4`, `tqdm` (Gemini CLI optional for YouTube URLs).

## Architecture

### Data Flow Pipeline
1. **`conference.py scrape`** → Scrapes conference list + talk content + IDs + thumbnails into `conference_json/` (uses `requests` + `BeautifulSoup`, no Selenium)
2. **`conference.py resources`** → Adds Gospel Library, Saints AI, BYU, YouTube (via Gemini CLI) URLs to `resources` array
3. **`conference.py markdown`** → Generates YAML frontmatter + callouts + wiki-linked Obsidian markdown
4. **AI enrichment** (`conference_ai.py`) → Adds `ai_resources` with summaries, topics, mindmaps (separate step, uses xAI Grok)
5. **Scripture pipeline** (`scripture_ai.py` / `scripture_md_updater.py`) → Scripture volumes

### JSON Structure (Conference)
`conference_json/{year}-{month}.json` contains:
- `sessions` → dict of session names, each with `talks` dict keyed by slug (e.g., `/2025/10/19oaks`)
- Each talk has: `title`, `speaker`, `url`, `filename`, `body` (array of typed items: heading/paragraph/image), `full_markdown`, `sources`, `resources`, `ai_resources`

### JSON Structure (Scripture)
`scriptures_json/{volume}.json` (e.g., `book_of_mormon.json`) contains books → chapters → verses, with `ai_resources` for summaries/tags.

### MD Output Conventions
- Conference talks go to `Conference/{year}-{MonthAbbr}/` (e.g., `Conference/2025-Oct/`)
- Scripture chapters go to `Scriptures/{Volume}/{## Book}/{Chapter}.md`
- The MD updater preserves user annotations below `# Notes` when updating (non-replace mode)
- Wiki-links format: `[[Book Chapter#verse|Display Text]]` for scriptures, `[[Filename|Display Text]]` for conference cross-references

## Key Conventions

- **Wiki-links**: Scripture references are converted to Obsidian wiki-links using `book_map` in `conference_scrape_talks.py` (maps URL abbreviations like `bofm/alma` → `Alma`). D&C uses `D&C` prefix in links.
- **AI model**: Uses `grok-4-1-fast-reasoning` via `xai_sdk.Client`. Requires `XAI_API_KEY` in `.env`.
- **Scraping**: `conference.py` uses `requests` + `BeautifulSoup` (no Selenium). Church site is SSR.
- **YouTube**: Uses Gemini CLI (`gemini -p`) to find video URLs instead of YouTube Data API.
- **Parallelism**: `ThreadPoolExecutor` with `max_workers=5-10` for AI calls. Uses `tqdm` for progress.
- **Retry logic**: AI calls retry up to 5x with exponential backoff. Scraping retries 2x.
- **Filenames**: Talk files named `{Title} — {LastName} {Year} {MonthAbbr}` with special characters sanitized.
- **Conference target parsing**: Scripts accept `YYYY-MM` (e.g., `2025-10`), `YYYY-YYYY` ranges, or single years. Months are `04`=April, `10`=October.

## Environment Variables (.env)

- `XAI_API_KEY` — Required for AI summary generation (conference_ai.py)
- Gemini CLI — Used for YouTube URL lookup (must be installed and logged in)
