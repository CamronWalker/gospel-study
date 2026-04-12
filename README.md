# Gospel Study in Obsidian

Scrapes LDS General Conference talks and scriptures, processes them into JSON, enriches with AI summaries, and generates Obsidian Markdown with cross-referenced wiki-links.

## Quick Start

```bash
# Scrape a conference (structure + talks + IDs + thumbnails + resources → JSON + Markdown)
py conference.py pipeline 2026-april

# Or run steps individually:
py conference.py scrape 2026-april          # scrape to JSON
py conference.py resources 2026-april       # add YouTube, Saints AI, BYU, Gospel Library links
py conference.py markdown 2026-april        # generate Obsidian markdown

# Re-scrape or overwrite existing data
py conference.py pipeline 2026-april --replace

# Year ranges work too
py conference.py scrape 2020-2025
```

## Project Structure

```
conference.py              # Unified conference pipeline (scrape, resources, markdown)
conference_ai.py           # AI summaries via xAI Grok (separate step)
scripture_ai.py            # AI summaries for scripture chapters
scripture_md_updater.py    # Generate scripture Markdown files

conference_json/           # Conference JSON files (source of truth)
scriptures_json/           # Scripture volume JSON files
Conference/                # Generated conference Markdown for Obsidian
Scriptures/                # Generated scripture Markdown for Obsidian
archive/                   # Old scripts and backup data
```

## Conference Pipeline

| Step | Command | What it does |
|------|---------|-------------|
| Scrape | `py conference.py scrape 2026-april` | Fetches conference list + all talk content → JSON |
| Resources | `py conference.py resources 2026-april` | Adds YouTube, Gospel Library, Saints AI, BYU links |
| Markdown | `py conference.py markdown 2026-april` | Generates Obsidian notes from JSON |
| All-in-one | `py conference.py pipeline 2026-april` | Runs scrape → resources → markdown |

Target formats: `2026-april`, `2026-04`, `2020-2025`, `2026`

### Resource Links

- **Gospel Library** — direct link to churchofjesuschrist.org
- **YouTube Video** — exact video link via playlist API (falls back to search URL if no API key)
- **Saints AI Study Guide** — saintsai.org study guide (2017+)
- **BYU Citation Index** — scriptures.byu.edu cross-references

### AI Summaries (separate step)

```bash
py conference_ai.py --update 2026-04              # add AI summaries to JSON
py conference_ai.py --update 2026-04 --force      # re-generate all
py conference.py markdown 2026-april --replace    # then regenerate markdown
```

## Scripture Pipeline

```bash
py scripture_ai.py --update "Matthew 5"           # AI summaries for a chapter
py scripture_ai.py --update new_testament.json     # entire volume
py scripture_md_updater.py                         # generate all scripture Markdown
```

## Dependencies

```bash
pip install requests beautifulsoup4 tqdm python-dotenv
```

Optional:
- `YOUTUBE_API_KEY` in `.env` — exact YouTube video links via playlist lookup (~3 API calls per conference)
- `XAI_API_KEY` in `.env` — AI summaries via xAI Grok (`conference_ai.py`)
- Without API keys, YouTube falls back to search URLs and AI step is skipped

See `.env.example` for setup.
