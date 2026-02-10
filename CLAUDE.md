# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project scrapes LDS General Conference talks and scriptures from churchofjesuschrist.org, processes them into JSON, enriches them with AI-generated summaries via the xAI SDK (Grok), and generates Obsidian-compatible Markdown files with cross-referenced wiki-links.

## Key Commands

```bash
# Step 1: Scrape conference talk lists into JSON skeletons
python3 conference_create_json.py 1971-2025

# Step 2: Scrape talk content (body, sources, footnotes)
python3 conference_scrape_talks.py 2020-2023          # year range
python3 conference_scrape_talks.py 2025-October       # single conference
python3 conference_scrape_talks.py 2020-2023 --replace  # overwrite existing

# Step 3: Generate external resource links (YouTube, BYU, Church News, Saints AI)
python3 conference_generate_resources.py 2020-2025 --resource all
python3 conference_generate_resources.py 2025-04 --resource youtube --replace

# Step 4: Add AI summaries, topics, and mindmaps to conference JSON
python3 conference_ai.py --update 2025-10
python3 conference_ai.py --update 2023-2025           # year range
python3 conference_ai.py --update 2025-10 --talk /2025/10/19oaks --update-summaries adult,youth
python3 conference_ai.py --update 2025-10 --update-questions
python3 conference_ai.py --update 2025-10 --force --debug

# Step 5: Generate/update Obsidian Markdown files from JSON
python3 conference_md_updater.py                      # all conferences
python3 conference_md_updater.py 2025 october         # specific conference
python3 conference_md_updater.py --replace            # full overwrite (default preserves notes below "# Notes")

# Scripture pipeline
python3 scripture_ai.py --update "Matthew 5" --debug  # specific chapter
python3 scripture_ai.py --update new_testament.json    # entire volume
python3 scripture_md_updater.py                        # generate all scripture MD files
```

No build system, test framework, or linter is configured. Scripts are run directly with Python.

## Architecture

### Data Flow Pipeline
1. **Scrape structure** (`conference_create_json.py`) → JSON skeletons with talk metadata in `conference_json/`
2. **Scrape content** (`conference_scrape_talks.py`) → Adds `body` (typed array), `full_markdown`, `sources` to JSON
3. **Add resources** (`conference_generate_resources.py`) → Adds YouTube, BYU, Church News, Saints AI URLs to `resources` array
4. **AI enrichment** (`conference_ai.py` / `scripture_ai.py`) → Adds `ai_resources` with summaries (adult/youth/children/new_members/non_members/kicker), topics with question-quote pairs, and Mermaid mindmaps
5. **MD generation** (`conference_md_updater.py` / `scripture_md_updater.py`) → YAML frontmatter + callouts + wiki-linked content for Obsidian

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
- **Parallelism**: `ThreadPoolExecutor` with `max_workers=5-10` for AI calls and scraping. Uses `tqdm` for progress.
- **Retry logic**: AI calls retry up to 5x with exponential backoff. Scraping retries 2x.
- **Filenames**: Talk files named `{Title} — {LastName} {Year} {MonthAbbr}` with special characters sanitized.
- **Conference target parsing**: Scripts accept `YYYY-MM` (e.g., `2025-10`), `YYYY-YYYY` ranges, or single years. Months are `04`=April, `10`=October.

## Environment Variables (.env)

- `XAI_API_KEY` — Required for AI summary generation
- `YOUTUBE_API_KEY` — Optional, for YouTube resource lookup
