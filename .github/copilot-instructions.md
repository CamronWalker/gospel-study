# Gospel Study Scraping and Processing Project

## Project Overview
This project scrapes LDS General Conference talks and scriptures from churchofjesuschrist.org, processes them into structured JSON, enriches with external resources and AI-generated summaries using x.ai, and generates/updates hierarchical Markdown files for Obsidian with preserved annotations and wiki-links for cross-references.

Key directories:
- `conference_json/`: Per-conference JSON files (e.g., `2024-october.json`) with sessions, talks, content, sources.
- `scriptures_json/`: Per-volume JSON files (e.g., `book_of_mormon.json`) with books, chapters, verses, resources.
- `Conference/`: Generated MD files for talks, organized by year-month.
- `Scriptures/`: Generated MD files for scriptures, in volume/book/chapter hierarchy.
- Root: Core Python scripts for scraping, AI processing, MD updating.

## Major Components and Data Flow
- **Scraping**: `conference_create_json.py` fetches conference structures and talk lists; `conference_scrape_talks.py` extracts detailed content, converts to markdown with wiki-links for scriptures/conferences, handles sources/footnotes.
- **AI Enrichment**: `scripture_ai.py` generates chapter summaries (child/normal/context), tags, related scriptures via x.ai API; `conference_ai.py` (WIP) for talks.
- **MD Generation**: `scripture_md_updater.py` creates/updates chapter MD with YAML frontmatter, resources, AI callouts, preserves verse annotations; `conference_md_updater.py` for talks.

Data Flow:
1. Scrape conference lists → JSON skeletons with talks/sessions.
2. Scrape talk content → Update JSON with body (headings/paragraphs/images), markdown, sources (footnotes).
3. AI processing → Add ai_resources (summaries, tags, related links) to chapters/talks.
4. Update MD → Generate frontmatter with cleaned resource keys (e.g., cfm_2024_url), AI sections, verse/talk content with wiki-links.

Structural decisions: JSON for intermediate storage enables modular processing; MD hierarchy mirrors scripture organization for Obsidian navigation; wiki-links facilitate cross-references without full DB.

Example: `conference_json/2024-october.json` has 'sessions' dict with talks including 'body' array, 'full_markdown', 'sources', 'resources' array.

## Critical Developer Workflows
- Scrape conference lists: `python conference_create_json.py 1971-2025` (handles April/October, saves per-conference JSON).
- Scrape talk content: `python conference_scrape_talks.py 2020-2023 [--replace]` (parallel retries, updates JSON with content; --replace resets existing).
- Add AI to scriptures: `python scripture_ai.py --update "Matthew 5" [--debug]` (specific chapter/book/volume, parallel with ThreadPoolExecutor, x.ai API via .env).
- Update scripture MD: `python scripture_md_updater.py` (processes all volumes, preserves below verses, uses book_orders for numbering).
- Debugging: Set --debug in AI scripts for prompts/responses; check Selenium logs in scraping; use tqdm for progress.

No explicit build/test commands; run scripts directly in Python env.

## Project-Specific Conventions
- JSON: snake_case keys; 'body' as typed array (heading/paragraph/image); wiki-links in markdown (e.g., [[Alma 5#1|Alma 5:1-3]][[Alma 5#2|]]...).
- MD: Frontmatter with cleaned keys (e.g., saints_ai_study_guide_url); callouts for AI summaries; hierarchical folders (e.g., Scriptures/Book of Mormon/01 1 Nephi/1 Nephi 1.md); preserves user annotations below verses.
- Scripture handling: book_map in scraping for abbreviations; D&C uses "D&C" in links; related scriptures parsed with ~ delimiter.
- Parallelism: ThreadPoolExecutor with tqdm for AI/scraping efficiency; max_workers=10.
- Error handling: Retries (e.g., 2x in scraping, 5x in AI with exponential backoff); skips failures.

Differs from common: Custom wiki-link parsing for scriptures/conferences instead of standard hyperlinks; AI prompts tailored for LDS context with specific formats.

## Integration Points and Dependencies
- External: Selenium/WebDriver for scraping (auto-manages ChromeDriver); x.ai SDK for AI (XAI_API_KEY in .env); optional YouTube API for resources.
- Cross-component: Scripts load .env; JSON read/write directly; scraping embeds wiki-links using book_map and conference JSON lookups.
- Python libs: selenium, webdriver-manager, requests, xai_sdk, dotenv, tqdm, concurrent.futures.

Reference `README.md` for workflows; `conference_scrape_talks.py` for link parsing patterns; `scripture_ai.py` for AI prompt structure.
