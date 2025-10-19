# Gospel Study Scraping and Processing Project

## Project Overview
This project scrapes LDS General Conference talks and scriptures, processes them into JSON format, adds resources and AI-generated summaries, and generates/updates Markdown files for Obsidian. The goal is to create cross-referenced study materials.

Key directories:
- `conference_json/`: JSON files for conferences (e.g., `2024-october.json`)
- `scriptures_json/`: JSON files for scripture volumes (e.g., `book_of_mormon.json`)
- Scripts are in the root directory.

## Major Components
- **Scraping**: `scrape_conference_talk_urls.py` fetches talk URLs; `scrape-conference.py` uses Selenium to extract talk content, footnotes, and scriptures.
- **Resource Addition**: Scripts like `add_conference_resource_newsroom.py` add external links (e.g., YouTube, Church News) to conference_resources.json.
- **AI Processing**: `scripture_gpt_summaries.py` uses x.ai API to generate summaries, tags, and related scriptures for chapters.
- **Markdown Updating**: `md_scripture_updater.py` creates/updates Obsidian MD files with frontmatter (YAML), resources, and verse content. `md_conference_updater.py` (WIP) for conferences.

## Data Flow
1. Scrape URLs → Talk list
2. Talk list → Scrape content → JSON with structured body (headings, paragraphs, images), sources, scriptures
3. Add resources → Update JSON with talk-resources array (e.g., Gospel Library URL, YouTube)
4. AI summaries (for scriptures) → Add ai_resources to chapters (summaries, tags, related scriptures)
5. Update MD → Generate frontmatter with cleaned keys (e.g., cfm_2024_url), preserve existing verse annotations

Example JSON structure in `conference_json/2024-october.json`:
- conference, year, month
- sessions: dict of lists with talk dicts (title, speaker, body, sources, scriptures, talk-resources)

## Critical Workflows
- Scrape URLs: `python3 scrape_conference_talk_urls.py <start_year> <end_year>`
- Scrape conference: `python scrape-conference.py <year> <month>` (uses threading for parallel scraping)
- Add newsroom resources: `python add_conference_resource_newsroom.py`
- Generate scripture summaries: `python scripture_gpt_summaries.py` (parallel processing with ThreadPoolExecutor, requires OPENAI_API_KEY in .env)
- Update scripture MD: `python md_scripture_updater.py` (preserves content below verses, uses book orders for folder structure)

Debugging: Use `debug=True` in AI scripts for verbose output. Check Selenium errors in scraping.

## Project Conventions
- JSON keys use snake_case; MD frontmatter uses cleaned lowercase keys (e.g., "Saints AI Study Guide" → saints_ai_study_guide_url)
- Scripture book maps in `scrape-conference.py` for wikilinks (e.g., 'bofm/alma' → 'Alma')
- Parallel processing with ThreadPoolExecutor and tqdm for progress
- Error handling: Retries in scraping, skips on failures
- MD files in hierarchical folders (e.g., Scriptures/Book of Mormon/01 1 Nephi/1 Nephi 1.md)

## Dependencies and Integration
- Python libs: selenium, webdriver-manager, requests, openai, dotenv, tqdm, concurrent.futures
- External: ChromeDriver (auto-managed), OpenAI API (via .env)
- Integration: Scripts load .env from root; JSON files read/written directly.

Reference `README.md` for sample talk-resources and overall goals.
