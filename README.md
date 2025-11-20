# Gospel Study in Obsidian

This project scrapes LDS General Conference talks and scriptures, processes them into JSON format, adds resources and AI-generated summaries, and generates/updates Markdown files for Obsidian. The goal is to create cross-referenced study materials.

## Project Structure

### Folders
- `conference_json/`: JSON files for conferences (e.g., `2024-october.json`)
- `scriptures_json/`: JSON files for scripture volumes (e.g., `book_of_mormon.json`)
- `Conference/`: Generated Markdown files for conference talks
- `Scriptures/`: Generated Markdown files for scriptures (created by scripts)

### Scripts
- `conference_ai.py`: Generates AI summaries, tags, and related resources for conference talks using x.ai API.
- `conference_md_updater.py`: Creates or updates Markdown files for conference talks in Obsidian, incorporating frontmatter, properties, AI summaries, and talk content.
- `conference_create_json.py`: Scrapes LDS General Conference years to create json files for each year listing talks.
- `conference_scrape_talks.py`: Scrapes the LDS General Conference talks listed in the conferences in folder `conference_json`. 
- `scripture_ai.py`: Generates AI summaries, tags, and related scriptures for scripture chapters using x.ai API.
- `scripture_md_updater.py`: Creates or updates Markdown files for scriptures in Obsidian, adding frontmatter, resources, AI summaries, and preserving verse content.

## Workflow
1. Scrape conference data: `python conference_scrape_to_json.py <year> <month>`
2. Add AI to conference: `python conference_ai.py <year> <month>`
3. Update conference MD: `python conference_md_updater.py <year> <month>`
4. Add AI to scriptures: `python scripture_ai.py`
5. Update scripture MD: `python scripture_md_updater.py`

## Dependencies
- Python libraries: selenium, webdriver-manager, requests, openai, dotenv, tqdm, concurrent.futures
- ChromeDriver (auto-managed)
- x.ai API key in .env for AI summaries
- YouTube API key in .env for resource addition

