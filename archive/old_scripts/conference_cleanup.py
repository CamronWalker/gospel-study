"""
Cleanup script for conference JSON files.

Fixes:
1. Unicode escapes (\u2014, \u2019, etc.) by re-saving with ensure_ascii=False
2. Scrapes thumbnail URLs from churchofjesuschrist.org and adds them to each talk
3. Reorders talks within sessions to match website presentation order

Usage Examples:
  # Clean up all JSON files (unicode fix + thumbnails + ordering)
  python3 conference_cleanup.py 1971-2025

  # Single conference
  python3 conference_cleanup.py 2025-10

  # Specific conference by name
  python3 conference_cleanup.py 2025-October

  # Just fix unicode across all files (no scraping needed, fast)
  python3 conference_cleanup.py --unicode-only
"""

import os
import json
import sys
import re
import time
import argparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm

JSON_DIR = 'conference_json'


def get_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)


def get_conference_filename(year, month):
    sanitized_conference = re.sub(r'[^a-z0-9\- ]', '', f"{year}-{month.lower()}", flags=re.IGNORECASE)
    return os.path.join(JSON_DIR, f"{sanitized_conference}.json")


def fix_unicode(file_path, pbar=None):
    """Re-save a JSON file with ensure_ascii=False to convert unicode escapes to actual characters."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    if pbar:
        pbar.write(f"Fixed unicode: {file_path}")


def fix_all_unicode():
    """Fix unicode escapes in all conference JSON files."""
    json_files = sorted([f for f in os.listdir(JSON_DIR) if f.endswith('.json')])
    pbar = tqdm(json_files, desc="Fixing unicode escapes")
    for filename in pbar:
        pbar.set_description(f"Fixing {filename}")
        fix_unicode(os.path.join(JSON_DIR, filename), pbar)


def scrape_thumbnails_and_order(driver, year, month_code, data, pbar=None):
    """Scrape thumbnail URLs and talk order from the conference page."""
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"

    try:
        driver.get(conference_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, 'app')))
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'ul.doc-map')))

        # Scroll to load full content
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(1)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        # Extract talk slugs, thumbnails, and order from the page
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')

        # Build a mapping of slug -> thumbnail and an ordered list of slugs
        slug_thumbnails = {}
        ordered_slugs = []

        for li in li_elements:
            try:
                a = li.find_element(By.TAG_NAME, 'a')
                href = a.get_attribute('href') or ''
            except:
                continue

            # Extract slug from href
            slug_match = re.search(rf'/{year}/{month_code}/([^?/]+)', href)
            if not slug_match:
                continue
            slug = slug_match.group(1)
            talk_key = f"/{year}/{month_code}/{slug}"

            # Skip session links
            if slug.endswith('-session'):
                continue

            ordered_slugs.append(talk_key)

            # Try to find thumbnail image
            try:
                img = li.find_element(By.TAG_NAME, 'img')
                src = img.get_attribute('src') or ''
                if src:
                    # Upgrade thumbnail width from 200px (or any size) to 300px
                    src = re.sub(r'%21\d+%2C', '%21300%2C', src)
                    slug_thumbnails[talk_key] = src
            except:
                pass

        # Apply thumbnails and reorder talks
        thumbnail_count = 0
        reorder_count = 0

        for session_name, session_data in data['sessions'].items():
            talks = session_data.get('talks', {})

            # Add thumbnails
            for talk_key, talk in talks.items():
                if talk_key in slug_thumbnails:
                    talk['thumbnail'] = slug_thumbnails[talk_key]
                    thumbnail_count += 1

            # Reorder talks based on website order
            session_slugs = [s for s in ordered_slugs if s in talks]
            if session_slugs:
                reordered = {}
                # First add talks in website order
                for slug in session_slugs:
                    reordered[slug] = talks[slug]
                # Then add any talks not found on the page (preserve them)
                for key in talks:
                    if key not in reordered:
                        reordered[key] = talks[key]
                if list(reordered.keys()) != list(talks.keys()):
                    reorder_count += 1
                session_data['talks'] = reordered

        if pbar:
            pbar.write(f"  Thumbnails added: {thumbnail_count}, Sessions reordered: {reorder_count}")

    except Exception as e:
        msg = f"Error scraping {year}/{month_code}: {e}"
        if pbar:
            pbar.write(msg)
        else:
            print(msg)


def cleanup_conference(driver, year, month, pbar=None):
    """Full cleanup for a single conference: unicode fix, thumbnails, ordering."""
    month_code = '04' if month.lower() in ['apr', 'april'] else '10' if month.lower() in ['oct', 'october'] else None
    if not month_code:
        if pbar:
            pbar.write(f"Invalid month: {month}")
        return

    file_path = get_conference_filename(year, month)
    if not os.path.exists(file_path):
        if pbar:
            pbar.write(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as f:
        data = json.load(f)

    # Scrape thumbnails and fix ordering
    scrape_thumbnails_and_order(driver, year, month_code, data, pbar)

    # Save with unicode fix
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    if pbar:
        pbar.write(f"Cleaned: {file_path}")


def parse_target(target):
    """Parse a target string into a list of (year, month) tuples.

    Supports:
      - YYYY-YYYY (year range, both April and October)
      - YYYY-MM (specific conference, MM = 04 or 10)
      - YYYY-Month (e.g., 2025-October)
    """
    conferences = []

    # Try YYYY-Month (e.g., 2025-October, 2025-April)
    match = re.match(r'^(\d{4})-(april|october|apr|oct)$', target, re.IGNORECASE)
    if match:
        year = int(match.group(1))
        month = match.group(2)
        if month.lower() in ['apr', 'april']:
            conferences.append((year, 'April'))
        else:
            conferences.append((year, 'October'))
        return conferences

    # Try YYYY-MM (e.g., 2025-04, 2025-10)
    match = re.match(r'^(\d{4})-(04|10)$', target)
    if match:
        year = int(match.group(1))
        month_code = match.group(2)
        month = 'April' if month_code == '04' else 'October'
        conferences.append((year, month))
        return conferences

    # Try YYYY-YYYY (year range)
    match = re.match(r'^(\d{4})-(\d{4})$', target)
    if match:
        start_year = int(match.group(1))
        end_year = int(match.group(2))
        for year in range(start_year, end_year + 1):
            conferences.append((year, 'April'))
            conferences.append((year, 'October'))
        return conferences

    # Try single year
    match = re.match(r'^(\d{4})$', target)
    if match:
        year = int(match.group(1))
        conferences.append((year, 'April'))
        conferences.append((year, 'October'))
        return conferences

    return conferences


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Clean up conference JSON files")
    parser.add_argument('target', nargs='?', help="Conference target: YYYY-YYYY, YYYY-MM, YYYY-Month, or YYYY")
    parser.add_argument('--unicode-only', action='store_true', help="Only fix unicode escapes (no scraping)")
    args = parser.parse_args()

    if args.unicode_only:
        fix_all_unicode()
        sys.exit(0)

    if not args.target:
        parser.error("Must specify a target (e.g., 2025-10, 1971-2025) or use --unicode-only")

    conferences = parse_target(args.target)
    if not conferences:
        print(f"Invalid target: {args.target}")
        print("Use YYYY-YYYY (range), YYYY-MM (specific), YYYY-Month, or YYYY")
        sys.exit(1)

    driver = None
    try:
        driver = get_driver()
        pbar = tqdm(conferences, desc="Cleaning conferences")
        for year, month in pbar:
            pbar.set_description(f"{year} {month}")
            cleanup_conference(driver, year, month, pbar)
    finally:
        if driver:
            driver.quit()
