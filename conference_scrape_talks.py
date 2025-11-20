"""
Sample Usage:
- To scrape talk content for a range of years (both April and October):
  python3 conference_scrape_talk_content.py 2020-2023
  (This will scrape content for all talks in the specified conferences and update JSON files like 'conference_json/2025-october.json'.)
- To scrape with replace:
  python3 conference_scrape_talk_content.py 2020-2023 --replace
  (This will replace existing body, full_markdown, sources.)
  TODO add conference talk parser to .md links file generator so need md filename in json to wikilink to
 
  """
import os
import json
import time
import sys
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
import argparse
import logging
# Suppress Selenium stacktraces
logging.getLogger('selenium').setLevel(logging.WARNING)
# Create conference_json directory if it doesn't exist
JSON_DIR = 'conference_json'
os.makedirs(JSON_DIR, exist_ok=True)
# Book map for scripture abbreviations to full names
book_map = {
    # Book of Mormon
    'bofm/1-ne': '1 Nephi',
    'bofm/2-ne': '2 Nephi',
    'bofm/jacob': 'Jacob',
    'bofm/enos': 'Enos',
    'bofm/jarom': 'Jarom',
    'bofm/omni': 'Omni',
    'bofm/w-of-m': 'Words of Mormon',
    'bofm/mosiah': 'Mosiah',
    'bofm/alma': 'Alma',
    'bofm/hel': 'Helaman',
    'bofm/3-ne': '3 Nephi',
    'bofm/4-ne': '4 Nephi',
    'bofm/morm': 'Mormon',
    'bofm/ether': 'Ether',
    'bofm/moro': 'Moroni',
    # Doctrine and Covenants
    'dc-testament/dc': 'D&C',
    # Old Testament
    'ot/gen': 'Genesis',
    'ot/ex': 'Exodus',
    'ot/lev': 'Leviticus',
    'ot/num': 'Numbers',
    'ot/deut': 'Deuteronomy',
    'ot/josh': 'Joshua',
    'ot/judg': 'Judges',
    'ot/ruth': 'Ruth',
    'ot/1-sam': '1 Samuel',
    'ot/2-sam': '2 Samuel',
    'ot/1-kgs': '1 Kings',
    'ot/2-kgs': '2 Kings',
    'ot/1-chr': '1 Chronicles',
    'ot/2-chr': '2 Chronicles',
    'ot/ezra': 'Ezra',
    'ot/neh': 'Nehemiah',
    'ot/esth': 'Esther',
    'ot/job': 'Job',
    'ot/ps': 'Psalms',
    'ot/prov': 'Proverbs',
    'ot/eccl': 'Ecclesiastes',
    'ot/song': 'Song of Solomon',
    'ot/isa': 'Isaiah',
    'ot/jer': 'Jeremiah',
    'ot/lam': 'Lamentations',
    'ot/ezek': 'Ezekiel',
    'ot/dan': 'Daniel',
    'ot/hosea': 'Hosea',
    'ot/joel': 'Joel',
    'ot/amos': 'Amos',
    'ot/obad': 'Obadiah',
    'ot/jonah': 'Jonah',
    'ot/micah': 'Micah',
    'ot/nahum': 'Nahum',
    'ot/hab': 'Habakkuk',
    'ot/zeph': 'Zephaniah',
    'ot/hag': 'Haggai',
    'ot/zech': 'Zechariah',
    'ot/mal': 'Malachi',
    # New Testament
    'nt/matt': 'Matthew',
    'nt/mark': 'Mark',
    'nt/luke': 'Luke',
    'nt/john': 'John',
    'nt/acts': 'Acts',
    'nt/rom': 'Romans',
    'nt/1-cor': '1 Corinthians',
    'nt/2-cor': '2 Corinthians',
    'nt/gal': 'Galatians',
    'nt/eph': 'Ephesians',
    'nt/phlp': 'Philippians',
    'nt/col': 'Colossians',
    'nt/1-thes': '1 Thessalonians',
    'nt/2-thes': '2 Thessalonians',
    'nt/1-tim': '1 Timothy',
    'nt/2-tim': '2 Timothy',
    'nt/titus': 'Titus',
    'nt/philem': 'Philemon',
    'nt/heb': 'Hebrews',
    'nt/james': 'James',
    'nt/1-pet': '1 Peter',
    'nt/2-pet': '2 Peter',
    'nt/1-jn': '1 John',
    'nt/2-jn': '2 John',
    'nt/3-jn': '3 John',
    'nt/jude': 'Jude',
    'nt/rev': 'Revelation',
    # Pearl of Great Price (included for completeness)
    'pgp/moses': 'Moses',
    'pgp/abr': 'Abraham',
    'pgp/js-m': 'Joseph Smith—Matthew',
    'pgp/js-h': 'Joseph Smith—History',
    'pgp/a-of-f': 'Articles of Faith',
}
def get_wikilink(href, text):
    try:
        if not href.startswith('http'):
            href = 'https://www.churchofjesuschrist.org' + href
        # Extract path from absolute or relative URL
        parsed_url = re.match(r'https?://[^/]+(/study/scriptures/[^?#]+)(\?[^#]*)?(#.*)?', href)
        if not parsed_url:
            return None
        path = parsed_url.group(1)
        query = parsed_url.group(2) or ''
        fragment = parsed_url.group(3) or ''
        parts = path.split('/')[3:] # ['bofm', 'alma', '5'] or ['bofm', 'alma', '5.6-8']
        if len(parts) < 2:
            return None
        corpus = parts[0]
        book_abbr = parts[1]
        chapter = None
        start_verse = None
        end_verse = None
        if len(parts) > 2:
            chapter_str = parts[2]
            if '.' in chapter_str:
                chap_match = re.match(r'(\d+)\.(\d+)(?:-(\d+))?', chapter_str)
                if chap_match:
                    chapter = chap_match.group(1)
                    start_verse = int(chap_match.group(2))
                    end_verse = int(chap_match.group(3)) if chap_match.group(3) else start_verse
            if chapter is None:
                chap_match = re.match(r'\d+', chapter_str)
                if chap_match:
                    chapter = chapter_str
        # Override with ?id= if present
        if query:
            query_params = dict(q.split('=') for q in query.lstrip('?').split('&') if '=' in q)
            id_param = query_params.get('id')
            if id_param:
                id_match = re.match(r'(?:p|verse)?(\d+)(?:-(?:p|verse)?(\d+))?', id_param)
                if id_match:
                    start_verse = int(id_match.group(1))
                    end_verse = int(id_match.group(2)) if id_match.group(2) else start_verse
        # If still no verses, check fragment
        if start_verse is None and fragment:
            frag_match = re.match(r'#(?:p|verse)?(\d+)(?:-(?:p|verse)?(\d+))?', fragment)
            if frag_match:
                start_verse = int(frag_match.group(1))
                end_verse = int(frag_match.group(2)) if frag_match.group(2) else start_verse
        key = f"{corpus}/{book_abbr}"
        book_name = book_map.get(key)
        if not book_name:
            return None
        # Special case for Doctrine and Covenants
        if book_name == 'D&C':
            base_name = f"D&C {chapter}" if chapter else "D&C"
        else:
            base_name = f"{book_name} {chapter}" if chapter else book_name
        # No verses → whole chapter link
        if start_verse is None:
            return f"[[{base_name}]]"
        # Verses present
        if book_name == 'D&C':
            display_text = f"D&C {chapter}:{start_verse}-{end_verse}" if start_verse != end_verse else f"D&C {chapter}:{start_verse}"
        else:
            display_text = f"{book_name} {chapter}:{start_verse}-{end_verse}" if start_verse != end_verse else f"{book_name} {chapter}:{start_verse}"
        links = [f"[[{base_name}#{start_verse}|{display_text}]]"]
        if start_verse != end_verse:
            for v in range(start_verse + 1, end_verse + 1):
                links.append(f"[[{base_name}#{v}|]]")
        return "".join(links)
    except Exception as e:
        print(f"Warning: Failed to parse scripture link {href}: {e}")
        return None
def html_to_markdown(html, is_source=False):
    html = re.sub(r'<em>(.*?)</em>', r'*\1*', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<i>(.*?)</i>', r'*\1*', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<strong>(.*?)</strong>', r'**\1**', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<b>(.*?)</b>', r'**\1**', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<span[^>]*>(.*?)</span>', r'\1', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<a[^>]*class="note-ref"[^>]*data-scroll-id="([^"]+)"[^>]*><sup[^>]*>(.*?)</sup></a>', r'[^\1]', html, flags=re.IGNORECASE | re.DOTALL)
    if is_source:
        html = re.sub(r'<a[^>]+class="backref"[^>]*>.*?</a>', '', html, flags=re.IGNORECASE | re.DOTALL)
    def link_repl(match):
        href = match.group(1)
        text = match.group(2)
        abs_href = href if href.startswith('http') else f"https://www.churchofjesuschrist.org{href}"
        wiki = get_wikilink(abs_href, text)
        if wiki:
            return wiki
        return f"[{text}]({abs_href})"
    html = re.sub(r'<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', link_repl, html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<[^>]+>', '', html)
    return html.strip()
def get_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)
def scrape_talk_content(url, driver):
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        try:
            body_element = driver.find_element(By.CLASS_NAME, 'body-block')
        except:
            try:
                body_element = driver.find_element(By.CLASS_NAME, 'body-content')
            except Exception as e:
                print(f"Error: Body container not found for talk at {url}: {e}")
                return None
        full_html = body_element.get_attribute('innerHTML')
        full_markdown = html_to_markdown(full_html)
        body = []
        all_elements = body_element.find_elements(By.CSS_SELECTOR, 'h1, h2, h3, h4, h5, h6, p, figure')
        paragraph_counter = 0
        for elem in all_elements:
            tag = elem.tag_name.lower()
            inner_html = elem.get_attribute('innerHTML')
            markdown = html_to_markdown(inner_html)
            if tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                level = int(tag[1])
                body.append({'type': 'heading', 'level': level, 'markdown': markdown})
            elif tag == 'p':
                if not markdown.strip():
                    continue
                classes = elem.get_attribute('class') or ''
                if 'article-footer' in classes or 'share-' in classes:
                    continue
                paragraph_counter += 1
                body.append({'paragraph': paragraph_counter, 'type': 'paragraph', 'markdown': markdown})
            elif tag == 'figure':
                try:
                    img = elem.find_element(By.TAG_NAME, 'img')
                    src = img.get_attribute('src')
                    alt = img.get_attribute('alt')
                    body.append({'type': 'image', 'src': src, 'alt': alt})
                except Exception as e:
                    print(f"Error extracting image for talk at {url}: {e}")
        sources = []
        try:
            notes_section = driver.find_element(By.CLASS_NAME, 'notes')
            ol = notes_section.find_element(By.TAG_NAME, 'ol')
            lis = ol.find_elements(By.TAG_NAME, 'li')
            for i, li in enumerate(lis):
                id_attr = li.get_attribute('id')
                number = i + 1
                inner_html = li.get_attribute('innerHTML')
                markdown = html_to_markdown(inner_html, True)
                sources.append({'number': number, 'id': id_attr, 'markdown': markdown})
        except:
            pass
        return {
            'full_markdown': full_markdown,
            'body': body,
            'sources': sources
        }
    except Exception as e:
        print(f"Error during scraping talk content {url}: {e}")
        return None
def get_conference_filename(year, month):
    sanitized_conference = re.sub(r'[^a-z0-9\- ]', '', f"{year}-{month.lower()}", flags=re.IGNORECASE)
    return os.path.join(JSON_DIR, f"{sanitized_conference}.json")
def process_conference(year, month, replace=False):
    filename = get_conference_filename(year, month)
    if not os.path.exists(filename):
        print(f"Skipping {year}-{month}: JSON file does not exist.")
        return
    with open(filename, 'r') as f:
        conference_data = json.load(f)
   
    updated = False
   
    # Always check and update resources if needed
    for session_name, session in conference_data['sessions'].items():
        if 'talks' in session:
            for talk_key, talk in session['talks'].items():
                if re.match(r'^\d{2}', talk_key):
                    url = talk.get('url')
                    if not url:
                        continue
                    if 'resources' not in talk or not talk['resources']:
                        talk['resources'] = [{'name': 'Gospel Library', 'url': url}]
                        updated = True
   
    # If replace, clear existing content fields to force re-scrape
    if replace:
        for session_name, session in conference_data['sessions'].items():
            if 'talks' in session:
                for talk_key, talk in session['talks'].items():
                    if re.match(r'^\d{2}', talk_key):
                        talk.pop('full_markdown', None)
                        talk.pop('body', None)
                        talk.pop('sources', None)
   
    # Count how many talks need content scraping
    num_to_scrape = 0
    for session_name, session in conference_data['sessions'].items():
        if 'talks' in session:
            for talk_key, talk in session['talks'].items():
                if re.match(r'^\d{2}', talk_key):
                    if not all(key in talk for key in ['full_markdown', 'body', 'sources']):
                        num_to_scrape += 1
   
    if num_to_scrape == 0:
        if updated:
            with open(filename, 'w') as f:
                json.dump(conference_data, f, indent=2)
            print(f"Updated conference data in {filename} (resources only)")
        else:
            print(f"Skipping {year}-{month}: All talks already have content and resources.")
        return
   
    # Proceed with scraping
    driver = get_driver()
    try:
        print(f"Processing talk content for {year}-{month}:")
        with tqdm(total=num_to_scrape, desc="Scraping content") as pbar:
            for session_name, session in conference_data['sessions'].items():
                if 'talks' in session:
                    for talk_key, talk in session['talks'].items():
                        if re.match(r'^\d{2}', talk_key):
                            url = talk.get('url')
                            if not url:
                                continue
                            needs_update = not all(key in talk for key in ['full_markdown', 'body', 'sources'])
                            if not needs_update:
                                continue
                            content = scrape_talk_content(url, driver)
                            if content:
                                talk['full_markdown'] = content['full_markdown']
                                talk['body'] = content['body']
                                talk['sources'] = content['sources']
                                updated = True
                            else:
                                print(f"Failed to scrape content at {url}")
                            pbar.update(1)
    finally:
        driver.quit()
   
    if updated:
        with open(filename, 'w') as f:
            json.dump(conference_data, f, indent=2)
        print(f"Updated conference data in {filename}")
    else:
        print(f"No updates made to {filename}")
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape General Conference talk content")
    parser.add_argument('year_range', help="Year range (e.g., 2020-2023)")
    parser.add_argument('--replace', action='store_true', help="Replace existing content and reset resources")
    args = parser.parse_args()
    if '-' in args.year_range:
        try:
            start_year, end_year = map(int, args.year_range.split('-'))
            for year in range(start_year, end_year + 1):
                for month in ['April', 'October']:
                    process_conference(year, month, args.replace)
        except ValueError:
            print('Invalid year range format. Use YYYY-YYYY.')
            sys.exit(1)
    else:
        print('Invalid input. Provide a year range like 2020-2023.')
        sys.exit(1)