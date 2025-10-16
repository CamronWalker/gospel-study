#!/usr/bin/env python3
"""
Sample Usage:
- To scrape an entire General Conference:
  python3 scrape-conference.py 2023 October

- To scrape a single talk:
  python3 scrape-conference.py "https://www.churchofjesuschrist.org/study/general-conference/2023/10/12nelson?lang=eng"
"""

import os
import json
import time
import sys
import re
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
from dotenv import load_dotenv

# Load .env from current directory
load_dotenv()

# Create conference_json directory
JSON_DIR = 'conference_json'
os.makedirs(JSON_DIR, exist_ok=True)

# [KEEP ALL book_map, get_wikilink, html_to_markdown, normalize_speaker, normalize_role - SAME AS BEFORE]
book_map = {
    'bofm/1-ne': '1 Nephi', 'bofm/2-ne': '2 Nephi', 'bofm/jacob': 'Jacob', 'bofm/enos': 'Enos',
    'bofm/jarom': 'Jarom', 'bofm/omni': 'Omni', 'bofm/w-of-m': 'Words of Mormon', 'bofm/mosiah': 'Mosiah',
    'bofm/alma': 'Alma', 'bofm/hel': 'Helaman', 'bofm/3-ne': '3 Nephi', 'bofm/4-ne': '4 Nephi',
    'bofm/morm': 'Mormon', 'bofm/ether': 'Ether', 'bofm/moro': 'Moroni',
    'dc-testament/dc': 'D&C',
    'ot/gen': 'Genesis', 'ot/ex': 'Exodus', 'ot/lev': 'Leviticus', 'ot/num': 'Numbers',
    'ot/deut': 'Deuteronomy', 'ot/josh': 'Joshua', 'ot/judg': 'Judges', 'ot/ruth': 'Ruth',
    'ot/1-sam': '1 Samuel', 'ot/2-sam': '2 Samuel', 'ot/1-kgs': '1 Kings', 'ot/2-kgs': '2 Kings',
    'ot/1-chr': '1 Chronicles', 'ot/2-chr': '2 Chronicles', 'ot/ezra': 'Ezra', 'ot/neh': 'Nehemiah',
    'ot/esth': 'Esther', 'ot/job': 'Job', 'ot/ps': 'Psalms', 'ot/prov': 'Proverbs',
    'ot/eccl': 'Ecclesiastes', 'ot/song': 'Song of Solomon', 'ot/isa': 'Isaiah', 'ot/jer': 'Jeremiah',
    'ot/lam': 'Lamentations', 'ot/ezek': 'Ezekiel', 'ot/dan': 'Daniel', 'ot/hosea': 'Hosea',
    'ot/joel': 'Joel', 'ot/amos': 'Amos', 'ot/obad': 'Obadiah', 'ot/jonah': 'Jonah',
    'ot/micah': 'Micah', 'ot/nahum': 'Nahum', 'ot/hab': 'Habakkuk', 'ot/zeph': 'Zephaniah',
    'ot/hag': 'Haggai', 'ot/zech': 'Zechariah', 'ot/mal': 'Malachi',
    'nt/matt': 'Matthew', 'nt/mark': 'Mark', 'nt/luke': 'Luke', 'nt/john': 'John',
    'nt/acts': 'Acts', 'nt/rom': 'Romans', 'nt/1-cor': '1 Corinthians', 'nt/2-cor': '2 Corinthians',
    'nt/gal': 'Galatians', 'nt/eph': 'Ephesians', 'nt/phlp': 'Philippians', 'nt/col': 'Colossians',
    'nt/1-thes': '1 Thessalonians', 'nt/2-thes': '2 Thessalonians', 'nt/1-tim': '1 Timothy',
    'nt/2-tim': '2 Timothy', 'nt/titus': 'Titus', 'nt/philem': 'Philemon', 'nt/heb': 'Hebrews',
    'nt/james': 'James', 'nt/1-pet': '1 Peter', 'nt/2-pet': '2 Peter', 'nt/1-jn': '1 John',
    'nt/2-jn': '2 John', 'nt/3-jn': '3 John', 'nt/jude': 'Jude', 'nt/rev': 'Revelation',
    'pgp/moses': 'Moses', 'pgp/abr': 'Abraham', 'pgp/js-m': 'Joseph Smith—Matthew',
    'pgp/js-h': 'Joseph Smith—History', 'pgp/a-of-f': 'Articles of Faith',
}

# [KEEP ALL SAME FUNCTIONS - get_wikilink, html_to_markdown, normalize_speaker, normalize_role]
def get_wikilink(href, text): # [SAME AS BEFORE]
    try:
        parsed_url = urlparse(href)
        if not parsed_url.path.startswith('/study/scriptures/'): return None
        parts = parsed_url.path.split('/')[3:]
        if len(parts) < 2: return None
        corpus, book_abbr = parts[0], parts[1]
        chapter = parts[2] if len(parts) > 2 else ''
        verses_str = ''
        if 'id' in parsed_url.query:
            verses_str = dict(q.split('=') for q in parsed_url.query.split('&')).get('id', '')
        elif parsed_url.fragment: verses_str = parsed_url.fragment[1:]
        verses_str = re.sub(r'^p', '', verses_str.lower())
        key = f"{corpus}/{book_abbr}"
        book_name = book_map.get(key)
        if not book_name: return None
        page_name = f"D&C {chapter}" if book_name == 'D&C' else f"{book_name} {chapter}"
        if not verses_str: return f"[[{page_name}|{text}]]"
        verse_parts = verses_str.split(',')
        all_verses = []
        for part in verse_parts:
            part = part.strip()
            if '-' in part:
                start, end = re.sub(r'^p', '', part.split('-')[0], flags=re.IGNORECASE).strip(), re.sub(r'^p', '', part.split('-')[1], flags=re.IGNORECASE).strip()
                all_verses.extend(range(int(start), int(end) + 1))
            else:
                all_verses.append(int(re.sub(r'^p', '', part, flags=re.IGNORECASE).strip()))
        if not all_verses: return f"[[{page_name}|{text}]]"
        md = f"[[{page_name}#{all_verses[0]}|{text}]]"
        for v in all_verses[1:]: md += f"[[{page_name}#{v}|]]"
        return md
    except: return None

def html_to_markdown(html, is_source=False): # [SAME AS BEFORE]
    html = re.sub(r'<em>(.*?)</em>', r'*\1*', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<i>(.*?)</i>', r'*\1*', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<strong>(.*?)</strong>', r'**\1**', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<b>(.*?)</b>', r'**\1**', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<span[^>]*>(.*?)</span>', r'\1', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<sup[^>]*><a[^>]+href="#([^"]+)"[^>]*>([^<]+)</a></sup>', r'[^ \1]', html, flags=re.IGNORECASE | re.DOTALL)
    if is_source: html = re.sub(r'<a[^>]+class="backref"[^>]*>.*?</a>', '', html, flags=re.IGNORECASE | re.DOTALL)
    def link_repl(match):
        href, text = match.group(1), match.group(2)
        abs_href = href if href.startswith('http') else f"https://www.churchofjesuschrist.org{href}"
        wiki = get_wikilink(abs_href, text)
        return wiki or f"[{text}]({abs_href})"
    html = re.sub(r'<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', link_repl, html, flags=re.IGNORECASE | re.DOTALL)
    return re.sub(r'<[^>]+>', '', html).strip()

def normalize_speaker(speaker): # [SAME]
    return re.sub(r'^(By|Elder|President|Sister|Brother)\s+', '', speaker, flags=re.IGNORECASE).strip()

def normalize_role(role): # [SAME]
    if not role: return None
    role = re.sub(r'^Of the ', '', role, flags=re.IGNORECASE).strip()
    role = re.sub(r'Quorum of the (Twelve|twelve|12) Apostles|Q_of_12|Council of the 12', 'Quorum of the 12', role, flags=re.IGNORECASE)
    role = re.sub(r'Q_of_70|70|Assistant to the Q_of_12|.*Seventy.*', 'Seventy', role, flags=re.IGNORECASE)
    role = re.sub(r'President of The Church of Jesus Christ of Latter-day Saints|President of the Church', 'President of the Church', role, flags=re.IGNORECASE)
    return role

def get_talk_id_from_url(url): # [SAME]
    match = re.search(r'/general-conference/(\d{4})/(\d{2})/([a-z0-9]+)', url)
    return f"{match.group(1)}/{match.group(2)}/{match.group(3)}" if match else None

def get_conference_sort_key(item): # [SAME]
    year_str, month_str = item[0].split('-')
    return (int(year_str), 4 if month_str == 'April' else 10)

def save_conference_resources(conference_data, year, month): # [SAME]
    conf_key = f"{year}-{month.capitalize()}"
    resources_file = os.path.join(JSON_DIR, 'conference_resources.json')
    all_resources = json.load(open(resources_file)) if os.path.exists(resources_file) else {}
    if conf_key not in all_resources: all_resources[conf_key] = {}
    for session_name, talks in conference_data['sessions'].items():
        for talk in talks:
            talk_id = get_talk_id_from_url(talk['url'])
            if talk_id and talk_id not in all_resources[conf_key]:
                all_resources[conf_key][talk_id] = {"Gospel Library": talk['url']}
    sorted_resources = dict(sorted(all_resources.items(), key=get_conference_sort_key))
    for conf_key in sorted_resources: sorted_resources[conf_key] = dict(sorted(sorted_resources[conf_key].items()))
    with open(resources_file, 'w') as f: json.dump(sorted_resources, f, indent=2)
    print(f"Saved Gospel Library URLs to {resources_file}")

def scrape_single_talk_raw(url, session_name, year=None, month=None, max_retries=3):
    """SIMPLEST possible scraper - NO PARALLEL"""
    for attempt in range(max_retries):
        try:
            options = Options()
            options.add_argument('--headless'); options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox'); options.add_argument('user-agent=Mozilla/5.0...')
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            driver.set_page_load_timeout(300)
            
            driver.get(url)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            time.sleep(3)
            
            talk_data = {'session': session_name, 'url': url}
            talk_data['title'] = driver.find_element(By.TAG_NAME, 'h1').text
            talk_data['speaker'] = normalize_speaker(driver.find_element(By.CLASS_NAME, 'author-name').text)
            try: talk_data['speaker_role'] = normalize_role(driver.find_element(By.CLASS_NAME, 'author-role').text)
            except: pass
            
            # Body
            try: body_element = driver.find_element(By.CLASS_NAME, 'body-block')
            except: body_element = driver.find_element(By.CLASS_NAME, 'body-content')
            talk_data['full_markdown'] = html_to_markdown(body_element.get_attribute('innerHTML'))
            
            # Sources
            talk_data['sources'] = []
            try:
                notes_section = driver.find_element(By.CLASS_NAME, 'notes')
                ol = notes_section.find_element(By.TAG_NAME, 'ol')
                for i, li in enumerate(ol.find_elements(By.TAG_NAME, 'li')):
                    talk_data['sources'].append({
                        'number': i + 1, 'id': li.get_attribute('id'),
                        'markdown': html_to_markdown(li.get_attribute('innerHTML'), True)
                    })
            except: pass
            
            driver.quit()
            return talk_data
            
        except Exception as e:
            driver.quit()
            if attempt < max_retries - 1:
                print(f"Retry {attempt+1}/3 for {url}: {e}")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"FAILED {url}: {e}")
                return None

def get_conference_filename(year, month):
    return os.path.join(JSON_DIR, f"{year}-{month.lower()}.json")

def scrape_conference(year, month):
    month_code = '04' if month.lower() in ['apr', 'april'] else '10'
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    conference = f"{year}-{month.capitalize()}"
    
    print(f"📋 Getting talk list from {conference_url}")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(conference_url)
    time.sleep(5)
    
    conference_data = {'conference': conference, 'year': year, 'month': month.capitalize(), 'sessions': {}}
    talk_list = []
    li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
    current_session = None
    
    for li in li_elements:
        a = li.find_element(By.TAG_NAME, 'a')
        href = a.get_attribute('href')
        full_url = f"https://www.churchofjesuschrist.org{href}" if href.startswith('/') else href
        last_segment = full_url.split('/')[-1].split('?')[0]
        
        if re.match(r'^\d{2}[a-z]+$', last_segment):
            if current_session:
                talk_list.append({
                    'url': full_url, 'session': current_session,
                    'title': li.find_element(By.CSS_SELECTOR, 'p.title').text,
                    'speaker': normalize_speaker(li.find_element(By.CSS_SELECTOR, 'p.author').text)
                })
        else:
            current_session = li.find_element(By.CSS_SELECTOR, 'p.title').text
            conference_data['sessions'][current_session] = []
    
    driver.quit()
    print(f"🚀 Scraping {len(talk_list)} talks...")
    
    # SIMPLE SEQUENTIAL PROCESSING - NO PARALLEL
    for i, talk_item in enumerate(tqdm(talk_list, desc="Scraping talks")):
        talk = scrape_single_talk_raw(talk_item['url'], talk_item['session'], year, month)
        if talk:
            conference_data['sessions'][talk_item['session']].append(talk)
    
    filename = get_conference_filename(year, month)
    with open(filename, 'w') as f: json.dump(conference_data, f, indent=2)
    print(f"\n✅ Saved {len(talk_list)} talks to {filename}")
    save_conference_resources(conference_data, year, month)

def scrape_single_talk(url):
    match = re.search(r'/general-conference/(\d{4})/(\d{2})/', url)
    if not match: print("❌ Invalid URL"); return
    year, month_code, _ = match.groups()
    month = 'April' if month_code == '04' else 'October'
    
    # Get session
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng")
    time.sleep(5)
    
    session_name = 'Unknown Session'
    talk_last_segment = url.split('/')[-1].split('?')[0]
    li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
    current_session = None
    
    for li in li_elements:
        a = li.find_element(By.TAG_NAME, 'a')
        href = f"https://www.churchofjesuschrist.org{a.get_attribute('href')}" if a.get_attribute('href').startswith('/') else a.get_attribute('href')
        last_seg = href.split('/')[-1].split('?')[0]
        if re.match(r'^\d{2}[a-z]+$', last_seg):
            if last_seg == talk_last_segment:
                session_name = current_session
                break
        else:
            current_session = li.find_element(By.CSS_SELECTOR, 'p.title').text
    
    driver.quit()
    
    talk = scrape_single_talk_raw(url, session_name, year, month)
    if not talk: print("❌ Failed to scrape talk"); return
    
    filename = get_conference_filename(year, month)
    conference_data = json.load(open(filename)) if os.path.exists(filename) else {'conference': f"{year}-{month.capitalize()}", 'year': year, 'month': month.capitalize(), 'sessions': {}}
    
    if session_name not in conference_data['sessions']: conference_data['sessions'][session_name] = []
    for existing in conference_data['sessions'][session_name]:
        if existing['title'] == talk['title']: existing.update(talk); print(f"✅ Updated '{talk['title']}'"); break
    else: conference_data['sessions'][session_name].append(talk); print(f"✅ Added '{talk['title']}'")
    
    with open(filename, 'w') as f: json.dump(conference_data, f, indent=2)
    save_conference_resources(conference_data, year, month)

if __name__ == '__main__':
    if len(sys.argv) == 3: scrape_conference(*sys.argv[1:])
    elif len(sys.argv) == 2: scrape_single_talk(sys.argv[1])
    else: print("Usage: python3 scrape-conference.py <year> <month> OR \"<full_url>\""); sys.exit(1)