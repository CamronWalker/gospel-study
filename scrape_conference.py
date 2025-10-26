"""
Sample Usage:
- To scrape an entire General Conference:
  python scrape-conference.py 2023 October
  (This will scrape all talks from the specified conference and save to a JSON file like 'conference_json/2023-october.json'. Note: Month must be 'April' or 'October'.)

- To scrape with replace:
  python scrape-conference.py 2023 October --replace
  (This will replace existing resources.)

- To scrape a single talk:
  python scrape-conference.py https://www.churchofjesuschrist.org/study/general-conference/2023/10/12nelson?lang=eng
  (This will scrape the individual talk, determine the conference, and add/update it in the corresponding conference JSON file like 'conference_json/2023-october.json'. If the file doesn't exist, it will create it with just that talk.)
"""

import os
import json
import time
import sys
import re
import requests
from urllib.parse import urlparse, quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import argparse
import logging

# Suppress Selenium stacktraces
logging.getLogger('selenium').setLevel(logging.WARNING)

# Load .env from the same directory as the script
load_dotenv()

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

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother|Bishop)\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'[^a-zA-Z0-9\s]', '', speaker)
    return speaker.strip()

def normalize_role(role):
    if not role:
        return None
    role = re.sub(r'^Of the ', '', role, flags=re.IGNORECASE).strip()
    role = re.sub(r'Quorum of the (Twelve|twelve|12) Apostles|Q_of_12|Council of the 12', 'Quorum of the 12', role, flags=re.IGNORECASE)
    role = re.sub(r'Q_of_70|70|Assistant to the Q_of_12|First Council of the Seventy|Presidency of the First Q_of_70|Emeritus member of the Seventy|Released Member of the Seventy|Former member of the Seventy', 'Seventy', role, flags=re.IGNORECASE)
    role = re.sub(r'President of The Church of Jesus Christ of Latter-day Saints|President of the Church', 'President of the Church', role, flags=re.IGNORECASE)
    return role

def get_uniform_talk_key(title):
    return re.sub(r'[^a-zA-Z0-9\s]', '', title.strip()).lower()

def get_author_title_key(title, speaker):
    norm_speaker = normalize_speaker(speaker).lower()
    norm_title = get_uniform_talk_key(title)
    return f"{norm_speaker}|{norm_title}"

def get_wikilink(href, text):
    try:
        parsed_url = urlparse(href)
        if not parsed_url.path.startswith('/study/scriptures/'):
            return None
        parts = parsed_url.path.split('/')[3:]
        if len(parts) < 2:
            return None
        corpus = parts[0]
        book_abbr = parts[1]
        chapter = ''
        if len(parts) > 2:
            chapter = parts[2]
        verses_str = ''
        if 'id' in parsed_url.query:
            query_params = dict(q.split('=') for q in parsed_url.query.split('&'))
            verses_str = query_params.get('id', '')
        elif parsed_url.fragment:
            verses_str = parsed_url.fragment[1:]
        verses_str = verses_str.lower()
        verses_str = re.sub(r'^p', '', verses_str)
        key = f"{corpus}/{book_abbr}"
        book_name = book_map.get(key)
        if not book_name:
            return None
        page_name = f"D&C {chapter}" if book_name == 'D&C' else f"{book_name} {chapter}"
        if not verses_str:
            return f"[[{page_name}|{text}]]"
        verse_parts = verses_str.split(',')
        all_verses = []
        for part in verse_parts:
            part = part.strip()
            if '-' in part:
                range_parts = part.split('-')
                start = re.sub(r'^p', '', range_parts[0], flags=re.IGNORECASE).strip()
                end = re.sub(r'^p', '', range_parts[1], flags=re.IGNORECASE).strip() if len(range_parts) > 1 else ''
                start_num = int(start) if start else None
                end_num = int(end) if end else None
                if start_num is not None and end_num is not None:
                    all_verses.extend(range(start_num, end_num + 1))
            else:
                part_num = re.sub(r'^p', '', part, flags=re.IGNORECASE).strip()
                if part_num:
                    all_verses.append(int(part_num))
        if not all_verses:
            return f"[[{page_name}|{text}]]"
        md = f"[[{page_name}#{all_verses[0]}|{text}]]"
        for v in all_verses[1:]:
            md += f"[[{page_name}#{v}|]]"
        return md
    except Exception as e:
        print(f"Error parsing scripture link: {e}")
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

def fetch_conference_videos(year, month):  # FIX: quota usage https://console.cloud.google.com/apis/api/youtube.googleapis.com/quotas?project=lds-gospel-study
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY is missing from .env file")
    channel_id = 'UCSdPpMokMoGCSSNShOecP9w'  # Official General Conference channel ID
    query = f"{month} {year} General Conference"
    videos = []
    page_token = None
    while True:
        api_url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&q={quote(query)}&type=video&maxResults=50&order=relevance&key={api_key}"
        if page_token:
            api_url += f"&pageToken={page_token}"
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                snippet = item['snippet']
                video_id = item['id']['videoId']
                title = snippet['title']
                videos.append({'title': title, 'video_id': video_id})
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        except requests.exceptions.RequestException as e:
            print(f"Error fetching videos: {e}")
            break
    if not videos:
        print(f"No videos found for {month} {year} General Conference")
    return videos

def scrape_talk_basics(url, session_name, year=None, month=None, driver=None):
    if driver is None:
        driver = get_driver()
        own_driver = True
    else:
        own_driver = False
    talk_data = {}
    talk_data['session'] = session_name
    talk_data['url'] = url
    talk_data['resources'] = [{'name': 'Gospel Library', 'url': url}]
    talk_data['resources'].append({'name': 'Saints AI Study Guide', 'url': url.replace('www.churchofjesuschrist.org', 'saintsai.org').split('?')[0] + '/study-guide'})
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        title_element = driver.find_element(By.TAG_NAME, 'h1')
        talk_data['title'] = title_element.text
        speaker_element = driver.find_element(By.CLASS_NAME, 'author-name')
        talk_data['speaker'] = normalize_speaker(speaker_element.text)
        speaker_role = None
        try:
            role_element = driver.find_element(By.CLASS_NAME, 'author-role')
            speaker_role = role_element.text
        except:
            pass
        talk_data['speaker_role'] = normalize_role(speaker_role)
        thumbnail = None
        try:
            thumbnail = driver.find_element(By.CSS_SELECTOR, 'img[class*="posterFallback"]').get_attribute('src')
        except:
            pass
        if not thumbnail:
            try:
                thumbnail = driver.find_element(By.CSS_SELECTOR, 'header img, .article-header img').get_attribute('src')
            except:
                pass
        if not thumbnail:
            try:
                thumbnail = driver.find_element(By.CSS_SELECTOR, 'img[src*="churchofjesuschrist.org/imgs"]').get_attribute('src')
            except:
                pass
        if not thumbnail:
            try:
                thumbnail = driver.find_element(By.TAG_NAME, 'img').get_attribute('src')
            except:
                pass
        talk_data['thumbnail'] = thumbnail
        subtitle = None
        try:
            subtitle_element = driver.find_element(By.CLASS_NAME, 'subtitle')
            subtitle = subtitle_element.text
        except:
            pass
        talk_data['subtitle'] = subtitle
        kicker = None
        try:
            kicker_element = driver.find_element(By.CLASS_NAME, 'kicker')
            kicker = kicker_element.text
        except:
            pass
        if not kicker:
            try:
                kicker = driver.find_element(By.CSS_SELECTOR, '.body-block p.intro, .body-content p.intro').text
            except:
                pass
        talk_data['kicker'] = kicker
        try:
            body_element = driver.find_element(By.CLASS_NAME, 'body-block')
        except:
            try:
                body_element = driver.find_element(By.CLASS_NAME, 'body-content')
            except Exception as e:
                print(f"Error: Body container not found for talk at {url}: {e}")
                return None
        full_html = body_element.get_attribute('innerHTML')
        talk_data['full_markdown'] = html_to_markdown(full_html)
        talk_data['body'] = []
        all_elements = body_element.find_elements(By.CSS_SELECTOR, 'h1, h2, h3, h4, h5, h6, p, figure')
        verse = 0
        for elem in all_elements:
            tag = elem.tag_name
            inner_html = elem.get_attribute('innerHTML')
            markdown = html_to_markdown(inner_html)
            if tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                level = int(tag[1])
                talk_data['body'].append({'type': 'heading', 'level': level, 'markdown': markdown})
            elif tag == 'p':
                this_verse = verse + 1
                verse += 1
                id_attr = elem.get_attribute('id')
                if id_attr and id_attr.startswith('p'):
                    rest = id_attr[1:]
                    if rest.isdigit():  # FIX: Only parse if the rest is numeric (handles non-numeric IDs like '_cdUJm')
                        id_num = int(rest)
                        if id_num:
                            this_verse = id_num
                            verse = max(verse, this_verse)
                talk_data['body'].append({'verse': this_verse, 'type': 'paragraph', 'markdown': markdown})
            elif tag == 'figure':
                try:
                    img = elem.find_element(By.TAG_NAME, 'img')
                    src = img.get_attribute('src')
                    alt = img.get_attribute('alt')
                    talk_data['body'].append({'type': 'image', 'src': src, 'alt': alt})
                except Exception as e:
                    print(f"Error extracting image for talk at {url}: {e}")
        talk_data['sources'] = []
        try:
            notes_section = driver.find_element(By.CLASS_NAME, 'notes')
            ol = notes_section.find_element(By.TAG_NAME, 'ol')
            lis = ol.find_elements(By.TAG_NAME, 'li')
            for i, li in enumerate(lis):
                id_attr = li.get_attribute('id')
                number = i + 1
                inner_html = li.get_attribute('innerHTML')
                markdown = html_to_markdown(inner_html, True)
                talk_data['sources'].append({'number': number, 'id': id_attr, 'markdown': markdown})
        except:
            pass
        return talk_data
    except Exception as e:
        print(f"Error during scraping talk basics {url}: {e}")
        return None
    finally:
        if own_driver:
            driver.quit()

def add_youtube_resource(talk, videos, replace=False):
    if not replace and any(r['name'] == 'YouTube Video' for r in talk['resources']):
        return
    norm_title = get_uniform_talk_key(talk['title'])
    norm_speaker = normalize_speaker(talk['speaker']).lower()
    for video in videos:
        video_title_lower = video['title'].lower()
        norm_video_title = re.sub(r'[^a-z0-9\s]', '', video_title_lower)
        norm_video_for_speaker = re.sub(r'[^a-z0-9\s]', '', video_title_lower)  # FIX: Normalize for speaker check to handle punctuation like "H."
        if norm_title in norm_video_title and norm_speaker in norm_video_for_speaker:
            url = f"https://www.youtube.com/watch?v={video['video_id']}"
            talk['resources'] = [r for r in talk['resources'] if r['name'] != 'YouTube Video']
            talk['resources'].append({'name': 'YouTube Video', 'url': url})
            return
    print(f"No matching YouTube video found for \"{talk['title']}\"")

def add_byu_resource(talk, byu_talks, conf_hash, replace=False):
    if not replace and any(r['name'] == 'BYU Citation Index' for r in talk['resources']):
        return
    norm_title = get_uniform_talk_key(talk['title'])
    norm_speaker = normalize_speaker(talk['speaker'])
    matching = next((b for b in byu_talks if get_uniform_talk_key(b['title']) == norm_title and normalize_speaker(b['speaker']) == norm_speaker), None)
    if matching:
        url = f"https://scriptures.byu.edu/#:t{matching['t_hash']}:g{conf_hash}"
        talk['resources'] = [r for r in talk['resources'] if r['name'] != 'BYU Citation Index']
        talk['resources'].append({'name': 'BYU Citation Index', 'url': url})

def fetch_byu_talks(driver, year, month):
    month_code = '04' if month.lower() == 'april' else '10'
    annual = 'A' if month_code == '04' else 'O'
    year_num = int(year) - 1830
    if annual == 'O':
        year_num += 2048
    conf_hash = format(year_num, 'x')
    byu_conf_url = f"https://scriptures.byu.edu/#::g{conf_hash}"
    byu_talks = []
    try:
        driver.get(byu_conf_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(5)
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.talksblock li')
        for li in li_elements:
            try:
                a = li.find_element(By.CSS_SELECTOR, 'a[onclick*="getTalk"]')
            except:
                continue
            onclick = a.get_attribute('onclick')
            id_match = re.search(r"getTalk\('(\d+)'\)", onclick)
            talk_id = id_match.group(1) if id_match else None
            if not talk_id:
                continue
            t_hash = format(int(talk_id), 'x')
            title = ''
            speaker = ''
            try:
                title = li.find_element(By.CSS_SELECTOR, 'div.talktitle').text.strip()
                speaker = li.find_element(By.CSS_SELECTOR, 'div.speaker').text.strip()
                speaker = normalize_speaker(speaker)
            except:
                continue
            if t_hash and title and speaker:
                byu_talks.append({'title': title, 'speaker': speaker, 't_hash': t_hash})
        return byu_talks, conf_hash
    except Exception as e:
        print(f"Error fetching BYU talks: {e}")
        return [], conf_hash

def add_church_news_resource(talk, year, month, replace=False, driver=None):
    if 'sustaining' in talk['title'].lower() or 'auditing' in talk['title'].lower():
        return
    if not replace and any(r['name'] == 'Church News' for r in talk['resources']):
        return
    own_driver = False
    if driver is None:
        driver = get_driver()
        own_driver = True
    query = quote(f"{month} {year} General Conference {talk['speaker']} {talk['title']}")
    search_url = f"https://www.thechurchnews.com/search?q={query}"
    found = False
    for attempt in range(2 if int(year) >= 2015 else 1):
        try:
            driver.get(search_url)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'filter_item')))
            filter_div = driver.find_element(By.CSS_SELECTOR, 'div.filter_item[data-filter-value="General Conference"]')
            if 'selectedFilterItem' not in filter_div.get_attribute('class'):
                driver.execute_script("searchPage.dofacetedsearch(0,'section','General Conference')")
                time.sleep(2)
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'queryly_item_row')))
            else:
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'queryly_item_row')))
            time.sleep(2)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            item_rows = soup.find_all('div', class_='queryly_item_row')
            norm_title = get_uniform_talk_key(talk['title'])
            title_words = set(norm_title.split())
            norm_speaker = normalize_speaker(talk['speaker']).lower()
            for row in item_rows:
                try:
                    a = row.find('a')
                    href = a['href'] if a else None
                    title_elem = row.find('div', class_='queryly_item_title')
                    title_text = title_elem.text.strip().lower() if title_elem else ''
                    norm_title_text = re.sub(r'[^a-z0-9\s]', '', title_text)
                    desc_elem = row.find('div', class_='queryly_item_description')
                    desc_text = desc_elem.text.strip().lower() if desc_elem else ''
                    norm_desc = re.sub(r'[^a-z0-9\s]', '', desc_text)
                    full_text = title_text + ' ' + desc_text
                    norm_full = norm_title_text + ' ' + norm_desc
                    norm_full_text = re.sub(r'[^a-z0-9\s]', '', full_text)  # Normalize full_text for speaker match to handle periods
                    full_words = set(norm_full.split())
                    overlap_ratio = len(title_words & full_words) / len(title_words) if title_words else 0
                    if 'episode' in full_text or 'podcast' in full_text:
                        continue
                    if overlap_ratio >= 0.7 and norm_speaker in norm_full_text:
                        full_href = href if href.startswith('https') else f"https://www.thechurchnews.com{href}"
                        talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Church News']
                        talk['resources'].append({'name': 'Church News', 'url': full_href})
                        found = True
                        break
                except:
                    continue
            if found:
                break
        except Exception as e:
            print(f"Error on attempt {attempt+1} for \"{talk['title']}\": {e}\nSearch URL: {search_url}")
    if not found:
        if int(year) >= 2015:
            talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Church News']
            talk['resources'].append({'name': 'Church News', 'url': search_url})
    if own_driver:
        driver.quit()

def get_conference_filename(year, month):
    sanitized_conference = re.sub(r'[^a-z0-9\- ]', '', f"{year}-{month.lower()}", flags=re.IGNORECASE)
    return os.path.join(JSON_DIR, f"{sanitized_conference}.json")

def scrape_conference(year, month, replace=False):
    month_code = '04' if month.lower() in ['apr', 'april'] else '10' if month.lower() in ['oct', 'october'] else None
    if not month_code:
        raise ValueError('Invalid month: Must be April or October')
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    conference = f"{year}-{month.capitalize()}"
    filename = get_conference_filename(year, month)
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            conference_data = json.load(f)
    else:
        conference_data = {'conference': conference, 'year': year, 'month': month.capitalize(), 'sessions': {}}
    driver = get_driver()
    talk_list = []
    try:
        driver.get(conference_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        for _ in range(5):
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(1)
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
        current_session_name = None
        for li in li_elements:
            a = li.find_element(By.TAG_NAME, 'a')
            href = a.get_attribute('href')
            full_url = href if href.startswith('https') else f"https://www.churchofjesuschrist.org{href}"
            last_segment = full_url.split('/')[-1].split('?')[0]
            if re.match(r'^\d{2}[a-z]+$', last_segment, re.IGNORECASE):
                if current_session_name:
                    talk_list.append({'url': full_url, 'session': current_session_name})
            else:
                try:
                    title_p = li.find_element(By.CSS_SELECTOR, 'p.title')
                    current_session_name = title_p.text
                except:
                    current_session_name = 'Unknown Session'
                if current_session_name not in conference_data['sessions']:
                    conference_data['sessions'][current_session_name] = []
        # Determine missing talks
        existing_urls = set()
        for session, talks in conference_data['sessions'].items():
            for talk in talks:
                existing_urls.add(talk['url'])
        missing_talks = [t for t in talk_list if t['url'] not in existing_urls]
        # Scrape missing talk basics
        total_missing = len(missing_talks)
        if total_missing > 0:
            print('Scraping missing talk basics:')
        with tqdm(total=total_missing, desc="Scraping missing basics") as pbar:
            for talk_item in missing_talks:
                talk = scrape_talk_basics(talk_item['url'], talk_item['session'], year, month, driver=driver)
                if talk:
                    conference_data['sessions'][talk_item['session']].append(talk)
                else:
                    print(f"Failed to scrape basics at {talk_item['url']}")
                pbar.update(1)
        # Fetch YouTube videos via search  # FIX: Changed from playlist to search
        videos = fetch_conference_videos(year, month)
        # Fetch batch resources
        byu_talks, conf_hash = fetch_byu_talks(driver, year, month)
        # Add missing resources to all talks
        total_talks = sum(len(talks) for talks in conference_data['sessions'].values())
        print('Adding missing resources to talks:')
        with tqdm(total=total_talks, desc="Adding resources") as pbar:
            for session_name, talks in conference_data['sessions'].items():
                for talk in talks:
                    add_youtube_resource(talk, videos, replace)
                    add_byu_resource(talk, byu_talks, conf_hash, replace)
                    add_church_news_resource(talk, year, month, replace, driver)
                    pbar.update(1)
        # Save
        with open(filename, 'w') as f:
            json.dump(conference_data, f, indent=2)
        print(f"Saved conference data to {filename}")
    finally:
        driver.quit()

def scrape_single_talk(url, replace=False):
    match = re.search(r'/general-conference/(\d{4})/(\d{2})/', url)
    year = match.group(1) if match else None
    month_code = match.group(2) if match else None
    month = 'April' if month_code == '04' else 'October' if month_code == '10' else None
    if not year or not month:
        print("Error: Cannot determine conference from URL.")
        return
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    driver = get_driver()
    try:
        driver.get(conference_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        for _ in range(5):
            driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
            time.sleep(1)
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
        current_session = 'Unknown Session'
        session_name = 'Unknown Session'
        talk_last_segment = url.split('/')[-1].split('?')[0]
        found = False
        for li in li_elements:
            a = li.find_element(By.TAG_NAME, 'a')
            href = a.get_attribute('href')
            full_url = href if href.startswith('https') else f"https://www.churchofjesuschrist.org{href}"
            last_seg = full_url.split('/')[-1].split('?')[0]
            if re.match(r'^\d{2}[a-z]+$', last_seg, re.IGNORECASE):
                if last_seg == talk_last_segment:
                    session_name = current_session
                    found = True
                    break
            else:
                try:
                    title_p = li.find_element(By.CSS_SELECTOR, 'p.title')
                    current_session = title_p.text
                except:
                    current_session = 'Unknown Session'
        if not found:
            print(f"Warning: Could not find session for talk at {url}. Using 'Unknown Session'.")
        talk_basics = scrape_talk_basics(url, session_name, year, month, driver=driver)
        if not talk_basics:
            print('Failed to scrape single talk basics')
            return
        filename = get_conference_filename(year, month)
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                conference_data = json.load(f)
        else:
            conference_data = {'conference': f"{year}-{month.capitalize()}", 'year': year, 'month': month.capitalize(), 'sessions': {}}
        if session_name not in conference_data['sessions']:
            conference_data['sessions'][session_name] = []
        existing_talk = next((t for t in conference_data['sessions'][session_name] if t['url'] == url), None)
        if existing_talk:
            if replace:
                existing_talk.update(talk_basics)
            print(f"Updated talk basics '{talk_basics['title']}' in {filename}" if replace else f"Talk basics already exist for '{talk_basics['title']}'")
            talk_to_update = existing_talk
        else:
            conference_data['sessions'][session_name].append(talk_basics)
            print(f"Added talk basics '{talk_basics['title']}' to {filename}")
            talk_to_update = talk_basics
        # Fetch YouTube videos via search for single talk (efficient, same as batch)  # FIX: Changed from playlist to search
        videos = fetch_conference_videos(year, month)
        # Fetch batch resources
        byu_talks, conf_hash = fetch_byu_talks(driver, year, month)
        # Add resources
        add_youtube_resource(talk_to_update, videos, replace)
        add_byu_resource(talk_to_update, byu_talks, conf_hash, replace)
        add_church_news_resource(talk_to_update, year, month, replace, driver)
        with open(filename, 'w') as f:
            json.dump(conference_data, f, indent=2)
        print(f"Saved updated data to {filename}")
    finally:
        driver.quit()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape General Conference data")
    parser.add_argument('year_or_url', help="Year or URL")
    parser.add_argument('month', nargs='?', choices=['April', 'October'], help="Month (April or October)")
    parser.add_argument('--replace', action='store_true', help="Replace existing resources")
    args = parser.parse_args()
    if args.month:
        year = int(args.year_or_url)
        month = args.month
        scrape_conference(year, month, args.replace)
    else:
        url = args.year_or_url
        if url.startswith('https://'):
            scrape_single_talk(url, args.replace)
        else:
            print('Invalid URL or year')
            sys.exit(1)