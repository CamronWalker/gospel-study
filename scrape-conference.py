"""
Sample Usage:
- To scrape an entire General Conference:
  python website_scraper.py 2023 October
  (This will scrape all talks from the specified conference and save to a JSON file like 'conference_json/2023-october.json'. Note: Month must be 'April' or 'October'.)

- To scrape a single talk:
  python website_scraper.py https://www.churchofjesuschrist.org/study/general-conference/2023/10/12nelson?lang=eng
  (This will scrape the individual talk, determine the conference, and add/update it in the corresponding conference JSON file like 'conference_json/2023-october.json'. If the file doesn't exist, it will create it with just that talk.)
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
from concurrent.futures import ThreadPoolExecutor

# Load .env from current directory (start dir)
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

def get_wikilink(href, text):
    try:
        parsed_url = urlparse(href)
        if not parsed_url.path.startswith('/study/scriptures/'):
            return None
        parts = parsed_url.path.split('/')[3:]  # after /study/scriptures/
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
        # Remove 'p' from the beginning case-insensitively
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
    # Handle emphasis and bold
    html = re.sub(r'<em>(.*?)</em>', r'*\1*', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<i>(.*?)</i>', r'*\1*', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<strong>(.*?)</strong>', r'**\1**', html, flags=re.IGNORECASE | re.DOTALL)
    html = re.sub(r'<b>(.*?)</b>', r'**\1**', html, flags=re.IGNORECASE | re.DOTALL)
    # Remove spans
    html = re.sub(r'<span[^>]*>(.*?)</span>', r'\1', html, flags=re.IGNORECASE | re.DOTALL)
    # Handle footnotes sup
    html = re.sub(r'<sup[^>]*><a[^>]+href="#([^"]+)"[^>]*>([^<]+)</a></sup>', r'[^ \1]', html, flags=re.IGNORECASE | re.DOTALL)
    # Remove backrefs in sources
    if is_source:
        html = re.sub(r'<a[^>]+class="backref"[^>]*>.*?</a>', '', html, flags=re.IGNORECASE | re.DOTALL)
    # Handle links
    def link_repl(match):
        href = match.group(1)
        text = match.group(2)
        abs_href = href if href.startswith('http') else f"https://www.churchofjesuschrist.org{href}"
        wiki = get_wikilink(abs_href, text)
        if wiki:
            return wiki
        return f"[{text}]({abs_href})"
    html = re.sub(r'<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', link_repl, html, flags=re.IGNORECASE | re.DOTALL)
    # Strip remaining tags
    html = re.sub(r'<[^>]+>', '', html)
    return html.strip()

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother)\s+', '', speaker, flags=re.IGNORECASE)
    return speaker.strip()

def normalize_role(role):
    if not role:
        return None
    role = re.sub(r'^Of the ', '', role, flags=re.IGNORECASE).strip()
    role = re.sub(r'Quorum of the (Twelve|twelve|12) Apostles|Q_of_12|Council of the 12', 'Quorum of the 12', role, flags=re.IGNORECASE)
    role = re.sub(r'Q_of_70|70|Assistant to the Q_of_12|First Council of the Seventy|Presidency of the First Q_of_70|Emeritus member of the Seventy|Released Member of the Seventy|Former member of the Seventy', 'Seventy', role, flags=re.IGNORECASE)
    role = re.sub(r'President of The Church of Jesus Christ of Latter-day Saints|President of the Church', 'President of the Church', role, flags=re.IGNORECASE)
    return role

def get_talk_id_from_url(url):
    """Extract talk ID from URL like '2023/10/12nelson' - uses URL month code (04/10)"""
    match = re.search(r'/general-conference/(\d{4})/(\d{2})/([a-z0-9]+)', url)
    if match:
        year = match.group(1)
        month_code = match.group(2)  # 04 or 10 from URL
        talk_id = match.group(3)
        return f"{year}/{month_code}/{talk_id}"
    return None

def get_conference_sort_key(item):
    """Get sort key for chronological ordering: (year, month_num) - item is (key, value) tuple"""
    conf_key = item[0]  # Extract the key string from the tuple
    year_str, month_str = conf_key.split('-')
    year = int(year_str)
    month_num = 4 if month_str == 'April' else 10
    return (year, month_num)

def save_conference_resources(conference_data, year, month):
    """Save Gospel Library URLs to conference_resources.json - chronological order, no duplicates"""
    conf_key = f"{year}-{month.capitalize()}"  # 2024-October (month name)
    resources_file = os.path.join(JSON_DIR, 'conference_resources.json')
    
    # Load existing resources or create new
    if os.path.exists(resources_file):
        with open(resources_file, 'r') as f:
            all_resources = json.load(f)
    else:
        all_resources = {}
    
    # Get or create conference section (no duplicates)
    if conf_key not in all_resources:
        all_resources[conf_key] = {}
    
    # Add each talk's Gospel Library URL (no duplicates) - uses URL month code
    for session_name, talks in conference_data['sessions'].items():
        for talk in talks:
            talk_id = get_talk_id_from_url(talk['url'])  # 2024/10/01homer
            if talk_id and talk_id not in all_resources[conf_key]:
                all_resources[conf_key][talk_id] = {
                    "Gospel Library": talk['url']
                }
    
    # Sort conferences chronologically
    sorted_resources = dict(sorted(all_resources.items(), key=get_conference_sort_key))
    # Sort talks within each conference alphabetically by talk_id
    for conf_key in sorted_resources:
        sorted_resources[conf_key] = dict(sorted(sorted_resources[conf_key].items()))
    
    # Save updated resources
    with open(resources_file, 'w') as f:
        json.dump(sorted_resources, f, indent=2)
    print(f"Saved Gospel Library URLs to {resources_file}")

def create_driver():
    """Create Chrome driver with proper timeout settings"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(300)
    driver.implicitly_wait(10)
    return driver

def scrape_talk(url, session_name, year=None, month=None):
    driver = create_driver()
    talk_data = {}
    talk_data['session'] = session_name
    talk_data['url'] = url
    try:
        driver.get(url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
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
        # Find body container
        try:
            body_element = driver.find_element(By.CLASS_NAME, 'body-block')
        except:
            try:
                body_element = driver.find_element(By.CLASS_NAME, 'body-content')
            except Exception as e:
                print(f"Error: Body container not found for talk at {url}: {e}")
                return None
        # Full markdown
        full_html = body_element.get_attribute('innerHTML')
        talk_data['full_markdown'] = html_to_markdown(full_html)
        # Structured body
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
                try:
                    id_attr = elem.get_attribute('id')
                    if id_attr and id_attr.startswith('p'):
                        id_num = int(id_attr[1:])
                        if id_num:
                            this_verse = id_num
                            verse = max(verse, this_verse)
                except:
                    pass
                talk_data['body'].append({'verse': this_verse, 'type': 'paragraph', 'markdown': markdown})
            elif tag == 'figure':
                try:
                    img = elem.find_element(By.TAG_NAME, 'img')
                    src = img.get_attribute('src')
                    alt = img.get_attribute('alt')
                    talk_data['body'].append({'type': 'image', 'src': src, 'alt': alt})
                except Exception as e:
                    print(f"Error extracting image for talk at {url}: {e}")
        # Sources/footnotes
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
        # Extract year and month from URL if not provided
        if not year or not month:
            match = re.search(r'/general-conference/(\d{4})/(\d{2})/', url)
            if match:
                year = match.group(1)
                month_code = match.group(2)
                month = 'April' if month_code == '04' else 'October'
            else:
                print(f"Error: Could not extract year/month from URL {url}")
                year = None
                month = None
        # Validation
        is_session_or_audit = 'session' in talk_data['title'].lower() or 'auditing' in talk_data['title'].lower() or 'sustaining' in talk_data['title'].lower() or 'introduction' in talk_data['title'].lower()
        if not is_session_or_audit:
            if not talk_data['body']:
                print(f"Error: No body found for talk \"{talk_data['title']}\" at {url}")
            if not talk_data['sources']:
                print(f"Error: No footnotes found for talk \"{talk_data['title']}\" at {url}")
        return talk_data
    except Exception as e:
        print(f"Error during scraping talk {url}: {e}")
        return None
    finally:
        driver.quit()

def scrape_talk_with_retry(url, session_name, year=None, month=None, max_retries=3):
    for attempt in range(max_retries):
        talk = scrape_talk(url, session_name, year, month)
        if talk:
            return talk
        print(f"Attempt {attempt + 1} failed for {url}. Retrying...")
        time.sleep(5 * (attempt + 1))  # Exponential backoff
    print(f"Failed to scrape {url} after {max_retries} attempts")
    return None

def get_conference_filename(year, month):
    """Generate the filename for a conference JSON file in the conference_json directory."""
    sanitized_conference = re.sub(r'[^a-z0-9\- ]', '', f"{year}-{month.lower()}", flags=re.IGNORECASE)
    return os.path.join(JSON_DIR, f"{sanitized_conference}.json")

def scrape_conference(year, month):
    month_code = '04' if month.lower() in ['apr', 'april'] else '10' if month.lower() in ['oct', 'october'] else None
    if not month_code:
        raise ValueError('Invalid month: Must be Apr/April or Oct/October')
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    conference = f"{year}-{month.capitalize()}"
    driver = create_driver()
    conference_data = {'conference': conference, 'year': year, 'month': month.capitalize(), 'sessions': {}}
    talk_list = []
    try:
        driver.get(conference_url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        # Scroll to load all
        try:
            for _ in range(5):
                driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(1)
        except Exception as e:
            print(f"Error scrolling conference page: {e}")
        # Find all talks
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
        current_session_name = None
        for li in li_elements:
            a = li.find_element(By.TAG_NAME, 'a')
            href = a.get_attribute('href')
            full_url = href if href.startswith('https') else f"https://www.churchofjesuschrist.org{href}"
            last_segment = full_url.split('/')[-1].split('?')[0]
            if re.match(r'^\d{2}[a-z]+$', last_segment, re.IGNORECASE):
                if current_session_name:
                    title = ''
                    try:
                        title = li.find_element(By.CSS_SELECTOR, 'p.title').text
                    except:
                        pass
                    speaker = ''
                    try:
                        speaker = li.find_element(By.CSS_SELECTOR, 'p.author').text
                        speaker = normalize_speaker(speaker)
                    except:
                        pass
                    talk_list.append({'url': full_url, 'session': current_session_name, 'title': title, 'speaker': speaker})
            else:
                try:
                    title_p = li.find_element(By.CSS_SELECTOR, 'p.title')
                    current_session_name = title_p.text
                except:
                    current_session_name = 'Unknown Session'
                conference_data['sessions'][current_session_name] = []
        
        # Scrape talks with SIMPLE parallel processing + REAL-TIME PROGRESS
        total_talks = len(talk_list)
        if total_talks > 0:
            print(f'Scraping {total_talks} talks in parallel (5 workers)...')
        
        with tqdm(total=total_talks, desc="Scraping talks", unit="talk") as pbar:
            with ThreadPoolExecutor(max_workers=5) as executor:
                # Submit all tasks
                futures = []
                for i, item in enumerate(talk_list):
                    future = executor.submit(scrape_talk_with_retry, item['url'], item['session'], year, month)
                    futures.append((i, future, item))
                
                # Process completed tasks as they finish
                completed = 0
                while completed < total_talks:
                    for i, future, item in futures:
                        if future.done():
                            try:
                                talk = future.result()
                                if talk:
                                    conference_data['sessions'][talk['session']].append(talk)
                                else:
                                    print(f"Failed: {item['title'] or 'Unknown'} ({item['url']})")
                                pbar.update(1)
                                completed += 1
                            except Exception as e:
                                print(f"Future error for {item['url']}: {e}")
                                pbar.update(1)
                                completed += 1
                            futures[i] = (i, None, None)  # Mark as processed
                            break
                    time.sleep(0.1)  # Prevent busy waiting
        
        # Save conference data
        filename = get_conference_filename(year, month)
        with open(filename, 'w') as f:
            json.dump(conference_data, f, indent=2)
        print(f"\n✅ Saved conference data to {filename}")
        # Save Gospel Library URLs to resources file
        save_conference_resources(conference_data, year, month)
    except Exception as e:
        print(f"Error scraping conference: {e}")
    finally:
        driver.quit()

def scrape_single_talk(url):
    match = re.search(r'/general-conference/(\d{4})/(\d{2})/', url)
    year = match.group(1) if match else None
    month_code = match.group(2) if match else None
    month = 'April' if month_code == '04' else 'October' if month_code == '10' else None
    if not year or not month:
        print("Error: Cannot determine conference from URL.")
        return
    conference = f"{year}-{month.capitalize()}"
    filename = get_conference_filename(year, month)
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    driver = create_driver()
    try:
        driver.get(conference_url)
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        # Scroll to load all
        try:
            for _ in range(5):
                driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(1)
        except Exception as e:
            print(f"Error scrolling conference page: {e}")
        # Find session
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
        # Scrape talk with retry
        talk = scrape_talk_with_retry(url, session_name, year, month)
        if not talk:
            print('Failed to scrape single talk')
            return
        # Load or create conference_data
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                conference_data = json.load(f)
        else:
            conference_data = {'conference': conference, 'year': year, 'month': month.capitalize(), 'sessions': {}}
        # Add or update talk (no duplicates by title)
        if session_name not in conference_data['sessions']:
            conference_data['sessions'][session_name] = []
        existing_talk = next((t for t in conference_data['sessions'][session_name] if t['title'] == talk['title']), None)
        if existing_talk:
            existing_talk.update(talk)
            print(f"Updated talk '{talk['title']}' in {filename}")
        else:
            conference_data['sessions'][session_name].append(talk)
            print(f"Added talk '{talk['title']}' to {filename}")
        # Save conference data
        with open(filename, 'w') as f:
            json.dump(conference_data, f, indent=2)
        print(f"Saved updated data to {filename}")
        # Save Gospel Library URL to resources file
        save_conference_resources(conference_data, year, month)
    except Exception as e:
        print(f"Error in scrape_single_talk: {e}")
    finally:
        driver.quit()

if __name__ == '__main__':
    args = sys.argv[1:]
    if len(args) == 2:
        year, month = args
        scrape_conference(year, month)
    elif len(args) == 1:
        url = args[0]
        if url.startswith('https://'):
            scrape_single_talk(url)
        else:
            print('Invalid URL')
    else:
        print('Usage: python website_scraper.py <year> <month> or python website_scraper.py <talk_url>')
        sys.exit(1)