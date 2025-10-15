"""
Sample Usage:
- To scrape an entire General Conference:
  python website_scraper.py 2023 October
  (This will scrape all talks from the specified conference and save to a JSON file like '2023-october.json'. Note: Month must be 'April' or 'October'.)

- To scrape a single talk:
  python website_scraper.py https://www.churchofjesuschrist.org/study/general-conference/2023/10/12nelson?lang=eng
  (This will scrape the individual talk, determine the conference, and add/update it in the corresponding conference JSON file like '2023-october.json'. If the file doesn't exist, it will create it with just that talk.)
"""

import os
import json
import time
import sys
import re
import requests
from urllib.parse import urlparse
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

# Load .env from parent directory
load_dotenv(dotenv_path='../.env')

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

def find_youtube_url(driver, title, speaker, year, month):
    from urllib.parse import quote
    search_query = f"{title} – {speaker} – General Conference – {month} {year}"
    encoded_query = quote(search_query)
    yt_channel = 'churchofjesuschristgeneralconf'
    yt_search_url = f"https://www.youtube.com/@{yt_channel}/search?query={encoded_query}"
    try:
        driver.get(yt_search_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'ytd-video-renderer')))
        video_links = driver.find_elements(By.CSS_SELECTOR, 'ytd-video-renderer a#video-title')
        if video_links:
            href = video_links[0].get_attribute('href')
            return href
    except Exception as e:
        print(f'Error finding YouTube URL for "{title}": {e}')
    return None

def scrape_talk(url, session_name, year=None, month=None):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    talk_data = {}
    talk_data['session'] = session_name
    talk_data['url'] = url
    talk_data['saintsai_url'] = url.replace('www.churchofjesuschrist.org', 'saintsai.org').split('?')[0] + '/study-guide'
    talk_data['byu_url'] = None
    talk_data['youtube_url'] = None
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)  # Reduced wait for dynamic elements
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
        # Find body container (try body-block first, fallback to body-content)
        try:
            body_element = driver.find_element(By.CLASS_NAME, 'body-block')
        except:
            try:
                body_element = driver.find_element(By.CLASS_NAME, 'body-content')
            except Exception as e:
                print(f"Error: Body container not found for talk at {url}: {e}")
                return None
        # Backup full body HTML, but convert to Markdown
        full_html = body_element.get_attribute('innerHTML')
        talk_data['full_markdown'] = html_to_markdown(full_html)
        # Extract structured body: headings, paragraphs (with verse from id if available), images
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
                # Check for id like "pX" to override verse
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
            # No sources/footnotes found, no log
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
        # Find YouTube URL
        if year and month:
            talk_data['youtube_url'] = find_youtube_url(driver, talk_data['title'], talk_data['speaker'], year, month)
        else:
            talk_data['youtube_url'] = None
        # Check for errors
        is_session_or_audit = 'session' in talk_data['title'].lower() or 'auditing' in talk_data['title'].lower() or 'sustaining' in talk_data['title'].lower()
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

def scrape_byu_talk_hashes(driver, byu_conf_url, conference_data, conf_hash):
    try:
        driver.get(byu_conf_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(5)  # Increased sleep
        byu_talks = []
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
                title = li.find_element(By.CSS_SELECTOR, 'div.talktitle').text
                speaker = li.find_element(By.CSS_SELECTOR, 'div.speaker').text
                speaker = normalize_speaker(speaker)
            except Exception as e:
                print(f"Error extracting title/speaker: {e}")
                continue
            if t_hash and title and speaker:
                byu_talks.append({'title': title.strip(), 'speaker': speaker, 't_hash': t_hash})
        # Match to conference_data
        for session_name, talks in conference_data['sessions'].items():
            for talk in talks:
                norm_title = talk['title'].strip()
                norm_speaker = talk['speaker']
                matching = next((b for b in byu_talks if b['title'] == norm_title and b['speaker'] == norm_speaker), None)
                if matching:
                    talk['byu_t_hash'] = matching['t_hash']
    except Exception as e:
        print(f"Error scraping BYU hashes: {e}")
    return conference_data

def consolidate_resources(conference_data, conf_hash=None):
    for session_name, talks in conference_data['sessions'].items():
        for t in talks:
            if 'talk-resources' not in t:
                t['talk-resources'] = []
            # Update or add each resource if the field is present
            if 'url' in t:
                t['talk-resources'] = [r for r in t['talk-resources'] if r['name'] != 'Gospel Library']
                t['talk-resources'].append({'name': 'Gospel Library', 'url': t['url']})
            if 'saintsai_url' in t:
                t['talk-resources'] = [r for r in t['talk-resources'] if r['name'] != 'Saints AI Study Guide']
                t['talk-resources'].append({'name': 'Saints AI Study Guide', 'url': t['saintsai_url']})
            if 'byu_t_hash' in t or 'byu_url' in t:
                t['talk-resources'] = [r for r in t['talk-resources'] if r['name'] != 'BYU Citation Index']
                if 'byu_url' not in t and 'byu_t_hash' in t and conf_hash:
                    t['byu_url'] = f"https://scriptures.byu.edu/#:t{t['byu_t_hash']}:g{conf_hash}"
                if 'byu_url' in t:
                    t['talk-resources'].append({'name': 'BYU Citation Index', 'url': t['byu_url']})
            if 'youtube_url' in t:
                t['talk-resources'] = [r for r in t['talk-resources'] if r['name'] != 'YouTube Video']
                t['talk-resources'].append({'name': 'YouTube Video', 'url': t['youtube_url']})
            if 'newsroom_summary_url' in t:
                t['talk-resources'] = [r for r in t['talk-resources'] if r['name'] != 'Church News Summary']
                t['talk-resources'].append({'name': 'Church News Summary', 'url': t['newsroom_summary_url']})
            # Pop the individual fields
            t.pop('url', None)
            t.pop('saintsai_url', None)
            t.pop('byu_url', None)
            t.pop('youtube_url', None)
            t.pop('newsroom_summary_url', None)
            t.pop('byu_t_hash', None)

def scrape_conference(year, month):
    month_code = '04' if month.lower() in ['apr', 'april'] else '10' if month.lower() in ['oct', 'october'] else None
    if not month_code:
        raise ValueError('Invalid month: Must be Apr/April or Oct/October')
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    conference = f"{year}-{month.capitalize()}"
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    conference_data = {'conference': conference, 'year': year, 'month': month.capitalize(), 'sessions': {}}
    talk_list = []
    try:
        driver.get(conference_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        # Scroll to load all if lazy
        try:
            for _ in range(5):
                driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(1)
        except Exception as e:
            print(f"Error scrolling conference page: {e}")
        # Find all li in ul.doc-map
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
        current_session_name = None
        for li in li_elements:
            a = li.find_element(By.TAG_NAME, 'a')
            href = a.get_attribute('href')
            full_url = href if href.startswith('https') else f"https://www.churchofjesuschrist.org{href}"
            last_segment = full_url.split('/')[-1].split('?')[0]
            if re.match(r'^\d{2}[a-z]+$', last_segment, re.IGNORECASE):
                # This is a talk li
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
                # This is a session li
                try:
                    title_p = li.find_element(By.CSS_SELECTOR, 'p.title')
                    current_session_name = title_p.text
                except:
                    current_session_name = 'Unknown Session'
                conference_data['sessions'][current_session_name] = []
        # Now scrape talks with progress
        total_talks = len(talk_list)
        if total_talks > 0:
            print('Scraping talks:')
        with tqdm(total=total_talks, desc="Scraping talks") as pbar:
            for talk_item in talk_list:
                talk = scrape_talk(talk_item['url'], talk_item['session'], year, month)
                if talk:
                    conference_data['sessions'][talk_item['session']].append(talk)
                else:
                    print(f"Failed to scrape talk at {talk_item['url']}")
                pbar.update(1)
        # Compute BYU conference hash
        annual = 'A' if month_code == '04' else 'O'
        year_num = int(year) - 1830
        if annual == 'O':
            year_num += 2048
        conf_hash = format(year_num, 'x')
        byu_conf_url = f"https://scriptures.byu.edu/#::g{conf_hash}"
        # Scrape BYU talk hashes and match
        try:
            scrape_byu_talk_hashes(driver, byu_conf_url, conference_data, conf_hash)
        except Exception as e:
            print(f"Error scraping BYU hashes: {e}")
        # Consolidate resources
        consolidate_resources(conference_data, conf_hash)
        # Save the entire conference data to a single JSON file
        sanitized_conference = re.sub(r'[^a-z0-9\- ]', '', conference, flags=re.IGNORECASE)
        file_name = f"{sanitized_conference}.json"
        with open(file_name, 'w') as f:
            json.dump(conference_data, f, indent=2)
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
    filename = f"{year}-{month.lower()}.json"
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(conference_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(3)
        # Scroll to load all if lazy
        try:
            for _ in range(5):
                driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')
                time.sleep(1)
        except Exception as e:
            print(f"Error scrolling conference page: {e}")
        # Find session for the talk
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
        # Scrape the talk
        talk = scrape_talk(url, session_name, year, month)
        if not talk:
            print('Failed to scrape single talk')
            return
        # Load or create conference_data
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                conference_data = json.load(f)
        else:
            conference_data = {'conference': conference, 'year': year, 'month': month.capitalize(), 'sessions': {}}
        # Add or update the talk
        if session_name not in conference_data['sessions']:
            conference_data['sessions'][session_name] = []
        existing_talk = next((t for t in conference_data['sessions'][session_name] if t['title'] == talk['title']), None)
        if existing_talk:
            existing_talk.update(talk)
            print(f"Updated talk '{talk['title']}' in {filename}")
        else:
            conference_data['sessions'][session_name].append(talk)
            print(f"Added talk '{talk['title']}' to {filename}")
        # Compute BYU hash
        annual = 'A' if month_code == '04' else 'O'
        year_num = int(year) - 1830
        if annual == 'O':
            year_num += 2048
        conf_hash = format(year_num, 'x')
        byu_conf_url = f"https://scriptures.byu.edu/#::g{conf_hash}"
        # Scrape BYU hashes for all current talks
        try:
            scrape_byu_talk_hashes(driver, byu_conf_url, conference_data, conf_hash)
        except Exception as e:
            print(f"Error fetching BYU hashes: {e}")
        # Consolidate resources
        consolidate_resources(conference_data, conf_hash)
        # Save
        with open(filename, 'w') as f:
            json.dump(conference_data, f, indent=2)
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