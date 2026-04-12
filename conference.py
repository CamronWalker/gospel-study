"""
Unified General Conference pipeline.

Usage:
  # Scrape a single conference (structure + talk content + IDs + thumbnails)
  py conference.py scrape 2026-april

  # Scrape a year range
  py conference.py scrape 2020-2025

  # Add resource URLs (Gospel Library, BYU, Saints AI, Church News)
  py conference.py resources 2026-april
  py conference.py resources 2026-april --only youtube  # exact matches with API key, search URLs without

  # Generate Obsidian markdown from JSON
  py conference.py markdown 2026-april
  py conference.py markdown 2026-april --replace  # overwrite existing files

  # Run full pipeline (scrape + resources + markdown)
  py conference.py pipeline 2026-april

  # Re-scrape talk content even if already present
  py conference.py scrape 2026-april --replace
"""

import os
import sys
import json
import re
import html
import argparse
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
from glob import glob
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

JSON_DIR = 'conference_json'
os.makedirs(JSON_DIR, exist_ok=True)

BASE_URL = 'https://www.churchofjesuschrist.org'
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

BOOK_MAP = {
    'bofm/1-ne': '1 Nephi', 'bofm/2-ne': '2 Nephi', 'bofm/jacob': 'Jacob',
    'bofm/enos': 'Enos', 'bofm/jarom': 'Jarom', 'bofm/omni': 'Omni',
    'bofm/w-of-m': 'Words of Mormon', 'bofm/mosiah': 'Mosiah', 'bofm/alma': 'Alma',
    'bofm/hel': 'Helaman', 'bofm/3-ne': '3 Nephi', 'bofm/4-ne': '4 Nephi',
    'bofm/morm': 'Mormon', 'bofm/ether': 'Ether', 'bofm/moro': 'Moroni',
    'dc-testament/dc': 'D&C',
    'ot/gen': 'Genesis', 'ot/ex': 'Exodus', 'ot/lev': 'Leviticus',
    'ot/num': 'Numbers', 'ot/deut': 'Deuteronomy', 'ot/josh': 'Joshua',
    'ot/judg': 'Judges', 'ot/ruth': 'Ruth', 'ot/1-sam': '1 Samuel',
    'ot/2-sam': '2 Samuel', 'ot/1-kgs': '1 Kings', 'ot/2-kgs': '2 Kings',
    'ot/1-chr': '1 Chronicles', 'ot/2-chr': '2 Chronicles', 'ot/ezra': 'Ezra',
    'ot/neh': 'Nehemiah', 'ot/esth': 'Esther', 'ot/job': 'Job',
    'ot/ps': 'Psalms', 'ot/prov': 'Proverbs', 'ot/eccl': 'Ecclesiastes',
    'ot/song': 'Song of Solomon', 'ot/isa': 'Isaiah', 'ot/jer': 'Jeremiah',
    'ot/lam': 'Lamentations', 'ot/ezek': 'Ezekiel', 'ot/dan': 'Daniel',
    'ot/hosea': 'Hosea', 'ot/joel': 'Joel', 'ot/amos': 'Amos',
    'ot/obad': 'Obadiah', 'ot/jonah': 'Jonah', 'ot/micah': 'Micah',
    'ot/nahum': 'Nahum', 'ot/hab': 'Habakkuk', 'ot/zeph': 'Zephaniah',
    'ot/hag': 'Haggai', 'ot/zech': 'Zechariah', 'ot/mal': 'Malachi',
    'nt/matt': 'Matthew', 'nt/mark': 'Mark', 'nt/luke': 'Luke',
    'nt/john': 'John', 'nt/acts': 'Acts', 'nt/rom': 'Romans',
    'nt/1-cor': '1 Corinthians', 'nt/2-cor': '2 Corinthians',
    'nt/gal': 'Galatians', 'nt/eph': 'Ephesians', 'nt/phlp': 'Philippians',
    'nt/col': 'Colossians', 'nt/1-thes': '1 Thessalonians',
    'nt/2-thes': '2 Thessalonians', 'nt/1-tim': '1 Timothy',
    'nt/2-tim': '2 Timothy', 'nt/titus': 'Titus', 'nt/philem': 'Philemon',
    'nt/heb': 'Hebrews', 'nt/james': 'James', 'nt/1-pet': '1 Peter',
    'nt/2-pet': '2 Peter', 'nt/1-jn': '1 John', 'nt/2-jn': '2 John',
    'nt/3-jn': '3 John', 'nt/jude': 'Jude', 'nt/rev': 'Revelation',
    'pgp/moses': 'Moses', 'pgp/abr': 'Abraham',
    'pgp/js-m': 'Joseph Smith—Matthew', 'pgp/js-h': 'Joseph Smith—History',
    'pgp/a-of-f': 'Articles of Faith',
}


def parse_target(target):
    """Parse a conference target string into a list of (year, month_name) tuples.

    Accepts: 2026-april, 2026-04, 2026-October, 2020-2025, 2026
    """
    t = target.strip()

    # YYYY-MonthName or YYYY-MonthAbbr
    m = re.match(r'^(\d{4})-(april|october|apr|oct)$', t, re.IGNORECASE)
    if m:
        month = 'April' if m.group(2).lower().startswith('a') else 'October'
        return [(int(m.group(1)), month)]

    # YYYY-MM (04 or 10)
    m = re.match(r'^(\d{4})-(04|10)$', t)
    if m:
        month = 'April' if m.group(2) == '04' else 'October'
        return [(int(m.group(1)), month)]

    # YYYY-YYYY range
    m = re.match(r'^(\d{4})-(\d{4})$', t)
    if m:
        start, end = int(m.group(1)), int(m.group(2))
        return [(y, mo) for y in range(start, end + 1) for mo in ('April', 'October')]

    # Single year
    m = re.match(r'^(\d{4})$', t)
    if m:
        y = int(m.group(1))
        return [(y, 'April'), (y, 'October')]

    print(f"Invalid target: {target}")
    print("Use: 2026-april, 2026-04, 2020-2025, or 2026")
    sys.exit(1)


def month_code(month):
    return '04' if month.lower() == 'april' else '10'


def json_path(year, month):
    return os.path.join(JSON_DIR, f"{year}-{month.lower()}.json")


def load_json(year, month):
    path = json_path(year, month)
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(year, month, data):
    path = json_path(year, month)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother|Bishop)\s+', '', speaker, flags=re.IGNORECASE)
    return speaker.strip()


def sanitize_filename(title):
    # Remove characters invalid in Windows filenames
    for ch in ['"', '?', '!', ',', '\u201c', '\u201d']:
        title = title.replace(ch, '')
    for ch in [':', '/', '\\', '|', '<', '>', '*']:
        title = title.replace(ch, '-')
    return re.sub(r'\s+', ' ', title).strip()


def generate_filename(title, speaker, year, month):
    lastname = speaker.split()[-1]
    month_short = month[:3]
    return sanitize_filename(f"{title} — {lastname} {year} {month_short}")


def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            resp.encoding = 'utf-8'  # Church site serves UTF-8 without charset header
            return resp
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def error(msg):
    tqdm.write(f"\033[91m{msg}\033[0m")


def all_talks(data):
    """Yield (session_name, talk_key, talk_dict) for every talk in a conference."""
    for session_name, session in data.get('sessions', {}).items():
        for talk_key, talk in session.get('talks', {}).items():
            yield session_name, talk_key, talk


# ---------------------------------------------------------------------------
# SCRAPE: conference list + talk content + IDs + thumbnails — all in one pass
# ---------------------------------------------------------------------------

def scrape_conference_list(year, month):
    """Scrape the conference overview page to get sessions, talks, and thumbnails."""
    mc = month_code(month)
    url = f"{BASE_URL}/study/general-conference/{year}/{mc}?lang=eng"
    resp = fetch(url)
    soup = BeautifulSoup(resp.text, 'html.parser')

    conference = f"{year}-{month}"
    data = {'conference': conference, 'year': year, 'month': month, 'sessions': {}}

    doc_map = soup.find('ul', class_='doc-map')
    if not doc_map:
        error(f"Could not find talk list for {conference}")
        return data

    # Structure: outer ul.doc-map > li[data-content-type=general-conference-session]
    #   each session li contains: div with p.title (session name) + inner ul.doc-map with talk lis
    for session_li in doc_map.find_all('li', recursive=False):
        # Get session name from the direct div > p.title
        session_title_p = session_li.find('p', class_='title', recursive=False)
        if not session_title_p:
            # Try inside a direct child div
            direct_div = session_li.find('div', recursive=False)
            session_title_p = direct_div.find('p', class_='title') if direct_div else None
        session_name = html.unescape(session_title_p.get_text(strip=True)) if session_title_p else 'Unknown Session'

        # Find the inner doc-map with talks
        inner_ul = session_li.find('ul', class_='doc-map')
        if not inner_ul:
            continue

        session_url = None
        talks = {}

        for li in inner_ul.find_all('li', recursive=False):
            a_tag = li.find('a')
            if not a_tag:
                continue

            href = a_tag.get('href', '')
            full_url = href if href.startswith('http') else f"{BASE_URL}{href}"
            slug = href.split('/')[-1].split('?')[0]

            title_p = a_tag.find('p', class_='title')
            title = html.unescape(title_p.get_text(strip=True)) if title_p else ''

            speaker_p = a_tag.find('p', class_='primaryMeta')
            speaker = html.unescape(speaker_p.get_text(strip=True)) if speaker_p else ''

            img = a_tag.find('img')
            thumbnail = None
            if img and img.get('src'):
                thumbnail = re.sub(r'%21\d+%2C', '%21300%2C', img['src'])

            # Session overview link (no speaker, slug ends with -session)
            if slug.endswith('-session'):
                session_url = full_url
                continue

            # It's a talk
            talk_key = f"/{year}/{mc}/{slug}"
            talk_data = {
                'title': title,
                'speaker': normalize_speaker(speaker),
                'url': full_url,
                'filename': generate_filename(title, normalize_speaker(speaker), year, month),
            }
            if thumbnail:
                talk_data['thumbnail'] = thumbnail
            talks[talk_key] = talk_data

        data['sessions'][session_name] = {'talks': talks, 'url': session_url}

    return data


def get_wikilink(href, text):
    """Convert a scripture URL to an Obsidian wikilink."""
    try:
        if not href.startswith('http'):
            href = f"{BASE_URL}{href}"
        parsed = re.match(r'https?://[^/]+(/study/scriptures/[^?#]+)(\?[^#]*)?(#.*)?', href)
        if not parsed:
            return None
        path = parsed.group(1)
        query = parsed.group(2) or ''
        fragment = parsed.group(3) or ''
        parts = path.split('/')[3:]
        if len(parts) < 2:
            return None

        corpus, book_abbr = parts[0], parts[1]
        chapter = start_verse = end_verse = None

        if len(parts) > 2:
            chapter_str = parts[2]
            chap_match = re.match(r'(\d+)\.(\d+)(?:-(\d+))?', chapter_str)
            if chap_match:
                chapter = chap_match.group(1)
                start_verse = int(chap_match.group(2))
                end_verse = int(chap_match.group(3)) if chap_match.group(3) else start_verse
            else:
                chap_match = re.match(r'\d+', chapter_str)
                if chap_match:
                    chapter = chapter_str

        if query:
            params = dict(q.split('=') for q in query.lstrip('?').split('&') if '=' in q)
            id_param = params.get('id')
            if id_param:
                id_match = re.match(r'(?:p|verse)?(\d+)(?:-(?:p|verse)?(\d+))?', id_param)
                if id_match:
                    start_verse = int(id_match.group(1))
                    end_verse = int(id_match.group(2)) if id_match.group(2) else start_verse

        if start_verse is None and fragment:
            frag_match = re.match(r'#(?:p|verse)?(\d+)(?:-(?:p|verse)?(\d+))?', fragment)
            if frag_match:
                start_verse = int(frag_match.group(1))
                end_verse = int(frag_match.group(2)) if frag_match.group(2) else start_verse

        key = f"{corpus}/{book_abbr}"
        book_name = BOOK_MAP.get(key)
        if not book_name:
            return None

        if book_name == 'D&C':
            base_name = f"D&C {chapter}" if chapter else "D&C"
        else:
            base_name = f"{book_name} {chapter}" if chapter else book_name

        if start_verse is None:
            return f"[[{base_name}]]"

        if start_verse == end_verse:
            display = f"{base_name}:{start_verse}"
        else:
            display = f"{base_name}:{start_verse}-{end_verse}"

        links = [f"[[{base_name}#{start_verse}|{display}]]"]
        if start_verse != end_verse:
            for v in range(start_verse + 1, end_verse + 1):
                links.append(f"[[{base_name}#{v}|]]")
        return ''.join(links)
    except Exception as e:
        error(f"Warning: Failed to parse scripture link {href}: {e}")
        return None


def get_conference_wikilink(href, text):
    """Convert a conference talk URL to an Obsidian wikilink."""
    try:
        if not href.startswith('http'):
            href = f"{BASE_URL}{href}"
        parsed = re.match(r'https?://[^/]+(/study/general-conference/[^?#]+)', href)
        if not parsed:
            return None
        parts = parsed.group(1).split('/')[3:]
        if len(parts) != 3:
            return None
        year, mc, slug = parts
        month_map = {'04': 'april', '10': 'october'}
        month = month_map.get(mc)
        if not month:
            return None
        json_file = json_path(int(year), month.capitalize())
        if not os.path.exists(json_file):
            return None
        with open(json_file, 'r') as f:
            conf_data = json.load(f)
        talk_key = f"/{year}/{mc}/{slug}"
        for session in conf_data['sessions'].values():
            if talk_key in session.get('talks', {}):
                fn = session['talks'][talk_key].get('filename')
                if fn:
                    return f"[[{fn}|{text}]]"
        return None
    except Exception:
        return None


def html_to_markdown(html_content, is_source=False):
    """Convert talk HTML to markdown with Obsidian wikilinks."""
    s = html.unescape(html_content)
    s = re.sub(r'<em>(.*?)</em>', r'*\1*', s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r'<i>(.*?)</i>', r'*\1*', s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r'<strong>(.*?)</strong>', r'**\1**', s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r'<b>(.*?)</b>', r'**\1**', s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r'<span[^>]*>(.*?)</span>', r'\1', s, flags=re.IGNORECASE | re.DOTALL)
    # Footnote superscript references
    s = re.sub(
        r'<a[^>]*class="note-ref"[^>]*data-scroll-id="([^"]+)"[^>]*><sup[^>]*>.*?</sup></a>',
        r'[^\1]', s, flags=re.IGNORECASE | re.DOTALL
    )
    if is_source:
        s = re.sub(r'<a[^>]+class="backref"[^>]*>.*?</a>', '', s, flags=re.IGNORECASE | re.DOTALL)

    def link_repl(match):
        href, text = match.group(1), match.group(2)
        abs_href = href if href.startswith('http') else f"{BASE_URL}{href}"
        wiki = get_wikilink(abs_href, text)
        if wiki:
            return wiki
        wiki = get_conference_wikilink(abs_href, text)
        if wiki:
            return wiki
        return f"[{text}]({abs_href})"

    s = re.sub(r'<a[^>]+href="([^"]+)"[^>]*>([^<]+)</a>', link_repl, s, flags=re.IGNORECASE | re.DOTALL)
    s = re.sub(r'<[^>]+>', '', s)
    return s.strip()


def scrape_talk_content(url):
    """Fetch a single talk page and extract body, sources, and full markdown."""
    try:
        resp = fetch(url)
        soup = BeautifulSoup(resp.text, 'html.parser')

        body_el = soup.find('div', class_='body-block') or soup.find('div', class_='body-content')
        if not body_el:
            error(f"No body found: {url}")
            return None

        full_markdown = html_to_markdown(str(body_el))
        body = []
        paragraph_counter = 0

        for elem in body_el.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'figure'], recursive=False):
            # Also check nested elements inside sections
            pass

        # Walk all relevant elements within the body-block (including nested in sections)
        body = []
        paragraph_counter = 0
        for elem in body_el.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'figure']):
            tag = elem.name.lower()
            inner_html = elem.decode_contents()
            md = html_to_markdown(inner_html)

            if tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                level = int(tag[1])
                body.append({'type': 'heading', 'level': level, 'markdown': md})
            elif tag == 'p':
                if not md.strip():
                    continue
                classes = ' '.join(elem.get('class', []))
                if 'article-footer' in classes or 'share-' in classes or 'reference' in classes or 'short-reference' in classes:
                    continue
                paragraph_counter += 1
                body.append({'paragraph': paragraph_counter, 'type': 'paragraph', 'markdown': md})
            elif tag == 'figure':
                img = elem.find('img')
                if img:
                    body.append({'type': 'image', 'src': img.get('src', ''), 'alt': img.get('alt', '')})

        # Sources / footnotes
        sources = []
        notes_section = soup.find(class_='notes')
        if notes_section:
            ol = notes_section.find('ol')
            if ol:
                for i, li in enumerate(ol.find_all('li', recursive=False)):
                    id_attr = li.get('id', '')
                    inner_html = li.decode_contents()
                    md = html_to_markdown(inner_html, is_source=True)
                    sources.append({'number': i + 1, 'id': id_attr, 'markdown': md})

        return {'full_markdown': full_markdown, 'body': body, 'sources': sources}
    except Exception as e:
        error(f"Error scraping {url}: {e}")
        return None


def add_talk_ids(data):
    """Add sortable talk_id field (YYYY-MM-SS-TT) to every talk."""
    mc = month_code(data['month'])
    for session_num, (_, session) in enumerate(data.get('sessions', {}).items(), start=1):
        for talk_num, (_, talk) in enumerate(session.get('talks', {}).items(), start=1):
            talk['talk_id'] = f"{data['year']}-{mc}-{session_num:02d}-{talk_num:02d}"


def cmd_scrape(conferences, replace=False):
    """Scrape conference structure and talk content."""
    for year, month in conferences:
        existing = load_json(year, month)
        if existing and not replace:
            # Check if all talks already have content
            needs_scrape = False
            for _, _, talk in all_talks(existing):
                if 'body' not in talk or not talk['body']:
                    needs_scrape = True
                    break
            if not needs_scrape:
                tqdm.write(f"Skipping {year} {month}: already scraped (use --replace to re-scrape)")
                continue

        tqdm.write(f"Scraping {year} {month} conference list...")
        data = scrape_conference_list(year, month)

        # If we have existing data with content and are replacing, merge structure
        # but still re-scrape content. If not replacing, keep existing content.
        if existing and not replace:
            # Merge: keep existing talk content, add any new talks from fresh scrape
            for sname, session in data['sessions'].items():
                for tk, talk in session['talks'].items():
                    # Find in existing
                    for ex_sname, ex_session in existing['sessions'].items():
                        if tk in ex_session.get('talks', {}):
                            ex_talk = ex_session['talks'][tk]
                            # Carry over existing content fields
                            for field in ['body', 'full_markdown', 'sources', 'resources',
                                          'ai_resources', 'talk_id', 'thumbnail']:
                                if field in ex_talk and field not in talk:
                                    talk[field] = ex_talk[field]
                            break

        total_talks = sum(len(s['talks']) for s in data['sessions'].values())
        talks_to_scrape = []
        for sname, session in data['sessions'].items():
            for tk, talk in session['talks'].items():
                if replace or 'body' not in talk or not talk.get('body'):
                    talks_to_scrape.append(talk)

        if talks_to_scrape:
            tqdm.write(f"Scraping {len(talks_to_scrape)}/{total_talks} talk pages...")
            for talk in tqdm(talks_to_scrape, desc=f"  {year} {month}"):
                content = scrape_talk_content(talk['url'])
                if content:
                    talk['full_markdown'] = content['full_markdown']
                    talk['body'] = content['body']
                    talk['sources'] = content['sources']
                else:
                    # Retry once
                    time.sleep(2)
                    content = scrape_talk_content(talk['url'])
                    if content:
                        talk['full_markdown'] = content['full_markdown']
                        talk['body'] = content['body']
                        talk['sources'] = content['sources']
                    else:
                        error(f"Failed after retry: {talk['url']}")

                # Ensure Gospel Library resource exists
                if 'resources' not in talk:
                    talk['resources'] = []
                if not any(r['name'] == 'Gospel Library' for r in talk['resources']):
                    talk['resources'].insert(0, {'name': 'Gospel Library', 'url': talk['url']})

        # Add talk IDs
        add_talk_ids(data)

        save_json(year, month, data)
        tqdm.write(f"Saved {json_path(year, month)} ({total_talks} talks)")


# ---------------------------------------------------------------------------
# RESOURCES: add external URLs to existing JSON
# ---------------------------------------------------------------------------

def add_gospel_library(talk, replace=False):
    if 'resources' not in talk:
        talk['resources'] = []
    if not replace and any(r['name'] == 'Gospel Library' for r in talk['resources']):
        return
    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Gospel Library']
    talk['resources'].insert(0, {'name': 'Gospel Library', 'url': talk['url']})


def add_saintsai(talk, year, replace=False):
    if year < 2017:
        return
    if 'resources' not in talk:
        talk['resources'] = []
    if not replace and any(r['name'] == 'Saints AI Study Guide' for r in talk['resources']):
        return
    base = talk['url'].replace(BASE_URL, 'https://saintsai.org').split('?')[0]
    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Saints AI Study Guide']
    talk['resources'].append({'name': 'Saints AI Study Guide', 'url': f"{base}/study-guide"})


def add_byu(talk, byu_talks, conf_hash, replace=False):
    if 'resources' not in talk:
        talk['resources'] = []
    if not replace and any(r['name'] == 'BYU Citation Index' for r in talk['resources']):
        return
    norm_title = re.sub(r'[^a-zA-Z0-9\s]', '', talk['title'].strip()).lower()
    norm_speaker = re.sub(r'[^a-zA-Z0-9\s]', '', normalize_speaker(talk['speaker'])).lower()
    for bt in byu_talks:
        bt_title = re.sub(r'[^a-zA-Z0-9\s]', '', bt['title']).lower()
        bt_speaker = re.sub(r'[^a-zA-Z0-9\s]', '', bt['speaker']).lower()
        if bt_title == norm_title and bt_speaker == norm_speaker:
            url = f"https://scriptures.byu.edu/#:t{bt['t_hash']}:g{conf_hash}"
            talk['resources'] = [r for r in talk['resources'] if r['name'] != 'BYU Citation Index']
            talk['resources'].append({'name': 'BYU Citation Index', 'url': url})
            return


def fetch_byu_talks(year, month):
    """Scrape BYU citation index for a conference using requests + BS4."""
    mc = month_code(month)
    year_num = int(year) - 1830
    if mc == '10':
        year_num += 2048
    conf_hash = format(year_num, 'x')

    # BYU scriptures site is JS-rendered, so we try but it may not work without Selenium
    # Fall back gracefully if it doesn't
    try:
        url = f"https://scriptures.byu.edu/api/talks?conf={conf_hash}"
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            talks = []
            for item in resp.json():
                talks.append({
                    'title': item.get('title', ''),
                    'speaker': normalize_speaker(item.get('speaker', '')),
                    't_hash': format(int(item.get('id', 0)), 'x'),
                })
            return talks, conf_hash
    except Exception:
        pass

    # If API didn't work, return empty — BYU links are nice-to-have
    return [], conf_hash


def fetch_youtube_videos(year, month):
    """Fetch conference talk videos from YouTube Data API. Returns (videos, from_playlist) or None."""
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        return None
    channel_id = 'UCSdPpMokMoGCSSNShOecP9w'  # Official LDS channel
    target_title = f"{month} {year} General Conference"

    try:
        # Try playlist search first (low quota cost)
        playlist_id = None
        page_token = None
        while True:
            url = f"https://www.googleapis.com/youtube/v3/playlists?part=snippet&channelId={channel_id}&maxResults=50&key={api_key}"
            if page_token:
                url += f"&pageToken={page_token}"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get('items', []):
                if item['snippet']['title'].lower() == target_title.lower():
                    playlist_id = item['id']
                    break
            if playlist_id:
                break
            page_token = data.get('nextPageToken')
            if not page_token:
                break

        if not playlist_id:
            tqdm.write(f"  YouTube: no playlist found for '{target_title}' — skipping API search to conserve quota")
            return None

        videos = []
        page_token = None
        while True:
            url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={playlist_id}&maxResults=50&key={api_key}"
            if page_token:
                url += f"&pageToken={page_token}"
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get('items', []):
                snippet = item['snippet']
                vid = snippet.get('resourceId', {}).get('videoId')
                if vid:
                    videos.append({'title': snippet['title'], 'video_id': vid})
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        return videos, True
    except Exception as e:
        error(f"YouTube API error: {e}")
        return None


def add_youtube(talk, year, month, videos=None, from_playlist=False, replace=False):
    """Add YouTube resource — exact match via API if available, search URL as fallback."""
    if 'sustaining' in talk['title'].lower() or 'auditing' in talk['title'].lower():
        return
    if 'resources' not in talk:
        talk['resources'] = []
    if not replace and any(r['name'] == 'YouTube Video' for r in talk['resources']):
        return

    # Try exact match from API videos
    if videos:
        norm_title = re.sub(r'[^a-zA-Z0-9\s]', '', talk['title'].strip()).lower()
        norm_speaker = re.sub(r'[^a-zA-Z0-9\s]', '', talk['speaker']).lower()
        for video in videos:
            vt = re.sub(r'[^a-z0-9\s]', '', video['title'].lower())
            if from_playlist:
                if norm_title in vt:
                    url = f"https://www.youtube.com/watch?v={video['video_id']}"
                    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'YouTube Video']
                    talk['resources'].append({'name': 'YouTube Video', 'url': url})
                    return
            else:
                if norm_title in vt and norm_speaker in vt:
                    url = f"https://www.youtube.com/watch?v={video['video_id']}"
                    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'YouTube Video']
                    talk['resources'].append({'name': 'YouTube Video', 'url': url})
                    return

    # Fallback: YouTube search URL
    query = quote(f"{month} {year} General Conference {talk['speaker']} {talk['title']}")
    url = f"https://www.youtube.com/results?search_query={query}"
    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'YouTube Video']
    talk['resources'].append({'name': 'YouTube Video', 'url': url})


def cmd_resources(conferences, replace=False, only=None):
    """Add resource URLs to conference JSON files."""
    resource_types = ['library', 'saintsai', 'byu', 'youtube'] if not only else [only]

    for year, month in conferences:
        data = load_json(year, month)
        if not data:
            tqdm.write(f"No JSON for {year} {month} — run 'scrape' first")
            continue

        tqdm.write(f"Adding resources for {year} {month}...")

        # BYU data (fetch once per conference)
        byu_talks, conf_hash = [], None
        if 'byu' in resource_types:
            byu_talks, conf_hash = fetch_byu_talks(year, month)
            if byu_talks:
                tqdm.write(f"  BYU: found {len(byu_talks)} talks")
            else:
                tqdm.write(f"  BYU: no data (JS-rendered site may need manual fallback)")

        # YouTube data (fetch once per conference if API key available)
        yt_videos, yt_from_playlist = None, False
        if 'youtube' in resource_types:
            result = fetch_youtube_videos(year, month)
            if result:
                yt_videos, yt_from_playlist = result
                tqdm.write(f"  YouTube API: found {len(yt_videos)} videos")
            else:
                tqdm.write(f"  YouTube: no API key, using search URLs")

        for _, _, talk in all_talks(data):
            if 'library' in resource_types:
                add_gospel_library(talk, replace)
            if 'saintsai' in resource_types:
                add_saintsai(talk, year, replace)
            if 'byu' in resource_types and byu_talks:
                add_byu(talk, byu_talks, conf_hash, replace)
            if 'youtube' in resource_types:
                add_youtube(talk, year, month, yt_videos, yt_from_playlist, replace)

        save_json(year, month, data)
        tqdm.write(f"  Saved {json_path(year, month)}")


# ---------------------------------------------------------------------------
# MARKDOWN: generate Obsidian files from JSON
# ---------------------------------------------------------------------------

def get_month_abbr(month):
    return {'april': 'Apr', 'october': 'Oct'}.get(month.lower(), month[:3])


def format_speaker_wikilink(speaker):
    parts = speaker.split()
    if len(parts) > 2 and len(parts[1]) == 1:
        return f"[[{parts[0]} {parts[1]}. {' '.join(parts[2:])}]]"
    return f"[[{speaker}]]"


def escape_brackets(md):
    def repl(match):
        content = match.group(1)
        if content.startswith('^') or content.startswith('[') or '|' in content or '(' in content:
            return match.group(0)
        return '\\[' + content + '\\]'
    return re.sub(r'\[([^\]]+)\](?!\()', repl, md)


def build_frontmatter(talk):
    lines = [
        '---',
        'publish: true',
        f'talk_id: "{talk.get("talk_id", "")}"',
        f'conference: "{talk["conference"]}"',
        f'year: {talk["year"]}',
        f'month: "{talk["month"].capitalize()}"',
        f'session: "{talk["session"]}"',
        f'speaker: "{format_speaker_wikilink(talk["speaker"])}"',
        f'speaker-role: "{talk.get("speaker_role", "")}"',
        f'title: "{talk["title"]}"',
        f'thumbnail: "{talk.get("thumbnail", "")}"',
    ]
    kicker = talk.get('ai_resources', {}).get('summaries', {}).get('kicker')
    if kicker:
        lines.append(f'kicker: "{kicker}"')
    lines.append('cssclasses: "conference"')
    lines.append('---')
    return '\n'.join(lines)


def build_properties(talk):
    lines = ['> [!Properties]+ Resources']
    lines.append(f'>Session: {talk["session"]}')
    lines.append(f'>URL: {talk["url"]}')
    lines.append('>Resources:')
    links = "    |    ".join(f"[{r['name']}]({r['url']})" for r in talk.get('resources', []))
    lines.append(f'>{links}')

    ai = talk.get('ai_resources', {})
    summaries = ai.get('summaries', {})
    if summaries:
        lines.append('>')
        lines.append('>> [!AI]- AI Summaries')
        for key, summary in summaries.items():
            title = f'AI Summary {key.replace("_", " ").title()}'
            lines.append(f'>>> [!AI]- {title}')
            lines.append('>>>' + summary)
            lines.append('>>')

    topics = ai.get('topics', [])
    if topics:
        filename = talk['filename'] + '.md'
        lines.append('>')
        lines.append('>> [!AI]- AI Topics')
        for topic in topics:
            lines.append(f'>>> [!topic]- {topic["name"]}')
            for pair in topic.get('question_quote_pairs', []):
                q = pair['question']
                quote_text = pair['quote'].strip('"')
                pk = pair['paragraph_key']
                lines.append(f'>>> - {q}')
                lines.append(f'>>>   > "{quote_text}"')
                lines.append(f'>>>   (See [[{filename}#{pk}|paragraph {pk}]])')
            lines.append('>>')

    return '\n'.join(lines) + '\n'


def build_talk_body(talk):
    youtube_url = None
    for r in talk.get('resources', []):
        if r['name'] == 'YouTube Video':
            youtube_url = r['url']
            break

    body = ['# Talk']
    if youtube_url:
        body.append(f'![{talk.get("title", "")}]({youtube_url})')
    kicker = talk.get('ai_resources', {}).get('summaries', {}).get('kicker')
    if kicker:
        body.append(kicker)
    body.append('')

    for item in talk.get('body', []):
        if not isinstance(item, dict) or 'markdown' not in item:
            if item.get('type') == 'image':
                body.append(f'![{item.get("alt", "")}]({item.get("src", "")})')
                body.append('')
            continue
        md = escape_brackets(item['markdown'])
        if item.get('type') == 'heading':
            level = item.get('level', 2) + 2
            body.append('#' * level + ' ' + md)
        elif item.get('type') == 'paragraph':
            if 'paragraph' in item:
                body.append(f"###### {item['paragraph']}")
                body.append(f"{item['paragraph']} {md}")
            else:
                body.append(md)
        else:
            body.append(md)
            body.append('')

    sources = talk.get('sources', [])
    if sources:
        body.append('')
        for src in sources:
            md = escape_brackets(src['markdown'])
            body.append(f'[^{src["id"]}]: {md}')

    return '\n'.join(body) + '\n'


def build_full_md(talk):
    md = build_frontmatter(talk) + '\n\n'
    md += build_properties(talk) + '\n'
    if 'invitation' in talk:
        md += '> [!invite]- Invitations\n' + talk['invitation'] + '\n\n'
    md += '# Notes\n\n\n\n'
    md += build_talk_body(talk)
    return md


def update_md_prefix(existing_md, talk):
    notes_pos = existing_md.find('# Notes')
    if notes_pos == -1:
        return build_full_md(talk)
    remaining = existing_md[notes_pos:]
    prefix = build_frontmatter(talk) + '\n\n'
    prefix += build_properties(talk) + '\n'
    if 'invitation' in talk:
        prefix += '> [!invite]- Invitations\n' + talk['invitation'] + '\n\n'
    return prefix + remaining


def cmd_markdown(conferences, replace=False):
    """Generate Obsidian markdown files from JSON."""
    for year, month in conferences:
        data = load_json(year, month)
        if not data:
            tqdm.write(f"No JSON for {year} {month} — run 'scrape' first")
            continue

        month_abbr = get_month_abbr(month)
        conf_folder = f"Conference/{year}-{month_abbr}"
        os.makedirs(conf_folder, exist_ok=True)

        count = 0
        for session_name, _, talk in all_talks(data):
            talk['conference'] = data['conference']
            talk['year'] = data['year']
            talk['month'] = data['month']
            talk['session'] = session_name

            filename = talk['filename'] + '.md'
            filepath = os.path.join(conf_folder, filename)

            if replace or not os.path.exists(filepath):
                content = build_full_md(talk)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    existing = f.read()
                updated = update_md_prefix(existing, talk)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(updated)
            count += 1

        tqdm.write(f"Generated {count} markdown files in {conf_folder}/")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='General Conference pipeline: scrape, enrich, and generate markdown.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  py conference.py scrape 2026-april
  py conference.py resources 2026-april
  py conference.py resources 2026-april --only youtube
  py conference.py markdown 2026-april
  py conference.py markdown 2026-april --replace
  py conference.py pipeline 2026-april
  py conference.py scrape 2020-2025 --replace
        """,
    )
    sub = parser.add_subparsers(dest='command', required=True)

    # scrape
    p_scrape = sub.add_parser('scrape', help='Scrape conference structure and talk content to JSON')
    p_scrape.add_argument('target', help='2026-april, 2026-04, 2020-2025, or 2026')
    p_scrape.add_argument('--replace', action='store_true', help='Re-scrape even if data exists')

    # resources
    p_res = sub.add_parser('resources', help='Add resource URLs to conference JSON')
    p_res.add_argument('target', help='Conference target')
    p_res.add_argument('--replace', action='store_true', help='Replace existing resources')
    p_res.add_argument('--only', choices=['library', 'saintsai', 'byu', 'youtube'],
                       help='Only update this resource type')

    # markdown
    p_md = sub.add_parser('markdown', help='Generate Obsidian markdown from JSON')
    p_md.add_argument('target', help='Conference target')
    p_md.add_argument('--replace', action='store_true', help='Overwrite existing markdown files')

    # pipeline
    p_pipe = sub.add_parser('pipeline', help='Run scrape + resources + markdown')
    p_pipe.add_argument('target', help='Conference target')
    p_pipe.add_argument('--replace', action='store_true', help='Replace existing data')

    args = parser.parse_args()
    conferences = parse_target(args.target)

    if args.command == 'scrape':
        cmd_scrape(conferences, args.replace)
    elif args.command == 'resources':
        cmd_resources(conferences, args.replace, getattr(args, 'only', None))
    elif args.command == 'markdown':
        cmd_markdown(conferences, args.replace)
    elif args.command == 'pipeline':
        cmd_scrape(conferences, args.replace)
        cmd_resources(conferences, args.replace)
        cmd_markdown(conferences, args.replace)


if __name__ == '__main__':
    main()
