"""
conference_generate_resources.py

Purpose:
Update ONLY the optional resources in already-existing conference_json/*.json files.

Features added per your exact specs:
- Accepts range (2020-2025), single year (2023), or specific conference (2025-04 / 2023-october / 2023-10)
- --resource is REQUIRED and can be youtube / byu / churchnews / saintsai / all
- --replace flag forces replace even if the resource already exists (otherwise skips if exists)
- Never does unnecessary work (YouTube/BYU/ChurchNews searches are skipped per-talk if the resource already exists and no --replace)
- Saints AI Study Guide is now included as a resource you can regenerate (it was missing from my first draft)
- Church News fallback changed to >= 2020 (2 attempts + fallback search URL if not found) — this matches "pre 2020" intent (pre-2020 get only 1 attempt and no fallback)
- One shared driver per conference for maximum speed
- YouTube API key loaded from .env (skips YouTube if missing)
- Easy to extend — just add new add_my_new_resource(talk, replace=False) function and add to choices list

Sample usage:
python conference_generate_resources.py 2020-2025 --resource all --replace
python conference_generate_resources.py 2023 --resource youtube
python conference_generate_resources.py 2025-04 --resource churchnews --replace
"""


import os
import json
import re
import time
import requests
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from tqdm import tqdm
from dotenv import load_dotenv
import argparse

load_dotenv()

JSON_DIR = 'conference_json'
os.makedirs(JSON_DIR, exist_ok=True)

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother|Bishop)\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'[^a-zA-Z0-9\s]', '', speaker)
    return speaker.strip()

def get_uniform_talk_key(title):
    return re.sub(r'[^a-zA-Z0-9\s]', '', title.strip()).lower()

def get_driver():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def fetch_conference_videos(year, month):
    api_key = os.getenv('YOUTUBE_API_KEY')
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY missing from .env — cannot update YouTube links")
    channel_id = 'UCSdPpMokMoGCSSNShOecP9w'
    target_title = f"{month} {year} General Conference"

    # playlist search first (low quota)
    playlist_id = None
    page_token = None
    while True:
        url = f"https://www.googleapis.com/youtube/v3/playlists?part=snippet&channelId={channel_id}&maxResults=50&key={api_key}"
        if page_token:
            url += f"&pageToken={page_token}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        for item in data.get('items', []):
            if item['snippet']['title'].lower() == target_title.lower():
                playlist_id = item['id']
                break
        if playlist_id:
            break
        page_token = data.get('nextPageToken')
        if not page_token:
            break

    if playlist_id:
        videos = []
        page_token = None
        while True:
            url = f"https://www.googleapis.com/youtube/v3/playlistItems?part=snippet&playlistId={playlist_id}&maxResults=50&key={api_key}"
            if page_token:
                url += f"&pageToken={page_token}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            for item in data.get('items', []):
                snippet = item['snippet']
                if 'resourceId' in snippet and snippet['resourceId'].get('videoId'):
                    videos.append({
                        'title': snippet['title'],
                        'video_id': snippet['resourceId']['videoId']
                    })
            page_token = data.get('nextPageToken')
            if not page_token:
                break
        return videos, True

    # fallback search if no playlist
    videos = []
    page_token = None
    query = quote(target_title)
    while True:
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&q={query}&type=video&maxResults=50&key={api_key}"
        if page_token:
            url += f"&pageToken={page_token}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        for item in data.get('items', []):
            videos.append({
                'title': item['snippet']['title'],
                'video_id': item['id']['videoId']
            })
        page_token = data.get('nextPageToken')
        if not page_token:
            break
    return videos, False

def fetch_byu_talks(driver, year, month):
    month_code = '04' if month.lower() == 'april' else '10'
    annual = 'A' if month_code == '04' else 'O'
    year_num = int(year) - 1830
    if annual == 'O':
        year_num += 2048
    conf_hash = format(year_num, 'x')
    byu_conf_url = f"https://scriptures.byu.edu/#::g{conf_hash}"

    driver.get(byu_conf_url)
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
    time.sleep(4)

    byu_talks = []
    li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.talksblock li')
    for li in li_elements:
        try:
            onclick = li.find_element(By.CSS_SELECTOR, 'a[onclick*="getTalk"]').get_attribute('onclick')
            t_hash = re.search(r"getTalk\('(\d+)'\)", onclick).group(1)
            title = li.find_element(By.CSS_SELECTOR, 'div.talktitle').text.strip()
            speaker = normalize_speaker(li.find_element(By.CSS_SELECTOR, 'div.speaker').text)
            byu_talks.append({'title': title, 'speaker': speaker, 't_hash': format(int(t_hash), 'x')})
        except:
            continue
    return byu_talks, conf_hash

def add_youtube_resource(talk, videos, is_from_playlist, replace=False):
    if 'resources' not in talk:
        talk['resources'] = []
    if 'sustaining' in talk['title'].lower() or 'auditing' in talk['title'].lower():
        return
    if not replace and any(r['name'] == 'YouTube Video' for r in talk['resources']):
        return

    norm_title = get_uniform_talk_key(talk['title'])
    norm_speaker = normalize_speaker(talk['speaker']).lower()

    for video in videos:
        video_title_lower = video['title'].lower()
        norm_video_title = re.sub(r'[^a-z0-9\s]', '', video_title_lower)

        if is_from_playlist:
            if norm_title in norm_video_title:
                url = f"https://www.youtube.com/watch?v={video['video_id']}"
                talk['resources'] = [r for r in talk['resources'] if r['name'] != 'YouTube Video']
                talk['resources'].append({'name': 'YouTube Video', 'url': url})
                return
        else:  # search fallback
            if norm_title in norm_video_title and norm_speaker in norm_video_title:
                url = f"https://www.youtube.com/watch?v={video['video_id']}"
                talk['resources'] = [r for r in talk['resources'] if r['name'] != 'YouTube Video']
                talk['resources'].append({'name': 'YouTube Video', 'url': url})
                return
    # no match → remove if existed (clean)
    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'YouTube Video']

def add_byu_resource(talk, byu_talks, conf_hash, replace=False):
    if 'resources' not in talk:
        talk['resources'] = []
    if not replace and any(r['name'] == 'BYU Citation Index' for r in talk['resources']):
        return

    norm_title = get_uniform_talk_key(talk['title'])
    norm_speaker = normalize_speaker(talk['speaker'])

    matching = next((b for b in byu_talks if get_uniform_talk_key(b['title']) == norm_title and normalize_speaker(b['speaker']) == norm_speaker), None)
    if matching:
        url = f"https://scriptures.byu.edu/#:t{matching['t_hash']}:g{conf_hash}"
        talk['resources'] = [r for r in talk['resources'] if r['name'] != 'BYU Citation Index']
        talk['resources'].append({'name': 'BYU Citation Index', 'url': url})

def add_church_news_resource(talk, year, month, replace=False, driver=None):
    if 'resources' not in talk:
        talk['resources'] = []
    if 'sustaining' in talk['title'].lower() or 'auditing' in talk['title'].lower():
        return
    if not replace and any(r['name'] == 'Church News' for r in talk['resources']):
        return

    attempts = 2 if int(year) >= 2020 else 1
    query = quote(f"{month} {year} General Conference {talk['speaker']} {talk['title']}")
    search_url = f"https://www.thechurchnews.com/search?q={query}"

    found = False
    for attempt in range(attempts):
        try:
            driver.get(search_url)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'filter_item')))
            filter_div = driver.find_element(By.CSS_SELECTOR, 'div.filter_item[data-filter-value="General Conference"]')
            if 'selectedFilterItem' not in filter_div.get_attribute('class'):
                driver.execute_script("searchPage.dofacetedsearch(0,'section','General Conference')")
                time.sleep(2)
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CLASS_NAME, 'queryly_item_row')))
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            rows = soup.find_all('div', class_='queryly_item_row')

            norm_title = get_uniform_talk_key(talk['title'])
            title_words = set(norm_title.split())
            norm_speaker = normalize_speaker(talk['speaker']).lower()

            for row in rows:
                a = row.find('a')
                href = a['href'] if a else None
                title_text = row.find('div', class_='queryly_item_title')
                title_text = title_text.text.strip().lower() if title_text else ''
                norm_title_text = re.sub(r'[^a-z0-9\s]', '', title_text)
                desc = row.find('div', class_='queryly_item_description')
                desc_text = desc.text.strip().lower() if desc else ''
                norm_desc = re.sub(r'[^a-z0-9\s]', '', desc_text)
                full_norm = norm_title_text + ' ' + norm_desc
                if 'episode' in (title_text + desc_text) or 'podcast' in (title_text + desc_text):
                    continue
                if len(title_words & set(full_norm.split())) / len(title_words) >= 0.7 and norm_speaker in full_norm:
                    full_href = href if href.startswith('https') else f"https://www.thechurchnews.com{href}"
                    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Church News']
                    talk['resources'].append({'name': 'Church News', 'url': full_href})
                    found = True
                    break
            if found:
                break
        except Exception as e:
            if attempt == attempts-1:
                print(f"Error searching Church News for \"{talk['title']}\": {e}")

    if not found and int(year) >= 2020:
        talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Church News']
        talk['resources'].append({'name': 'Church News', 'url': search_url})

def add_gospel_library_resource(talk, year, replace=False):
    if int(year) < 1971:
        return
    if 'resources' not in talk:
        talk['resources'] = []
    if not replace and any(r['name'] == 'Gospel Library' for r in talk['resources']):
        return

    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Gospel Library']
    talk['resources'].insert(0, {'name': 'Gospel Library', 'url': talk['url']})

def add_saintsai_resource(talk, year, replace=False):
    year_int = int(year)
    if year_int < 2017:
        if 'resources' in talk:
            talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Saints AI Study Guide']
            if talk['resources'] == []:
                del talk['resources']
        return
    # for >=2017
    if 'resources' not in talk:
        talk['resources'] = []
    if not replace and any(r['name'] == 'Saints AI Study Guide' for r in talk['resources']):
        return

    base_url = talk['url'].replace('https://www.churchofjesuschrist.org', 'https://saintsai.org').split('?')[0]
    study_guide_url = base_url + '/study-guide'
    talk['resources'] = [r for r in talk['resources'] if r['name'] != 'Saints AI Study Guide']
    talk['resources'].append({'name': 'Saints AI Study Guide', 'url': study_guide_url})

    if 'resources' in talk and talk['resources'] == []:
        del talk['resources']

def get_conference_filename(year, month):
    sanitized = re.sub(r'[^a-z0-9\-]', '', f"{year}-{month.lower()}")
    return os.path.join(JSON_DIR, f"{sanitized}.json")

def parse_conference_spec(spec):
    spec = spec.lower().strip()
    if re.fullmatch(r'\d{4}-\d{4}', spec):
        start, end = map(int, spec.split('-'))
        confs = [(str(y), 'April') for y in range(start, end+1)] + [(str(y), 'October') for y in range(start, end+1)]
        return confs
    elif re.fullmatch(r'\d{4}', spec):
        y = spec
        return [(y, 'April'), (y, 'October')]
    elif re.fullmatch(r'\d{4}-(04|10|april|october)', spec):
        yearr, code = spec.split('-')
        month = 'April' if code in ('04', 'april') else 'October'
        return [(yearr, month)]
    else:
        raise ValueError("Invalid format. Use 2020-2025, 2023, or 2025-04/2023-october/2023-10")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Add/replace resources in existing conference JSONs")
    parser.add_argument('conference_spec', help="2020-2025 or 2023 or 2025-04 / 2023-october")
    parser.add_argument('--resource', required=True, choices=['youtube', 'byu', 'churchnews', 'saintsai', 'library', 'all'],
                        help="Which resource(s) to update")
    parser.add_argument('--replace', action='store_true', help="Force replace even if exists")
    args = parser.parse_args()

    conferences = parse_conference_spec(args.conference_spec)

    to_update = ['youtube', 'byu', 'churchnews', 'saintsai', 'library'] if args.resource == 'all' else [args.resource]

    for year, month in conferences:
        print(f"\nProcessing {year} {month} General Conference...")
        filename = get_conference_filename(year, month)
        if not os.path.exists(filename):
            print(f"  No JSON file found — skipping")
            continue

        with open(filename, 'r') as f:
            data = json.load(f)

        videos = is_from_playlist = None, False
        byu_talks = conf_hash = None, None
        driver = None

        try:
            if 'youtube' in to_update:
                videos, is_from_playlist = fetch_conference_videos(year, month)
        except ValueError as e:
            print(e)
            to_update = [r for r in to_update if r != 'youtube']

        need_driver = 'byu' in to_update or 'churchnews' in to_update
        if need_driver:
            driver = get_driver()
            if 'byu' in to_update:
                try:
                    byu_talks, conf_hash = fetch_byu_talks(driver, year, month)
                except Exception as e:
                    print(f"BYU fetch failed: {e}")
                    byu_talks = []

        total_talks = sum(len(session_data['talks']) for session_data in data['sessions'].values())

        with tqdm(total=total_talks, desc=f"  {year} {month}") as pbar:
            for session_name, session_data in data['sessions'].items():
                for talk in session_data['talks'].values():
                    if 'library' in to_update:
                        add_gospel_library_resource(talk, year, args.replace)
                    if 'youtube' in to_update and videos is not None:
                        add_youtube_resource(talk, videos, is_from_playlist, args.replace)
                    if 'byu' in to_update and byu_talks is not None:
                        add_byu_resource(talk, byu_talks, conf_hash, args.replace)
                    if 'churchnews' in to_update:
                        add_church_news_resource(talk, year, month, args.replace, driver=driver)
                    if 'saintsai' in to_update:
                        add_saintsai_resource(talk, year, args.replace)
                    pbar.update(1)

        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"  Saved {filename}")

        if driver:
            driver.quit()
    print("\nAll done!")