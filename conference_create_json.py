"""
Sample Usage:
- To scrape a range of General Conferences:
  python3 conference_scrape_to_json.py 2020-2023
  (This will scrape the list of talks from all April and October conferences in the range and save to JSON files like 'conference_json/2020-april.json', 'conference_json/2020-october.json', etc.)
"""
import os
import json
import sys
import re
import argparse
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm
# Create conference_json directory if it doesn't exist
JSON_DIR = 'conference_json'
os.makedirs(JSON_DIR, exist_ok=True)
def normalize_author(author):
    author = re.sub(r'By\s+', '', author, flags=re.IGNORECASE)
    author = re.sub(r'^(Elder|President|Sister|Brother|Bishop)\s+', '', author, flags=re.IGNORECASE)
    return author.strip()
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
def scrape_conference(driver, year, month, pbar):
    month_code = '04' if month.lower() in ['apr', 'april'] else '10' if month.lower() in ['oct', 'october'] else None
    if not month_code:
        raise ValueError('Invalid month: Must be April or October')
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    conference = f"{year}-{month.capitalize()}"
    filename = get_conference_filename(year, month)
    conference_data = {'conference': conference, 'year': year, 'month': month.capitalize(), 'sessions': {}}
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
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
        current_session_name = None
        for li in li_elements:
            ps = li.find_elements(By.TAG_NAME, 'p')
            if len(ps) == 0:
                continue
            title = ps[0].text.strip()
            author = None
            if len(ps) >= 2:
                author = ps[1].text.strip()
            has_a = False
            full_url = None
            slug = None
            try:
                a = li.find_element(By.TAG_NAME, 'a')
                href = a.get_attribute('href')
                full_url = href if href.startswith('https') else f"https://www.churchofjesuschrist.org{href}"
                slug = full_url.split('/')[-1].split('?')[0]
                has_a = True
            except:
                pass
            if author and author != title:
                # talk
                if current_session_name is None:
                    current_session_name = 'Unknown Session'
                if current_session_name not in conference_data['sessions']:
                    conference_data['sessions'][current_session_name] = {"talks": {}, "url": None}
                if has_a and slug:
                    conference_data['sessions'][current_session_name]["talks"][slug] = {
                        "title": author,
                        "speaker": normalize_author(title),
                        "url": full_url
                    }
            else:
                # session related
                if current_session_name is None or title != current_session_name:
                    current_session_name = title
                    if current_session_name not in conference_data['sessions']:
                        conference_data['sessions'][current_session_name] = {"talks": {}, "url": None}
                if has_a:
                    conference_data['sessions'][current_session_name]["url"] = full_url
        # Save
        with open(filename, 'w') as f:
            json.dump(conference_data, f, indent=2)
    except Exception as e:
        pbar.write(f"Error scraping {conference}: {e}")
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape General Conference talk list")
    parser.add_argument('year_range', help="Year range (e.g., 2020-2023)")
    args = parser.parse_args()
    try:
        start_year, end_year = map(int, args.year_range.split('-'))
        conferences = [(year, month) for year in range(start_year, end_year + 1) for month in ['April', 'October']]
        driver = get_driver()
        pbar = tqdm(conferences, desc="Scraping conferences")
        for year, month in pbar:
            pbar.set_description(f"{year} {month}")
            scrape_conference(driver, year, month, pbar)
    except ValueError:
        print('Invalid year range format. Use YYYY-YYYY.')
        sys.exit(1)
    finally:
        if 'driver' in locals():
            driver.quit()