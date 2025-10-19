"""
Sample Usage:
- To add BYU Citation Index for conferences from 2020 to 2025:
  python add_conference_resource_citation_index.py 2020 2025
  (This will process April and October conferences in the specified years, scrape BYU for talk hashes, match to talks, and add URLs to conference_resources.json. Existing resources will be skipped.)
"""

import os
import json
import sys
import re
import time
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import utils

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother)\s+', '', speaker, flags=re.IGNORECASE)
    return speaker.strip()

def compute_conf_hash(year, month):
    month_code = '04' if month.lower() in ['apr', 'april'] else '10' if month.lower() in ['oct', 'october'] else None
    if not month_code:
        raise ValueError('Invalid month: Must be Apr/April or Oct/October')
    annual = 'A' if month_code == '04' else 'O'
    year_num = int(year) - 1830
    if annual == 'O':
        year_num += 2048
    conf_hash = format(year_num, 'x')
    return conf_hash

def scrape_byu_talk_hashes(byu_conf_url):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    byu_talks = {}
    try:
        driver.get(byu_conf_url)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(5)  # Increased sleep
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
                continue
            if t_hash and title and speaker:
                key = utils.get_author_title_key(title, speaker)
                byu_talks[key] = t_hash
    except Exception as e:
        pass
    finally:
        driver.quit()
    return byu_talks

def get_talk_key(talk, year, month):
    """Generate author|title key"""
    return utils.get_author_title_key(talk['title'], talk['speaker'])

def process_conference(conference_data, output_resources):
    year = conference_data.get('year')
    month = conference_data.get('month')
    conference_key = f"{year}-{month.replace(' ', '-').title()}"
    
    if not year or not month:
        return

    try:
        conf_hash = compute_conf_hash(year, month)
    except ValueError as e:
        return
    
    byu_conf_url = f"https://scriptures.byu.edu/#::g{conf_hash}"
    byu_talks = scrape_byu_talk_hashes(byu_conf_url)

    if not byu_talks:
        return
    
    if conference_key not in output_resources:
        output_resources[conference_key] = {}
    
    # Flatten all talks
    all_talks = []
    for session_name, talks in conference_data['sessions'].items():
        all_talks.extend(talks)

        # Count talks that need the resource
    total_to_add = 0
    for talk in all_talks:
        key = utils.get_author_title_key(talk['title'], talk['speaker'])
        if key in byu_talks:
            talk_key = get_talk_key(talk, year, month)
            if "BYU Citation Index" not in output_resources[conference_key].get(talk_key, {}):
                total_to_add += 1

    added_count = 0
    with tqdm(total=total_to_add, desc=f"Processing {conference_key}") as pbar:
        for talk in all_talks:
            pbar.set_description(f"Processing \"{talk['title']}\"")
            key = utils.get_author_title_key(talk['title'], talk['speaker'])
            if key in byu_talks:
                t_hash = byu_talks[key]
                url = f"https://scriptures.byu.edu/#:t{t_hash}:g{conf_hash}"
                talk_key = get_talk_key(talk, year, month)
                if "BYU Citation Index" not in output_resources[conference_key].get(talk_key, {}):
                    output_resources[conference_key][talk_key] = {
                        **output_resources[conference_key].get(talk_key, {}),
                        "BYU Citation Index": url
                    }
                    added_count += 1
                    pbar.update(1)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: python add_conference_resource_citation_index.py start_year end_year')
        sys.exit(1)
    
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    
    # Load existing conference_resources.json or create new
    output_file = 'conference_json/conference_resources.json'
    try:
        with open(output_file, 'r') as f:
            output_resources = json.load(f)
    except FileNotFoundError:
        output_resources = {}
    
    # Prepare list of conferences to process
    conf_list = []
    for year in range(start_year, end_year + 1):
        for month in ['April', 'October']:
            json_file = os.path.join('conference_json', f"{year}-{month.lower()}.json")
            if os.path.exists(json_file):
                conf_key = f"{year}-{month}"
                conf_list.append((year, month, conf_key, json_file))
    
    if conf_list:
        pbar = tqdm(conf_list)
        for year, month, conf_key, json_file in pbar:
            pbar.set_description(f"Processing {conf_key}")
            try:
                with open(json_file, 'r') as f:
                    conference_data = json.load(f)
                process_conference(conference_data, output_resources)
            except Exception as e:
                pass
    
    # Save to conference_resources.json
    with open(output_file, 'w') as f:
        json.dump(output_resources, f, indent=2)

    # Show summary
    total_added = sum(len(resources) for resources in output_resources.values() if isinstance(resources, dict))
    print(f"Total talks with BYU Citation Index: {total_added}")