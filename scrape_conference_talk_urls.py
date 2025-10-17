"""
Sample Usage:
- To scrape conferences from 2020 to 2025:
  python script.py 2020 2025
  (This will scrape resources for April and October conferences in the specified years and save to 'conference_json/conference_resources.json'. Existing conferences will be skipped.)

- To run in debug mode (scrape all and output differences):
  python script.py 2020 2025 --debug
  (This will scrape all conferences, compare with existing data, print differences, and update the file.)
"""

import os
import json
import sys
import time
import re
import difflib
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

JSON_DIR = 'conference_json'
os.makedirs(JSON_DIR, exist_ok=True)

def get_conference_resources(year, month):
    month_code = '04' if month.lower() == 'april' else '10'
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_code}?lang=eng"
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    talk_urls = {}
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
            print(f"Error scrolling conference page for {year} {month}: {e}")
        # Find all li in ul.doc-map
        li_elements = driver.find_elements(By.CSS_SELECTOR, 'ul.doc-map > li')
        for li in li_elements:
            a = li.find_element(By.TAG_NAME, 'a')
            href = a.get_attribute('href')
            full_url = href if href.startswith('https') else f"https://www.churchofjesuschrist.org{href}"
            last_segment = full_url.split('/')[-1].split('?')[0]
            if re.match(r'^\d{2}[a-z]+$', last_segment, re.IGNORECASE):
                path = f"{year}/{month_code}/{last_segment}"
                talk_urls[path] = {"Gospel Library": full_url}
    except Exception as e:
        print(f"Error scraping conference {year} {month}: {e}")
    finally:
        driver.quit()
    return talk_urls

def dict_diff(d1, d2):
    """Compute differences between two dicts."""
    diff = []
    for key in set(d1) | set(d2):
        if key not in d2:
            diff.append(f"+ {key}: {d1[key]}")
        elif key not in d1:
            diff.append(f"- {key}: {d2[key]}")
        elif d1[key] != d2[key]:
            diff.append(f"~ {key}: {d1[key]} (was {d2[key]})")
    return diff

if __name__ == '__main__':
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python script.py start_year end_year [--debug]")
        sys.exit(1)
    start_year = int(sys.argv[1])
    end_year = int(sys.argv[2])
    debug = len(sys.argv) == 4 and sys.argv[3] == '--debug'
    
    filename = os.path.join(JSON_DIR, 'conference_resources.json')
    resources = {}
    existing_resources = {}
    
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            existing_resources = json.load(f)
        resources = existing_resources.copy()
    
    # Prepare list of conferences to process
    conf_list = []
    for year in range(start_year, end_year + 1):
        for month in ['April', 'October']:
            conf_key = f"{year}-{month}"
            should_scrape = debug or conf_key not in existing_resources
            if should_scrape:
                conf_list.append((year, month, conf_key))
    
    if conf_list:
        pbar = tqdm(conf_list)
        for year, month, conf_key in pbar:
            pbar.set_description(f"Scraping {conf_key}")
            talk_dict = get_conference_resources(year, month)
            if talk_dict:
                new_talk_dict = dict(sorted(talk_dict.items()))
                if debug and conf_key in existing_resources:
                    old_talk_dict = existing_resources[conf_key]
                    differences = dict_diff(new_talk_dict, old_talk_dict)
                    if differences:
                        print(f"Differences for {conf_key}:")
                        for d in differences:
                            print(d)
                    else:
                        print(f"No differences for {conf_key}")
                resources[conf_key] = new_talk_dict
    else:
        print("No conferences to scrape.")
    
    # Sort conferences in chronological order
    resources = dict(sorted(resources.items()))
    
    with open(filename, 'w') as f:
        json.dump(resources, f, indent=2)
    print(f"Saved resources to {filename}")