"""
Sample Usage:
- To add newsroom summaries to all talks in a JSON file:
  python newsroom_adder.py 2023-october.json
  (This will process all talks, search for newsroom URLs using Grok, prompt for manual input on misses, and update the JSON file in place.)

- To add a newsroom summary to a single talk by title:
  python newsroom_adder.py 2023-october.json --talk "Think Celestial!"
  (This will target only the specified talk title (case-insensitive), search using Grok, prompt if not found, and update the JSON.)

- To add a manual newsroom URL to a single talk:
  python newsroom_adder.py 2023-october.json --talk "Think Celestial!" --manual https://thechurchnews.com/example-summary
  (This skips the Grok search and directly adds the provided URL to the specified talk, updating the JSON.)
"""

import os
import json
import sys
import re
from dotenv import load_dotenv
from xai_sdk import Client
from xai_sdk.chat import user
from xai_sdk.search import SearchParameters, web_source
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Load .env from parent directory
load_dotenv(dotenv_path='../.env')
# Now access the key
XAI_API_KEY = os.getenv('XAI_API_KEY')

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother)\s+', '', speaker, flags=re.IGNORECASE)
    return speaker.strip()

def get_speaker_search_term(speaker, speaker_role):
    prefix = ''
    if speaker_role:
        role_lower = speaker_role.lower()
        if 'president of the church' in role_lower:
            prefix = 'president '
        elif 'quorum' in role_lower or 'seventy' in role_lower:
            prefix = 'elder '
        elif 'president' in role_lower:  # for general organization presidents, assuming women
            prefix = 'sister '
        # add more if needed, e.g., brother
    last_name = speaker.split()[-1].lower()
    return prefix + last_name

def find_newsroom_summary_url_with_grok(title, speaker, speaker_role, year, month):
    speaker_search = get_speaker_search_term(speaker, speaker_role)
    prompt = f'Search The Church News for the summary URL of the General Conference talk titled "{title}" by {speaker_search} from {month} {year}. Reply only with the URL if found, or "Not found" if not.'
    try:
        client = Client(api_key=XAI_API_KEY)
        chat = client.chat.create(
            model="grok-4-fast",
            search_parameters=SearchParameters(
                mode="auto",
                max_search_results=5,
                return_citations=True,
                sources=[
                    web_source(allowed_websites=["thechurchnews.com"]),
                ],
            ),
        )
        chat.append(user(prompt))
        response = chat.sample()
        content = response.content.strip()
        if content.startswith('http') and 'thechurchnews.com' in content:
            return content
        else:
            return None
    except Exception as e:
        print(f'Error finding newsroom summary URL with Grok for "{title}": {e}')
        return None

def add_newsroom_to_talk(talk, year, month, manual_url=None, force=False):
    has_newsroom = any(res['name'] == "Church News Summary" for res in talk.get('talk-resources', []))
    if has_newsroom and not force:
        print(f"Newsroom summary already present for \"{talk['title']}\". Skipping.")
        return False
    # Remove existing if any
    talk['talk-resources'] = [r for r in talk['talk-resources'] if r['name'] != "Church News Summary"]
    url = manual_url
    if not url:
        url = find_newsroom_summary_url_with_grok(talk['title'], talk['speaker'], talk['speaker_role'], year, month)
    if url:
        talk['talk-resources'].append({'name': "Church News Summary", 'url': url})
        return True
    else:
        return False

def process_single_talk(conference_data, target_title, manual_url=None):
    found = False
    year = conference_data.get('year')
    month = conference_data.get('month')
    if not year or not month:
        print("Error: Year or month not found in JSON.")
        return
    for session_name, talks in conference_data['sessions'].items():
        for talk in talks:
            if talk['title'].lower() == target_title.lower():
                found = True
                force = bool(manual_url)
                added = add_newsroom_to_talk(talk, year, month, manual_url, force)
                if not added and not manual_url:
                    user_input = input(f"No newsroom summary found for \"{talk['title']}\". Enter manual URL or press Enter to skip: ").strip()
                    if user_input:
                        add_newsroom_to_talk(talk, year, month, user_input, force=True)
                break
        if found:
            break
    if not found:
        print(f"Talk with title \"{target_title}\" not found.")

def process_all_talks(conference_data):
    year = conference_data.get('year')
    month = conference_data.get('month')
    if not year or not month:
        print("Error: Year or month not found in JSON.")
        return
    all_talks = [talk for talks in conference_data['sessions'].values() for talk in talks]
    total_talks = len(all_talks)
    if total_talks == 0:
        print("No talks found in JSON.")
        return
    print('Adding newsroom summaries:')
    with tqdm(total=total_talks, desc="Processing talks") as pbar:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(add_newsroom_to_talk, talk, year, month) for talk in all_talks]
            for future in as_completed(futures):
                added = future.result()
                pbar.update(1)
    # Handle misses interactively
    misses = [talk for talk in all_talks if not any(res['name'] == "Church News Summary" for res in talk.get('talk-resources', []))]
    for talk in misses:
        user_input = input(f"No newsroom summary found for \"{talk['title']}\". Enter manual URL or press Enter to skip: ").strip()
        if user_input:
            add_newsroom_to_talk(talk, year, month, user_input, force=True)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Add newsroom summaries to JSON file.')
    parser.add_argument('json_file', help='Path to the JSON file')
    parser.add_argument('--talk', help='Title of the single talk to process')
    parser.add_argument('--manual', help='Manual URL to add for the talk (if --talk is specified)')
    args = parser.parse_args()
    
    try:
        with open(args.json_file, 'r') as f:
            conference_data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)
    
    if args.talk:
        process_single_talk(conference_data, args.talk, args.manual)
    else:
        process_all_talks(conference_data)
    
    # Save back to the same file
    with open(args.json_file, 'w') as f:
        json.dump(conference_data, f, indent=2)
    print(f"Updated JSON saved to {args.json_file}")

import os
import json
import sys
import re
from dotenv import load_dotenv
from xai_sdk import Client
from xai_sdk.chat import user
from xai_sdk.search import SearchParameters, web_source
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Load .env from parent directory
load_dotenv(dotenv_path='../.env')
# Now access the key
XAI_API_KEY = os.getenv('XAI_API_KEY')

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother)\s+', '', speaker, flags=re.IGNORECASE)
    return speaker.strip()

def get_speaker_search_term(speaker, speaker_role):
    prefix = ''
    if speaker_role:
        role_lower = speaker_role.lower()
        if 'president of the church' in role_lower:
            prefix = 'president '
        elif 'quorum' in role_lower or 'seventy' in role_lower:
            prefix = 'elder '
        elif 'president' in role_lower:  # for general organization presidents, assuming women
            prefix = 'sister '
        # add more if needed, e.g., brother
    last_name = speaker.split()[-1].lower()
    return prefix + last_name

def find_newsroom_summary_url_with_grok(title, speaker, speaker_role, year, month):
    speaker_search = get_speaker_search_term(speaker, speaker_role)
    prompt = f'Search The Church News for the summary URL of the General Conference talk titled "{title}" by {speaker_search} from {month} {year}. Reply only with the URL if found, or "Not found" if not.'
    try:
        client = Client(api_key=XAI_API_KEY)
        chat = client.chat.create(
            model="grok-4-fast",
            search_parameters=SearchParameters(
                mode="auto",
                max_search_results=5,
                return_citations=True,
                sources=[
                    web_source(allowed_websites=["thechurchnews.com"]),
                ],
            ),
        )
        chat.append(user(prompt))
        response = chat.sample()
        content = response.content.strip()
        if content.startswith('http') and 'thechurchnews.com' in content:
            return content
        else:
            return None
    except Exception as e:
        print(f'Error finding newsroom summary URL with Grok for "{title}": {e}')
        return None

def add_newsroom_to_talk(talk, year, month, manual_url=None):
    has_newsroom = any(res['name'] == "Church News Summary" for res in talk.get('talk-resources', []))
    if has_newsroom:
        print(f"Newsroom summary already present for \"{talk['title']}\". Skipping.")
        return False
    url = manual_url
    if not url:
        url = find_newsroom_summary_url_with_grok(talk['title'], talk['speaker'], talk['speaker_role'], year, month)
    if url:
        talk['talk-resources'].append({'name': "Church News Summary", 'url': url})
        return True
    else:
        return False

def process_single_talk(conference_data, target_title, manual_url=None):
    found = False
    year = conference_data.get('year')
    month = conference_data.get('month')
    if not year or not month:
        print("Error: Year or month not found in JSON.")
        return
    for session_name, talks in conference_data['sessions'].items():
        for talk in talks:
            if talk['title'].lower() == target_title.lower():
                found = True
                added = add_newsroom_to_talk(talk, year, month, manual_url)
                if not added and not manual_url:
                    user_input = input(f"No newsroom summary found for \"{talk['title']}\". Enter manual URL or press Enter to skip: ").strip()
                    if user_input:
                        add_newsroom_to_talk(talk, year, month, user_input)
                break
        if found:
            break
    if not found:
        print(f"Talk with title \"{target_title}\" not found.")

def process_all_talks(conference_data):
    year = conference_data.get('year')
    month = conference_data.get('month')
    if not year or not month:
        print("Error: Year or month not found in JSON.")
        return
    all_talks = [talk for talks in conference_data['sessions'].values() for talk in talks]
    total_talks = len(all_talks)
    if total_talks == 0:
        print("No talks found in JSON.")
        return
    print('Adding newsroom summaries:')
    with tqdm(total=total_talks, desc="Processing talks") as pbar:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(add_newsroom_to_talk, talk, year, month) for talk in all_talks]
            for future in as_completed(futures):
                added = future.result()
                pbar.update(1)
    # Handle misses interactively
    misses = [talk for talk in all_talks if not any(res['name'] == "Church News Summary" for res in talk.get('talk-resources', []))]
    for talk in misses:
        user_input = input(f"No newsroom summary found for \"{talk['title']}\". Enter manual URL or press Enter to skip: ").strip()
        if user_input:
            add_newsroom_to_talk(talk, year, month, user_input)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Add newsroom summaries to JSON file.')
    parser.add_argument('json_file', help='Path to the JSON file')
    parser.add_argument('--talk', help='Title of the single talk to process')
    parser.add_argument('--manual', help='Manual URL to add for the talk (if --talk is specified)')
    args = parser.parse_args()
    
    try:
        with open(args.json_file, 'r') as f:
            conference_data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)
    
    if args.talk:
        process_single_talk(conference_data, args.talk, args.manual)
    else:
        process_all_talks(conference_data)
    
    # Save back to the same file
    with open(args.json_file, 'w') as f:
        json.dump(conference_data, f, indent=2)
    print(f"Updated JSON saved to {args.json_file}")