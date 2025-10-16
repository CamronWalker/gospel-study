"""
Sample Usage:
- To add newsroom summaries to all talks in a JSON file and output to conference_resources.json:
  python newsroom_adder.py 2023-october.json
  (This will process all talks, search for newsroom URLs using Grok with retry (limit 4→8), 
   prompt for manual input on misses, and output to conference_resources.json)

- To add a newsroom summary to a single talk by title:
  python newsroom_adder.py 2023-october.json --talk "Think Celestial!"
  (This will target only the specified talk title (case-insensitive), search using Grok with retry, 
   prompt if not found, and output to conference_resources.json)

- To add a manual newsroom summary to a single talk:
  python newsroom_adder.py 2023-october.json --talk "Think Celestial!" --manual https://thechurchnews.com/example-summary
  (This skips the Grok search and directly adds the provided URL to the specified talk, outputs to conference_resources.json)
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

# Load .env from same directory
load_dotenv()
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

def get_talk_key(talk, year, month):
    """Generate URL-style key: year/month/daylastnames"""
    day = talk.get('day', '01')  # default to 01 if no day
    speaker = normalize_speaker(talk['speaker'])
    last_name = speaker.split()[-1].lower()
    return f"{year}/{month}/{day}{last_name}"

def find_newsroom_summary_url_with_grok(title, speaker, speaker_role, year, month, search_limit=4):
    speaker_search = get_speaker_search_term(speaker, speaker_role)
    prompt = f'Search The Church News for the summary URL of the General Conference talk titled "{title}" by {speaker_search} from {month} {year}. Reply only with the URL if found, or "Not found" if not.'
    try:
        client = Client(api_key=XAI_API_KEY)
        chat = client.chat.create(
            model="grok-4-fast",
            search_parameters=SearchParameters(
                mode="auto",
                max_search_results=search_limit,
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

def find_newsroom_summary_url_with_retry(title, speaker, speaker_role, year, month):
    """Try with limit 4, then retry with limit 8 if not found"""
    # First attempt with limit 4
    url = find_newsroom_summary_url_with_grok(title, speaker, speaker_role, year, month, search_limit=4)
    if url:
        return url
    
    # Retry with limit 8
    print(f'Retrying with higher search limit (8) for "{title}"...')
    url = find_newsroom_summary_url_with_grok(title, speaker, speaker_role, year, month, search_limit=8)
    return url

def process_single_talk(conference_data, target_title, manual_url=None, output_resources=None):
    found = False
    year = conference_data.get('year')
    month = conference_data.get('month')
    conference_key = f"{year}-{month.replace(' ', '-').title()}"
    
    if not year or not month:
        print("Error: Year or month not found in JSON.")
        return
    for session_name, talks in conference_data['sessions'].items():
        for talk in talks:
            if talk['title'].lower() == target_title.lower():
                found = True
                talk_key = get_talk_key(talk, year, month)
                
                # Check if already exists
                if output_resources.get(conference_key, {}).get(talk_key, {}).get("Church News Summary") and not manual_url:
                    print(f"Newsroom summary already present for \"{talk['title']}\". Skipping.")
                    break
                
                url = manual_url
                if not url:
                    url = find_newsroom_summary_url_with_retry(talk['title'], talk['speaker'], talk['speaker_role'], year, month)
                
                if url:
                    if conference_key not in output_resources:
                        output_resources[conference_key] = {}
                    output_resources[conference_key][talk_key] = {
                        **output_resources[conference_key].get(talk_key, {}),
                        "Church News Summary": url
                    }
                    print(f"Added newsroom summary for \"{talk['title']}\"")
                else:
                    user_input = input(f"No newsroom summary found for \"{talk['title']}\". Enter manual URL or press Enter to skip: ").strip()
                    if user_input:
                        if conference_key not in output_resources:
                            output_resources[conference_key] = {}
                        output_resources[conference_key][talk_key] = {
                            **output_resources[conference_key].get(talk_key, {}),
                            "Church News Summary": user_input
                        }
                        print(f"Added manual newsroom summary for \"{talk['title']}\"")
                break
        if found:
            break
    if not found:
        print(f"Talk with title \"{target_title}\" not found.")

def process_all_talks(conference_data, output_resources):
    year = conference_data.get('year')
    month = conference_data.get('month')
    conference_key = f"{year}-{month.replace(' ', '-').title()}"
    
    if not year or not month:
        print("Error: Year or month not found in JSON.")
        return
    
    if conference_key not in output_resources:
        output_resources[conference_key] = {}
    
    all_talks = [talk for talks in conference_data['sessions'].values() for talk in talks]
    total_talks = len(all_talks)
    if total_talks == 0:
        print("No talks found in JSON.")
        return
    
    # Filter talks that need processing
    talks_to_process = []
    for talk in all_talks:
        talk_key = get_talk_key(talk, year, month)
        if "Church News Summary" not in output_resources[conference_key].get(talk_key, {}):
            talks_to_process.append(talk)
    
    print(f'Adding newsroom summaries to {len(talks_to_process)} talks:')
    with tqdm(total=len(talks_to_process), desc="Processing talks") as pbar:
        with ThreadPoolExecutor(max_workers=20) as executor:
            # Submit all futures first
            future_to_talk = {}
            for talk in talks_to_process:
                future = executor.submit(
                    find_newsroom_summary_url_with_retry, 
                    talk['title'], talk['speaker'], talk['speaker_role'], year, month
                )
                future_to_talk[future] = talk  # Store mapping future -> talk
            
            # Process completed futures
            for future in as_completed(future_to_talk):
                talk = future_to_talk[future]
                talk_key = get_talk_key(talk, year, month)
                url = future.result()
                pbar.update(1)
                
                if url:
                    output_resources[conference_key][talk_key] = {
                        **output_resources[conference_key].get(talk_key, {}),
                        "Church News Summary": url
                    }
    
    # Handle misses interactively
    misses = []
    for talk in talks_to_process:
        talk_key = get_talk_key(talk, year, month)
        if "Church News Summary" not in output_resources[conference_key].get(talk_key, {}):
            misses.append((talk, talk_key))
    
    for talk, talk_key in misses:
        user_input = input(f"No newsroom summary found for \"{talk['title']}\". Enter manual URL or press Enter to skip: ").strip()
        if user_input:
            output_resources[conference_key][talk_key] = {
                **output_resources[conference_key].get(talk_key, {}),
                "Church News Summary": user_input
            }

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Add newsroom summaries to conference_resources.json')
    parser.add_argument('json_file', help='Path to the input JSON file')
    parser.add_argument('--talk', help='Title of the single talk to process')
    parser.add_argument('--manual', help='Manual URL to add for the talk (if --talk is specified)')
    args = parser.parse_args()
    
    try:
        with open(args.json_file, 'r') as f:
            conference_data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}")
        sys.exit(1)
    
    # Load existing conference_resources.json or create new
    output_file = 'conference_resources.json'
    try:
        with open(output_file, 'r') as f:
            output_resources = json.load(f)
    except FileNotFoundError:
        output_resources = {}
    
    if args.talk:
        process_single_talk(conference_data, args.talk, args.manual, output_resources)
    else:
        process_all_talks(conference_data, output_resources)
    
    # Save to conference_resources.json
    with open(output_file, 'w') as f:
        json.dump(output_resources, f, indent=2)
    print(f"Updated resources saved to {output_file}")
    
    # Show example of output structure
    year = conference_data.get('year')
    month = conference_data.get('month')
    conference_key = f"{year}-{month.replace(' ', '-').title()}"
    if conference_key in output_resources and output_resources[conference_key]:
        print(f"\nExample output structure for {conference_key}:")
        first_key = next(iter(output_resources[conference_key]))
        print(f"  {first_key}: {{ \"Church News Summary\": \"{output_resources[conference_key][first_key].get('Church News Summary', 'not found')}\" }}")