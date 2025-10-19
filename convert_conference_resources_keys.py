"""
Convert conference_resources.json keys from URL-style to lowercase title.
"""

import os
import json
import re

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother)\s+', '', speaker, flags=re.IGNORECASE)
    return speaker.strip()

def get_talk_key_from_url(url):
    """Extract talk key from URL, e.g., /2024/04/14dennis -> 2024/04/14dennis"""
    match = re.search(r'/study/general-conference/(\d{4}/\d{2}/\d{2}\w+)', url)
    return match.group(1) if match else None

def main():
    # Load conference_resources.json
    with open('conference_json/conference_resources.json', 'r') as f:
        resources = json.load(f)

    # Load all conference JSON files to get titles
    title_map = {}
    for conf_key in resources:
        year, month = conf_key.split('-')
        month = month.lower()
        json_file = f'conference_json/{year}-{month}.json'
        if os.path.exists(json_file):
            with open(json_file, 'r') as f:
                conf_data = json.load(f)
            for session in conf_data['sessions'].values():
                for talk in session:
                    url = talk['url']
                    talk_key = get_talk_key_from_url(url)
                    title = talk['title'].strip().lower()
                    if talk_key:
                        title_map[talk_key] = title

    # Convert resources
    new_resources = {}
    for conf_key, talks in resources.items():
        new_talks = {}
        for talk_key, res in talks.items():
            title = title_map.get(talk_key, talk_key)  # fallback to talk_key if not found
            new_talks[title] = res
        new_resources[conf_key] = new_talks

    # Save back
    with open('conference_json/conference_resources.json', 'w') as f:
        json.dump(new_resources, f, indent=2)

    print("Converted keys to lowercase titles.")

if __name__ == '__main__':
    main()