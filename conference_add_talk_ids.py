"""
Add talk_id property to conference JSON files for sortable talk ordering.

Format: YYYY-MM-SS-TT where:
  YYYY = year
  MM = month code (04 or 10)
  SS = zero-padded session number (01, 02, ...)
  TT = zero-padded talk number within session (01, 02, ...)

Usage:
  python3 conference_add_talk_ids.py 1971-2025    # all conferences
  python3 conference_add_talk_ids.py 2025-10      # single conference
  python3 conference_add_talk_ids.py 2025-October  # single conference by name
"""

import os
import json
import sys
import re
from tqdm import tqdm

JSON_DIR = 'conference_json'


def get_conference_filename(year, month):
    sanitized_conference = re.sub(r'[^a-z0-9\- ]', '', f"{year}-{month.lower()}", flags=re.IGNORECASE)
    return os.path.join(JSON_DIR, f"{sanitized_conference}.json")


def parse_target(target):
    """Parse a target string into a list of (year, month) tuples."""
    conferences = []

    match = re.match(r'^(\d{4})-(april|october|apr|oct)$', target, re.IGNORECASE)
    if match:
        year = int(match.group(1))
        month = match.group(2)
        conferences.append((year, 'April' if month.lower() in ['apr', 'april'] else 'October'))
        return conferences

    match = re.match(r'^(\d{4})-(04|10)$', target)
    if match:
        year = int(match.group(1))
        month = 'April' if match.group(2) == '04' else 'October'
        conferences.append((year, month))
        return conferences

    match = re.match(r'^(\d{4})-(\d{4})$', target)
    if match:
        for year in range(int(match.group(1)), int(match.group(2)) + 1):
            conferences.append((year, 'April'))
            conferences.append((year, 'October'))
        return conferences

    match = re.match(r'^(\d{4})$', target)
    if match:
        year = int(match.group(1))
        conferences.append((year, 'April'))
        conferences.append((year, 'October'))
        return conferences

    return conferences


def add_talk_ids(year, month, pbar=None):
    """Add talk_id to each talk in a conference JSON file."""
    month_code = '04' if month.lower() == 'april' else '10'
    file_path = get_conference_filename(year, month)

    if not os.path.exists(file_path):
        if pbar:
            pbar.write(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as f:
        data = json.load(f)

    talk_count = 0
    for session_num, (session_name, session_data) in enumerate(data.get('sessions', {}).items(), start=1):
        talks = session_data.get('talks', {})
        for talk_num, (talk_key, talk) in enumerate(talks.items(), start=1):
            talk['talk_id'] = f"{year}-{month_code}-{session_num:02d}-{talk_num:02d}"
            talk_count += 1

    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    if pbar:
        pbar.write(f"Added {talk_count} talk_ids: {file_path}")


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 conference_add_talk_ids.py <target>")
        print("  target: YYYY-YYYY, YYYY-MM, YYYY-Month, or YYYY")
        sys.exit(1)

    conferences = parse_target(sys.argv[1])
    if not conferences:
        print(f"Invalid target: {sys.argv[1]}")
        sys.exit(1)

    pbar = tqdm(conferences, desc="Adding talk IDs")
    for year, month in pbar:
        pbar.set_description(f"{year} {month}")
        add_talk_ids(year, month, pbar)
