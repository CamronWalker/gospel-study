import re

def normalize_speaker(speaker):
    speaker = re.sub(r'By\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'^(Elder|President|Sister|Brother|Bishop)\s+', '', speaker, flags=re.IGNORECASE)
    speaker = re.sub(r'[^a-zA-Z0-9\s]', '', speaker)
    return speaker.strip()

def get_uniform_talk_key(title):
    """Generate uniform key from talk title: strip, lowercase, remove symbols and unicode."""
    return re.sub(r'[^a-zA-Z0-9\s]', '', title.strip()).lower()

def get_author_title_key(title, speaker):
    """Generate author|title key."""
    norm_speaker = normalize_speaker(speaker).lower()
    norm_title = get_uniform_talk_key(title)
    return f"{norm_speaker}|{norm_title}"

def get_url_key(author, title, year=None, month=None):
    """Lookup URL key based on author and title."""
    import json
    import os
    normalized_author = normalize_speaker(author)
    normalized_title = get_uniform_talk_key(title)
    filename = os.path.join('conference_json', 'conference_resources.json')
    if not os.path.exists(filename):
        return None
    with open(filename, 'r') as f:
        data = json.load(f)
    for conf_key, talks in data.items():
        if year and month:
            conf_year, conf_month = conf_key.split('-')
            if int(conf_year) != year or conf_month.lower() != month.lower():
                continue
        for url_key, info in talks.items():
            if info.get('author') == normalized_author and info.get('title') == normalized_title:
                return url_key
    return None