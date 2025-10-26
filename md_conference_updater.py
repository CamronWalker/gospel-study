import json
import os
import re
import argparse
from glob import glob

def sanitize_filename(title):
    # Remove invalid filename characters
    return re.sub(r'[\/:*?"<>|]', '', title).strip()

def get_month_abbr(month):
    month_map = {
        'april': 'Apr',
        'october': 'Oct'
    }
    return month_map.get(month.lower(), month)

def get_last_name(speaker):
    parts = speaker.split()
    return parts[-1] if parts else ''

def format_wikilink(speaker):
    # Add period if middle initial
    parts = speaker.split()
    if len(parts) > 2 and len(parts[1]) == 1:
        return f"[[{parts[0]} {parts[1]}. {' '.join(parts[2:])}]]"
    return f"[[{speaker}]]"

def escape_brackets(md):
    def repl(match):
        content = match.group(1)
        if content.startswith('^') or content.startswith('[') or '|' in content or '(' in content:
            return match.group(0)
        else:
            return '\\[' + content + '\\]'

    # Match [content] not followed by (
    pattern = r'\[([^\]]+)\](?!\()'
    md = re.sub(pattern, repl, md)
    return md

def build_frontmatter(talk):
    frontmatter = [
        '---',
        'publish: true',
        f'conference: "{talk["conference"]}"',
        f'year: {talk["year"]}',
        f'month: "{talk["month"].capitalize()}"',  # Full month name
        f'session: "{talk["session"]}"',
        f'speaker: "{format_wikilink(talk["speaker"])}"',
        f'speaker-role: "{talk["speaker_role"]}"',
        f'title: "{talk["title"]}"',
        f'thumbnail: "{talk["thumbnail"]}"',
    ]
    if talk.get('kicker'):
        frontmatter.append(f'kicker: "{talk["kicker"]}"')
    frontmatter.append('cssclasses: "conference"')
    frontmatter.append('---')
    return '\n'.join(frontmatter)

def build_properties(talk):
    properties = ['> [!properties]- Talk Details']
    properties.append(f'Session: {talk["session"]}')
    properties.append(f'URL: {talk["url"]}')
    properties.append('Resources:')
    for res in talk.get('resources', []):
        properties.append(f'- [{res["name"]}]({res["url"]})')
    return '\n'.join(properties) + '\n'

def build_ai_summary(talk):
    if 'ai_summary' in talk:
        return '> [!ai]- AI Summary\n' + talk['ai_summary'] + '\n'
    return ''

def build_invitation(talk):
    if 'invitation' in talk:
        return '> [!invite]- Invitations\n' + talk['invitation'] + '\n'
    return ''

def find_youtube_url(resources):
    for res in resources:
        if res['name'] == 'YouTube Video':
            return res['url']
    return None

def build_talk_body(talk):
    youtube_url = find_youtube_url(talk.get('resources', []))
    body = ['# Talk']
    if youtube_url:
        body.append(f'![]({youtube_url})')
    if talk.get('kicker'):
        body.append(talk['kicker'])
    body.append('')  # blank line

    # Build body from body list
    for item in talk.get('body', []):
        if not isinstance(item, dict):
            continue
        if 'markdown' not in item:
            body.append('')
            continue
        md = escape_brackets(item['markdown'])
        if 'type' in item and item['type'] == 'heading':
            level = item.get('level', 2) + 2  # Add two heading levels
            body.append('#' * level + ' ' + md)
        elif 'type' in item and item['type'] == 'paragraph':
            if 'verse' in item:
                body.append(f"###### {item['verse']}")
                body.append(f"{item['verse']} {md}")
            else:
                body.append(md)
        else:
            body.append(md)  # other types without heading
        body.append('')  # blank line after each

    # Footnotes
    sources = talk.get('sources', [])
    if sources:
        for src in sources:
            num = src['number']
            id_ = src['id']
            md = escape_brackets(src['markdown'])
            body.append(f'[^{id_}]: {md}')

    return '\n'.join(body) + '\n'

def build_full_md(talk):
    md = build_frontmatter(talk) + '\n\n'
    md += build_properties(talk) + '\n'
    md += build_ai_summary(talk) + '\n'
    md += build_invitation(talk) + '\n'
    md += '# Notes\n\n\n\n'
    md += build_talk_body(talk)
    return md

def update_md_prefix(existing_md, talk):
    # Find the position of '# Notes'
    notes_pos = existing_md.find('# Notes')
    if notes_pos == -1:
        # If no # Notes, full replace
        return build_full_md(talk)
    
    # Everything after # Notes inclusive
    remaining_content = existing_md[notes_pos:]
    
    # Generate new prefix
    new_prefix = build_frontmatter(talk) + '\n\n'
    new_prefix += build_properties(talk) + '\n'
    new_prefix += build_ai_summary(talk) + '\n'
    new_prefix += build_invitation(talk) + '\n'
    
    # Combine
    return new_prefix + remaining_content

def process_conference(json_file, replace=False):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    year = str(data['year'])
    month = data['month'].lower()
    conf_folder = f"Conference/{year}-{get_month_abbr(month)}"
    os.makedirs(conf_folder, exist_ok=True)

    for session_name, talks in data['sessions'].items():
        for talk in talks:
            talk['conference'] = data['conference']
            talk['year'] = data['year']
            talk['month'] = data['month']
            talk['session'] = session_name

            title = talk['title']
            speaker_last = get_last_name(talk['speaker'])
            mon_abbr = get_month_abbr(month)
            filename = f"{sanitize_filename(title)} — {speaker_last} {year} {mon_abbr}.md"
            filepath = os.path.join(conf_folder, filename)

            if replace or not os.path.exists(filepath):
                md_content = build_full_md(talk)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(md_content)
            else:
                with open(filepath, 'r', encoding='utf-8') as f:
                    existing = f.read()
                updated = update_md_prefix(existing, talk)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(updated)

def main():
    parser = argparse.ArgumentParser(description="Process General Conference JSON files.")
    parser.add_argument('--replace', action='store_true', help='Replace existing files fully.')
    parser.add_argument('args', nargs='*', help='Year and optional Month (April or October).')

    args = parser.parse_args()
    replace = args.replace
    params = args.args

    json_dir = 'conference_json'
    all_files = glob(os.path.join(json_dir, '*.json'))

    if not params:
        # Process all
        for json_file in all_files:
            process_conference(json_file, replace)
    elif len(params) == 1:
        # Year, process both April and October
        year = params[0]
        for month in ['april', 'october']:
            json_file = os.path.join(json_dir, f"{year}-{month}.json")
            if os.path.exists(json_file):
                process_conference(json_file, replace)
    elif len(params) == 2:
        # Year and Month
        year, month = params
        month = month.lower()
        json_file = os.path.join(json_dir, f"{year}-{month}.json")
        if os.path.exists(json_file):
            process_conference(json_file, replace)
    else:
        print("Invalid arguments. Use: [--replace] [year [month]]")

if __name__ == '__main__':
    main()