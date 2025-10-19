import requests
from bs4 import BeautifulSoup
import json
import re
from urllib.parse import urljoin, quote
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Function to create a session with retries
def create_session():
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

session = create_session()

def fetch_page(url):
    response = session.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.text, 'html.parser')

def extract_sessions(soup):
    sessions = {}
    current_session = None
    for element in soup.find_all(['h2', 'li']):
        if element.name == 'h2':
            current_session = element.text.strip()
            sessions[current_session] = []
        elif current_session and 'class' in element.attrs and 'talk' in element.attrs['class']:
            a = element.find('a')
            if a:
                talk = {
                    'session': current_session,
                    'url': urljoin("https://www.churchofjesuschrist.org", a['href']),
                    'title': a.find('span', class_='title').text.strip() if a.find('span', class_='title') else '',
                    'speaker': a.find('span', class_='speaker').text.strip() if a.find('span', class_='speaker') else ''
                }
                sessions[current_session].append(talk)
    return sessions

def fetch_talk_details(talk_url):
    soup = fetch_page(talk_url)
    
    # Extract thumbnail
    thumbnail_img = soup.find('img', attrs={'srcset': re.compile(r'.*')})
    thumbnail_url = ''
    if thumbnail_img and 'srcset' in thumbnail_img.attrs:
        # Parse srcset to get a URL
        srcset = thumbnail_img['srcset'].split(',')
        thumbnail_url = srcset[0].strip().split(' ')[0] if srcset else ''
    
    # Extract speaker_role, subtitle, kicker
    speaker_role = soup.find('p', class_='author-role').text.strip() if soup.find('p', class_='author-role') else ''
    subtitle = soup.find('p', class_='subtitle').text.strip() if soup.find('p', class_='subtitle') else None
    kicker = soup.find('p', class_='kicker').text.strip() if soup.find('p', class_='kicker') else None
    
    # Extract full_markdown and body
    body_content = soup.find('div', class_='body-block')
    full_markdown = ''
    body = []
    verse = 1
    if body_content:
        for child in body_content.children:
            if child.name == 'p':
                md = child.text.strip()
                if md:
                    full_markdown += md + '\n\n'
                    body.append({
                        'verse': verse,
                        'type': 'paragraph',
                        'markdown': md
                    })
                    verse += 1
            elif child.name in ['h2', 'h3', 'h4']:
                md = child.text.strip()
                if md:
                    level = int(child.name[1])
                    full_markdown += '#' * level + ' ' + md + '\n\n'
                    body.append({
                        'type': 'heading',
                        'level': level,
                        'markdown': md
                    })
                    # In example, headings have verse sometimes, but adjust as needed
                    verse += 1
    
    # Extract sources (footnotes)
    sources = []
    notes_section = soup.find('div', id='notesSection')
    if notes_section:
        for i, note in enumerate(notes_section.find_all('li'), start=1):
            md = note.text.strip()
            # Remove number if present
            md = re.sub(r'^\d+\.\s*', '', md)
            sources.append({
                'number': i,
                'id': f'note{i}',
                'markdown': md
            })
    
    return {
        'speaker_role': speaker_role,
        'thumbnail': thumbnail_url,
        'subtitle': subtitle,
        'kicker': kicker,
        'full_markdown': full_markdown.strip(),
        'body': body,
        'sources': sources
    }

# Resource functions - add new ones here
def get_gospel_library(talk):
    return {"name": "Gospel Library", "url": talk['url']}

def get_saints_ai(talk):
    talk_id = talk['url'].split('/')[-1].split('?')[0]
    year = talk['url'].split('/')[-4]
    month_short = talk['url'].split('/')[-3]
    return {"name": "Saints AI Study Guide", "url": f"https://saintsai.org/study/general-conference/{year}/{month_short}/{talk_id}/study-guide"}

def get_byu_citation(talk):
    # Example construction; adjust if needed
    title_quoted = quote(talk['title'])
    return {"name": "BYU Citation Index", "url": f"https://scriptures.byu.edu/#::t{title_quoted}"}

def get_youtube(talk):
    # To get actual YouTube, would need search; placeholder with search link
    query = quote(f"general conference {talk['title']} {talk['speaker']} youtube")
    search_url = f"https://www.youtube.com/results?search_query={query}"
    # In real script, parse to find video url; for now, return search
    return {"name": "YouTube Video", "url": search_url}

def get_church_news(talk):
    query = quote(f"church news summary {talk['title']} {talk['speaker']}")
    search_url = f"https://www.thechurchnews.com/search?q={query}"
    return {"name": "Church News Summary", "url": search_url}

# List of resource functions; add new funcs to this list
resource_functions = [
    get_gospel_library,
    get_saints_ai,
    get_byu_citation,
    get_youtube,
    get_church_news
]

def add_resources(talk):
    resources = []
    for func in resource_functions:
        try:
            res = func(talk)
            if res:
                resources.append(res)
        except Exception:
            pass  # Skip if error
    talk['resources'] = resources

def scrape_conference(year, month):
    month_short = '04' if month.lower() == 'april' else '10'
    conference_url = f"https://www.churchofjesuschrist.org/study/general-conference/{year}/{month_short}?lang=eng"
    soup = fetch_page(conference_url)
    sessions = extract_sessions(soup)
    
    conference_data = {
        "conference": f"{year}-{month.capitalize()}",
        "year": year,
        "month": month.capitalize(),
        "sessions": sessions
    }
    
    for session_name, talks in sessions.items():
        for talk in talks:
            details = fetch_talk_details(talk['url'])
            talk.update(details)
            add_resources(talk)
    
    output_file = f"{year}-{month.lower()}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(conference_data, f, indent=2, ensure_ascii=False)
    
    print(f"Output saved to {output_file}")

# Usage: python scrape_conference.py 2024 October
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python scrape_conference.py <year> <month>")
        sys.exit(1)
    year, month = sys.argv[1], sys.argv[2]
    scrape_conference(year, month)