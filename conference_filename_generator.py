import json
import os
import argparse

def generate_filename(title, speaker, year, month):
    lastname = speaker.split()[-1]
    month_short = month[:3]
    filename = f"{title} — {lastname} {year} {month_short}"
    return filename

def process_conference_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    year = data['year']
    month = data['month']
    
    for session_name, session_data in data['sessions'].items():
        if 'talks' in session_data:
            for talk_key, talk in session_data['talks'].items():
                title = talk['title']
                speaker = talk['speaker']
                filename = generate_filename(title, speaker, year, month)
                talk['filename'] = filename
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def main():
    parser = argparse.ArgumentParser(description='Add filename keys to conference JSON files.')
    parser.add_argument('--year', type=int, help='Optional: Process only files for this year (e.g., 2025)')
    args = parser.parse_args()

    conference_json_dir = 'conference_json'
    json_files = [f for f in os.listdir(conference_json_dir) if f.endswith('.json')]
    json_files.sort()  # Sort to process in order

    if args.year:
        json_files = [f for f in json_files if f.startswith(str(args.year))]
        if not json_files:
            print(f"No files found for year {args.year}")
            return

    for filename in json_files:
        file_path = os.path.join(conference_json_dir, filename)
        print(f"Processing {filename}")
        process_conference_json(file_path)
    print(f"Processed {len(json_files)} file(s).")

if __name__ == "__main__":
    main()