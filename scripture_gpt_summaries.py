# This script generates AI-powered summaries for chapters in LDS scripture JSON files using the xAI SDK.
# It supports updating entire volumes, specific books, or individual chapters.
# Requires an XAI_API_KEY environment variable set in a .env file.
# JSON files are expected in the 'lds_scriptures_json' directory.

# Usage Examples:
# 1. Update an entire volume:
#    python3 gpt_summaries.py --update new_testament.json
#
# 2. Update all chapters in a specific book:
#    python3 gpt_summaries.py --update Matthew
#
# 3. Update a specific chapter in a book:
#    python3 gpt_summaries.py --update "Matthew 5"
#
# 4. Update with debug logging enabled:
#    python3 gpt_summaries.py --update "Matthew 5" --debug
#
# Note: For books with aliases like "D&C", it will automatically map to "Doctrine and Covenants".
# Ensure the JSON files (e.g., new_testament.json) contain the structured data with books, chapters, and verses.

import os
import re
import json
import time
from dotenv import load_dotenv
import argparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from xai_sdk import Client
from xai_sdk.chat import user
from xai_sdk.search import SearchParameters, web_source
from urllib.parse import urlparse

# Load environment variables from .env file
load_dotenv()

# Set your xAI API key (retrieve from environment variable for security)
api_key = os.getenv('XAI_API_KEY')
client = Client(api_key=api_key)

# List of volume files
volumes = [
    "old_testament.json",
    "new_testament.json",
    "book_of_mormon.json",
    "doctrine_and_covenants.json",
    "pearl_of_great_price.json"
]

# Book aliases
book_aliases = {
    "D&C": "Doctrine and Covenants"
}

def get_domain(url):
    parsed = urlparse(url)
    domain = parsed.netloc
    if domain.startswith("www."):
        domain = domain[4:]
    return domain

def parse_related_scriptures(s):
    import re
    s = s.strip("[] ")
    parts = s.split(';')
    result = []
    for part in parts:
        part = part.strip()
        if part:
            splitted = re.split(r'\s*~\s*', part, maxsplit=1)
            if len(splitted) == 2:
                link = splitted[0].strip()
                desc = splitted[1].strip()
                result.append({"link": link, "description": desc})
    return result

# Function to generate AI summaries using xAI SDK
def generate_ai_summaries(book, chapter, verses, allowed_websites=None, debug=False):
    if allowed_websites is None:
        allowed_websites = []
    if not allowed_websites:
        allowed_websites = ["churchofjesuschrist.org", "scriptures.byu.edu"]
    
    # Concatenate verses into a single text block
    chapter_text = "\n".join([f"Verse {verse}: {text}" for verse, text in sorted(verses.items(), key=lambda x: int(x[0]))])
    
    # Prompt for three summaries
    prompt = (
        f"Provide the following for '{book} {chapter}' from the scriptures:\n"
        f"1. Child Summary: A simple summary (1-2 sentences, max 50 words) as if explaining to a young child.\n"
        f"2. Normal Summary: A detailed summary (2-3 sentences, max 100 words) capturing the main events, teachings, and themes. Could include references wiki-link references to verses from the same chapter (e.g., [[{book} {chapter}#1|v1-10]] — description of event or theme) \n"
        f"3. Context Summary: A brief summary (1 sentence) including speaker, location, audience, and context.\n"
        f"4. Tags: Provide 1-3 doctrinal tags related to The Church of Jesus Christ of Latter-day Saints, starting with #Gospel/ (e.g., #Gospel/Atonement #Gospel/Faith #Gospel/EndureToTheEnd), separated by spaces.\n"
        f"5. Related Scriptures: Search the internet for 1-3 related scriptures (chapters or specific verses) from other religious texts with similar themes, teachings, or events. For each, provide a wiki-style link and a brief (max 25 words). Compare and contrast the reason for its relevance. Prioritize referencing to an entire chapter or shorter verse ranges and avoid ranges of greater than 5 verses.\n"
        f"Notes:\n"
        f"- Do not start summaries with redundant references to the Book / Chapter name (e.g., 'In {book} {chapter}' or 'In this chapter').\n"
        f"- Use wiki-style links in Obsidian format (e.g., [[John 1]] or [[1 Nephi 1#5|1 Nephi 1:5-8]][[1 Nephi 1#6|]][[1 Nephi 1#7|]][[1 Nephi 1#8|]]) for specific verses or ranges, displaying the range in the first link and linking subsequent verses without display text.\n"
        f"- When linking an entire chapter, do not use verse reference style. Include only a link to the file (e.g., [[Alma 5]])./n"
        f"- When linking a verse range, reference the first verse and include the range in the description, then reference each verse in the range as a link with an empty string as the description(e.g., [[Genesis 5#2|Genesis 5:2-4]][[Genesis 5#3|]][[Genesis 5#4|]])./n"
        f"- For Doctrine and Covenants, use [[D&C 1#5]] instead of spelling out 'Doctrine and Covenants'.\n"
        f"- Only include the chapter reference (e.g., '[[{book} {chapter}]]') in the output, not the full chapter text.\n"
        f"Chapter Reference: {book} {chapter}\n\n"
        f"Output Format:\n"
        f"Child Summary: [child summary here]\n"
        f"Normal Summary: [normal summary here]\n"
        f"Context Summary: [context summary here]\n"
        f"Tags: [tags here]\n"
        f"Related Scriptures: [wikilink1 ~ Brief reason; wikilink2 ~ Brief reason; ...]\n"
    )
    
    if debug:
        print(f"Debug: Prompt for {book} {chapter}:\n{prompt}\n")
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            chat = client.chat.create(
                model="grok-4",
                temperature=0.7,
                max_tokens=4096,
                search_parameters=SearchParameters(
                    mode="auto",
                    max_search_results=5,
                    sources=[web_source(allowed_websites=allowed_websites)]
                )
            )
            chat.append(user(prompt))
            response = chat.sample()
            
            if debug:
                print(f"Debug: Reasoning Content for {book} {chapter}:\n{response.reasoning_content}\n")
                print(f"Debug: Full response for {book} {chapter}:\n{response}\n")
                if hasattr(response, 'citations'):
                    print(f"Debug: Citations: {response.citations}\n")
            
            output = response.content.strip()
            
            if debug:
                print(f"Debug: Raw output for {book} {chapter}:\n{output}\n")
            
            # Better parsing to handle multi-line summaries
            lines = output.split("\n")
            current = None
            child_summary_lines = []
            normal_summary_lines = []
            context_summary_lines = []
            tags_lines = []
            related_scriptures_lines = []
            
            for line in lines:
                if line.startswith("Child Summary:"):
                    current = child_summary_lines
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current.append(content)
                elif line.startswith("Normal Summary:"):
                    current = normal_summary_lines
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current.append(content)
                elif line.startswith("Context Summary:"):
                    current = context_summary_lines
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current.append(content)
                elif line.startswith("Tags:"):
                    current = tags_lines
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current.append(content)
                elif line.startswith("Related Scriptures:"):
                    current = related_scriptures_lines
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current.append(content)
                else:
                    if current is not None and line.strip():
                        current.append(line.strip())
            
            child_summary = " ".join(child_summary_lines)
            normal_summary = " ".join(normal_summary_lines)
            context_summary = " ".join(context_summary_lines)
            tags_str = " ".join(tags_lines)
            related_scriptures_str = " ".join(related_scriptures_lines)
            
            prompt_tokens = response.usage.prompt_tokens if hasattr(response.usage, 'prompt_tokens') else 0
            completion_tokens = response.usage.completion_tokens
            reasoning_tokens = response.usage.reasoning_tokens if hasattr(response.usage, 'reasoning_tokens') else 0
            searches = response.usage.num_sources_used if hasattr(response.usage, 'num_sources_used') else 0
            
            if debug:
                print(f"Debug: Parsed summaries for {book} {chapter}:")
                print(f"Child: {child_summary}")
                print(f"Normal: {normal_summary}")
                print(f"Context: {context_summary}")
                print(f"Tags: {tags_str}")
                print(f"Related Scriptures Str: {related_scriptures_str}\n")
                print(f"Input Tokens: {prompt_tokens}")
                print(f"Completion Tokens: {completion_tokens}")
                print(f"Reasoning Tokens: {reasoning_tokens}")
                print(f"Searches: {searches}\n")
            
            return child_summary, normal_summary, context_summary, tags_str, related_scriptures_str, prompt_tokens, completion_tokens, reasoning_tokens, searches
        except Exception as e:
            wait_time = 2 ** attempt
            print(f"Error generating summary for {book} {chapter}: {e}. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)
    print(f"Max retries exceeded for {book} {chapter}")
    return "", "", "", "", "", 0, 0, 0, 0

# Helper to find book in list of books
def find_book(books, book_name):
    for b in books:
        if b["name"].lower() == book_name.lower():
            return b
    return None

# Helper to find chapter in list of chapters
def find_chapter(chapters, chapter_num):
    for c in chapters:
        if str(c["number"]) == chapter_num:
            return c
    return None

# Function to update a specific chapter in the JSON
def update_chapter(data, volume_name, books, book_name, chapter_num, file_path, debug=False):
    book = find_book(books, book_name)
    if book:
        chapter = find_chapter(book["chapters"], chapter_num)
        if chapter:
            print(f"Processing {book_name} {chapter_num}")
            resources = chapter.get("chapter_resources", [])
            allowed_websites = list(set(get_domain(r["url"]) for r in resources if "url" in r))
            verses_list = chapter.get("verses", [])
            verses = {str(v["number"]): v["text"] for v in verses_list}
            if verses:
                child, normal, context, tags, related_str, in_tokens, comp_tokens, reas_tokens, searches = generate_ai_summaries(book_name, chapter_num, verses, allowed_websites=allowed_websites, debug=debug)
                related_list = parse_related_scriptures(related_str)
                if "ai_resources" not in chapter:
                    chapter["ai_resources"] = {}
                chapter["ai_resources"]["child_summary"] = child
                chapter["ai_resources"]["summary"] = normal
                chapter["ai_resources"]["context_summary"] = context
                chapter["ai_resources"]["tags"] = tags
                chapter["ai_resources"]["related_scriptures"] = related_list
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)
                print(f"Sum for this operation - Input Tokens: {in_tokens}, Completion Tokens: {comp_tokens}, Reasoning Tokens: {reas_tokens}, Searches: {searches}")
                return True
    return False

# Function to update all chapters in a book
def update_book(data, volume_name, books, book_name, file_path, debug=False):
    book = find_book(books, book_name)
    if book:
        chapter_tasks = [(book_name, chapter) for chapter in book["chapters"]]
        total_chapters = len(chapter_tasks)
        total_input = 0
        total_completion = 0
        total_reasoning = 0
        total_searches = 0

        def process_chapter(task):
            bk_name, bk_chapter = task
            chapter_num = str(bk_chapter["number"])
            resources = bk_chapter.get("chapter_resources", [])
            allowed_websites = list(set(get_domain(r["url"]) for r in resources if "url" in r))
            verses_list = bk_chapter.get("verses", [])
            verses = {str(v["number"]): v["text"] for v in verses_list}
            if verses:
                return bk_chapter, generate_ai_summaries(bk_name, chapter_num, verses, allowed_websites=allowed_websites, debug=debug)
            return bk_chapter, None

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(process_chapter, task) for task in chapter_tasks]
            for future in tqdm(as_completed(futures), total=total_chapters, desc=f"Processing {book_name}", unit="chapter"):
                chapter, result = future.result()
                if result:
                    child, normal, context, tags, related_str, in_tokens, comp_tokens, reas_tokens, searches = result
                    related_list = parse_related_scriptures(related_str)
                    if "ai_resources" not in chapter:
                        chapter["ai_resources"] = {}
                    chapter["ai_resources"]["child_summary"] = child
                    chapter["ai_resources"]["summary"] = normal
                    chapter["ai_resources"]["context_summary"] = context
                    chapter["ai_resources"]["tags"] = tags
                    chapter["ai_resources"]["related_scriptures"] = related_list
                    total_input += in_tokens
                    total_completion += comp_tokens
                    total_reasoning += reas_tokens
                    total_searches += searches

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"Sum for the batch - Input Tokens: {total_input}, Completion Tokens: {total_completion}, Reasoning Tokens: {total_reasoning}, Searches: {total_searches}")
        return True
    return False

# Function to update all books in a volume
def update_volume(file_path, debug=False):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    volume_name = list(data.keys())[0]
    books = data[volume_name]
    chapter_tasks = []
    for book in books:
        book_name = book["name"]
        for chapter in book["chapters"]:
            chapter_tasks.append((book_name, chapter))
    total_chapters = len(chapter_tasks)
    total_input = 0
    total_completion = 0
    total_reasoning = 0
    total_searches = 0

    def process_chapter(task):
        bk_name, bk_chapter = task
        chapter_num = str(bk_chapter["number"])
        resources = bk_chapter.get("chapter_resources", [])
        allowed_websites = list(set(get_domain(r["url"]) for r in resources if "url" in r))
        verses_list = bk_chapter.get("verses", [])
        verses = {str(v["number"]): v["text"] for v in verses_list}
        if verses:
            return bk_chapter, generate_ai_summaries(bk_name, chapter_num, verses, allowed_websites=allowed_websites, debug=debug)
        return bk_chapter, None

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_chapter, task) for task in chapter_tasks]
        for future in tqdm(as_completed(futures), total=total_chapters, desc=f"Processing {volume_name}", unit="chapter"):
            chapter, result = future.result()
            if result:
                child, normal, context, tags, related_str, in_tokens, comp_tokens, reas_tokens, searches = result
                related_list = parse_related_scriptures(related_str)
                if "ai_resources" not in chapter:
                    chapter["ai_resources"] = {}
                chapter["ai_resources"]["child_summary"] = child
                chapter["ai_resources"]["summary"] = normal
                chapter["ai_resources"]["context_summary"] = context
                chapter["ai_resources"]["tags"] = tags
                chapter["ai_resources"]["related_scriptures"] = related_list
                total_input += in_tokens
                total_completion += comp_tokens
                total_reasoning += reas_tokens
                total_searches += searches

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"Sum for the batch - Input Tokens: {total_input}, Completion Tokens: {total_completion}, Reasoning Tokens: {total_reasoning}, Searches: {total_searches}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update AI summaries in JSON scripture files.")
    parser.add_argument("--update", required=True, help="Volume file (e.g., new_testament.json), book (e.g., Matthew), or book chapter (e.g., Matthew 5)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    target = args.update.strip()
    processed = False
    debug = args.debug

    if target.endswith(".json"):
        # Update entire volume
        file_path = os.path.join("lds_scriptures_json", target)
        if os.path.exists(file_path):
            update_volume(file_path, debug=debug)
            processed = True
        else:
            print(f"Volume file {target} not found.")
    else:
        parts = target.split()
        is_chapter_update = len(parts) > 1 and re.match(r'^\d+$', parts[-1])
        if is_chapter_update:
            chapter_num = parts[-1]
            book_input = ' '.join(parts[:-1])
        else:
            book_input = target
            chapter_num = None

        book_name = book_aliases.get(book_input, book_input)

        for vol_file in volumes:
            file_path = os.path.join("lds_scriptures_json", vol_file)
            if not os.path.exists(file_path):
                continue
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            volume_name = list(data.keys())[0]
            books = data[volume_name]

            if chapter_num is not None:
                # Update specific chapter
                if update_chapter(data, volume_name, books, book_name, chapter_num, file_path, debug=debug):
                    processed = True
                    break
            else:
                # Update entire book
                if update_book(data, volume_name, books, book_name, file_path, debug=debug):
                    processed = True
                    break

    if processed:
        print("Summaries have been added to the scripture files successfully.")
    else:
        print("No updates performed.")