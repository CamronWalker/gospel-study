"""
This script processes JSON files containing scripture data and updates corresponding markdown files
with resource links and AI-generated summaries. It ensures that existing verse content like footnotes or highlights are preserved.
"""

import os
import json
import re

# Define the lists of books for each category in their standard order
ot_books = [
    "Genesis", "Exodus", "Leviticus", "Numbers", "Deuteronomy", "Joshua", "Judges", "Ruth",
    "1 Samuel", "2 Samuel", "1 Kings", "2 Kings", "1 Chronicles", "2 Chronicles", "Ezra",
    "Nehemiah", "Esther", "Job", "Psalms", "Proverbs", "Ecclesiastes", "Song of Solomon",
    "Isaiah", "Jeremiah", "Lamentations", "Ezekiel", "Daniel", "Hosea", "Joel", "Amos",
    "Obadiah", "Jonah", "Micah", "Nahum", "Habakkuk", "Zephaniah", "Haggai", "Zechariah",
    "Malachi"
]
nt_books = [
    "Matthew", "Mark", "Luke", "John", "Acts", "Romans", "1 Corinthians", "2 Corinthians",
    "Galatians", "Ephesians", "Philippians", "Colossians", "1 Thessalonians", "2 Thessalonians",
    "1 Timothy", "2 Timothy", "Titus", "Philemon", "Hebrews", "James", "1 Peter", "2 Peter",
    "1 John", "2 John", "3 John", "Jude", "Revelation"
]
bom_books = [
    "1 Nephi", "2 Nephi", "Jacob", "Enos", "Jarom", "Omni", "Words of Mormon", "Mosiah",
    "Alma", "Helaman", "3 Nephi", "4 Nephi", "Mormon", "Ether", "Moroni"
]
pogp_books = [
    "Moses", "Abraham", "Joseph Smith--Matthew", "Joseph Smith--History", "Articles of Faith"
]
dc_books = [
    "Doctrine and Covenants", "Official Declaration 1", "Official Declaration 2"
]

json_files = {
    "Old Testament": "old_testament.json",
    "New Testament": "new_testament.json",
    "Book of Mormon": "book_of_mormon.json",
    "Pearl of Great Price": "pearl_of_great_price.json",
    "Doctrine and Covenants": "doctrine_and_covenants.json"
}

book_orders = {
    "Old Testament": ot_books,
    "New Testament": nt_books,
    "Book of Mormon": bom_books,
    "Pearl of Great Price": pogp_books,
    "Doctrine and Covenants": dc_books
}

tag_map = {
    "Old Testament": "Scripture/OT",
    "New Testament": "Scripture/NT",
    "Book of Mormon": "Scripture/BoM",
    "Doctrine and Covenants": "Scripture/DandC",
    "Pearl of Great Price": "Scripture/PoGP"
}

# Function to clean name for front matter key
def clean_key(name):
    return name.lower().replace(' ', '_').replace('-', '_').replace('--', '_').replace("'", "").replace('(', '').replace(')', '')

# Function to generate the top portion (frontmatter and callouts)
def generate_top_portion(resources_list, category, ai_resources, book_name, chapter_num, book_number):
    tag = tag_map.get(category, "")

    # Handle AI summaries
    if ai_resources:
        context_summary = ai_resources.get("context_summary", "NA")
        child_summary = ai_resources.get("child_summary", "NA")
        normal_summary = ai_resources.get("summary", "NA")
        tags = ai_resources.get("tags", "")
    else:
        context_summary = "NA"
        child_summary = "NA"
        normal_summary = "NA"
        tags = ""

    top_content = "---\n"
    top_content += "publish: true\n"
    top_content += "tags:\n"
    top_content += "  - no-graph\n"
    if tag:
        top_content += f"  - {tag}\n"
    top_content += "cssclasses:\n"
    top_content += "  - scriptures\n"
    top_content += f"context_summary: {context_summary}\n"
    top_content += f"child_summary: {child_summary}\n"
    top_content += f"summary: {normal_summary}\n"
    top_content += f"volume: {category}\n"
    top_content += f"book: {book_name}\n"
    top_content += f"book_number: {book_number}\n"
    top_content += f"chapter: {chapter_num}\n"
    # Loop through resources to add to front matter
    for res in resources_list:
        if res["name"].startswith("CFM "):
            match = re.search(r"CFM (\d{4})", res["name"])
            if match:
                year = match.group(1)
                key = f"cfm_{year}_url"
            else:
                key = clean_key(res["name"]) + "_url"
        else:
            key = clean_key(res["name"]) + "_url"
        top_content += f"{key}: {res['url']}\n"
    top_content += "---\n"

    # Write chapter details with hyperlinks
    top_content += ">[!Properties]+ Resources\n"
    links = "    |    ".join(f"[{res['name']}]({res['url']})" for res in resources_list)
    top_content += f">{links}\n"

    # Write AI summaries
    top_content += ">>[!AI]- AI Context\n"
    top_content += f">>{context_summary}\n>\n"
    top_content += ">>[!AI]- AI Child Summary\n"
    top_content += f">>{child_summary}\n>\n"
    top_content += ">>[!AI]- AI Summary\n"
    top_content += f">>{normal_summary}\n"
    top_content += f">\n>{tags}\n"
    return top_content

# Function to generate verses content (for new files)
def generate_verses(verses):
    verses_content = ""
    for verse_num in sorted(verses.keys()):
        verse_text = verses[verse_num]
        verses_content += f"###### {verse_num}\n"
        verses_content += f"{verse_num} {verse_text}\n"
    return verses_content

# Function to update a chapter file
def update_chapter_file(file_path, verses, resources_list, category, ai_resources, book_name, chapter_num, book_number):
    top_content = generate_top_portion(resources_list, category, ai_resources, book_name, chapter_num, book_number)

    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        
        # Find the index of the line starting with "###### 1"
        verse_start_index = None
        for i, line in enumerate(lines):
            if line.strip().startswith("###### 1"):
                verse_start_index = i
                break
        
        if verse_start_index is not None:
            preserved_content = "".join(lines[verse_start_index:])
        else:
            # If no ###### 1 found, treat as new or preserve nothing; here we generate verses
            preserved_content = generate_verses(verses)
    else:
        # If file doesn't exist, generate full content
        preserved_content = generate_verses(verses)

    # Write the updated content
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(top_content)
        f.write(preserved_content)

# Process each category
for category, json_filename in json_files.items():
    json_file = os.path.join("lds_scriptures_json", json_filename)
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
        books_list = data.get(category, [])  # List of book dicts

    for book_dict in books_list:
        book_name = book_dict["name"]
        if category != "Doctrine and Covenants":
            order_list = book_orders[category]
            try:
                book_index = order_list.index(book_name.replace("--", "—")) + 1
            except ValueError:
                try:
                    book_index = order_list.index(book_name) + 1
                except ValueError:
                    print(f"Warning: Book '{book_name}' not found in order list for {category}.")
                    continue
            book_for_folder = book_name.replace("—", "--")
            full_book_folder = os.path.join("Scriptures", category, f"{book_index:02d} {book_for_folder}")
            os.makedirs(full_book_folder, exist_ok=True)
            book_fm = book_name
            book_number = book_index
        else:
            full_book_folder = os.path.join("Scriptures", category)
            os.makedirs(full_book_folder, exist_ok=True)

        for chapter_dict in book_dict["chapters"]:
            chapter_num = chapter_dict["number"]
            verses = {v["number"]: v["text"] for v in chapter_dict["verses"]}
            resources_list = chapter_dict.get("chapter_resources", [])
            ai_resources = chapter_dict.get("ai_resources", None)

            if category == "Doctrine and Covenants":
                if book_name == "Sections":
                    book_fm = "Doctrine and Covenants"
                    book_number = 1
                    chapter_fm = chapter_num
                    file_name = f"D&C {chapter_num}.md"
                elif "Official Declaration" in book_name:
                    num = book_name.split()[-1]
                    book_fm = f"Official Declaration {num}"
                    book_number = int(num) + 1
                    chapter_fm = chapter_num
                    file_name = f"Official Declaration {num}.md"
                else:
                    continue  # Skip if not sections or OD
            else:
                chapter_fm = chapter_num
                file_name = f"{book_for_folder} {chapter_num}.md"

            file_path = os.path.join(full_book_folder, file_name)
            update_chapter_file(file_path, verses, resources_list, category, ai_resources, book_fm, chapter_fm, book_number)

print("Scripture files have been updated successfully.")