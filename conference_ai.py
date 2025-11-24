# This script generates AI-powered summaries and resources for talks in LDS General Conference JSON files using the xAI SDK.
# It supports updating entire conferences or ranges of conferences.
# Requires an XAI_API_KEY environment variable set in a .env file.
# JSON files are expected in the 'conference_json' directory.

# Usage Examples:
# 1. Update a single conference:
#    python3 conference_ai.py --update 2025-10
#
# 2. Update a range of conferences (April and October for each year):
#    python3 conference_ai.py --update 2023-2025
#
# 3. Update with search enabled for related content:
#    python3 conference_ai.py --update 2025-10 --search
#
# 4. Update with debug logging enabled:
#    python3 conference_ai.py --update 2025-10 --debug
#
# Note: Conferences are April (04) and October (10). Input 'YYYY-MM' for specific, 'YYYY1-YYYY2' for range.

import os
import re
import json
import time
from dotenv import load_dotenv
import argparse
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file first so values like XAI_API_KEY are available
load_dotenv()

# Suppress noisy C++/absl/grpc logs that can appear before the logging system is initialized.
# These env vars should be set before importing modules that initialize gRPC/TensorFlow/absl.
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", os.getenv("TF_CPP_MIN_LOG_LEVEL", "2"))
os.environ.setdefault("ABSL_CPP_MIN_LOG_LEVEL", os.getenv("ABSL_CPP_MIN_LOG_LEVEL", "2"))
os.environ.setdefault("GRPC_VERBOSITY", os.getenv("GRPC_VERBOSITY", "ERROR"))
os.environ.setdefault("GRPC_TRACE", os.getenv("GRPC_TRACE", ""))

# Now import the xAI SDK (and any other libraries that might initialize gRPC/absl)
from xai_sdk import Client
from xai_sdk.chat import user
from xai_sdk.search import SearchParameters, web_source

# Set your xAI API key (retrieve from environment variable for security)
api_key = os.getenv('XAI_API_KEY')
client = Client(api_key=api_key)

# Month mapping for conferences
month_map = {'04': 'april', '10': 'october', '4': 'april'}

# Conference months
conference_months = ['april', 'october']

def parse_conference_target(target):
    """Parse target to list of file names like '2025-october.json'."""
    files = []
    parts = target.split('-')
    if len(parts) == 2:
        if len(parts[1]) == 4:  # Year range, e.g., 2023-2025
            start_year = int(parts[0])
            end_year = int(parts[1])
            for year in range(start_year, end_year + 1):
                for month in conference_months:
                    files.append(f"{year}-{month}.json")
        else:  # Single conference, e.g., 2025-10
            year = int(parts[0])
            month_num = parts[1]
            month_name = month_map.get(month_num, month_num)
            files.append(f"{year}-{month_name}.json")
    else:
        # Assume single year, both months
        year = int(target)
        for month in conference_months:
            files.append(f"{year}-{month}.json")
    return files

def generate_summaries(title, speaker, full_text, debug=False):
    """Generate summaries for different audiences."""
    prompt = (
        f"Provide the following summaries for the talk '{title}' by {speaker}:\n"
        f"1. Adult Summary: A detailed summary (3-4 sentences, max 150 words) for adults, capturing main points, teachings, and applications.\n"
        f"2. Youth Summary: An engaging summary (2-3 sentences, max 100 words) for youth, relating to their lives and challenges.\n"
        f"3. Children Summary: A simple, story-like summary (1-2 sentences, max 50 words) for children.\n"
        f"4. New Members Summary: A welcoming summary (2-3 sentences, max 100 words) explaining key concepts for new members.\n"
        f"5. Non-Members Summary: An accessible introduction (2-3 sentences, max 100 words) to the talk's message for non-members.\n"
        f"Notes:\n"
        f"- Do not start summaries with redundant references to the title or speaker.\n"
        f"- Keep concise and focused on the essence.\n"
        f"Talk text:\n{full_text}\n\n"
        f"Output Format:\n"
        f"Adult Summary: [adult summary here]\n"
        f"Youth Summary: [youth summary here]\n"
        f"Children Summary: [children summary here]\n"
        f"New Members Summary: [new members summary here]\n"
        f"Non-Members Summary: [non-members summary here]\n"
    )

    if debug:
        tqdm.write(f"Debug: Prompt for summaries ({title}):\n{prompt}\n")
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            chat = client.chat.create(
                model="grok-4-1-fast-reasoning",
                temperature=0.7,
                max_tokens=2048
            )
            chat.append(user(prompt))
            response = chat.sample()
            
            if debug:
                tqdm.write(f"Debug: Response for summaries ({title}):\n{response.content}\n")
            
            output = response.content.strip()
            lines = output.split("\n")
            summaries = {
                "adult": "",
                "youth": "",
                "children": "",
                "new_members": "",
                "non_members": ""
            }
            current_key = None
            current_lines = []
            for line in lines:
                line = line.strip()
                if line.startswith("Adult Summary:"):
                    current_key = "adult"
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current_lines = [content]
                elif line.startswith("Youth Summary:"):
                    if current_key:
                        summaries[current_key] = " ".join(current_lines)
                    current_key = "youth"
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current_lines = [content]
                elif line.startswith("Children Summary:"):
                    if current_key:
                        summaries[current_key] = " ".join(current_lines)
                    current_key = "children"
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current_lines = [content]
                elif line.startswith("New Members Summary:"):
                    if current_key:
                        summaries[current_key] = " ".join(current_lines)
                    current_key = "new_members"
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current_lines = [content]
                elif line.startswith("Non-Members Summary:"):
                    if current_key:
                        summaries[current_key] = " ".join(current_lines)
                    current_key = "non_members"
                    content = line.split(":", 1)[1].strip() if ":" in line else ""
                    if content:
                        current_lines = [content]
                else:
                    if current_key and line:
                        current_lines.append(line)
            if current_key:
                summaries[current_key] = " ".join(current_lines)
            
            prompt_tokens = response.usage.prompt_tokens if hasattr(response.usage, 'prompt_tokens') else 0
            completion_tokens = response.usage.completion_tokens if hasattr(response.usage, 'completion_tokens') else 0
            reasoning_tokens = response.usage.reasoning_tokens if hasattr(response.usage, 'reasoning_tokens') else 0
            searches = response.usage.num_sources_used if hasattr(response.usage, 'num_sources_used') else 0
            
            if debug:
                tqdm.write(f"Debug: Parsed summaries for {title}: {summaries}\n")
                tqdm.write(f"Tokens: Input {prompt_tokens}, Completion {completion_tokens}, Reasoning {reasoning_tokens}, Searches {searches}\n")
            
            return summaries, prompt_tokens, completion_tokens, reasoning_tokens, searches
        except Exception as e:
            wait_time = 2 ** attempt
            tqdm.write(f"Error generating summaries for {title}: {e}. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)
    tqdm.write(f"Max retries exceeded for summaries ({title})")
    return {}, 0, 0, 0, 0

def generate_topics(title, speaker, body, debug=False):
    """Generate key topics with summaries, questions, and quotes using body paragraphs."""
    # Construct body text with paragraph numbers
    body_text = ""
    for item in body:
        if item.get("type") == "paragraph":
            para_num = item.get("paragraph")
            markdown = item.get("markdown", "")
            body_text += f"Paragraph {para_num}: {markdown}\n"
        elif item.get("type") == "heading":
            level = item.get("level", 2)
            markdown = item.get("markdown", "")
            body_text += f"{'#' * level} {markdown}\n"
    
    prompt = (
        f"Identify 3-5 key topics from the talk '{title}' by {speaker}, preferably based on section headings if present.\n"
        f"For each topic:\n"
        f"- Topic Name: A brief, descriptive name.\n"
        f"- Paragraphs: List of paragraph numbers covered by this topic, e.g., [1-3,5,7] (use ranges where appropriate).\n"
        f"- Summary: 1-2 sentences summarizing the topic (max 75 words).\n"
        f"- Question-Quote Pairs: 1-4 pairs (prioritize meaningful, impactful pairs over quantity; ensure at least 1 per topic; avoid redundant or unimpactful ones), each with a Question (open-ended for reflection/discussion) matched to a Quote (direct, concise excerpt from the talk in the same paragraph(s) as the topic—do not quote full paragraphs unless the entire paragraph is the key idea; prefer 1-2 sentences or phrases; include (Paragraph X) after the quote).\n"
        f"Notes:\n"
        f"- Topics should cover main themes or sections, and quotes must come from the paragraphs listed for that topic.\n"
        f"- Questions should encourage personal application related to the quote.\n"
        f"- Quotes should be exact and concise excerpts.\n"
        f"Body:\n{body_text}\n\n"
        f"Output Format:\n"
        f"Topics:\n"
        f"- Topic Name: [name]\n"
        f"  Paragraphs: [1-3,5]\n"
        f"  Summary: [summary]\n"
        f"  Question-Quote Pairs:\n"
        f"  - Question: [q1]\n"
        f"    Quote: \"[quote1]\" (Paragraph 1)\n"
        f"  - Question: [q2]\n"
        f"    Quote: \"[quote2]\" (Paragraph 2)\n"
        f"- Next Topic Name: [name]\n"
        f"  ...\n"
    )
    
    if debug:
        tqdm.write(f"Debug: Prompt for topics ({title}):\n{prompt}\n")
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            chat = client.chat.create(
                model="grok-4-1-fast-reasoning",
                temperature=0.7,
                max_tokens=2048
            )
            chat.append(user(prompt))
            response = chat.sample()
            
            if debug:
                tqdm.write(f"Debug: Response for topics ({title}):\n{response.content}\n")
            
            output = response.content.strip()
            topics = []
            lines = output.split("\n")
            current_topic = None
            current_section = None
            current_pair = None
            for line in lines:
                line_stripped = line.strip()
                if line_stripped.startswith("- Topic Name:"):
                    if current_topic:
                        if current_pair:
                            current_topic["question_quote_pairs"].append(current_pair)
                        topics.append(current_topic)
                    current_topic = {
                        "name": "",
                        "paragraphs": [],
                        "summary": "",
                        "question_quote_pairs": []
                    }
                    current_section = None
                    current_pair = None
                    name_match = re.match(r"- Topic Name: (.+)", line_stripped)
                    if name_match:
                        current_topic["name"] = name_match.group(1).strip()
                elif current_topic:
                    if line_stripped.startswith("Paragraphs:"):
                        paras_str = line_stripped.split(":", 1)[1].strip() if ":" in line_stripped else ""
                        current_topic["paragraphs"] = [p.strip() for p in paras_str.strip("[] ").split(",")]
                    elif line_stripped.startswith("Summary:"):
                        current_section = "summary"
                        content = line_stripped.split(":", 1)[1].strip() if ":" in line_stripped else ""
                        current_topic["summary"] = content
                    elif line_stripped.startswith("Question-Quote Pairs:"):
                        current_section = "pairs"
                    elif current_section == "summary" and line_stripped and not line_stripped.startswith("-"):
                        current_topic["summary"] += " " + line_stripped
                    elif current_section == "pairs":
                        if line_stripped.startswith("- Question:"):
                            if current_pair:
                                current_topic["question_quote_pairs"].append(current_pair)
                            current_pair = {"question": "", "quote": "", "paragraph_key": ""}
                            q_content = line_stripped.split(":", 1)[1].strip() if ":" in line_stripped else ""
                            current_pair["question"] = q_content
                        elif "Quote:" in line_stripped and current_pair:
                            # Robust regex for quote parsing, ignoring exact indentation
                            quote_match = re.match(r'Quote:\s*"([^"]*)"\s*\(Paragraph\s*(\d+)\)', line_stripped)
                            if quote_match:
                                quote_text = quote_match.group(1)
                                para_num = quote_match.group(2)
                                current_pair["quote"] = f'"{quote_text}"'
                                current_pair["paragraph_key"] = para_num
                            else:
                                # Fallback if regex fails, extract manually
                                if ' (Paragraph ' in line_stripped:
                                    parts = line_stripped.split(' (Paragraph ')
                                    if len(parts) == 2:
                                        before_para = parts[0]
                                        para_part = parts[1].rstrip(')')
                                        if 'Quote:' in before_para and '"' in before_para:
                                            quote_start = before_para.find('"') + 1
                                            quote_end = before_para.rfind('"')
                                            if quote_start > 0 and quote_end > quote_start:
                                                quote_text = before_para[quote_start:quote_end]
                                                current_pair["quote"] = f'"{quote_text}"'
                                                current_pair["paragraph_key"] = para_part
            if current_topic:
                if current_pair:
                    current_topic["question_quote_pairs"].append(current_pair)
                topics.append(current_topic)
            
            prompt_tokens = response.usage.prompt_tokens if hasattr(response.usage, 'prompt_tokens') else 0
            completion_tokens = response.usage.completion_tokens if hasattr(response.usage, 'completion_tokens') else 0
            reasoning_tokens = response.usage.reasoning_tokens if hasattr(response.usage, 'reasoning_tokens') else 0
            searches = response.usage.num_sources_used if hasattr(response.usage, 'num_sources_used') else 0
            
            if debug:
                tqdm.write(f"Debug: Parsed topics for {title}: {topics}\n")
                tqdm.write(f"Tokens: Input {prompt_tokens}, Completion {completion_tokens}, Reasoning {reasoning_tokens}, Searches {searches}\n")
            
            return topics, prompt_tokens, completion_tokens, reasoning_tokens, searches
        except Exception as e:
            wait_time = 2 ** attempt
            tqdm.write(f"Error generating topics for {title}: {e}. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)
    tqdm.write(f"Max retries exceeded for topics ({title})")
    return [], 0, 0, 0, 0

def parse_related_content(s, is_scriptures=False):
    """Parse related items from string like 'item1 ~ reason; item2 ~ reason'."""
    if not s:
        return []
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

def generate_related(title, speaker, full_text, search_enabled=False, debug=False):
    """Generate related talks and scriptures, with search if enabled."""
    if not search_enabled:
        return [], [], 0, 0, 0, 0
    
    allowed_websites = ["churchofjesuschrist.org", "scriptures.byu.edu"]
    prompt = (
        f"Search for 2-3 related general conference talks and 2-3 related scriptures for the talk '{title}' by {speaker}.\n"
        f"For each related talk: Provide 'Title by Speaker, Conference YYYY Month' and brief reason (max 25 words).\n"
        f"For each related scripture: Wiki-style link (e.g., [[Matthew 5]] or [[D&C 88#118|D&C 88:118-126]][[D&C 88#119|]]...) and brief reason (max 25 words).\n"
        f"Prioritize official sources.\n"
        f"Talk text:\n{full_text}\n\n"
        f"Output Format:\n"
        f"Related Talks: [talk1 ~ reason; ...]\n"
        f"Related Scriptures: [link1 ~ reason; ...]\n"
    )
    
    if debug:
        tqdm.write(f"Debug: Prompt for related ({title}):\n{prompt}\n")
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            search_params = SearchParameters(
                mode="auto",
                max_search_results=5,
                sources=[web_source(allowed_websites=allowed_websites)]
            ) if search_enabled else None
            chat = client.chat.create(
                model="grok-4-1-fast-reasoning",
                temperature=0.7,
                max_tokens=1024,
                search_parameters=search_params
            )
            chat.append(user(prompt))
            response = chat.sample()
            
            if debug:
                tqdm.write(f"Debug: Response for related ({title}):\n{response.content}\n")
            
            output = response.content.strip()
            # Parse Related Talks and Scriptures
            related_talks_str = ""
            related_scriptures_str = ""
            lines = output.split("\n")
            for line in lines:
                if line.startswith("Related Talks:"):
                    related_talks_str = line.split(":", 1)[1].strip() if ":" in line else ""
                elif line.startswith("Related Scriptures:"):
                    related_scriptures_str = line.split(":", 1)[1].strip() if ":" in line else ""
            
            related_talks = parse_related_content(related_talks_str, is_scriptures=False)
            related_scriptures = parse_related_content(related_scriptures_str, is_scriptures=True)
            
            prompt_tokens = response.usage.prompt_tokens if hasattr(response.usage, 'prompt_tokens') else 0
            completion_tokens = response.usage.completion_tokens if hasattr(response.usage, 'completion_tokens') else 0
            reasoning_tokens = response.usage.reasoning_tokens if hasattr(response.usage, 'reasoning_tokens') else 0
            searches = response.usage.num_sources_used if hasattr(response.usage, 'num_sources_used') else 0
            
            if debug:
                tqdm.write(f"Debug: Parsed related for {title}: Talks {related_talks}, Scriptures {related_scriptures}\n")
                tqdm.write(f"Tokens: Input {prompt_tokens}, Completion {completion_tokens}, Reasoning {reasoning_tokens}, Searches {searches}\n")
            
            return related_talks, related_scriptures, prompt_tokens, completion_tokens, reasoning_tokens, searches
        except Exception as e:
            wait_time = 2 ** attempt
            tqdm.write(f"Error generating related for {title}: {e}. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)
    tqdm.write(f"Max retries exceeded for related ({title})")
    return [], [], 0, 0, 0, 0

def process_talk(sessions, session_name, talk_id, search_enabled, debug):
    """Process a single talk."""
    session = sessions[session_name]
    talk = session["talks"][talk_id]
    title = talk.get("title", "")
    speaker = talk.get("speaker", "")
    full_text = talk.get("full_markdown", "")
    body = talk.get("body", [])
    
    if not full_text:
        return talk, 0, 0, 0, 0
    
    # Generate summaries using full_text
    summaries, s_in, s_comp, s_reas, s_search = generate_summaries(title, speaker, full_text, debug)
    
    # Generate topics using body
    topics, t_in, t_comp, t_reas, t_search = generate_topics(title, speaker, body, debug)
    
    # Generate related using full_text
    related_talks, related_scriptures, r_in, r_comp, r_reas, r_search = generate_related(title, speaker, full_text, search_enabled, debug)
    
    total_in = s_in + t_in + r_in
    total_comp = s_comp + t_comp + r_comp
    total_reas = s_reas + t_reas + r_reas
    total_search = s_search + t_search + r_search
    
    if "ai_resources" not in talk:
        talk["ai_resources"] = {}
    talk["ai_resources"]["summaries"] = summaries
    talk["ai_resources"]["topics"] = topics
    talk["ai_resources"]["related_talks"] = related_talks
    talk["ai_resources"]["related_scriptures"] = related_scriptures
    
    return talk, total_in, total_comp, total_reas, total_search

def update_conference(file_path, search_enabled=False, debug=False, show_talk_progress=True):
    """Update all talks in a conference JSON."""
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    sessions = data.get("sessions", {})
    talk_tasks = []
    for session_name in sessions:
        talks = sessions[session_name].get("talks", {})
        for talk_id in talks:
            talk_tasks.append((sessions, session_name, talk_id))
    
    total_talks = len(talk_tasks)
    total_input = 0
    total_completion = 0
    total_reasoning = 0
    total_searches = 0

    def process_task(task):
        return process_talk(*task, search_enabled, debug)

    with ThreadPoolExecutor(max_workers=100) as executor:
        futures = [executor.submit(process_task, task) for task in talk_tasks]
        if show_talk_progress:
            # Show a per-talk progress bar (existing behavior) — useful when updating a single conference.
            for future in tqdm(as_completed(futures), total=total_talks, desc=f"Processing {os.path.basename(file_path)}", unit="talk"):
                _, in_t, comp_t, reas_t, search_t = future.result()
                total_input += in_t
                total_completion += comp_t
                total_reasoning += reas_t
                total_searches += search_t
        else:
            # When processing multiple conferences, avoid many nested per-talk progress bars.
            # Instead process the conference silently here and let the caller show a conference-level progress bar.
            for future in as_completed(futures):
                _, in_t, comp_t, reas_t, search_t = future.result()
                total_input += in_t
                total_completion += comp_t
                total_reasoning += reas_t
                total_searches += search_t

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tqdm.write(f"Conference updated: Input Tokens: {total_input}, Completion: {total_completion}, Reasoning: {total_reasoning}, Searches: {total_searches}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update AI resources in JSON conference files.")
    parser.add_argument("--update", required=True, help="Conference (e.g., 2025-10) or range (e.g., 2023-2025)")
    parser.add_argument("--search", action="store_true", help="Enable search for related talks/scriptures")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    target = args.update.strip()
    search_enabled = args.search
    debug = args.debug

    file_names = parse_conference_target(target)
    processed = False
    base_dir = "conference_json"

    # If multiple conferences requested, show an outer progress bar over conferences and
    # run each conference without a per-talk tqdm (because each conference is already parallel).
    if len(file_names) > 1:
        for file_name in tqdm(file_names, desc="Conferences", unit="conf"):
            file_path = os.path.join(base_dir, file_name)
            if os.path.exists(file_path):
                # show_talk_progress=True so each conference shows its own per-talk bar
                update_conference(file_path, search_enabled, debug, show_talk_progress=True)
                processed = True
            else:
                tqdm.write(f"Conference file {file_name} not found.")
    else:
        # Single conference: keep per-talk progress bars for detail
        for file_name in file_names:
            file_path = os.path.join(base_dir, file_name)
            if os.path.exists(file_path):
                update_conference(file_path, search_enabled, debug, show_talk_progress=True)
                processed = True
            else:
                tqdm.write(f"Conference file {file_name} not found.")

    if processed:
        tqdm.write("AI resources have been added to the conference files successfully.")
    else:
        tqdm.write("No updates performed.")