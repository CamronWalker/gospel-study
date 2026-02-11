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
# 5. Update specific summaries (e.g., only adult and youth) for a talk:
#    python3 conference_ai.py --update 2025-10 --talk /2025/10/19oaks --update-summaries adult,youth
#
# 6. Update questions (regenerates topics):
#    python3 conference_ai.py --update 2025-10 --update-questions
#
# 7. Force overwrite existing data:
#    python3 conference_ai.py --update 2025-10 --force
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

def generate_summaries(title, speaker, full_text, summary_types='all', debug=False):
    """Generate summaries for specified audiences or all."""
    all_types = ['adult', 'youth', 'children', 'new_members', 'non_members', 'kicker']
    if summary_types == 'all':
        selected = all_types
    else:
        selected = [t.strip() for t in summary_types.split(',') if t.strip() in all_types]
        if not selected:
            return {}, 0, 0, 0, 0

    prompt_parts = []
    if 'adult' in selected:
        prompt_parts.append("1. Adult Summary: A detailed summary (3-4 sentences, max 150 words) for adults, capturing main points, teachings, and applications.")
    if 'youth' in selected:
        prompt_parts.append("2. Youth Summary: An engaging summary (2-3 sentences, max 100 words) for youth, relating to their lives and challenges.")
    if 'children' in selected:
        prompt_parts.append("3. Children Summary: A simple, story-like summary (1-2 sentences, max 50 words) for children.")
    if 'new_members' in selected:
        prompt_parts.append("4. New Members Summary: A welcoming summary (2-3 sentences, max 100 words) explaining key concepts for new members.")
    if 'non_members' in selected:
        prompt_parts.append("5. Non-Members Summary: An accessible introduction (2-3 sentences, max 100 words) to the talk's message for non-members.")
    if 'kicker' in selected:
        prompt_parts.append("6. Kicker: A compelling 5-10 word phrase to draw readers into the talk's message.")

    if not prompt_parts:
        return {}, 0, 0, 0, 0

    prompt = (
        f"Provide the following for the talk '{title}' by {speaker}:\n"
        + "\n".join(prompt_parts) + "\n"
        f"Notes:\n"
        f"- Do not start summaries with redundant references to the title or speaker.\n"
        f"- Keep concise and focused on the essence.\n"
        f"Talk text:\n{full_text}\n\n"
        f"Output Format:\n"
    )
    for t in selected:
        prompt += f"{t.capitalize()} Summary: [{t} summary here]\n" if t != 'kicker' else f"Kicker: [kicker here]\n"

    if debug:
        tqdm.write(f"Debug: Prompt for summaries ({title}, types: {selected}):\n{prompt}\n")
    
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
            summaries = {}
            current_key = None
            current_lines = []
            for line in lines:
                line = line.strip()
                for t in selected:
                    start_str = f"{t.capitalize()} Summary:" if t != 'kicker' else "Kicker:"
                    if line.startswith(start_str):
                        if current_key:
                            summaries[current_key] = " ".join(current_lines)
                        current_key = t
                        content = line.split(":", 1)[1].strip() if ":" in line else ""
                        if content:
                            current_lines = [content]
                        break
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

def generate_topics(title, speaker, body, update_questions_only=False, debug=False):
    """Generate key topics with summaries, questions, and quotes using body paragraphs.
    If update_questions_only, regenerate only question-quote pairs (but for simplicity, regenerate all topics)."""
    # Note: For now, update_questions_only just regenerates all topics, as separating pairs is complex.
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

def generate_mindmap(title, speaker, body, debug=False):
    """Generate a Mermaid mindmap with central idea, sub-branches, and keywords using body paragraphs."""
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
        f"Generate a Mermaid mindmap diagram for the talk '{title}' by {speaker}.\n"
        f"The central idea should be the main theme or core message of the talk (keep it concise, 5-10 words).\n"
        f"Create 3-5 sub-branches representing key topics or sections.\n"
        f"Under each sub-branch, add 2-4 keywords or short phrases as child nodes.\n"
        f"Use Mermaid mindmap syntax.\n"
        f"Notes:\n"
        f"- Keep the structure simple and hierarchical.\n"
        f"- Use double parentheses (( )) for the central idea.\n"
        f"- Use indentation for branches and sub-nodes.\n"
        f"- Ensure keywords are relevant and extracted or derived from the talk.\n"
        f"Body:\n{body_text}\n\n"
        f"Output ONLY the Mermaid code, starting with 'mindmap' and properly indented. Do not include any explanations or code fences."
    )
    
    if debug:
        tqdm.write(f"Debug: Prompt for mindmap ({title}):\n{prompt}\n")
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            chat = client.chat.create(
                model="grok-4-1-fast-reasoning",
                temperature=0.5,
                max_tokens=1024
            )
            chat.append(user(prompt))
            response = chat.sample()
            
            if debug:
                tqdm.write(f"Debug: Response for mindmap ({title}):\n{response.content}\n")
            
            mindmap_code = response.content.strip()
            # Clean up if code fences are present despite instructions
            if mindmap_code.startswith("```mermaid"):
                mindmap_code = mindmap_code.split("\n", 1)[1].rsplit("\n", 1)[0].strip()
            elif mindmap_code.startswith("```"):
                mindmap_code = mindmap_code.strip("```").strip()
            
            prompt_tokens = response.usage.prompt_tokens if hasattr(response.usage, 'prompt_tokens') else 0
            completion_tokens = response.usage.completion_tokens if hasattr(response.usage, 'completion_tokens') else 0
            reasoning_tokens = response.usage.reasoning_tokens if hasattr(response.usage, 'reasoning_tokens') else 0
            searches = response.usage.num_sources_used if hasattr(response.usage, 'num_sources_used') else 0
            
            if debug:
                tqdm.write(f"Debug: Parsed mindmap for {title}:\n{mindmap_code}\n")
                tqdm.write(f"Tokens: Input {prompt_tokens}, Completion {completion_tokens}, Reasoning {reasoning_tokens}, Searches {searches}\n")
            
            return mindmap_code, prompt_tokens, completion_tokens, reasoning_tokens, searches
        except Exception as e:
            wait_time = 2 ** attempt
            tqdm.write(f"Error generating mindmap for {title}: {e}. Retrying in {wait_time}s... (Attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)
    tqdm.write(f"Max retries exceeded for mindmap ({title})")
    return "", 0, 0, 0, 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update LDS General Conference JSON with AI summaries.")
    parser.add_argument("--update", required=True, help="Conference target (e.g., 2025-10 or 2023-2025)")
    parser.add_argument("--force", action="store_true", help="Force overwrite existing data")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument("--search", action="store_true", help="Enable search for related content")
    parser.add_argument("--talk", help="Specific talk slug to update (e.g., /2025/10/19oaks)")
    parser.add_argument("--update-summaries", help="Specific summaries to update (e.g., adult,youth)")
    parser.add_argument("--update-questions", action="store_true", help="Update questions/topics")

    args = parser.parse_args()

    conference_files = parse_conference_target(args.update)
    json_dir = "conference_json"  # Adjust if different

    total_prompt_tokens = total_completion_tokens = total_reasoning_tokens = total_searches = 0

    for conf_file in conference_files:
        file_path = os.path.join(json_dir, conf_file)
        if not os.path.exists(file_path):
            tqdm.write(f"File not found: {file_path}")
            continue

        with open(file_path, "r") as f:
            data = json.load(f)

        # Flatten talks from nested sessions
        all_talks = []
        for session_name, session in data.get("sessions", {}).items():
            for slug, talk in session.get("talks", {}).items():
                talk["slug"] = slug  # Add slug for matching
                all_talks.append(talk)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for talk in all_talks:
                if args.talk and talk["slug"] != args.talk.lstrip("/"):  # Handle optional leading /
                    continue

                title = talk.get("title")
                speaker = talk.get("speaker")
                body = talk.get("body", [])
                full_text = " ".join([item.get("markdown", "") for item in body if item.get("type") == "paragraph"])

                ai_res = talk.get("ai_resources", {})
                has_summaries = "summaries" in ai_res
                has_topics = "topics" in ai_res
                has_mindmap = "mindmap" in ai_res

                # Decide what to update
                do_summ = args.update_summaries or (not has_summaries or args.force)
                do_topics = args.update_questions or (not has_topics or args.force)
                do_mindmap = not has_mindmap or args.force  # Always add/update mindmap if missing or force

                if not (do_summ or do_topics or do_mindmap):
                    continue

                search_params = SearchParameters(web_source()) if args.search else None

                if do_summ:
                    future_summ = executor.submit(generate_summaries, title, speaker, full_text, args.update_summaries or "all", args.debug)
                    futures.append((talk, "summ", future_summ))
                if do_topics:
                    future_topics = executor.submit(generate_topics, title, speaker, body, args.update_questions, args.debug)
                    futures.append((talk, "topics", future_topics))
                if do_mindmap:
                    future_mindmap = executor.submit(generate_mindmap, title, speaker, body, args.debug)
                    futures.append((talk, "mindmap", future_mindmap))

            for talk, typ, future in tqdm(futures, desc=f"Processing talks in {conf_file}"):
                result = future.result()
                if typ == "summ":
                    summaries, pt, ct, rt, s = result
                    if summaries:
                        talk.setdefault("ai_resources", {})["summaries"] = summaries
                    total_prompt_tokens += pt
                    total_completion_tokens += ct
                    total_reasoning_tokens += rt
                    total_searches += s
                elif typ == "topics":
                    topics, pt, ct, rt, s = result
                    if topics:
                        talk.setdefault("ai_resources", {})["topics"] = topics
                    total_prompt_tokens += pt
                    total_completion_tokens += ct
                    total_reasoning_tokens += rt
                    total_searches += s
                elif typ == "mindmap":
                    mindmap, pt, ct, rt, s = result
                    if mindmap:
                        talk.setdefault("ai_resources", {})["mindmap"] = mindmap
                    total_prompt_tokens += pt
                    total_completion_tokens += ct
                    total_reasoning_tokens += rt
                    total_searches += s

        # Save updated JSON
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tqdm.write(f"Updated {file_path}. Total tokens: Prompt {total_prompt_tokens}, Completion {total_completion_tokens}, Reasoning {total_reasoning_tokens}, Searches {total_searches}")