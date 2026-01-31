import os
import re
import json
import pandas as pd
from typing import Dict, Any, Optional, List
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# --- Directory Helper ---
# Resolves paths relative to the project root (HSDB)
PROJECT_ROOT = Path(__file__).parent.parent
def resolve_path(relative_path: str) -> str:
    return str(PROJECT_ROOT / relative_path)

# --- Configuration ---
# Updated to use a more generic relative path or environment variable
INPUT_FILE = os.getenv("FILING_INPUT_PATH", resolve_path("filings/GOOGL/10K/sample.md"))
CACHE_FILE = resolve_path("filing_metadata.json")

# Initialize Direct DeepSeek Client
client = OpenAI(
    api_key=os.getenv('DEEPSEEK_API_KEY'), 
    base_url="https://api.deepseek.com"
)

# --- Helper: Direct API Call ---
def _query_model(prompt: str, context: str, system_msg: str = "You are a financial analyst.") -> str:
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": f"CONTEXT:\n{context}\n\nINSTRUCTION: {prompt}"}
            ],
            response_format={"type": "json_object"} if "JSON" in prompt.upper() else None,
            temperature=0.0 
        )
        return response.choices[0].message.content or ""
    except Exception as e:
        print(f"[API ERROR] {e}")
        return ""

# --- Pipeline Functions ---

def extract_toc(filing_content: str):
    print("--- Extracting Table of Contents ---")
    context_chunk = filing_content[0:15000]
    prompt = """
    Analyze the context. Locate the 'Table of Contents'.
    Extract 'Item', 'Description', and 'Page' number.
    Return a JSON list of objects: [{"item": "Item 1", "description": "Business", "page": "1"}, ...]
    """
    content = _query_model(prompt, context_chunk)
    try:
        clean_json = re.sub(r"```json\n|```", "", content).strip()
        return json.loads(clean_json)
    except: return []

def extract_page_number_format(filing_content: str, chunk_boundary: List[int] = [40000, 60000]):
    print("--- Determining Page Number Format ---")
    context_chunk = filing_content[chunk_boundary[0]:chunk_boundary[1]]
    prompt = r""" 
    Identify the page number pattern. Write a Python Regex to capture the digits.
    Output ONLY the regex inside triple angle brackets: ===REGEX===
    """
    raw_output = _query_model(prompt, context_chunk)
    match = re.search(r"===(.*?)===", raw_output, re.DOTALL)
    return match.group(1).strip() if match else None

# ... [slice_filing_by_page_num and extract_raw_tables remain same logic, just ensure internal paths use resolve_path]

# --- SPLIT BY ITEM ---
SECTION_PATTERNS = {
    "ITEM_1": {
        "start": [r'(?:^|\n)\s*ITEM\s+1[\.\:\-]?\s+(?:BUSINESS)?'],
        "end":   [r'(?:^|\n)\s*ITEM\s+1A[\.\:\-]?\s+', r'(?:^|\n)\s*ITEM\s+1B[\.\:\-]?\s+', r'(?:^|\n)\s*ITEM\s+2[\.\:\-]?\s+']
    },
    "ITEM_1A": {
        "start": [r'(?:^|\n)\s*ITEM\s+1A[\.\:\-]?\s+(?:RISK\s+FACTORS)?'],
        "end":   [r'(?:^|\n)\s*ITEM\s+1B[\.\:\-]?\s+', r'(?:^|\n)\s*ITEM\s+2[\.\:\-]?\s+']
    },
    "ITEM_7": {
        "start": [r'(?:^|\n)\s*ITEM\s+7[\.\:\-]?\s+(?:MANAGEMENT)?'],
        # Ends at 7A (Quant Risk) OR 8 (Financials) if 7A is skipped
        "end":   [r'(?:^|\n)\s*ITEM\s+7A[\.\:\-]?\s+', r'(?:^|\n)\s*ITEM\s+8[\.\:\-]?\s+']
    },
    "ITEM_8": {
        "start": [r'(?:^|\n)\s*ITEM\s+8[\.\:\-]?\s+(?:FINANCIAL)?'],
        "end":   [r'(?:^|\n)\s*ITEM\s+9[\.\:\-]?\s+']
    }
}

def extract_section(
    filing_content: str, 
    section_key: str, 
    min_length: int = 2000
) -> Optional[str]:
    """
    Extracts the content of a specific Item from a 10-K/10-Q filing using the 
    'Longest Block Heuristic' to avoid Table of Contents and Page Headers.

    Args:
        filing_content (str): The full raw text of the filing.
        section_key (str): Key from SECTION_PATTERNS (e.g., "ITEM_1", "ITEM_7").
        min_length (int): Minimum characters for a block to be considered valid.
                          Defaults to 2000 (approx 1 page) to filter TOCs.

    Returns:
        Optional[str]: The extracted text block, or None if validation fails.
    """
    patterns = SECTION_PATTERNS.get(section_key)
    if not patterns:
        print(f"[ERR] No patterns defined for section: {section_key}")
        return None

    # 1. FIND ALL MARKERS
    # We compile with MULTILINE to handle start-of-line anchors correctly
    start_matches = []
    for p in patterns['start']:
        start_matches.extend(re.finditer(p, filing_content, re.IGNORECASE | re.MULTILINE))
    
    end_matches = []
    for p in patterns['end']:
        end_matches.extend(re.finditer(p, filing_content, re.IGNORECASE | re.MULTILINE))

    # Sort by position in text
    start_matches.sort(key=lambda x: x.start())
    end_matches.sort(key=lambda x: x.start())

    if not start_matches:
        print(f"[WARN] Start marker not found for {section_key}")
        return None

    candidates = []

    # 2. PAIRING LOGIC
    # For every "Item X" found (could be 50+ due to headers/TOC), 
    # find the NEAREST subsequent "Item Y" (End Marker).
    for s_match in start_matches:
        s_idx = s_match.start()
        
        # Filter for ends that appear AFTER this start
        valid_ends = [e for e in end_matches if e.start() > s_idx]
        
        if valid_ends:
            # The closest valid end marks the boundary of this candidate block
            e_match = valid_ends[0]
            e_idx = e_match.start()
            
            length = e_idx - s_idx
            
            # 3. HEURISTIC FILTER
            # TOC entries are usually < 500 chars. Page headers are < 100.
            # Real sections are usually > 5,000 chars.
            if length > min_length:
                candidates.append({
                    'start': s_idx,
                    'end': e_idx,
                    'length': length,
                    'content': filing_content[s_idx:e_idx]
                })

    # 4. SELECT WINNER
    if not candidates:
        print(f"[WARN] No valid block found for {section_key} > {min_length} chars.")
        return None

    # The "Real" section is almost always the longest contiguous block.
    # We sort by length descending.
    best_candidate = max(candidates, key=lambda x: x['length'])
    
    # 5. VALIDATION (Basic)
    # Ensure the content isn't just whitespace or junk
    clean_content = best_candidate['content'].strip()
    if len(clean_content) < min_length:
        return None

    print(f"--- Extracted {section_key}: {best_candidate['length']} chars "
          f"(Start: {best_candidate['start']}, End: {best_candidate['end']}) ---")
    
    return clean_content

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"File {INPUT_FILE} not found. Check your environment variables.")
        exit()

    # When saving CSVs in Step 4, use the resolved path:
    # output_dir = resolve_path("extracted_tables/GOOGL/2025")