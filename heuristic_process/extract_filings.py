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
def extract_filing_item(
    filing_content: str, 
    item_identifier: str, 
    custom_end_markers: Optional[List[str]] = None,
    min_length: int = 1500
) -> Optional[str]:
    """
    Generalized function to extract ANY Item from ANY filing using Longest Block Heuristic.
    Updated to be Markdown-Resilient (ignores **, #, ##, > and | table markers).
    """
    
    # --- REGEX EXPLANATION ---
    # 1. (?:^|\n)             -> Start of string or new line
    # 2. \s* -> Optional whitespace
    # 3. (?:[\#\*\_\-\>\|]+\s*)? -> OPTIONAL Markdown chars (#, *, _, -, >, |) followed by space.
    #                            Added '\|' to handle table rows like "| Item 1. | Business |"
    # 4. ITEM                 -> Literal "ITEM" (Case Insensitive)
    # 5. \s+                  -> Required whitespace
    # 6. ID + \b              -> The Item Number (e.g. "1") followed by a word boundary
    
    # Markdown-Resilient Regex Prefix (Updated with pipe for tables)
    md_prefix = r'(?:^|\n)\s*(?:[\#\*\_\-\>\|]+\s*)?'
    
    start_pattern = md_prefix + r'ITEM\s+' + re.escape(item_identifier) + r'\b'
    
    # 2. DYNAMIC END PATTERN GENERATION
    if custom_end_markers:
        markers = custom_end_markers
    else:
        # Smart Inference for End Markers
        markers = []
        if item_identifier == "1":
            markers = ["1A", "1B", "2"]
        elif item_identifier == "7":
            markers = ["7A", "8"]
        elif item_identifier == "8":
            markers = ["9", "9A"]
        elif item_identifier.isdigit():
            next_num = str(int(item_identifier) + 1)
            markers = [f"{item_identifier}A", next_num]
        elif item_identifier.endswith("A"):
            base_num = item_identifier[:-1]
            markers = [f"{base_num}B", str(int(base_num) + 1)]
        else:
            markers = ["SIGNATURES", "PART II", "PART III"]

    # Construct Regex for End Markers (Same MD resilience)
    end_patterns = []
    for m in markers:
        end_patterns.append(md_prefix + r'ITEM\s+' + re.escape(m) + r'\b')
    
    # Add Generic "PART" boundaries
    end_patterns.append(md_prefix + r'PART\s+(?:I|II|III|IV)\b')

    # 3. EXTRACTION LOGIC
    # Compile with IGNORECASE and MULTILINE
    starts = [m for m in re.finditer(start_pattern, filing_content, re.IGNORECASE)]
    
    ends = []
    for pat in end_patterns:
        ends.extend(re.finditer(pat, filing_content, re.IGNORECASE))
    
    # Sort by position
    starts.sort(key=lambda x: x.start())
    ends.sort(key=lambda x: x.start())

    candidates = []

    for s in starts:
        # Find nearest valid end after this start
        valid_ends = [e for e in ends if e.start() > s.start()]
        
        if valid_ends:
            e = valid_ends[0]
            length = e.start() - s.start()
            
            # Heuristic: Filter out TOC entries (usually small)
            if length > min_length:
                candidates.append({
                    'length': length,
                    'start': s.start(),
                    'end': e.start(),
                    'content': filing_content[s.start():e.start()]
                })

    # 4. VALIDATION & SELECTION
    if not candidates:
        if len(starts) > 0:
            print(f"   [Logic] Found {len(starts)} start markers, but no valid end marker > {min_length} chars.")
        else:
            print(f"   [Logic] No start markers found for Item {item_identifier} (Regex: {start_pattern})")
            
        return None

    # Return the Longest Block
    best_match = max(candidates, key=lambda x: x['length'])
    
    print(f"   [Logic] Extracted Item {item_identifier}: {best_match['length']} chars.")
    return best_match['content'].strip()

if __name__ == "__main__":
    if not os.path.exists(INPUT_FILE):
        print(f"File {INPUT_FILE} not found. Check your environment variables.")
        exit()

    # When saving CSVs in Step 4, use the resolved path:
    # output_dir = resolve_path("extracted_tables/GOOGL/2025")