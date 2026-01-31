from agno.models.deepseek import DeepSeek
from agno.models.ollama import Ollama
from agno.agent import Agent
import pandas as pd
import numpy as np
import io
import os
import re
import json
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
INPUT_FILE = "C:/Users/user/Documents/GitHub/edgar/filings/GOOGL/10K/2025-02-05_0001652044-25-000014.md"
CACHE_FILE = "filing_metadata.json"

# Local SLM
ollama_agent = Agent(
    model=Ollama(id='qwen3:latest', host=os.getenv("OLLAMA_HOST")),
)

# Stronger API model
deepseek_agent = Agent(
    model=DeepSeek(id='deepseek-chat', api_key=os.getenv("DEEPSEEK_API_KEY")),
)

# --- Pipeline Functions ---

def extract_toc(agent: Agent, filing_content: str):
    """
    Step 1: Extract the table of contents from the start of the filing.
    """
    print("--- Extracting Table of Contents ---")
    # Limit context to first 10k chars to find TOC headers
    context_chunk = filing_content[0:15000]
    
    prompt = """
    Analyze the provided context in dependencies. Locate the 'Table of Contents' or the table with similar usage.
    Extract the 'Item' (e.g., Item 1, Item 7), 'Description', and 'Page' number.
    
    Return a JSON list of objects:
    [{"item": "Item 1", "description": "Business", "page": "1"}, ...]
    """
    
    response = agent.run(prompt, dependencies={'provided text': context_chunk}, add_dependencies_to_context=True)
    try:
        # cleanup json formatting if model adds ```json ... ```
        clean_json = re.sub(r"```json\n|```", "", response.content).strip()
        return json.loads(clean_json)
    except json.JSONDecodeError:
        print("Error parsing TOC JSON")
        return []

def extract_page_number_format(agent: Agent, filing_content: str, chunk_boundary: List[int] = [40000, 60000]):
    """
    Step 2: Determine the regex format of the page number.
    """
    print("--- Determining Page Number Format ---")
    # Grab a chunk from the middle where page numbers are likely consistent
    if len(chunk_boundary) >= 2:
        if chunk_boundary[0] < chunk_boundary[1]:
            context_chunk = filing_content[chunk_boundary[0]:chunk_boundary[1]]
        else:
            print(f"chunk boundary value error. First element must smaller than second element.")
            return
    else:
        print("chunk boundary must contain at least 2 elements")
        return
    
    prompt = r""" 
    Analyze the provided text chunk. Identify the recurring pattern used for page numbers (e.g., "Page 55", "55", "- 55 -", "<div align='center'>55.</div>").
    
    Task: Write a Python Regex to capture the page number digit(s).
    
    CRITICAL OUTPUT RULE: 
    1. Output ONLY the regex string.
    2. Wrap the regex inside triple angle brackets like this: ===REGEX===

    Example output: ===^\s*(\d+)\s*$===
    Example output: ===<div align='center'>\s*(\d+)\.?\s*</div>===
    """
    
    try:
        response = agent.run(prompt, dependencies={'provided context': context_chunk}, add_dependencies_to_context=True)
        raw_output = response.content.strip()

        # 1. Extraction: Look for content inside === ===
        match = re.search(r"===(.*?)===", raw_output, re.DOTALL)
        if match:
            regex_pattern = match.group(1).strip()
        else:
            # Fallback: Look for code blocks if the model ignored delimiters
            code_match = re.search(r"```(?:regex)?\s*(.*?)\s*```", raw_output, re.DOTALL)
            if code_match:
                regex_pattern = code_match.group(1).strip()
            else:
                # If the model just dumped text, this is likely garbage, but we can try strictly cleaning it
                print("   > Warning: Model did not use delimiters. Attempting to use full output (risky).")
                regex_pattern = raw_output

        print(f"   > Candidate Regex: {regex_pattern}")

        # 2. Validation: Must compile and match something in the chunk
        try:
            compiled_re = re.compile(regex_pattern, re.MULTILINE)
            test_matches = compiled_re.findall(context_chunk)
        except re.error as e:
            print(f"   > Validation FAILED: Invalid Regex syntax ({e})")
            return None

        if len(test_matches) > 0:
            print(f"   > Validation SUCCESS: Found {len(test_matches)} matches (e.g., {test_matches[:3]})")
            return regex_pattern
        else:
            print(f"   > Validation FAILED: Regex '{regex_pattern}' found 0 matches in the sample text.")
            print("   > The AI likely hallucinated a format not present in this chunk.")
            return None

    except Exception as e:
        print(f"Error in regex extraction: {e}")
        return None 

def slice_filing_by_page_num(filing_content: str, page_regex: str, start_marker: Optional[int], end_marker: Optional[int]):
    """
    Step 3: Slice the filing content based on page number markers.
    
    PURE UTILITY LOGIC:
    - If start_marker is provided (int): It MUST exist in the regex matches. We slice AFTER it.
    - If start_marker is None: We start at index 0 (Start of File).
    - If end_marker is provided (int): It MUST exist in the regex matches. We slice AT the end of it.
    - If end_marker is None: We slice to the end of the file.
    
    Returns:
        str: The sliced text.
        None: If a provided marker could not be found or indices are invalid.
    """
    print(f"\n--- [Step 3] Slicing Content ---")
    print(f"   > Request: Start after Marker {start_marker if start_marker is not None else '[Start of File]'} "
          f"-> End at Marker {end_marker if end_marker is not None else '[End of File]'}")
    
    try:
        # Compile regex
        pattern = re.compile(page_regex, re.MULTILINE | re.IGNORECASE)
        
        # Build Map: { page_number (int) : match_object }
        page_map = {}
        for match in pattern.finditer(filing_content):
            try:
                p_num = int(match.group(1))
                page_map[p_num] = match
            except (ValueError, IndexError):
                continue
        
        # --- Determine Start Index ---
        start_index = 0
        if start_marker is not None:
            if start_marker in page_map:
                start_index = page_map[start_marker].end()
                print(f"   > Found Start Marker (Page {start_marker}) at index {start_index}.")
            else:
                print(f"   > CRITICAL ERROR: Start Marker Page {start_marker} not found in document.")
                return None
        else:
            print("   > No Start Marker provided. Starting from Index 0.")

        # --- Determine End Index ---
        end_index = len(filing_content)
        if end_marker is not None:
            if end_marker in page_map:
                end_index = page_map[end_marker].end()
                print(f"   > Found End Marker (Page {end_marker}) at index {end_index}.")
            else:
                print(f"   > CRITICAL ERROR: End Marker Page {end_marker} not found in document.")
                return None
        else:
             print("   > No End Marker provided. Going to End of File.")

        # --- Validation ---
        if start_index >= end_index:
            print(f"   > CRITICAL ERROR: Start Index ({start_index}) >= End Index ({end_index}).")
            return None

        # --- Slice ---
        return filing_content[start_index:end_index]

    except Exception as e:
        print(f"   > Exception during slicing: {e}")
        return None

def extract_raw_tables(filing_part: str):
    """
    Step 4: "Dumb" Block Extractor.
    
    Logic:
    - Scans for contiguous blocks of text where lines start and end with '|'.
    - Does NOT attempt to identify headers or merge cells.
    - Captures raw grid geometry (including empty columns) for LLM reconstruction.
    - Captures 200 chars pre-table and 100 chars post-table for context.
    """
    print("--- Extracting Raw Table Blocks ---")
    extracted_data = []
    
    if not filing_part:
        return extracted_data

    lines = filing_part.split('\n')
    total_lines = len(lines)
    i = 0
    
    while i < total_lines:
        line = lines[i].strip()
        
        # 1. DETECT START OF TABLE BLOCK
        # Strict rule: Line must start and end with a pipe character
        if line.startswith('|') and line.endswith('|'):
            
            # --- Capture Context (Pre-Table) ---
            # Look back ~20 lines to ensure we get the full 200 chars
            start_ctx_idx = max(0, i - 20)
            # Join lines with newline to preserve structure
            full_pre_text = "\n".join(lines[start_ctx_idx:i])
            # Slice strict last 200 chars
            pre_context = full_pre_text[-200:]

            # --- Capture The Full Block ---
            raw_grid = []
            j = i
            while j < total_lines:
                curr_line = lines[j].strip()
                
                # Check if continuity exists
                if curr_line.startswith('|') and curr_line.endswith('|'):
                    # RAW SPLIT:
                    # 1. Strip outer pipes using strip('|')
                    # 2. Split by internal pipes
                    # 3. Strip whitespace from individual cells
                    # Result: ['Year Ended', '', '2023', ...]
                    row_cells = [c.strip() for c in curr_line.strip('|').split('|')]
                    raw_grid.append(row_cells)
                    j += 1
                else:
                    break
            
            # Update main index `i` to jump over this block
            i = j
            
            # --- Capture Context (Post-Table) ---
            # Look forward ~10 lines
            end_ctx_idx = min(total_lines, j + 10)
            full_post_text = "\n".join(lines[j:end_ctx_idx])
            # Slice strict first 100 chars
            post_context = full_post_text[:100]

            # --- Store Result ---
            # Filter out tiny blocks (e.g. single lines of |---|) that are likely noise
            if len(raw_grid) > 1:
                extracted_data.append({
                    "grid": raw_grid,          # List of Lists
                    "pre_context": pre_context,
                    "post_context": post_context
                })
                print(f"   > Found Block {len(extracted_data)}: {len(raw_grid)} rows.")
        
        else:
            i += 1

    print(f"   > Total blocks extracted: {len(extracted_data)}")
    return extracted_data 

# --- Cache Management ---
def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"   > [Cache] Error reading cache: {e}")
    return {}

def save_cache(new_data: dict):
    """
    Updates the cache with new data. preserves existing keys if not overwritten.
    """
    current_data = load_cache()
    current_data.update(new_data)
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(current_data, f, indent=2)
        print(f"   > [Cache] Updated {CACHE_FILE}")
    except Exception as e:
        print(f"   > [Cache] Error saving cache: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    # 0. Load File
    if not os.path.exists(INPUT_FILE):
        print(f"File {INPUT_FILE} not found.")
        exit()
        
    print(f"Reading file: {INPUT_FILE}...")
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        full_content = f.read()

    # --- Step 0: Check Cache ---
    cache = load_cache()
    toc = cache.get("toc")
    page_regex = cache.get("page_regex")

    # --- Step 1: TOC ---
    if not toc:
        toc = extract_toc(deepseek_agent, full_content)
        if toc:
            # Save immediately if successful
            save_cache({"toc": toc})
        else:
            print("CRITICAL: TOC extraction failed. Stopping.")
            exit()
    else:
        print(f"   > Loaded {len(toc)} TOC items from cache.")

    # --- Step 2: Page Regex ---
    if not page_regex:
        page_regex = extract_page_number_format(deepseek_agent, full_content)
        
        if page_regex:
            # Save immediately ONLY if validation passed
            save_cache({"page_regex": page_regex})
        else:
            # STOP HERE. Do not guess.
            print("\nCRITICAL: Page regex extraction failed validation.")
            print("Action Required: Check the 'context_chunk' in Step 2 or refine the prompt.")
            print("We will NOT save this result to cache.")
            exit()
    else:
        print(f"   > Loaded Regex from cache: {page_regex}")    

    # 3. Retrieve from TOC the page of an item and slice the filing to get actutal text
    # Get the start page and end page of an item (e.g. Item 8) from TOC (note that if Item 8 start from 30, bcs the page number is foot note, we should extract with start page = 29)
    TARGET_ITEM = "Item 8"
    
    target_start_page = None
    target_end_page = None

    for i, entry in enumerate(toc):
        item_label = entry.get("item", "").strip()
        
        if TARGET_ITEM in item_label:
            try:
                target_start_page = int(entry.get("page"))
                
                # Determine end page based on the NEXT item
                if i + 1 < len(toc):
                    target_end_page = int(toc[i+1].get("page"))
                else:
                    print(f"   > {TARGET_ITEM} is the last item. Cannot determine distinct end page from TOC.")
                    target_end_page = None 
                
                print(f"   > Target: {TARGET_ITEM} (Page {target_start_page} to {target_end_page})")
                break
            except ValueError:
                print(f"   > Error parsing page number for {item_label}")

    # Slice and return the Item 8 text
    if target_start_page is not None and target_end_page is not None:
        slicing_start_marker = None
        if target_start_page > 1:
            slicing_start_marker = target_start_page - 1
        else:
            slicing_start_marker = None

        slicing_end_marker = target_end_page - 1

        # Execute Slice
        item_content = slice_filing_by_page_num(
            full_content, 
            page_regex, 
            start_marker=slicing_start_marker, 
            end_marker=slicing_end_marker
        )

        if item_content:
            print(f"\n--- Extraction Successful ({len(item_content)} chars) ---")
            print(item_content[:500])
            print("-----")
            print(item_content[-500:])
        else:
            print("CRITICAL: Slicing failed.")
            exit()
    else:
        print(f"Could not find boundaries for {TARGET_ITEM}")

    # Step 4
    if item_content:
        print(f"\n--- Extraction Successful ({len(item_content)} chars) ---")
        
        # --- Step 4: Extract Raw Grids ---
        raw_blocks = extract_raw_tables(item_content)
        
        if raw_blocks:
            print(f"\n--- Saving {len(raw_blocks)} Raw Grids to CSV ---")
            
            output_dir = "HSDB/extracted_tables/GOOGL/2025"
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            for idx, block in enumerate(raw_blocks):
                grid = block['grid']
                df = pd.DataFrame(grid)
                
                csv_filename = f"{output_dir}/table_{idx + 1}_raw.csv"
                df.to_csv(csv_filename, index=False, header=False, encoding='utf-8')
                
                # Save Context Metadata
                txt_filename = f"{output_dir}/table_{idx + 1}_context.txt"
                with open(txt_filename, "w", encoding="utf-8") as f:
                    f.write(f"PRE-TABLE CONTEXT (200 chars):\n{'-'*30}\n")
                    f.write(block['pre_context'])
                    f.write(f"\n\nPOST-TABLE CONTEXT (100 chars):\n{'-'*30}\n")
                    f.write(block['post_context'])
                
                print(f"   > Saved: {csv_filename}")

        else:
            print("   > No table blocks found in this section.")