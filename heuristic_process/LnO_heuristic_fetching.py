import os
import re
import json
import tiktoken
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

# Internal imports for the test suite
from utils.gather_requirement_LnO import find_anchor_10k, find_secondary_anchor, find_context_filings
from utils.trigger_filter import filter_for_deepseek_usage
from utils.fetching import DEFAULT_ROOT, FOLDER_MAP, FILINGS_DIR_NAME

load_dotenv()

client = OpenAI(
    api_key=os.getenv('DEEPSEEK_API_KEY'), 
    base_url="https://api.deepseek.com"
)

# --- HELPER: TOKEN-AWARE TRUNCATION ---
def _truncate_to_token_limit(text: str, limit: int = 120000) -> str:
    """
    Truncates text to fit within DeepSeek's context window (128k).
    """
    try:
        encoding = tiktoken.get_encoding("cl100k_base")
        tokens = encoding.encode(text)
        if len(tokens) <= limit: return text
        print(f"[WARN] Input too long ({len(tokens)} tokens). Truncating to {limit} tokens.")
        return encoding.decode(tokens[:limit])
    except NameError:
        return text[:limit*4]

# --- HELPER: GENERIC API CALL ---
def _query_deepseek(context_text: str, system_prompt: str, user_instruction: str, 
                   form_type: str = "generic") -> Dict[str, Any]:
    
    if not context_text: return {}

    # 1. Apply Safety Filter (Facet-Aware)
    safe_text = filter_for_deepseek_usage(context_text, form_type)
    
    # 2. Apply Token Truncation
    final_text = _truncate_to_token_limit(safe_text)

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"DOCUMENT CONTEXT:\n{final_text}"}, 
                {"role": "user", "content": user_instruction}
            ],
            response_format={"type": "json_object"},
            temperature=0.0 
        )
        content = response.choices[0].message.content
        return json.loads(content) if content else {}
    except Exception as e:
        print(f"[API ERROR] {e}")
        return {}

# --- FETCHING FUNCTIONS ---

def fetching_from_DEF14A(filing_text: str) -> Dict[str, Any]:
    """
    Extracts Governance Risk and Compensation Signals from the Proxy Statement (DEF 14A).
    """
    system_prompt = (
        "You are an expert financial analyst parsing an SEC DEF 14A (Proxy Statement). "
        "Extract structured data strictly according to the requested JSON schema. "
        "Output must be valid JSON."
    )
    
    # Detailed instructions restored:
    user_instruction = """
    Analyze the Proxy Statement and extract the following JSON object. 
    If a value is not explicitly stated, return null.

    {
        "board_structure": {
            "total_board_size": int | null, 
            // INSTRUCTION: Count the total number of director nominees listed in the 'Election of Directors' table.
            
            "independent_director_count": int | null, 
            // INSTRUCTION: Count how many nominees are explicitly identified as 'Independent' in the table or their bios.
            
            "is_ceo_chairman_combined": boolean | null, 
            // INSTRUCTION: Check the 'Board Leadership Structure' section. True if the CEO also holds the title of 'Chairman'. False if there is a separate 'Independent Chairman' or 'Lead Independent Director' acts as chair.
            
            "classified_board": boolean | null, 
            // INSTRUCTION: True if directors are divided into classes (Class I, II, III) with staggered terms. False if all directors are elected annually.
            
            "independent_director_ratio": float | null 
            // INSTRUCTION: Calculate (Independent Directors / Total Directors). Return as decimal (e.g., 0.85).
        },
        "insider_alignment": {
            "executive_ownership_percent": float | null, 
            // INSTRUCTION: Locate the table 'Security Ownership of Certain Beneficial Owners and Management'. Extract the total % for 'All executive officers and directors as a group'.
            
            "pledged_shares_flag": boolean | null 
            // INSTRUCTION: Search footnotes of ownership table or 'Compensation Discussion and Analysis' for 'pledge'. True if any shares are pledged as collateral.
        },
        "pay_ratio_ceo_to_median": int | null 
        // INSTRUCTION: Locate the 'CEO Pay Ratio' section. Extract the ratio (e.g., if 254:1, return 254).
    }
    """
    
    return _query_deepseek(filing_text, system_prompt, user_instruction, "DEF 14A")


def fetching_from_10K(filing_text: str) -> Dict[str, Any]:
    """
    Extracts Operational Efficiency and supplementary Governance data from 10-K.
    """
    system_prompt = (
        "You are an expert financial analyst parsing an SEC 10-K. "
        "Focus on Item 1 (Business), Item 7 (MD&A), and Item 8 (Financials). "
        "Output valid JSON."
    )
    
    # Detailed instructions restored:
    user_instruction = """
    Analyze the 10-K and extract the following JSON object.
    
    {
        "workforce_dynamics": {
            "total_employees": int | null, 
            // INSTRUCTION: Search Item 1 'Business' or 'Human Capital' for the total count of full-time employees.
            
            "fiscal_year_revenue_mm": float | null,
            // INSTRUCTION: Search 'Consolidated Statements of Income'. Extract Total Revenue for the most recent fiscal year. Convert to Millions (e.g., if $3,000,000,000, return 3000.0).
            
            "yoy_headcount_change": float | null, 
            // INSTRUCTION: If the text mentions the % increase/decrease in employees compared to prior year, return as decimal (e.g. 0.05). Else null.
            
            "total_revenue_mm": float | null 
            // INSTRUCTION: (Duplicate check) Same as fiscal_year_revenue_mm.
        },
        "restructuring_activity": {
            "active_restructuring_program": boolean | null, 
            // INSTRUCTION: True if 'Restructuring charges' or 'Impairment' appear as line items in the Income Statement or are discussed in MD&A.
            
            "last_charge_amount_mm": float | null 
            // INSTRUCTION: The dollar value of restructuring charges (in Millions) for the most recent year.
        },
        "dual_class_structure": boolean | null, 
        // INSTRUCTION: Check the cover page or Item 1A 'Risk Factors'. True if there are Class A/B shares with different voting rights (e.g., 10 votes vs 1 vote).
        
        "cfo_tenure_years": float | null 
        // INSTRUCTION: Locate 'Executive Officers of the Registrant'. Calculate years since the current CFO's start date. If start date not found, return null.
    }
    """
    
    return _query_deepseek(filing_text, system_prompt, user_instruction, "10-K")


def fetching_from_8K(combined_8k_text: str) -> Dict[str, Any]:
    """
    Extracts Event-Driven Stability signals from 8-Ks.
    """
    if not combined_8k_text.strip():
        return {"last_12m_departures": 0, "auditor_change_flag": False, "shareholder_rights_plan": False}

    system_prompt = "You are an expert risk analyst parsing 8-K filings. Output valid JSON."
    
    # Detailed instructions restored:
    user_instruction = """
    Analyze the provided 8-K texts and extract:
    
    {
        "last_12m_departures": int, 
        // INSTRUCTION: Count the number of unique 8-K items classified as 'Item 5.02' that mention the resignation, retirement, or termination of C-Level officers (CEO, CFO, COO, President).
        
        "auditor_change_flag": boolean, 
        // INSTRUCTION: True if any filing contains 'Item 4.01' (Changes in Registrant's Certifying Accountant).
        
        "shareholder_rights_plan": boolean 
        // INSTRUCTION: True if any filing contains 'Item 3.03' and mentions adoption of a 'Shareholder Rights Plan' or 'Poison Pill'.
    }
    """
    
    return _query_deepseek(combined_8k_text, system_prompt, user_instruction, "8-K")


# --- TEST SUITE ---

if __name__ == "__main__":
    import sys
    
    # Ensure we use the configuration from utils.fetching
    # DEFAULT_ROOT is Path("./") and FILINGS_DIR_NAME is "SnP500_filings"

    test_ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    test_year = sys.argv[2] if len(sys.argv) > 2 else "2017"

    def load_primary_content(meta: dict) -> str:
        """
        Locates and reads the primary document using the metadata.
        Prioritizes the pre-calculated '_source_path' from iter_filing_metadata.
        """
        # 1. Determine the folder path
        if '_source_path' in meta:
             # Best practice: Use the path already found by the iterator
            instance_dir = Path(meta['_source_path'])
        else:
            # Fallback: Reconstruct path strictly obeying utils/fetching.py structure
            folder_type = FOLDER_MAP.get(meta['form'], meta['form'])
            # CRITICAL FIX: Include FILINGS_DIR_NAME in the path
            instance_dir = DEFAULT_ROOT / FILINGS_DIR_NAME / meta['ticker'] / folder_type / f"{meta['filing_date']}_{meta['accession_number']}"
        
        # 2. Identify the Primary Document file
        primary_file_info = next((f for f in meta.get('saved_files', []) 
                                if f.get('purpose') == 'Primary Document'), None)
        
        if primary_file_info:
            full_path = instance_dir / primary_file_info['saved_as']
            if full_path.exists():
                with open(full_path, 'r', encoding='utf-8') as f:
                    return f.read()
        return ""

    print(f"--- Starting LnO Heuristic Test for {test_ticker} (FY {test_year}) ---")

    # 1. Locate and process 10-K
    meta_10k = find_anchor_10k(test_ticker, test_year)
    if meta_10k:
        content_10k = load_primary_content(meta_10k)
        if content_10k:
            print(f"[1/3] Processing 10-K for {test_ticker} (FY {test_year})")
            res_10k = fetching_from_10K(content_10k)
            print(json.dumps(res_10k, indent=4))
            
            # 2. Locate and process Proxy (DEF 14A)
            meta_def = find_secondary_anchor(test_ticker, meta_10k['filing_date'])
            if meta_def:
                content_def = load_primary_content(meta_def)
                if content_def:
                    print(f"\n[2/3] Processing DEF 14A")
                    res_def = fetching_from_DEF14A(content_def)
                    print(json.dumps(res_def, indent=4))

                    # 3. Locate and process 8-Ks
                    metas_8k, _ = find_context_filings(test_ticker, meta_def['filing_date'])
                    if metas_8k:
                        print(f"\n[3/3] Processing {len(metas_8k)} 8-K filings...")
                        combined_8k = ""
                        for m8 in metas_8k:
                            c8 = load_primary_content(m8)
                            if c8:
                                combined_8k += f"\n--- FILING: {m8['filing_date']} ---\n{c8}"
                        
                        if combined_8k:
                            res_8k = fetching_from_8K(combined_8k)
                            print(json.dumps(res_8k, indent=4))
                    else:
                        print("\n[3/3] No 8-K filings found in the context window.")
                else:
                    print(f"\n[!] DEF 14A found but content could not be loaded.")
            else:
                print(f"\n[!] DEF 14A not found after 10-K date.")
        else:
             print(f"[!] 10-K metadata found but content file missing.")
    else:
        print(f"[!] No 10-K found for {test_ticker} in {test_year}")