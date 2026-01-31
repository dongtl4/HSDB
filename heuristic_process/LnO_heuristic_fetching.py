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
from utils.fetching import DEFAULT_ROOT, FOLDER_MAP, FILINGS_DIR_NAME, get_filing_paths, iter_filing_metadata, _load_json

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


# --- TEST SUITE & EXECUTION ---
if __name__ == "__main__":
    # 1. Configuration
    TEST_TICKER = "AAPL"
    TEST_YEAR = 2017 
    HSDB_ROOT = Path("./") 
    
    OUTPUT_FILE = f"test_output/LnO_extraction_{TEST_TICKER}_{TEST_YEAR}.json"
    os.makedirs(Path("./test_output"), exist_ok=True)

    print(f"=== RUNNING HEURISTIC EXTRACTION TEST: {TEST_TICKER} FY{TEST_YEAR} ===")

    # Helper to load file content based on metadata
    def _load_filing_content(meta: Dict, root: Path) -> str:
        if not meta or '_source_path' not in meta:
            # Fallback path reconstruction if _source_path is missing
            # This handles the case where metadata was loaded but path wasn't injected
            # Structure: SnP500_filings/TICKER/FORM/DATE_ACCESSION
            form_map = {"10-K": "10-K", "DEF 14A": "Proxy_Statement", "8-K": "8-K"}
            f_name = f"{meta.get('filing_date')}_{meta.get('accession_number')}"
            folder = root / "SnP500_filings" / meta.get('ticker', TEST_TICKER) / form_map.get(meta.get('form')) / f_name
        else:
            folder = Path(meta['_source_path'])

        if not folder.exists():
            print(f"[ERR] Folder not found: {folder}")
            return ""

        # Find Primary Document
        primary_file = None
        saved_files = meta.get('saved_files', [])
        
        # Priority 1: Explicit "Primary Document" purpose
        for f in saved_files:
            if f.get('purpose') == 'Primary Document':
                primary_file = f.get('saved_as')
                break
        
        # Priority 2: Fallback to any .md or .txt
        if not primary_file:
            for f in folder.glob("*.md"):
                primary_file = f.name
                break

        if primary_file:
            try:
                return (folder / primary_file).read_text(encoding='utf-8')
            except Exception as e:
                print(f"[ERR] Read failed for {primary_file}: {e}")
                return ""
        return ""

    results = {
        "ticker": TEST_TICKER,
        "fiscal_year": TEST_YEAR,
        "extraction_log": [],
        "data": {}
    }

    # 2. Pipeline Execution
    
    # A. 10-K (Anchor)
    print("\n[1/3] Fetching 10-K Data...")
    meta_10k = find_anchor_10k(TEST_TICKER, TEST_YEAR, HSDB_ROOT)
    
    if meta_10k:
        print(f"   -> Found 10-K filed on {meta_10k.get('filing_date')}")
        text_10k = _load_filing_content(meta_10k, HSDB_ROOT)
        
        if text_10k:
            data_10k = fetching_from_10K(text_10k)
            results["data"]["10k_efficiency"] = data_10k
            print("   -> Extraction Complete.")
        else:
            print("   -> [FAIL] Could not load 10-K text.")
    else:
        print("   -> [FAIL] No 10-K found.")

    # B. Proxy (Secondary Anchor)
    print("\n[2/3] Fetching Proxy Data...")
    # Need 10-K date for relative search
    k10_date = meta_10k.get('filing_date') if meta_10k else None
    
    if k10_date:
        meta_proxy = find_secondary_anchor(TEST_TICKER, k10_date, HSDB_ROOT)
        if meta_proxy:
            print(f"   -> Found Proxy filed on {meta_proxy.get('filing_date')}")
            text_proxy = _load_filing_content(meta_proxy, HSDB_ROOT)
            
            if text_proxy:
                data_proxy = fetching_from_DEF14A(text_proxy)
                results["data"]["proxy_governance"] = data_proxy
                print("   -> Extraction Complete.")
            else:
                print("   -> [FAIL] Could not load Proxy text.")
        else:
            print("   -> [WARN] No Proxy found in window.")
    else:
        print("   -> [SKIP] Cannot find Proxy without 10-K date.")

    # C. 8-Ks (Context)
    print("\n[3/3] Fetching 8-K Events...")
    proxy_date = meta_proxy.get('filing_date') if 'meta_proxy' in locals() and meta_proxy else None
    
    if proxy_date:
        list_8ks, _ = find_context_filings(TEST_TICKER, proxy_date, HSDB_ROOT)
        print(f"   -> Found {len(list_8ks)} relevant 8-Ks.")
        
        combined_8k_text = ""
        for m in list_8ks:
            txt = _load_filing_content(m, HSDB_ROOT)
            if txt:
                combined_8k_text += f"\n--- 8-K FILED {m.get('filing_date')} ---\n{txt}\n"
        
        if combined_8k_text:
            data_8k = fetching_from_8K(combined_8k_text)
            results["data"]["8k_stability"] = data_8k
            print("   -> Extraction Complete.")
        else:
            print("   -> [INFO] No text content in 8-Ks (or no 8-Ks found).")
            results["data"]["8k_stability"] = fetching_from_8K("") # Get defaults
    else:
        print("   -> [SKIP] Context search requires Proxy date.")


    # 3. Output
    print("\n" + "="*30)
    print("FINAL EXTRACTION RESULT")
    print("="*30)
    print(json.dumps(results["data"], indent=2))
    
    # Save to file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=4)
        print(f"\nSaved full results to: {OUTPUT_FILE}")