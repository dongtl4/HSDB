import re
from typing import Dict, Any, List

# Internal imports
from heuristic_process.extract_filings import extract_filing_item
from utils.fetching import iter_filing_metadata
from utils.llm_helper import query_deepseek

# --- HELPER: KEYWORD SNIPING ---
def snippet_around_matches(text: str, keywords: List[str], window_size: int = 4000) -> str:
    """
    Finds keywords in a massive text block and extracts windows around them.
    Used to find specific Notes (Tax, Warranty) deep inside Item 8.
    """
    if not text: return ""
    
    text_lower = text.lower()
    indices = []
    
    # 1. Find all keyword hits
    for kw in keywords:
        # Simple string find is faster and usually sufficient for headers
        # We can add regex if needed, but headers are usually consistent
        start = 0
        while True:
            idx = text_lower.find(kw.lower(), start)
            if idx == -1: break
            indices.append(idx)
            start = idx + len(kw)
            
    if not indices:
        return ""

    # 2. Sort and Merge Overlapping Windows
    indices.sort()
    merged_ranges = []
    
    if indices:
        # Initialize first range
        curr_start = max(0, indices[0] - window_size)
        curr_end = min(len(text), indices[0] + window_size)
        
        for idx in indices[1:]:
            start = max(0, idx - window_size)
            end = min(len(text), idx + window_size)
            
            if start < curr_end: # Overlap
                curr_end = max(curr_end, end)
            else:
                merged_ranges.append((curr_start, curr_end))
                curr_start = start
                curr_end = end
        merged_ranges.append((curr_start, curr_end))

    # 3. Extract Text
    snippets = []
    for start, end in merged_ranges:
        snippets.append(f"... {text[start:end]} ...")
        
    return "\n".join(snippets)

# --- 1. ACTIVIST STAKE (SC 13D) COUNTING ---
def count_activist_filings(ticker: str, fiscal_year_start: str, fiscal_year_end: str) -> int:
    try:
        filings = iter_filing_metadata(
            ticker=ticker,
            form="Activist_State", 
            start_date=fiscal_year_start,
            end_date=fiscal_year_end
        )
        return len(filings)
    except Exception as e:
        print(f"[WARN] Failed to count activist filings: {e}")
        return 0

# --- 2. 10-K EXTRACTION (Labor, Legal, Customer, Tax) ---
def fetching_from_10K_SA(filing_text: str) -> Dict[str, Any]:
    if not filing_text: return {}

    # --- A. Item 1: Business & Human Capital (Start of file) ---
    item_1 = extract_filing_item(filing_text, "1", min_length=500) or ""
    # We take the first 40k chars of Item 1, usually enough for Human Capital
    context_item_1 = item_1[:40000]
    
    # --- B. Item 3: Legal Proceedings (Start of file) ---
    item_3 = extract_filing_item(filing_text, "3", min_length=100) or ""
    context_item_3 = item_3[:10000]
    
    # --- C. Item 8: Financial Notes (Deep in file) ---
    # We need "Income Taxes", "Commitments", "Contingencies", "Warranty"
    item_8 = extract_filing_item(filing_text, "8", min_length=1000) or ""
    
    # Use Snippet Logic instead of blind truncation
    target_keywords = [
        "Income Taxes", "Unrecognized Tax Benefits", # For Tax
        "Commitments and Contingencies", "Legal Proceedings", # For Legal Accruals
        "Product Warranty", "Guarantees" # For Warranty
    ]
    context_item_8 = snippet_around_matches(item_8, target_keywords, window_size=3000)
    
    # Fallback: if keywords fail (rare), take the end of Item 8 where notes usually are? 
    # Or just the beginning if the file is weird. 
    if not context_item_8:
        # If no keywords found, fallback to first 30k chars (Tables) + last 30k chars (Notes often at end?)
        # Actually, let's just take a standard chunk if search fails.
        context_item_8 = item_8[:50000]

    combined_context = (
        f"--- ITEM 1 (BUSINESS / HUMAN CAPITAL) ---\n{context_item_1}\n\n"
        f"--- ITEM 3 (LEGAL PROCEEDINGS) ---\n{context_item_3}\n\n"
        f"--- ITEM 8 (NOTES: TAX, LEGAL, WARRANTY) ---\n{context_item_8}"
    )

    system_prompt = (
        "You are an expert financial analyst parsing an SEC 10-K. "
        "Extract strict 'Hard Data' facts for Stakeholder Analysis. "
        "Output strictly valid JSON." # Added JSON requirement explicitly
    )

    user_instruction = """
    Analyze the provided 10-K sections (Item 1, 3, and specific Notes) and extract the following JSON.

    {
        "labor_relations": {
            "unionized_workforce_percent": float | null,
            // INSTRUCTION: Search Item 1 'Human Capital'. Look for % of employees represented by unions/works councils. Return as decimal.
            
            "work_stoppage_flag": boolean,
            // INSTRUCTION: Search Item 1. True if 'strikes' or 'work stoppages' are mentioned as ACTIVE/RECENT events.
            
            "female_employee_percent": float | null,
            // INSTRUCTION: Search Item 1 'Diversity'. Global % of women. Return as decimal.
            
            "minority_employee_percent": float | null,
            // INSTRUCTION: Search Item 1 'Diversity'. % of employees from underrepresented groups. Return as decimal.
            
            "voluntary_turnover_percent": float | null
            // INSTRUCTION: Search Item 1. Look for 'turnover rate' or 'attrition'. Return as decimal.
        },
        "legal_and_regulatory": {
            "active_class_actions_flag": boolean,
            // INSTRUCTION: Search Item 3 'Legal Proceedings'. True if a 'class action' is listed as pending/filed.
            
            "loss_contingency_accrual_mm": float | null,
            // INSTRUCTION: Search Item 8 Notes ('Commitments' or 'Legal'). Find specific 'accrued liability' for legal matters. Convert to Millions.
            
            "unrecognized_tax_benefits_mm": float | null,
            // INSTRUCTION: Search Item 8 Notes 'Income Taxes'. Extract 'Balance at end of year' (or period) for 'Unrecognized Tax Benefits'. Convert to Millions.
            
            "environmental_fines_mm": float
            // INSTRUCTION: Search Item 1 or 3. Extract total penalties/fines paid to environmental agencies. Return 0.0 if none.
        },
        "customer_quality": {
            "warranty_provision_mm": float | null,
            // INSTRUCTION: Search Item 8 Notes 'Product Warranty' or 'Guarantees'. Extract expense 'Provision for warranties' (the cost added this year). Convert to Millions.
            
            "warranty_liability_mm": float | null
            // INSTRUCTION: Search Item 8 Notes. Extract 'Balance at end of period' (Liability). Convert to Millions.
        }
    }
    """

    return query_deepseek(combined_context, system_prompt, user_instruction, "10-K")

# --- 3. PROXY (DEF 14A) EXTRACTION (Shareholder Proposals) ---
def fetching_from_DEF14A_SA(filing_text: str) -> Dict[str, Any]:
    if not filing_text: return {"shareholder_proposals_count": 0}

    # Added "Output JSON" to system prompt to satisfy API
    system_prompt = "You are parsing a Proxy Statement (DEF 14A). Count shareholder proposals. Output strictly valid JSON."
    
    user_instruction = """
    Analyze the Proxy Statement.
    
    {
        "shareholder_proposals_count": int
        // INSTRUCTION: Count proposals submitted by SHAREHOLDERS (often starting at Proposal 4 or 5).
        // Exclude proposals submitted by the Board (e.g., Election of Directors, Auditor, Say on Pay).
    }
    """
    
    return query_deepseek(filing_text, system_prompt, user_instruction, "DEF 14A")

# --- 4. 8-K EXTRACTION (Voting Results) ---
def fetching_from_8K_SA(combined_8k_text: str) -> Dict[str, Any]:
    if not combined_8k_text or not combined_8k_text.strip(): return {}

    system_prompt = "You are parsing 8-K filings for Item 5.07 Voting Results. Output strictly valid JSON."
    
    user_instruction = """
    Analyze the 8-K content for 'Item 5.07 Submission of Matters to a Vote of Security Holders'.
    Extract the following JSON:

    {
        "say_on_pay_support_percent": float | null,
        // INSTRUCTION: 'Advisory vote to approve executive compensation'. Calculate: For / (For + Against + Abstain). Return as decimal.
        
        "director_election_min_support_percent": float | null
        // INSTRUCTION: Calculate approval % for EACH director. Return the LOWEST approval percentage found.
    }
    """
    
    return query_deepseek(combined_8k_text, system_prompt, user_instruction, "8-K")