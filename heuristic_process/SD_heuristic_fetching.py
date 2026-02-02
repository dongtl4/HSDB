import json
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Internal imports
from utils.gather_requirement import find_anchor_10k, find_context_filings
from utils.fetching import DEFAULT_ROOT, FOLDER_MAP, FILINGS_DIR_NAME
from utils.llm_helper import truncate_to_token_limit, query_deepseek
from heuristic_process.extract_filings import extract_filing_item

load_dotenv()

def fetching_from_10K(filing_text: str) -> Dict[str, Any]:
    """
    Extracts Corporate Strategy, Capital Allocation, and Outlook from 10-K.
    Uses generic 'extract_filing_item' to reliably slice Item 1, 7, and 1A.
    """
    
    # 1. Reuse existing tool to slice the document
    section_1 = extract_filing_item(filing_text, "1", min_length=1000)
    section_7 = extract_filing_item(filing_text, "7", min_length=1000)
    section_1a = extract_filing_item(filing_text, "1A", min_length=100) 
    
    context_text = ""
    found_sections = []
    
    # Build Context for LLM
    if section_1: 
        context_text += f"\n\n--- ITEM 1 (BUSINESS) ---\n{section_1}"
        found_sections.append("Item 1")
    
    if section_7: 
        context_text += f"\n\n--- ITEM 7 (MD&A) ---\n{section_7}"
        found_sections.append("Item 7")
        
    # For Item 1A, we mainly need word count, but we give a snippet to the LLM 
    risk_word_count = 0
    if section_1a:
        risk_word_count = len(section_1a.split())
        context_text += f"\n\n--- ITEM 1A (RISK FACTORS - SNIPPET) ---\n{section_1a[:4000]}"
        found_sections.append("Item 1A")
    
    # Fallback
    if not context_text:
        print("[!] Standard slicing failed. Using raw truncation.")
        context_text = truncate_to_token_limit(filing_text, 15000)

    system_prompt = (
        "You are an expert financial analyst. "
        "Extract text-heavy strategic insights strictly according to the JSON schema. "
        "Preserve the original phrasing of the company's management where possible."
    )
    
    # CRITICAL UPDATE: Improved instructions for strategy pillars
    user_instruction = """
    Analyze the provided 10-K sections and extract the following JSON object.
    
    {
        "corporate_strategy": {
            "stated_strategic_pillars": ["string"], 
            // INSTRUCTION: Identify 3-5 key strategic themes or pillars explicitly discussed in the 'Strategy' subsection.
            // EXAMPLES: "Expand ecosystem of services", "Accelerate innovation in hardware", "Grow emerging market presence".
            // NEGATIVE CONSTRAINT: Do NOT list generic Item 1 section headers like "Products", "Competition", "Employees", or "Manufacturing". 
            // If no specific strategy section exists, summarize the business focus from the Overview.
            
            "business_overview_text": "string | null", 
            // INSTRUCTION: Extract the first 1-2 paragraphs of the 'Overview' or 'General' section describing what the company does.
            
            "strategy_discussion_text": "string | null" 
            // INSTRUCTION: Extract the raw text block under the 'Strategy' or 'Business Strategy' header. 
            // If the text is very long, extract the most critical 2-3 paragraphs.
        },
        "capital_allocation_framework": {
            "capital_allocation_priorities_text": "string | null", 
            // INSTRUCTION: Extract text from MD&A 'Liquidity' section discussing how cash is prioritized (e.g. 'invest in growth, then dividends...').
            
            "dividend_and_buyback_policy_text": "string | null" 
            // INSTRUCTION: Extract text describing the current share repurchase program authorization and dividend policy.
        },
        "forward_looking_guidance": {
            "management_outlook_discussion": "string | null", 
            // INSTRUCTION: Extract text from MD&A section labeled 'Outlook', 'Future Expectations', or 'Trend Information'.
            
            "top_risk_factors_summary": "string | null" 
            // INSTRUCTION: Summarize the top 3 bolded headers from the Risk Factors section (Item 1A).
        }
    }
    """
    
    response = query_deepseek(context_text, system_prompt, user_instruction, "10-K (Strategy)")
    
    # Inject the heuristically calculated word count
    if response and 'forward_looking_guidance' in response:
        response['forward_looking_guidance']['item_1a_risk_factors_word_count'] = risk_word_count
        
    return response

def fetching_from_8K(combined_8k_text: str) -> Dict[str, Any]:
    """
    Extracts Acquisition and Divestiture events from 8-Ks.
    """
    if not combined_8k_text.strip():
        return {"recent_material_acquisitions": [], "recent_material_divestitures": []}

    system_prompt = (
        "You are an expert M&A analyst. "
        "Identify completed acquisitions and divestitures from 8-K filings (Item 2.01). "
        "Output valid JSON."
    )
    
    user_instruction = """
    Analyze the 8-K filings and extract:
    
    {
        "recent_material_acquisitions": [
            {
                "target_name": "string",
                "deal_value_mm": float | null,
                "closing_date": "YYYY-MM-DD",
                "description": "string"
            }
        ],
        "recent_material_divestitures": [
             {
                "asset_name": "string",
                "sale_price_mm": float | null,
                "closing_date": "YYYY-MM-DD",
                "description": "string"
            }
        ]
    }
    
    INSTRUCTIONS:
    - Only include events where the company BOUGHT (Acquisition) or SOLD (Divestiture) a business/asset.
    - Ignore executive changes or earnings releases.
    - Convert deal values to Millions (USD).
    """
    
    return query_deepseek(combined_8k_text, system_prompt, user_instruction, "8-K (M&A)")