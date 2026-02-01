import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, List

# --- INTERNAL IMPORTS ---
# Assumes extract_filings.py has been updated with the functions we wrote
from heuristic_process.extract_filings import _query_model, extract_filing_item

# ==============================================================================
# TRACK A: QUANTITATIVE EXTRACTOR (Segments from R-Files)
# ==============================================================================

def identify_segment_file(metadata: Dict) -> Optional[str]:
    """
    Uses LLM to select the most appropriate R-file for segment data.
    """
    # 1. Filter Potential Candidates (R-Files usually have document_type='HTML')
    # We include 'purpose' and 'description' to help the LLM decide.
    candidates = []
    for f in metadata.get('saved_files', []):
        # We focus on HTML (tables) and maybe specific Markdown sections
        if f.get('document_type') == 'HTML' or "details" in (f.get('purpose') or "").lower():
            candidates.append(f"{f['saved_as']} | {f.get('purpose', 'N/A')} | {f.get('description', 'N/A')}")

    if not candidates:
        return None

    # Limit candidates to avoid context overflow (though R-lists are usually < 150 items)
    candidates_str = "\n".join(candidates[:150])

    prompt = """
    You are an expert SEC filing analyzer. 
    Your task is to identify the single specific file that contains the "Operating Segment" financial performance table.
    
    CRITERIA FOR SELECTION:
    1.  **Primary Goal:** Find the table showing Revenue and Operating Income (or Adjusted EBITDA) broken down by Business Segment (e.g., "Cloud", "Devices", "Services").
    2.  **Secondary Goal:** If no business segment table exists, find the "Disaggregated Revenue" table. It might be in consolidated income statement table.
    3.  **Keywords to Look For:** "Segment Information", "Results by Segment", "Operating Segments", "Reportable Segments", "Revenue by Product".
    4.  **Avoid:** "Geographic" tables (unless mixed with products), "Eliminations" tables, or generic "Revenue Recognition" text policies.

    EXAMPLES:
    - Candidate: "HTML_R15.md | Details - Segment Information" -> SELECT
    - Candidate: "HTML_R4.md | Details - Revenue" -> CHECK (If R15 doesn't exist, this might be it)
    - Candidate: "HTML_R30.md | Details - Geographic Information" -> AVOID (Unless no product segment file exists)
    - Candidate: "HTML_R2.md | Balance Sheet" -> REJECT

    INPUT LIST:
    {candidate_list}

    INSTRUCTIONS:
    - Analyze the input list.
    - Return ONLY the filename of the best match.
    - If no suitable file is found, return "None".
    """

    response = _query_model(
        prompt.format(candidate_list=candidates_str), 
        context="",  # Context is embedded in prompt for this specific task
        system_msg="You are a precise file selector."
    )

    clean_filename = response.strip().replace("'", "").replace('"', "").split()[0] # basic cleanup
    
    # Validation: Ensure the returned filename actually exists in the list
    for c in candidates:
        if c.startswith(clean_filename):
            return clean_filename
            
    return None

def get_segment_data_from_metadata(
    ticker: str, 
    metadata: Dict, 
    fiscal_year: int
) -> Dict[str, Any]:
    """
    Step 1: Locate and reconstruct the Quantitative Segment Table.
    
    Args:
        ticker (str): Company ticker.
        metadata (Dict): Content of metadata.json for the target 10-K.
        fiscal_year (int): The target fiscal year to extract data for.

    Returns:
        Dict: JSON object containing 'product_segments' and 'geographic_segments'.
    """
    print(f"--- [Track A] Extracting Segment Data for {ticker} (FY {fiscal_year}) ---")
    
    # 1. FIND TARGET FILE (LLM Assisted)
    print("   > Identifying Segment Table via LLM Selector...")
    target_filename = identify_segment_file(metadata)

    if not target_filename or target_filename == "None":
        print("   [WARN] LLM could not identify a Segment R-file.")
        # Optional: Fallback to old heuristic here if desired, or return empty
        return {}

    print(f"   > Selected Target File: {target_filename}")

    # 2. RESOLVE PATH
    if '_source_path' in metadata:
        file_path = Path(metadata['_source_path']) / target_filename
    else:
        file_path = Path(target_filename)

    if not file_path.exists():
        print(f"   [ERR] Segment file not found at: {file_path}")
        return {}

    # 3. SEMANTIC RECONSTRUCTION
    try:
        raw_content = file_path.read_text(encoding='utf-8')
        context = raw_content[:15000]

        prompt = f"""
        You are a forensic accountant fixing a broken Markdown table from a 10-K filing.
        The table structure (rows/columns) may be wrapped or misaligned.

        TARGET FISCAL YEAR: {fiscal_year}
        (Focus on the most recent data column usually found on the left).

        TASK:
        Extract structured data for two categories based on the text provided:
        1. "product_segments": Operating Segments (Revenue, Operating Income, Assets).
        2. "geographic_segments": Geographic Regions (Revenue).

        RULES:
        - Ignore "Total", "Consolidated", "Eliminations", "Corporate" rows.
        - If a value is wrapped to the next line, associate it with the preceding label.
        - Convert all numbers to pure Floats (e.g., (500) -> -500.0).
        - If 'Operating Income' or 'Assets' are not listed, set them to null.
        - 'revenue_amount' is REQUIRED.

        OUTPUT JSON SCHEMA:
        {{
            "product_segments": [
                {{ "segment_name": "Cloud", "revenue_amount": 10230.5, "operating_income": 400.0, "assets": 5000.0 }}
            ],
            "geographic_segments": [
                {{ "region": "North America", "revenue_amount": 50000.0 }}
            ]
        }}
        """

        response_str = _query_model(prompt, context, system_msg="You are a precise data extractor.")
        
        clean_json = re.sub(r"```json\n|```", "", response_str).strip()
        data = json.loads(clean_json)
        
        prod_count = len(data.get('product_segments', []))
        geo_count = len(data.get('geographic_segments', []))
        print(f"   [Track A] Success: Found {prod_count} Products, {geo_count} Regions.")
        
        return data

    except Exception as e:
        print(f"   [ERR] Semantic Reconstruction failed: {e}")
        return {}


# ==============================================================================
# TRACK B: QUALITATIVE EXTRACTOR (Context from Text)
# ==============================================================================

def extract_business_context(filing_content: str) -> Dict[str, Any]:
    """
    Step 2: Extract qualitative business context from Item 1 (Business).
    
    Args:
        filing_content (str): The full raw text of the 10-K/10-Q.

    Returns:
        Dict: JSON object mapping to MarketPosition and BusinessCharacteristics.
    """
    print(f"--- [Track B] Extracting Business Context ---")

    # 1. SPLIT SECTION (Item 1)
    item_1_text = extract_filing_item(filing_content, "1")
    
    if not item_1_text:
        print("   [WARN] Item 1 (Business) not found. Context will be empty.")
        return {}

    # 2. LLM EXTRACTION
    context_chunk = item_1_text[:25000]

    prompt = """
    Analyze the 'Business' section of this 10-K filing.
    Extract the following specific structured data points.

    1. COMPETITION (MarketPosition):
       - List specific named companies mentioned as competitors.
    
    2. SEASONALITY (BusinessCharacteristics):
       - Boolean: Is the business seasonal?
       - Description: Short quote describing the pattern (e.g. "stronger in Q4").
    
    3. WORKFORCE (BusinessCharacteristics):
       - Total number of full-time employees (Integer).
    
    4. CUSTOMERS (MarketPosition):
       - List specific named major customers if mentioned (e.g. "Walmart", "US Govt").
       - Max dependency %: If text says "Customer A accounted for 15% of revenue", extract 15.0.

    5. GOVERNMENT DEPENDENCY (ConcentrationRisk):
       - Boolean: True if the company explicitly states material revenue from government contracts.

    OUTPUT JSON SCHEMA:
    {
        "market_position": {
            "competitors": ["CompA", "CompB"],
            "major_customers": ["CustA"],
            "top_customer_revenue_percent": 15.0,
            "government_contract_dependency": false
        },
        "business_characteristics": {
            "is_seasonal": true,
            "seasonality_desc": "Sales are generally higher in the fourth quarter.",
            "employees_total": 154000,
            "significant_raw_materials": [],
            "distribution_channels": ["Direct-to-consumer", "Retail"]
        }
    }
    """

    try:
        response_str = _query_model(prompt, context_chunk, system_msg="You are a business analyst.")
        
        clean_json = re.sub(r"```json\n|```", "", response_str).strip()
        data = json.loads(clean_json)
        
        print("   [Track B] Success: Business Context extracted.")
        return data

    except Exception as e:
        print(f"   [ERR] Context extraction failed: {e}")
        return {}