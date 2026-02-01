import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, List

# --- INTERNAL IMPORTS ---
from heuristic_process.extract_filings import _query_model, extract_filing_item

# ==============================================================================
# TRACK A: QUANTITATIVE EXTRACTOR (Segments from R-Files)
# ==============================================================================

def identify_segment_files(metadata: Dict) -> List[str]:
    """
    Uses LLM to select ALL relevant R-files for segment data.
    Prioritizes both "Summary" tables and "Detail" breakdowns.
    """
    # 1. Filter Potential Candidates
    candidates = []
    for f in metadata.get('saved_files', []):
        if f.get('document_type') == 'HTML' or "details" in (f.get('purpose') or "").lower():
            # Create a rich description for the LLM
            candidates.append(f"{f['saved_as']} | {f.get('purpose', 'N/A')} | {f.get('description', 'N/A')}")

    if not candidates:
        return []

    candidates_str = "\n".join(candidates[:150])

    prompt = """
    You are an expert SEC filing analyzer.
    Identify ALL files relevant to **Operating Segments** (Product/Service) and **Geographic Segments**.
    
    CRITERIA:
    1. **Include Summaries:** Main "Segment Information" notes (often containing Operating Income).
    2. **Include Details:** Tables labeled "Revenue details", "Disaggregated Revenue", or "Results by Component".
    3. **Avoid Duplicates:** If two files look identical, pick the one with "Details".
    4. **Limit:** Select the top 1-3 most relevant files.
    
    INPUT CANDIDATES:
    {candidate_list}

    OUTPUT JSON FORMAT:
    {{
        "selected_files": ["filename1", "filename2"]
    }}
    """

    try:
        response = _query_model(
            prompt.format(candidate_list=candidates_str), 
            context="", 
            system_msg="You are a precise file selector. Return JSON only."
        )
        clean_json = re.sub(r"```json\n|```", "", response).strip()
        selection = json.loads(clean_json)
        
        # Validation: Ensure returned files exist in our candidate list
        valid_filenames = [c.split(" | ")[0] for c in candidates]
        cleaned_selection = []
        for f in selection.get("selected_files", []):
            if f in valid_filenames:
                cleaned_selection.append(f)
        
        return cleaned_selection

    except Exception as e:
        print(f"   [WARN] File Selector failed: {e}")
        return []

def _extract_segments_from_context(context: str, fiscal_year: int) -> Dict[str, Any]:
    """
    Extracts structured segment data from a (potentially combined) text context.
    """
    prompt = f"""
    You are a forensic accountant extracting financial data from 10-K notes.
    The context may contain multiple tables (Summary, Details, Geography).

    TARGET FISCAL YEAR: {fiscal_year}
    
    TASK:
    Consolidate the information to extract:
    1. "product_segments": Operating Segments (Revenue, Operating Income, Assets).
       - Use "Operating Income" from summary tables if available.
       - Use granular breakdown from "Details" tables if available.
    2. "geographic_segments": Geographic Regions (Revenue).

    OUTPUT JSON SCHEMA:
    {{
        "product_segments": [
            {{ "segment_name": "Cloud", "revenue_amount": 10230.5, "operating_income": 400.0, "assets": 5000.0 }}
        ],
        "geographic_segments": [
            {{ "region": "North America", "revenue_amount": 50000.0 }}
        ]
    }}

    RULES:
    - Ignore "Total", "Eliminations", "Corporate", "Consolidated".
    - Convert numbers to pure Floats. 
    - 'revenue_amount' is REQUIRED.
    """

    try:
        # We allow a larger context window since we might have concatenated files
        response_str = _query_model(prompt, context[:25000], system_msg="You are a precise data extractor.")
        clean_json = re.sub(r"```json\n|```", "", response_str).strip()
        return json.loads(clean_json)
    except Exception as e:
        print(f"   [ERR] Extraction failed: {e}")
        return {}

def get_segment_data_from_metadata(
    ticker: str, 
    metadata: Dict, 
    fiscal_year: int
) -> Dict[str, Any]:
    """
    Step 1: Locate and reconstruct Quantitative Segment Tables (Multi-Source).
    """
    print(f"--- [Track A] Extracting Segment Data for {ticker} (FY {fiscal_year}) ---")
    
    # 1. IDENTIFY RELEVANT FILES
    target_files = identify_segment_files(metadata)
    
    if not target_files:
        print("   [WARN] No segment files identified.")
        return {}
    
    print(f"   > Selected Source(s): {target_files}")

    # 2. AGGREGATE CONTENT
    base_path = Path(metadata.get('_source_path', '.'))
    combined_context = ""
    
    for filename in target_files:
        path = base_path / filename
        if path.exists():
            file_content = path.read_text(encoding='utf-8')
            # Add headers to help LLM distinguish files
            combined_context += f"\n\n--- SOURCE FILE: {filename} ---\n\n"
            combined_context += file_content
    
    if not combined_context:
        print("   [ERR] Failed to read content from selected files.")
        return {}

    # 3. UNIFIED EXTRACTION
    print("   > Extracting consolidated data from combined context...")
    data = _extract_segments_from_context(combined_context, fiscal_year)
    
    prod_count = len(data.get('product_segments', []) or [])
    geo_count = len(data.get('geographic_segments', []) or [])
    print(f"   [Track A] Success: Found {prod_count} Products, {geo_count} Regions.")
    
    return data


# ==============================================================================
# TRACK B: QUALITATIVE EXTRACTOR (Context from Text)
# ==============================================================================

def extract_business_context(filing_content: str) -> Dict[str, Any]:
    """
    Step 2: Extract qualitative business context from Item 1 (Business).
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