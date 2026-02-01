import json
import re
from pathlib import Path
from typing import Dict, Any, Optional, List

# --- INTERNAL IMPORTS ---
from heuristic_process.extract_filings import _query_model, extract_filing_item

# ==============================================================================
# TRACK A: QUANTITATIVE EXTRACTOR (Segments from R-Files)
# ==============================================================================

def identify_segment_sources(metadata: Dict) -> Dict[str, Optional[str]]:
    """
    Uses LLM to select up to TWO distinct source files:
    1. One for Operating/Product Segments.
    2. One for Geographic Information.
    """
    # 1. Filter Potential Candidates
    candidates = []
    for f in metadata.get('saved_files', []):
        if f.get('document_type') == 'HTML' or "details" in (f.get('purpose') or "").lower():
            candidates.append(f"{f['saved_as']} | {f.get('purpose', 'N/A')} | {f.get('description', 'N/A')}")

    if not candidates:
        return {"product_source": None, "geo_source": None}

    candidates_str = "\n".join(candidates[:150])

    prompt = """
    You are an expert SEC filing analyzer.
    Identify the best source files for TWO distinct categories of segment data.
    
    INPUT CANDIDATES:
    {candidate_list}

    TASKS:
    1. PRODUCT_SOURCE: Find the table showing "Operating Segments" (Revenue/Income by Business Unit).
       - Keywords: "Segment Information", "Results by Segment", "Reportable Segments".
    2. GEO_SOURCE: Find the table showing "Geographic Information" (Revenue by Country/Region).
       - Keywords: "Geographic Information", "Revenue by Geography".
       - Note: Often the same file as PRODUCT_SOURCE. If so, return the same filename.

    OUTPUT JSON FORMAT:
    {{
        "product_source": "filename_or_null",
        "geo_source": "filename_or_null"
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
        
        # Validate existence
        valid_files = [c.split(" | ")[0] for c in candidates]
        
        p_src = selection.get("product_source")
        if p_src not in valid_files: p_src = None
        
        g_src = selection.get("geo_source")
        if g_src not in valid_files: g_src = None

        return {"product_source": p_src, "geo_source": g_src}

    except Exception as e:
        print(f"   [WARN] Selector failed: {e}")
        return {"product_source": None, "geo_source": None}

def _extract_segments_from_content(content: str, fiscal_year: int, mode: str = "BOTH") -> Dict[str, Any]:
    """
    Helper to run the extraction prompt on specific content.
    mode: 'BOTH', 'PRODUCT_ONLY', 'GEO_ONLY'
    """
    # Adjust prompt focus based on mode
    focus_instruction = ""
    if mode == "PRODUCT_ONLY":
        focus_instruction = "Focus ONLY on 'product_segments'. Return empty list for geographic."
    elif mode == "GEO_ONLY":
        focus_instruction = "Focus ONLY on 'geographic_segments'. Return empty list for product."
    
    context = content[:15000] # Limit context

    prompt = f"""
    You are a forensic accountant fixing a broken Markdown table from a 10-K filing.
    TARGET FISCAL YEAR: {fiscal_year}

    TASK:
    Extract structured data. {focus_instruction}
    
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
    - Ignore "Total", "Eliminations", "Corporate".
    - 'revenue_amount' is REQUIRED.
    """

    try:
        response_str = _query_model(prompt, context, system_msg="You are a precise data extractor.")
        clean_json = re.sub(r"```json\n|```", "", response_str).strip()
        return json.loads(clean_json)
    except Exception:
        return {}

def get_segment_data_from_metadata(
    ticker: str, 
    metadata: Dict, 
    fiscal_year: int
) -> Dict[str, Any]:
    """
    Step 1: Locate and reconstruct Quantitative Segment Tables (Dual Source Support).
    """
    print(f"--- [Track A] Extracting Segment Data for {ticker} (FY {fiscal_year}) ---")
    
    # 1. IDENTIFY SOURCES
    sources = identify_segment_sources(metadata)
    p_file = sources.get("product_source")
    g_file = sources.get("geo_source")
    
    print(f"   > Product Source: {p_file}")
    print(f"   > Geo Source:     {g_file}")

    if not p_file and not g_file:
        print("   [WARN] No segment files identified.")
        return {}

    final_data = {"product_segments": [], "geographic_segments": []}
    
    # 2. RESOLVE PATHS
    base_path = Path(metadata.get('_source_path', '.'))

    # 3. EXTRACTION STRATEGY
    
    # Case A: Same File (or only one exists)
    if p_file == g_file and p_file is not None:
        path = base_path / p_file
        if path.exists():
            content = path.read_text(encoding='utf-8')
            data = _extract_segments_from_content(content, fiscal_year, mode="BOTH")
            final_data = data

    # Case B: Distinct Files
    else:
        # Extract Product
        if p_file:
            path = base_path / p_file
            if path.exists():
                print("   > Extracting Product Data...")
                content = path.read_text(encoding='utf-8')
                data = _extract_segments_from_content(content, fiscal_year, mode="PRODUCT_ONLY")
                final_data["product_segments"] = data.get("product_segments", [])

        # Extract Geo
        if g_file:
            path = base_path / g_file
            if path.exists():
                print("   > Extracting Geographic Data...")
                content = path.read_text(encoding='utf-8')
                data = _extract_segments_from_content(content, fiscal_year, mode="GEO_ONLY")
                final_data["geographic_segments"] = data.get("geographic_segments", [])

    prod_count = len(final_data.get('product_segments', []) or [])
    geo_count = len(final_data.get('geographic_segments', []) or [])
    print(f"   [Track A] Success: Found {prod_count} Products, {geo_count} Regions.")
    
    return final_data

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