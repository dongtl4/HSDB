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
    
    # 1. FIND TARGET FILE
    # Filter for file purpose containing "Segment" AND ("Revenue" OR "Information")
    target_filename = None
    for f in metadata.get('saved_files', []):
        purpose = (f.get('purpose') or "").lower()
        if "segment" in purpose and ("revenue" in purpose or "information" in purpose):
            # Prioritize "Details" tables over "Narrative" text
            if "details" in purpose:
                target_filename = f['saved_as']
                break
    
    # Fallback: If no "Details" found, try any segment file
    if not target_filename:
        for f in metadata.get('saved_files', []):
            if "segment" in (f.get('purpose') or "").lower():
                target_filename = f['saved_as']
                break

    if not target_filename:
        print("   [WARN] No dedicated Segment R-file found in metadata.")
        return {}

    # 2. RESOLVE PATH
    # Handles both pre-injected '_source_path' (from Manager) or assumes relative path
    if '_source_path' in metadata:
        file_path = Path(metadata['_source_path']) / target_filename
    else:
        # Fallback: This might fail if strict pathing isn't handled by caller
        print("   [WARN] Metadata missing '_source_path'. Assuming CWD.")
        file_path = Path(target_filename)

    if not file_path.exists():
        print(f"   [ERR] Segment file not found at: {file_path}")
        return {}

    # 3. SEMANTIC RECONSTRUCTION
    try:
        raw_content = file_path.read_text(encoding='utf-8')
        # Limit context to first 15k chars (tables are usually compact)
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
        
        # Strip Markdown and Parse
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
    # Using the generalized splitter we wrote earlier
    item_1_text = extract_filing_item(filing_content, "1")
    
    if not item_1_text:
        print("   [WARN] Item 1 (Business) not found. Context will be empty.")
        return {}

    # 2. LLM EXTRACTION
    # We pass the first 25k chars of Item 1 (usually sufficient for Intro, Seasonality, Competition)
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