import os
import json
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

# --- INTERNAL IMPORTS ---
# We rely on your existing extraction tools
from heuristic_process.extract_filings import extract_filing_item
from utils.llm_helper import query_deepseek

load_dotenv()

# --- HELPER: SMART TABLE LOOKUP ---

def _find_best_table(metadata: Optional[Dict], finger_print_keywords: List[str]) -> str:
    """
    Scans content of ALL HTML_Rx.md files referenced in metadata. 
    Returns the content of the first table that contains ALL the fingerprint keywords.
    """
    if not metadata or 'saved_files' not in metadata or '_source_path' not in metadata:
        return ""
    
    base_path = Path(metadata['_source_path'])
    
    # Filter for HTML markdown files
    candidates = [
        f for f in metadata['saved_files'] 
        if f.get('document_type') == 'HTML' and f.get('saved_as', '').endswith('.md')
    ]

    for file_info in candidates:
        file_path = base_path / file_info['saved_as']
        if not file_path.exists():
            continue
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                
            # Check if all keywords exist in this table (Case Insensitive)
            content_lower = content.lower()
            if all(k.lower() in content_lower for k in finger_print_keywords):
                return content  # Found a match!
        except Exception:
            continue
            
    return ""

# --- COMPONENT EXTRACTORS ---

def _extract_inventory_data(full_10k_text: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Extracts Inventory Breakdown.
    Priority 1: Smart Table Scan (Looking for "Raw materials", "Finished goods" in HTML files).
    Priority 2: Text Scan in Item 8 (Window around "Inventories" header in Notes).
    """
    # 1. Try Smart Table Scan
    table_content = _find_best_table(metadata, ["raw materials", "finished goods"])
    
    source_text = ""
    source_type = ""

    if table_content:
        source_text = table_content
        source_type = "High-Confidence HTML Table"
    else:
        # 2. Fallback: Smart Window in Item 8
        # We need to find the "Inventories" Note, which is deep in Item 8, not at the top.
        item_8 = extract_filing_item(full_10k_text, "8")
        
        if item_8:
            # Heuristic: Find "Inventories" header followed closely by "Raw materials"
            # We look for the pattern and grab a generous window around it.
            # \s+ matches newlines/spaces. .{0,1000} allows for some text between header and data.
            match = re.search(r'(NOTE\s+\d+|Inventories)(?:.|\n){0,1000}(Raw materials)', item_8, re.IGNORECASE)
            
            if match:
                # Grab 500 chars before and 3000 chars after the match
                start_idx = max(0, match.start() - 500)
                end_idx = min(len(item_8), match.end() + 3000)
                source_text = item_8[start_idx:end_idx]
                source_type = "Item 8 Note Text Window"
            else:
                # Last resort fallback: Top of Item 8 (Balance Sheet often has totals)
                source_text = item_8[:15000]
                source_type = "Item 8 Top Snippet"
        else:
            return {
                "raw_materials_value": None,
                "work_in_process_value": None,
                "finished_goods_value": None
            }

    system_prompt = "You are a financial data extractor. Extract exact inventory breakdown values."
    user_instruction = f"""
    CONTEXT SOURCE: {source_type}
    
    Extract the following Inventory Breakdown in JSON format.
    * Values must be in MILLIONS (float).
    * If the text says "$3,402" and the header says "in millions", output 3402.0.
    * If specific component is not found, return null.
    
    Output JSON:
    {{
        "raw_materials_value": float | null,
        "work_in_process_value": float | null,
        "finished_goods_value": float | null
    }}
    """
    
    return query_deepseek(source_text, system_prompt, user_instruction, "10-K")

def _extract_supply_chain_and_ip(item_1_text: str, item_1a_text: str, item_7_text: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Combines Text (Item 1, 1A, 7) + Smart Table Scan for R&D Expenses.
    """
    # 1. R&D Table Scan (Income Statement)
    # Look for table with "Research and development" AND "Net sales" or "Revenue"
    rd_table = _find_best_table(metadata, ["Research and development", "Operating expenses"])
    
    combined_text = (
        f"--- R&D TABLE SOURCE ---\n{rd_table}\n"
        f"--- ITEM 1 (BUSINESS) ---\n{item_1_text[:20000]}\n"
        f"--- ITEM 1A (RISK FACTORS) ---\n{item_1a_text}\n"
        f"--- ITEM 7 (MD&A) ---\n{item_7_text[:20000]}"
    )

    system_prompt = "You are an Operations & Technology analyst. Extract Supply Chain and IP signals."
    user_instruction = """
    Analyze the provided text to extract the following JSON object. Return strictly JSON.

    {
        "supply_chain": {
            "major_suppliers": ["list", "of", "entity_names"], 
            // INSTRUCTION: specific company names identified as sole or major suppliers.
            
            "geographic_dependencies": ["list", "of", "regions"], 
            // INSTRUCTION: regions (e.g., "China", "Taiwan") mentioned as critical manufacturing hubs or risk areas.
            
            "raw_material_volatility_snippet": "string" 
            // INSTRUCTION: Brief quote describing volatile raw materials (e.g. "fluctuations in cobalt prices").
        },
        "intellectual_property": {
            "rd_expenses": float | null, 
            // INSTRUCTION: R&D expense (in Millions) for the most recent year. Look for 'Research and development' line item.
            
            "patents_issued_count": int | null, 
            // INSTRUCTION: Total count of issued patents held.
            
            "patents_pending_count": int | null
             // INSTRUCTION: Total count of pending patent applications.
        }
    }
    """
    return query_deepseek(combined_text, system_prompt, user_instruction, "10-K")

def _extract_ops_infrastructure(item_2_text: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Extracts property metrics.
    Priority 1: Smart Table Scan (Looking for "Square feet", "Owned", "Leased")
    Priority 2: Item 2 Text
    """
    # 1. Property Table Scan
    prop_table = _find_best_table(metadata, ["Square feet", "Owned", "Leased"])
    
    # 2. Combine with Text
    source_text = f"--- TABLE DATA ---\n{prop_table}\n--- TEXT DATA ---\n{item_2_text}"
    
    # If both are empty, return nulls immediately
    if not item_2_text and not prop_table:
         return {
            "total_square_footage": None, "owned_square_footage": None, 
            "leased_square_footage": None, "facilities_count": None
        }

    system_prompt = "You are a Real Estate and Operations analyst. Extract property metrics."
    user_instruction = """
    Analyze Item 2 (Properties) and any attached tables. Extract JSON:
    
    {
        "total_square_footage": float | null,
        "owned_square_footage": float | null,
        "leased_square_footage": float | null,
        "facilities_count": int | null 
        // INSTRUCTION: Count distinct manufacturing plants, data centers, or labs mentioned.
    }
    """
    return query_deepseek(source_text, system_prompt, user_instruction, "10-K")

def _extract_cyber_10k(item_1c_text: str) -> Dict[str, Any]:
    """
    Extracts Cybersecurity posture from Item 1C.
    FIX: Prompt explicitly requests JSON to satisfy API requirements.
    """
    if not item_1c_text:
        return {"cyber_insurance_mentioned": False, "reported_incidents": []}

    system_prompt = "You are a Cybersecurity risk analyst. You must return valid JSON."
    user_instruction = """
    Analyze Item 1C. Extract the following JSON structure:
    {
        "cyber_insurance_mentioned": boolean, 
        // INSTRUCTION: True if 'insurance' coverage for cyber risks is explicitly stated.
        
        "reported_incidents": [
            {"date_reported": "YYYY-MM-DD", "description": "summary string"}
        ]
        // INSTRUCTION: List any material breaches disclosed in this section. If none, return empty list.
    }
    """
    return query_deepseek(item_1c_text, system_prompt, user_instruction, "10-K")

# --- MAIN ENTRY POINTS ---

def fetching_ONT_from_10K(filing_text: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Primary extraction function for 10-K filings.
    """
    print("   [OnT] Segmenting 10-K...")
    
    # 1. Text Segmentation
    item_1 = extract_filing_item(filing_text, "1") or ""
    item_1a = extract_filing_item(filing_text, "1A", min_length=1000) or ""
    item_1c = extract_filing_item(filing_text, "1C", min_length=100) or ""
    
    # FIX: Lower min_length to 100 chars to catch short Item 2 (e.g., "See Schedule X")
    item_2 = extract_filing_item(filing_text, "2", min_length=100) or ""
    
    item_7 = extract_filing_item(filing_text, "7") or ""

    # 2. Parallel Extraction with Smart Table Support
    print("   [OnT] Extracting Inventory...")
    inventory_data = _extract_inventory_data(filing_text, metadata)
    
    print("   [OnT] Extracting Supply Chain & IP...")
    supply_ip_data = _extract_supply_chain_and_ip(item_1, item_1a, item_7, metadata)
    
    print("   [OnT] Extracting Infrastructure...")
    ops_infra_data = _extract_ops_infrastructure(item_2, metadata)
    
    print("   [OnT] Extracting Cyber Posture...")
    cyber_data = _extract_cyber_10k(item_1c)

    # 3. Aggregate
    return {
        "supply_chain": supply_ip_data.get("supply_chain", {}),
        "inventory_breakdown": inventory_data,
        "operational_infrastructure": ops_infra_data,
        "intellectual_property": supply_ip_data.get("intellectual_property", {}),
        "cybersecurity": cyber_data
    }

def fetching_ONT_from_8K(filing_text: str) -> Optional[Dict[str, str]]:
    """
    Extracts material cybersecurity incidents from a single 8-K filing.
    FIX: Prompt explicitly requests JSON.
    """
    # Fast check for Item 1.05 header
    if not re.search(r'(?:ITEM|Item)\s+1\.05', filing_text, re.IGNORECASE):
        return None

    system_prompt = "You are a Cybersecurity risk analyst parsing an 8-K. Return valid JSON."
    user_instruction = """
    Analyze the text for 'Item 1.05 Material Cybersecurity Incidents'.
    Extract JSON:
    {
        "date_reported": "YYYY-MM-DD",
        "description": "Brief summary of the incident disclosed."
    }
    """
    return query_deepseek(filing_text[:10000], system_prompt, user_instruction, "8-K")