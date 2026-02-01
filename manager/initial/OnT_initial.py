import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict

# --- INTERNAL MODULES ---
from utils.fetching import iter_filing_metadata
from utils.gather_requirement_LnO import find_anchor_10k
from schema.operation_and_technology_schema import OpsTechnologyFacet
from heuristic_process.OnT_heuristic_fetching import (
    fetching_ONT_from_10K, 
    fetching_ONT_from_8K
)

# Setup Logger
logger = logging.getLogger(__name__)

def _load_primary_document(metadata: Dict) -> str:
    """
    Helper to safely load the content of the primary document (Markdown) 
    identified in the filing metadata.
    """
    if not metadata or '_source_path' not in metadata:
        return ""
    
    base_path = Path(metadata['_source_path'])
    
    # 1. Prioritize "Primary Document"
    primary_file_info = next(
        (f for f in metadata.get('saved_files', []) if f.get('purpose') == 'Primary Document'), 
        None
    )
    
    # 2. Fallback: First .md file that isn't a table (HTML_R)
    if not primary_file_info:
        for f in metadata.get('saved_files', []):
            if f['saved_as'].endswith('.md') and not f['saved_as'].startswith('HTML_R'):
                primary_file_info = f
                break
    
    if primary_file_info:
        file_path = base_path / primary_file_info['saved_as']
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                logger.error(f"Failed to read file {file_path}: {e}")
    
    return ""

def process_ont_initial(ticker: str, target_year: int) -> Optional[OpsTechnologyFacet]:
    """
    Orchestrates the creation of the Operations & Technology Facet for a specific fiscal year.
    
    Process:
    1. Phase 3: Locate 10-K for the target year using shared util and extract baseline data.
    2. Phase 4: Locate 8-Ks within the reporting period and extract Cyber Incidents.
    3. Merge and Validate using the Pydantic Schema.
    """
    print(f"--- [OnT Manager] Starting Initialization for {ticker} FY{target_year} ---")

    # --- PHASE 3: 10-K EXTRACTION ---
    
    # 1. Locate the 10-K (Uses standard logic: matches FY, takes latest filing)
    selected_10k = find_anchor_10k(ticker, target_year)
            
    if not selected_10k:
        print(f"   [ERROR] No 10-K found for {ticker} Fiscal Year {target_year}")
        return None

    print(f"   [OnT Manager] Found 10-K: {selected_10k.get('accession_number')} (Filed: {selected_10k.get('filing_date')})")

    # 2. Load Content
    content_10k = _load_primary_document(selected_10k)
    if not content_10k:
        print("   [ERROR] 10-K content could not be loaded.")
        return None

    # 3. Execute Extraction (Includes Smart Table Lookup via metadata)
    extracted_data = fetching_ONT_from_10K(content_10k, selected_10k)
    
    # --- PHASE 4: 8-K ENRICHMENT (CYBERSECURITY) ---
    
    # Define Enrichment Window: The Fiscal Year
    # We want to catch incidents that happened *during* the reporting period.
    period_end = selected_10k.get('period_of_report') # e.g. "2024-09-30"
    
    if period_end:
        try:
            dt_end = datetime.strptime(period_end, "%Y-%m-%d")
            dt_start = dt_end - timedelta(days=365)
            
            search_start = dt_start.strftime("%Y-%m-%d")
            search_end = period_end
        except ValueError:
            # Fallback if date format is weird
            search_start = f"{target_year}-01-01"
            search_end = f"{target_year}-12-31"
    else:
        # Fallback to Calendar Year if metadata is missing period
        search_start = f"{target_year}-01-01"
        search_end = f"{target_year}-12-31"

    print(f"   [OnT Manager] Scanning 8-Ks for Cyber Incidents ({search_start} to {search_end})...")
    
    metas_8k = iter_filing_metadata(ticker, "8-K", search_start, search_end)
    cyber_incidents = []

    for m8 in metas_8k:
        content_8k = _load_primary_document(m8)
        if not content_8k: 
            continue
            
        incident = fetching_ONT_from_8K(content_8k)
        if incident:
            print(f"      [!] Found Incident in 8-K {m8['filing_date']}")
            cyber_incidents.append(incident)

    # --- MERGE & VALIDATE ---
    
    # Merge 8-K incidents into the Cybersecurity model
    if cyber_incidents:
        if 'reported_incidents' not in extracted_data['cybersecurity'] or extracted_data['cybersecurity']['reported_incidents'] is None:
             extracted_data['cybersecurity']['reported_incidents'] = []
        
        # Avoid duplicates
        existing_dates = {i.get('date_reported') for i in extracted_data['cybersecurity']['reported_incidents'] if i}
        
        for inc in cyber_incidents:
            if inc.get('date_reported') not in existing_dates:
                extracted_data['cybersecurity']['reported_incidents'].append(inc)

    # Validate with Pydantic Schema
    try:
        facet_model = OpsTechnologyFacet(**extracted_data)
        print("   [SUCCESS] Operations & Technology Facet constructed.")
        return facet_model
    except Exception as e:
        print(f"   [ERROR] Schema Validation Failed: {e}")
        # In a real pipeline, you might want to log this but return partial data
        return None

if __name__ == "__main__":
    # Quick Local Test
    import sys
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    y = int(sys.argv[2]) if len(sys.argv) > 2 else 2017
    
    result = process_ont_initial(t, y)
    if result:
        print(result.model_dump_json(indent=2))