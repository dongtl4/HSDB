import os
import json
import psycopg2
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

# --- INTERNAL IMPORTS ---
from schema.strategic_direction_schema import (
    StrategicDirectionFacet,
    CorporateStrategy,
    CapitalAllocation,
    FutureOutlook
)
from utils.fetching import DEFAULT_ROOT, FILINGS_DIR_NAME, FOLDER_MAP
from utils.gather_requirement import find_anchor_10k, find_context_filings
from heuristic_process.SD_heuristic_fetching import fetching_from_10K, fetching_from_8K

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

FACET_NAME = "STRATEGIC_DIRECTION"

# --- HELPER: FILE LOADING ---
def _load_primary_content(meta: Dict) -> str:
    """Resolves and loads the text content of the primary document from metadata."""
    if not meta: return ""
    
    if '_source_path' in meta:
        instance_dir = Path(meta['_source_path'])
    else:
        # Fallback reconstruction of path
        folder_type = FOLDER_MAP.get(meta.get('form'), meta.get('form'))
        instance_dir = DEFAULT_ROOT / FILINGS_DIR_NAME / meta['ticker'] / folder_type / f"{meta['filing_date']}_{meta['accession_number']}"

    primary_file = next((f for f in meta.get('saved_files', []) if f.get('purpose') == 'Primary Document'), None)
    
    if primary_file:
        full_path = instance_dir / primary_file['saved_as']
        if full_path.exists():
            with open(full_path, 'r', encoding='utf-8') as f:
                return f.read()
    return ""

# --- MAIN AGGREGATION LOGIC ---
def build_sd_snapshot(ticker: str, fiscal_year: int) -> Tuple[Optional[StrategicDirectionFacet], Optional[str]]:
    """
    Returns:
        Tuple containing:
        1. The populated Facet Object (or None)
        2. The Filing Date of the 10-K (used as valid_from date)
    """
    print(f"--- Building Strategic Direction Snapshot for {ticker} (FY {fiscal_year}) ---")

    # 1. LOCATE 10-K (Anchor)
    meta_10k = find_anchor_10k(ticker, fiscal_year)
    if not meta_10k:
        print(f"[WARN] No 10-K found for {ticker} FY{fiscal_year}. Aborting.")
        return None, None
    
    filing_date = meta_10k.get('filing_date')

    # 2. LOCATE 8-Ks (Context: M&A Events)
    # We look for 8-Ks in the window leading up to the 10-K
    meta_8ks, _ = find_context_filings(ticker, filing_date)
    
    # 3. EXTRACT CONTENT
    # A. 10-K Strategy & Outlook
    txt_10k = _load_primary_content(meta_10k)
    raw_10k = fetching_from_10K(txt_10k) if txt_10k else {}

    # B. 8-K M&A Events
    combined_8k_txt = ""
    for m in meta_8ks:
        content = _load_primary_content(m)
        if content:
            combined_8k_txt += f"\n--- FILING: {m['filing_date']} ---\n{content}"
    raw_8k = fetching_from_8K(combined_8k_txt)

    # 4. MAP TO SCHEMA
    
    # A. Corporate Strategy
    corp_strat = CorporateStrategy(**raw_10k.get('corporate_strategy', {}))

    # B. Capital Allocation (Merge Text from 10-K + Events from 8-K)
    cap_alloc_data = raw_10k.get('capital_allocation_framework', {})
    cap_alloc_data['recent_material_acquisitions'] = raw_8k.get('recent_material_acquisitions', [])
    cap_alloc_data['recent_material_divestitures'] = raw_8k.get('recent_material_divestitures', [])
    
    cap_alloc = CapitalAllocation(**cap_alloc_data)

    # C. Forward Looking Guidance
    outlook = FutureOutlook(**raw_10k.get('forward_looking_guidance', {}))

    # D. Final Facet
    facet = StrategicDirectionFacet(
        corporate_strategy=corp_strat,
        capital_allocation_framework=cap_alloc,
        forward_looking_guidance=outlook
    )

    return facet, filing_date

# --- DATABASE PERSISTENCE ---
def save_snapshot_to_db(ticker: str, fiscal_year: int, filing_date: str, facet_data: StrategicDirectionFacet):
    """
    Saves the snapshot to HSDB.
    """
    json_data = facet_data.model_dump_json()
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1. Get Entity CIK
        cur.execute("SELECT cik FROM entities WHERE ticker = %s", (ticker,))
        res = cur.fetchone()
        
        if not res:
            print(f"[ERR] Entity {ticker} not found in DB 'entities' table.")
            return
        
        entity_cik = res[0]
        trigger_event = f"10-K FY{fiscal_year}" 
        
        # 2. Insert Snapshot
        query = """
            INSERT INTO entity_facet_snapshots 
            (entity_cik, facet_name, valid_from, trigger_event, data, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING id;
        """
        
        valid_from = filing_date if filing_date else f"{fiscal_year+1}-01-01"

        cur.execute(query, (entity_cik, FACET_NAME, valid_from, trigger_event, json_data))
        new_id = cur.fetchone()[0]
        
        conn.commit()
        print(f"✅ [DB] Snapshot saved for {ticker} (FY{fiscal_year}) | ID: {new_id} | CIK: {entity_cik}")

    except Exception as e:
        print(f"❌ [DB] Error saving snapshot: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

# --- EXECUTION ---
if __name__ == "__main__":
    import sys
    
    target_ticker = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    target_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2023

    # 1. Build
    facet, filing_date = build_sd_snapshot(target_ticker, target_year)
    
    if facet:
        # 2. Preview
        print("\n--- GENERATED FACET DATA ---")
        print(facet.model_dump_json(indent=2))
        
        # 3. Save
        save_snapshot_to_db(target_ticker, target_year, filing_date, facet)