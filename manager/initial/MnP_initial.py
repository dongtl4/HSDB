import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

# --- INTERNAL IMPORTS ---
# Corrected import based on your schema file
from schema.market_and_product_schema import (
    MarketProductFacet, 
    ProductSegment, 
    GeographicSegment,
    MarketPosition, 
    BusinessCharacteristics
)
from utils.fetching import DEFAULT_ROOT, FILINGS_DIR_NAME, FOLDER_MAP, iter_filing_metadata
from heuristic_process.MnP_heuristic_fetching import (
    get_segment_data_from_metadata, extract_business_context
)

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

FACET_NAME = "MARKET_PRODUCT"

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
def build_mnp_snapshot(ticker: str, fiscal_year: int) -> Tuple[Optional[MarketProductFacet], Optional[str]]:
    """
    Returns:
        Tuple containing:
        1. The populated Facet Object (or None)
        2. The Filing Date of the 10-K (used as valid_from date)
    """
    print(f"--- Building Market & Product Snapshot for {ticker} (FY {fiscal_year}) ---")

    # 1. LOCATE FILING (10-K Only)
    all_filings = iter_filing_metadata(ticker, "10-K", root=DEFAULT_ROOT)
    meta_10k = next((f for f in all_filings if f.get('fiscal_year') == fiscal_year), None)

    if not meta_10k:
        print(f"[WARN] No 10-K found for {ticker} FY{fiscal_year}. Aborting.")
        return None, None
    
    filing_date = meta_10k.get('filing_date')

    # 2. EXTRACT RAW DATA
    # Track A: Quantitative (Segments)
    track_a_data = get_segment_data_from_metadata(ticker, meta_10k, fiscal_year)

    # Track B: Qualitative (Context)
    txt_10k = _load_primary_content(meta_10k)
    track_b_data = extract_business_context(txt_10k) if txt_10k else {}

    # 3. MAP TO SCHEMA
    
    # A. Product Segments
    p_segments = []
    for item in track_a_data.get('product_segments', []):
        # Strict mapping: only include fields defined in ProductSegment
        p_segments.append(ProductSegment(
            segment_name=item.get('segment_name', 'Unknown'),
            revenue_amount=item.get('revenue_amount', 0.0), # Required field
            operating_income=item.get('operating_income'),
            assets=item.get('assets')
        ))

    # B. Geographic Segments
    g_segments = []
    for item in track_a_data.get('geographic_segments', []):
        g_segments.append(GeographicSegment(
            region=item.get('region', 'Unknown'),
            revenue_amount=item.get('revenue_amount', 0.0) # Required field
        ))

    # C. Market Position
    mp_raw = track_b_data.get('market_position', {})
    market_pos = MarketPosition(
        competitors=mp_raw.get('competitors', []),
        major_customers=mp_raw.get('major_customers', []),
        top_customer_revenue_percent=mp_raw.get('top_customer_revenue_percent'),
        government_contract_dependency=mp_raw.get('government_contract_dependency')
    )

    # D. Business Characteristics
    bc_raw = track_b_data.get('business_characteristics', {})
    biz_char = BusinessCharacteristics(
        is_seasonal=bc_raw.get('is_seasonal'),
        seasonality_desc=bc_raw.get('seasonality_desc'),
        employees_total=bc_raw.get('employees_total'),
        # Note: backlog_amount is in schema but not currently extracted by heuristics, defaulting to None
        backlog_amount=None, 
        significant_raw_materials=bc_raw.get('significant_raw_materials', []),
        distribution_channels=bc_raw.get('distribution_channels', [])
    )

    # E. Construct Final Facet (Corrected Class Name and Fields)
    facet = MarketProductFacet(
        product_segments=p_segments,
        geographic_segments=g_segments,
        market_position=market_pos,
        business_characteristics=biz_char
    )

    return facet, filing_date

# --- DATABASE PERSISTENCE ---
def save_snapshot_to_db(ticker: str, fiscal_year: int, filing_date: str, facet_data: MarketProductFacet):
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
        # Metadata like source and date are stored here in the DB columns
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
    facet, filing_date = build_mnp_snapshot(target_ticker, target_year)
    
    if facet:
        # 2. Preview
        print("\n--- GENERATED FACET DATA ---")
        print(facet.model_dump_json(indent=2))
        
        # 3. Save
        save_snapshot_to_db(target_ticker, target_year, filing_date, facet)