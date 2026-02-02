import os
import json
import psycopg2
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

# --- DATABASE PERSISTENCE ---
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

FACET_NAME = "OPS_TECHNOLOGY"

def _save_to_database(ticker: str, fiscal_year: int, filing_date: str, facet_data: OpsTechnologyFacet):
    """
    Persists the extracted OpsTechnologyFacet to the database.
    Uses 'INSERT ... ON CONFLICT DO UPDATE' (Upsert) logic.
    """
    json_data = facet_data.model_dump_json()
    conn = None
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
        if conn:
            conn.rollback()
        print(f"   [ERROR] Database commit failed: {e}")
    finally:
        if conn:
            conn.close()

# --- FILE LOADING ---

def _load_primary_document(metadata: Dict) -> str:
    """Helper to safely load the content of the primary document."""
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

# --- MAIN LOGIC ---

def process_ont_initial(ticker: str, target_year: int, save_db: bool = True) -> Optional[OpsTechnologyFacet]:
    """
    Orchestrates the creation of the Operations & Technology Facet.
    
    1. Extracts baseline from 10-K.
    2. Enriches with 8-K Cyber Incidents.
    3. Validates Schema.
    4. Pushes to Database.
    """
    print(f"--- [OnT Manager] Starting Initialization for {ticker} FY{target_year} ---")

    # --- PHASE 3: 10-K EXTRACTION ---
    
    # 1. Locate the 10-K
    selected_10k = find_anchor_10k(ticker, target_year)
            
    if not selected_10k:
        print(f"   [ERROR] No 10-K found for {ticker} Fiscal Year {target_year}")
        return None

    print(f"   [OnT Manager] Found 10-K: {selected_10k.get('accession_number')} (Filed: {selected_10k.get('filing_date')})")
    filing_date = selected_10k.get('filing_date')

    # 2. Load Content
    content_10k = _load_primary_document(selected_10k)
    if not content_10k:
        print("   [ERROR] 10-K content could not be loaded.")
        return None

    # 3. Execute Extraction
    extracted_data = fetching_ONT_from_10K(content_10k, selected_10k)
    
    # --- PHASE 4: 8-K ENRICHMENT ---
    
    period_end = selected_10k.get('period_of_report')
    
    if period_end:
        try:
            dt_end = datetime.strptime(period_end, "%Y-%m-%d")
            dt_start = dt_end - timedelta(days=365)
            search_start = dt_start.strftime("%Y-%m-%d")
            search_end = period_end
        except ValueError:
            search_start = f"{target_year}-01-01"
            search_end = f"{target_year}-12-31"
    else:
        search_start = f"{target_year}-01-01"
        search_end = f"{target_year}-12-31"

    print(f"   [OnT Manager] Scanning 8-Ks for Cyber Incidents ({search_start} to {search_end})...")
    
    metas_8k = iter_filing_metadata(ticker, "8-K", search_start, search_end)
    cyber_incidents = []

    for m8 in metas_8k:
        content_8k = _load_primary_document(m8)
        if not content_8k: continue
            
        incident = fetching_ONT_from_8K(content_8k)
        if incident:
            print(f"      [!] Found Incident in 8-K {m8['filing_date']}")
            cyber_incidents.append(incident)

    # --- MERGE ---
    if cyber_incidents:
        if extracted_data['cybersecurity'].get('reported_incidents') is None:
             extracted_data['cybersecurity']['reported_incidents'] = []
        
        existing_dates = {i.get('date_reported') for i in extracted_data['cybersecurity']['reported_incidents'] if i}
        
        for inc in cyber_incidents:
            if inc.get('date_reported') not in existing_dates:
                extracted_data['cybersecurity']['reported_incidents'].append(inc)

    # --- VALIDATE & SAVE ---
    try:
        facet_model = OpsTechnologyFacet(**extracted_data)
        print("   [SUCCESS] Operations & Technology Facet constructed.")

        if save_db:
            _save_to_database(ticker, target_year, filing_date, facet_model)
            
        return facet_model
    except Exception as e:
        print(f"   [ERROR] Schema Validation Failed: {e}")
        return None

if __name__ == "__main__":
    import sys
    t = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    y = int(sys.argv[2]) if len(sys.argv) > 2 else 2024
    
    # Run with DB save enabled by default
    result = process_ont_initial(t, y, save_db=True)
    if result:
        print(result.model_dump_json(indent=2))