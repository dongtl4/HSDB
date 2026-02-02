import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List

# --- INTERNAL IMPORTS ---
from schema.stakeholder_analysis_schema import (
    StakeholderAnalysisFacet,
    ShareholderDemocracy,
    LegalAndRegulatory,
    LaborRelations,
    CustomerQuality
)

from utils.fetching import DEFAULT_ROOT, FILINGS_DIR_NAME, FOLDER_MAP, iter_filing_metadata
from heuristic_process.SA_heuristic_fetching import (
    fetching_from_10K_SA,
    fetching_from_DEF14A_SA,
    fetching_from_8K_SA,
    count_activist_filings
)

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

FACET_NAME = "STAKEHOLDER"

# --- HELPER: FILE LOADING ---
def _load_primary_content(meta: Dict) -> str:
    """Resolves and loads the text content of the primary document from metadata."""
    if not meta: return ""
    
    # 1. Try to use the source path if it exists (fastest)
    if '_source_path' in meta:
        instance_dir = Path(meta['_source_path'])
    else:
        # 2. Fallback: Reconstruct path strictly obeying utils/fetching.py structure
        folder_type = FOLDER_MAP.get(meta.get('form'), meta.get('form'))
        instance_dir = DEFAULT_ROOT / FILINGS_DIR_NAME / meta['ticker'] / folder_type / f"{meta['filing_date']}_{meta['accession_number']}"

    # 3. Find the file marked as "Primary Document"
    primary_file = next((f for f in meta.get('saved_files', []) if f.get('purpose') == 'Primary Document'), None)
    
    if primary_file:
        full_path = instance_dir / primary_file['saved_as']
        if full_path.exists():
            with open(full_path, 'r', encoding='utf-8') as f:
                return f.read()
    return ""

def _parse_date(date_str: str) -> Optional[datetime]:
    if not date_str: return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None

# --- MAIN AGGREGATION LOGIC ---
def build_sa_snapshot(ticker: str, fiscal_year: int) -> Tuple[Optional[StakeholderAnalysisFacet], Optional[str]]:
    """
    Builds the Stakeholder Analysis snapshot by aggregating data from:
    1. 10-K (Labor, Legal, Quality)
    2. DEF 14A (Shareholder Proposals)
    3. 8-K (Voting Results)
    4. SC 13D (Activist Stakes)
    
    Returns: (FacetObject, FilingDate)
    """
    print(f"--- Building Stakeholder Analysis Snapshot for {ticker} (FY {fiscal_year}) ---")

    # 1. LOCATE ANCHOR 10-K
    # We need the 10-K to establish the "Period of Report" for time-window calculations
    all_10ks = iter_filing_metadata(ticker, "10-K", root=DEFAULT_ROOT)
    meta_10k = next((f for f in all_10ks if str(f.get('fiscal_year')) == str(fiscal_year)), None)

    if not meta_10k:
        print(f"[WARN] No 10-K found for {ticker} FY{fiscal_year}. Aborting.")
        return None, None
    
    filing_date = meta_10k.get('filing_date')
    period_of_report_str = meta_10k.get('period_of_report')
    
    if not period_of_report_str:
        print(f"[WARN] 10-K found but missing 'period_of_report'. Aborting.")
        return None, None

    period_date = _parse_date(period_of_report_str)

    # --- 2. EXTRACT DATA FROM 10-K ---
    # Sources: Item 1 (Labor), Item 3 (Legal), Item 8 Notes (Warranty, Tax)
    content_10k = _load_primary_content(meta_10k)
    data_10k = fetching_from_10K_SA(content_10k) if content_10k else {}

    # --- 3. EXTRACT DATA FROM PROXY (DEF 14A) ---
    # Window: 0 to 120 days after Period End
    proxy_start = period_of_report_str
    proxy_end = (period_date + timedelta(days=120)).strftime("%Y-%m-%d")
    
    proxies = iter_filing_metadata(ticker, "DEF 14A", proxy_start, proxy_end, root=DEFAULT_ROOT)
    data_proxy = {}
    if proxies:
        # Use the first proxy found in the window
        content_proxy = _load_primary_content(proxies[0])
        data_proxy = fetching_from_DEF14A_SA(content_proxy) if content_proxy else {}

    # --- 4. EXTRACT DATA FROM 8-K (VOTING RESULTS) ---
    # Window: 0 to 180 days after Period End (Annual meetings happen later)
    vote_start = period_of_report_str
    vote_end = (period_date + timedelta(days=180)).strftime("%Y-%m-%d")
    
    eight_ks = iter_filing_metadata(ticker, "8-K", vote_start, vote_end, root=DEFAULT_ROOT)
    combined_8k_content = ""
    
    # Heuristic: Only load 8-Ks that likely contain Item 5.07
    # Note: We load them to check content because metadata description isn't always reliable for Items
    for m in eight_ks:
        txt = _load_primary_content(m)
        if "5.07" in txt or "Submission of Matters" in txt:
            combined_8k_content += f"\n--- FILING: {m['filing_date']} ---\n{txt}\n"
    
    data_8k = fetching_from_8K_SA(combined_8k_content) if combined_8k_content else {}

    # --- 5. COUNT ACTIVIST STAKES (SC 13D) ---
    # Window: 1 Year lookback from Period End (The Fiscal Year itself)
    activist_end = period_of_report_str
    activist_start = (period_date - timedelta(days=365)).strftime("%Y-%m-%d")
    activist_count = count_activist_filings(ticker, activist_start, activist_end)

    # --- 6. MAP TO SCHEMA ---
    
    # A. Shareholder Democracy
    democracy = ShareholderDemocracy(
        say_on_pay_support_percent=data_8k.get('say_on_pay_support_percent'),
        director_election_min_support_percent=data_8k.get('director_election_min_support_percent'),
        shareholder_proposals_count=data_proxy.get('shareholder_proposals_count', 0),
        activist_13d_filing_count=activist_count
    )

    # B. Legal & Regulatory
    legal_raw = data_10k.get('legal_and_regulatory', {})
    legal = LegalAndRegulatory(
        active_class_actions_flag=legal_raw.get('active_class_actions_flag', False),
        loss_contingency_accrual_mm=legal_raw.get('loss_contingency_accrual_mm'),
        unrecognized_tax_benefits_mm=legal_raw.get('unrecognized_tax_benefits_mm'),
        environmental_fines_mm=legal_raw.get('environmental_fines_mm', 0.0)
    )

    # C. Labor Relations
    labor_raw = data_10k.get('labor_relations', {})
    labor = LaborRelations(
        unionized_workforce_percent=labor_raw.get('unionized_workforce_percent'),
        work_stoppage_flag=labor_raw.get('work_stoppage_flag', False),
        female_employee_percent=labor_raw.get('female_employee_percent'),
        minority_employee_percent=labor_raw.get('minority_employee_percent'),
        voluntary_turnover_percent=labor_raw.get('voluntary_turnover_percent')
    )

    # D. Customer Quality
    quality_raw = data_10k.get('customer_quality', {})
    quality = CustomerQuality(
        warranty_provision_mm=quality_raw.get('warranty_provision_mm'),
        warranty_liability_mm=quality_raw.get('warranty_liability_mm')
    )

    # E. Construct Final Facet
    facet = StakeholderAnalysisFacet(
        shareholder_democracy=democracy,
        legal_and_regulatory=legal,
        labor_relations=labor,
        customer_quality=quality
    )

    return facet, filing_date

# --- DATABASE PERSISTENCE ---
def save_snapshot_to_db(ticker: str, fiscal_year: int, filing_date: str, facet_data: StakeholderAnalysisFacet):
    """
    Saves the snapshot to HSDB (Postgres).
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
        
        # Fallback date if filing_date missing
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
    facet, filing_date = build_sa_snapshot(target_ticker, target_year)
    
    if facet:
        # 2. Preview
        print("\n--- GENERATED FACET DATA ---")
        print(facet.model_dump_json(indent=2))
        
        # 3. Save
        save_snapshot_to_db(target_ticker, target_year, filing_date, facet)