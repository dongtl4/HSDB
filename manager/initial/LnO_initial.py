import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, Tuple

# --- INTERNAL IMPORTS ---
from schema.leadership_and_organization_schema import (
    LeadershipOrgFacet, GovernanceRisk, ExecutiveStability, OperationalEfficiency,
    BoardStructure, InsiderAlignment, CSuiteTurnover, CompensationSignals,
    InsiderTradingSummary, WorkforceDynamics, RestructuringActivity
)
from utils.fetching import DEFAULT_ROOT, FILINGS_DIR_NAME, FOLDER_MAP
from utils.gather_requirement_LnO import find_anchor_10k, find_secondary_anchor, find_context_filings
from heuristic_process.LnO_heuristic_fetching import (
    fetching_from_10K, fetching_from_DEF14A, fetching_from_8K
)
from utils.parse_form4 import get_insider_activity_data

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

FACET_NAME = "LEADERSHIP_ORG"

# --- HELPER: FILE LOADING ---
def _load_primary_content(meta: Dict) -> str:
    """Resolves and loads the text content of the primary document from metadata."""
    if not meta: return ""
    
    if '_source_path' in meta:
        instance_dir = Path(meta['_source_path'])
    else:
        folder_type = FOLDER_MAP.get(meta.get('form'), meta.get('form'))
        instance_dir = DEFAULT_ROOT / FILINGS_DIR_NAME / meta['ticker'] / folder_type / f"{meta['filing_date']}_{meta['accession_number']}"

    primary_file = next((f for f in meta.get('saved_files', []) if f.get('purpose') == 'Primary Document'), None)
    
    if primary_file:
        full_path = instance_dir / primary_file['saved_as']
        if full_path.exists():
            with open(full_path, 'r', encoding='utf-8') as f:
                return f.read()
    return ""

def _get_folder_path(meta: Dict) -> Optional[Path]:
    if not meta: return None
    if '_source_path' in meta:
        return Path(meta['_source_path'])
    folder_type = FOLDER_MAP.get(meta.get('form'), meta.get('form'))
    return DEFAULT_ROOT / FILINGS_DIR_NAME / meta['ticker'] / folder_type / f"{meta['filing_date']}_{meta['accession_number']}"

# --- MAIN AGGREGATION LOGIC ---
def build_lno_snapshot(ticker: str, fiscal_year: int) -> Tuple[Optional[LeadershipOrgFacet], Optional[str]]:
    """
    Returns:
        Tuple containing:
        1. The populated Facet Object (or None)
        2. The Filing Date of the 10-K (used as valid_from date)
    """
    print(f"--- Building LnO Snapshot for {ticker} (FY {fiscal_year}) ---")

    # 1. LOCATE FILINGS
    meta_10k = find_anchor_10k(ticker, fiscal_year)
    if not meta_10k:
        print(f"[WARN] No 10-K found for {ticker} FY{fiscal_year}. Aborting.")
        return None, None

    k10_filing_date = meta_10k.get('filing_date')
    meta_proxy = find_secondary_anchor(ticker, k10_filing_date)
    
    meta_8ks, meta_form4s = [], []
    if meta_proxy:
        meta_8ks, meta_form4s = find_context_filings(ticker, meta_proxy['filing_date'])
    else:
        print(f"[WARN] No Proxy found after 10-K. Governance data will be partial.")

    # 2. EXTRACT RAW DATA
    txt_10k = _load_primary_content(meta_10k)
    raw_10k = fetching_from_10K(txt_10k) if txt_10k else {}

    txt_proxy = _load_primary_content(meta_proxy)
    raw_proxy = fetching_from_DEF14A(txt_proxy) if txt_proxy else {}

    combined_8k_txt = ""
    for m in meta_8ks:
        content = _load_primary_content(m)
        if content:
            combined_8k_txt += f"\n--- FILING: {m['filing_date']} ---\n{content}"
    raw_8k = fetching_from_8K(combined_8k_txt)

    form4_paths = []
    for m in meta_form4s:
        p = _get_folder_path(m)
        if p and p.exists(): form4_paths.append(p)
    
    raw_insider = get_insider_activity_data(form4_paths)
    insider_summary = raw_insider.get('summary', {})

    # 3. MAP TO SCHEMA
    
    # A. Governance Risk
    board_data = raw_proxy.get('board_structure') or {}
    board_data['dual_class_structure'] = raw_10k.get('dual_class_structure')

    gov_risk = GovernanceRisk(
        board_structure=BoardStructure(**board_data),
        insider_alignment=InsiderAlignment(**(raw_proxy.get('insider_alignment') or {})),
        shareholder_rights_plan=raw_8k.get('shareholder_rights_plan', False)
    )

    # B. Executive Stability
    c_suite_data = {
        "last_12m_departures": raw_8k.get('last_12m_departures', 0),
        "auditor_change_flag": raw_8k.get('auditor_change_flag', False),
        "cfo_tenure_years": raw_10k.get('cfo_tenure_years')
    }
    
    trading_data = InsiderTradingSummary(
        total_buy_volume_usd=insider_summary.get('total_buy_usd', 0.0),
        total_sell_volume_usd=insider_summary.get('total_sell_usd', 0.0),
        transaction_count=insider_summary.get('transaction_count', 0),
        unique_insiders_count=len(insider_summary.get('unique_insiders', [])),
        net_activity_usd=insider_summary.get('net_activity_usd', 0.0)
    )

    exec_stability = ExecutiveStability(
        c_suite_turnover=CSuiteTurnover(**c_suite_data),
        compensation_signals=CompensationSignals(
            pay_ratio_ceo_to_median=raw_proxy.get('pay_ratio_ceo_to_median'),
            insider_trading=trading_data
        )
    )

    # C. Operational Efficiency
    # Sanitization for Float fields that might return null from LLM
    restruct_raw = raw_10k.get('restructuring_activity') or {}
    if restruct_raw.get('last_charge_amount_mm') is None:
        restruct_raw['last_charge_amount_mm'] = 0.0

    op_efficiency = OperationalEfficiency(
        workforce_dynamics=WorkforceDynamics(**(raw_10k.get('workforce_dynamics') or {})),
        restructuring_activity=RestructuringActivity(**restruct_raw)
    )

    facet = LeadershipOrgFacet(
        governance_risk=gov_risk,
        executive_stability=exec_stability,
        operational_efficiency=op_efficiency
    )

    return facet, k10_filing_date

# --- DATABASE PERSISTENCE ---
def save_snapshot_to_db(ticker: str, fiscal_year: int, filing_date: str, facet_data: LeadershipOrgFacet):
    """
    Saves the snapshot to HSDB.
    FIXED: Uses 'cik' instead of 'id', maps to 'entity_cik', and maps 'valid_from'.
    """
    json_data = facet_data.model_dump_json()
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1. Get Entity CIK (The true Primary Key)
        # Schema: entities(cik, ticker, ...)
        cur.execute("SELECT cik FROM entities WHERE ticker = %s", (ticker,))
        res = cur.fetchone()
        
        if not res:
            print(f"[ERR] Entity {ticker} not found in DB 'entities' table.")
            return
        
        entity_cik = res[0]
        trigger_event = f"10-K FY{fiscal_year}"

        # 2. Insert Snapshot
        # Schema: entity_facet_snapshots(id, entity_cik, facet_name, valid_from, trigger_event, data, ...)
        query = """
            INSERT INTO entity_facet_snapshots 
            (entity_cik, facet_name, valid_from, trigger_event, data, created_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING id;
        """
        
        # Ensure valid_from is a date string or object
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
    target_year = int(sys.argv[2]) if len(sys.argv) > 2 else 2017

    # 1. Build
    facet, filing_date = build_lno_snapshot(target_ticker, target_year)
    
    if facet:
        # 2. Preview
        print("\n--- GENERATED FACET DATA ---")
        print(facet.model_dump_json(indent=2))
        
        # 3. Save
        save_snapshot_to_db(target_ticker, target_year, filing_date, facet)