import os
import json
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from pathlib import Path
from datetime import datetime
from utils.financial_converter import SnapshotConverter

# --- CONFIGURATION ---
BASE_DIR = Path("./")
CSV_DIR = BASE_DIR / "csv_statement"

# Database Credentials
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

# 1. CORE FILES: The filing is SKIPPED if these are missing.
REQUIRED_FILES = {
    "income_statement": ["income_statement.csv", "incomestatement.csv"],
    "balance_sheet": ["balancesheet.csv", "balance_sheet.csv"],
    "cash_flow": ["cashflow.csv", "cash_flow.csv"]
}

# 2. OPTIONAL FILES: Added if found, but won't stop the process if missing.
OPTIONAL_FILES = {
    "equity_statement": ["equity_statement.csv", "equity.csv"],
    "comprehensive_income": ["comprehensive_income.csv", "comprehensiveincome.csv"],
    "schedule_of_investment": ["schedule_of_investment.csv", "scheduleofinvestment.csv"]
}

def get_db_connection():
    """Establishes connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"CRITICAL DB ERROR: {e}")
        return None

def get_entity_cik(cursor, ticker):
    """Retrieves the CIK (VARCHAR) for a given ticker."""
    try:
        cursor.execute("SELECT cik FROM entities WHERE ticker = %s", (ticker,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"  [SKIP] Entity '{ticker}' not found in DB.")
            return None
    except Exception as e:
        print(f"  [ERROR] Database lookup failed for {ticker}: {e}")
        return None

def parse_folder_info(folder_name):
    """
    Parses the folder name to extract the filing date and accession number.
    Format expected: YYYY-MM-DD_ACCESSION
    """
    try:
        parts = folder_name.split('_')
        date_str = parts[0]
        accession_num = parts[1] if len(parts) > 1 else "UNKNOWN"
        valid_from = datetime.strptime(date_str, "%Y-%m-%d")
        return valid_from, accession_num
    except (ValueError, IndexError):
        return None, "UNKNOWN"

def check_snapshot_exists(cursor, entity_cik, valid_from, filing_type):
    """Checks if a snapshot already exists for this entity, date, and type."""
    trigger_event = f"{filing_type} Filing"
    try:
        query = """
            SELECT 1 FROM entity_facet_snapshots 
            WHERE entity_cik = %s 
              AND facet_name = 'FINANCIAL_HEALTH' 
              AND valid_from = %s 
              AND trigger_event = %s
        """
        cursor.execute(query, (entity_cik, valid_from, trigger_event))
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"  [WARN] Duplication check failed: {e}")
        return False

def find_file(folder_path, possible_names):
    """Helper to find a file case-insensitively."""
    try:
        for filename in os.listdir(folder_path):
            if filename.lower() in [n.lower() for n in possible_names]:
                return folder_path / filename
    except OSError:
        pass
    return None

def build_snapshot_data(files_map):
    """
    Converts CSVs into the hierarchical JSON structure using financial_converter.
    """
    snapshot = {}
    for statement_type, file_path in files_map.items():
        try:
            df = pd.read_csv(file_path)
            converter = SnapshotConverter(df)
            snapshot[statement_type] = json.loads(converter.get_json())
        except Exception as e:
            print(f"    [WARN] Failed to convert {statement_type}: {e}")
    return snapshot

def insert_financial_health(cursor, entity_cik, valid_from, accession_num, filing_type, data):
    """Inserts the snapshot into entity_facet_snapshots using CIK."""
    meta_wrapper = {
        "meta": {
            "source": "SEC EDGAR",
            "accession_number": accession_num,
            "filing_type": filing_type,
            "processed_at": datetime.now().isoformat()
        },
        "financials": data
    }

    sql = """
        INSERT INTO entity_facet_snapshots 
        (entity_cik, facet_name, valid_from, trigger_event, data)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    cursor.execute(sql, (
        entity_cik,             
        'FINANCIAL_HEALTH',     
        valid_from,             
        f"{filing_type} Filing",
        Json(meta_wrapper)      
    ))

def process_ticker_to_db(ticker):
    print(f"\n=== Processing Ticker: {ticker} ===")
    
    ticker_path = CSV_DIR / ticker
    if not ticker_path.exists():
        print(f"Error: Directory not found: {ticker_path}")
        return

    conn = get_db_connection()
    if not conn: return
    cur = conn.cursor()

    try:
        # 1. Resolve Entity CIK
        entity_cik = get_entity_cik(cur, ticker)
        if not entity_cik: return

        count_success = 0
        
        # 2. Iterate over filing types
        for filing_type in ["10-K", "10-Q"]:
            type_path = ticker_path / filing_type
            if not type_path.exists(): continue
            
            # Sort folders chronologically
            filing_folders = sorted(type_path.iterdir())

            for filing_folder in filing_folders:
                if not filing_folder.is_dir(): continue

                folder_name = filing_folder.name
                valid_from, accession_num = parse_folder_info(folder_name)
                
                # Check duplication
                if valid_from:
                    if check_snapshot_exists(cur, entity_cik, valid_from, filing_type):
                        print(f"    [SKIP] Duplicate found for {folder_name}")
                        continue
                else:
                    print(f"    [WARN] Date parse failed for '{folder_name}'. Using NOW().")
                    valid_from = datetime.now()

                # --- File Validation ---
                files_found = {}
                missing_required = []

                # Check Required Files
                for key, potential_names in REQUIRED_FILES.items():
                    found_path = find_file(filing_folder, potential_names)
                    if found_path:
                        files_found[key] = found_path
                    else:
                        missing_required.append(key)

                if missing_required:
                    print(f"    [SKIP] {folder_name} missing required: {missing_required}")
                    continue

                # Check Optional Files (Equity, Comprehensive Income, Investments)
                for key, potential_names in OPTIONAL_FILES.items():
                    found_path = find_file(filing_folder, potential_names)
                    if found_path:
                        files_found[key] = found_path
                
                # --- Processing & Insertion ---
                try:
                    snapshot_data = build_snapshot_data(files_found)
                    
                    if not snapshot_data:
                        print(f"    [SKIP] No valid data converted for {folder_name}")
                        continue

                    insert_financial_health(
                        cur, 
                        entity_cik, 
                        valid_from,
                        accession_num,
                        filing_type, 
                        snapshot_data
                    )
                    conn.commit()
                    
                    # Detailed status report
                    stmts = list(snapshot_data.keys())
                    print(f"    -> ✅ Added {filing_type} for {folder_name.split('_')[0]} (Statements: {len(stmts)})")
                    count_success += 1
                    
                except Exception as e:
                    conn.rollback()
                    print(f"    -> ❌ FAILED {folder_name}: {e}")

        print(f"--- Finished {ticker}: {count_success} snapshots added. ---")

    except Exception as e:
        print(f"CRITICAL ERROR processing {ticker}: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    target_input = input("Enter ticker to process (or 'ALL'): ").strip().upper()
    if target_input == 'ALL':
        if CSV_DIR.exists():
            all_tickers = [d.name for d in CSV_DIR.iterdir() if d.is_dir()]
            for t in all_tickers:
                process_ticker_to_db(t)
    else:
        process_ticker_to_db(target_input)