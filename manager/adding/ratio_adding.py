import psycopg2
import json
import re
from datetime import datetime
from psycopg2.extras import DictCursor

# --- IMPORTS ---
# We assume these exist in your directory based on previous steps
from HSDB.utils.ratio_calculation import calculate_ratios
from HSDB.schema.financial_health_schema import EntityFinancialRatios

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

# MAG_SEVEN = ['AAPL', 'MSFT', 'GOOGL', 'META', 'TSLA', 'NVDA', 'AMZN']
MAG_SEVEN = ['AAPL']

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_mag_seven_ciks(conn):
    """
    Returns a dict mapping {cik: ticker} for the Mag 7.
    """
    tickers = tuple(MAG_SEVEN)
    cur = conn.cursor()
    query = "SELECT cik, ticker FROM entities WHERE ticker IN %s"
    cur.execute(query, (tickers,))
    mapping = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    return mapping

def process_and_update_ratios():
    conn = get_connection()
    # Use DictCursor to access columns by name
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        # 1. Get CIKs
        print("--- identifying Magnificent Seven CIKs ---")
        cik_map = get_mag_seven_ciks(conn)
        if not cik_map:
            print("No entities found. Check your 'entities' table.")
            return

        ciks = tuple(cik_map.keys())
        print(f"Found {len(ciks)} entities: {list(cik_map.values())}")

        # 2. Select Snapshots for these entities
        print("--- Fetching Snapshots ---")
        query = """
            SELECT id, entity_cik, valid_from, trigger_event, data 
            FROM entity_facet_snapshots 
            WHERE entity_cik IN %s 
              AND facet_name = 'FINANCIAL_HEALTH'
            ORDER BY entity_cik, valid_from DESC
        """
        cur.execute(query, (ciks,))
        rows = cur.fetchall()
        print(f"Found {len(rows)} snapshots to process.")

        success_count = 0
        skip_count = 0

        # 3. Iterate and Process
        for row in rows:
            snapshot_id = row['id']
            cik = row['entity_cik']
            valid_from = row['valid_from'] # This is a datetime object
            trigger_event = row['trigger_event'] or ""
            current_data = row['data']
            
            ticker = cik_map.get(cik)
            
            # Format date for the calculate_ratios function (YYYY-MM-DD)
            valid_from_str = valid_from.strftime('%Y-%m-%d')
            
            # Extract filing type from trigger_event (e.g., "10-Q Filing" -> "10-Q")
            # Default to 10-Q if not found, but try to parse
            filing_type = "10-Q"
            if "10-K" in trigger_event.upper():
                filing_type = "10-K"
            elif "10-Q" in trigger_event.upper():
                filing_type = "10-Q"
            
            print(f"\nProcessing {ticker} | {valid_from_str} | {filing_type} (ID: {snapshot_id})")

            try:
                # --- A. CALCULATE RATIOS ---
                ratios_obj = calculate_ratios(ticker, valid_from_str, filing_type)
                
                if not ratios_obj:
                    print(f"  [SKIP] Ratio calculation returned None/Empty.")
                    skip_count += 1
                    continue
                
                # --- B. PREPARE DATA ---
                # Convert Pydantic model to dict
                ratios_dict = ratios_obj.model_dump(exclude_none=True)
                
                # --- C. UPDATE JSON BLOB ---
                # We update the local dictionary first
                current_data['ratios'] = ratios_dict
                
                # --- D. DB UPDATE ---
                update_sql = """
                    UPDATE entity_facet_snapshots 
                    SET data = %s 
                    WHERE id = %s
                """
                # Create a new cursor for the update to avoid messing with the iteration cursor
                update_cur = conn.cursor()
                update_cur.execute(update_sql, (json.dumps(current_data), snapshot_id))
                conn.commit()
                update_cur.close()
                
                print(f"  [SUCCESS] Ratios added to DB.")
                success_count += 1

            except Exception as e:
                conn.rollback()
                print(f"  [ERROR] Failed to process snapshot {snapshot_id}: {e}")

        print("\n" + "="*40)
        print(f"FINISHED. Success: {success_count}, Skipped: {skip_count}")
        print("="*40)

    except Exception as e:
        print(f"Critical Error: {e}")
    finally:
        cur.close()
        conn.close()

def revert_ratios_process():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        print("--- [REVERT MODE] Cleaning Ratios for Magnificent Seven ---")
        cik_map = get_mag_seven_ciks(conn)
        if not cik_map:
            print("No entities found.")
            return

        ciks = tuple(cik_map.keys())

        # Select snapshots that strictly belong to Mag 7 and Financial Health
        query = """
            SELECT id, entity_cik, data 
            FROM entity_facet_snapshots 
            WHERE entity_cik IN %s 
              AND facet_name = 'FINANCIAL_HEALTH'
        """
        cur.execute(query, (ciks,))
        rows = cur.fetchall()
        print(f"Scanning {len(rows)} snapshots for removal...")

        reverted_count = 0
        
        for row in rows:
            snapshot_id = row['id']
            current_data = row['data']
            ticker = cik_map.get(row['entity_cik'])
            
            # Check if 'ratios' key exists
            if 'ratios' in current_data:
                # Remove the key
                del current_data['ratios']
                
                # Update DB
                update_sql = "UPDATE entity_facet_snapshots SET data = %s WHERE id = %s"
                update_cur = conn.cursor()
                update_cur.execute(update_sql, (json.dumps(current_data), snapshot_id))
                conn.commit()
                update_cur.close()
                
                print(f"  [CLEANED] Removed ratios from {ticker} (ID: {snapshot_id})")
                reverted_count += 1
            else:
                pass

    except Exception as e:
        conn.rollback()
        print(f"Critical Error during revert: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    revert_ratios_process()