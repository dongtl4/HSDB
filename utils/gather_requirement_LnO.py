import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple

from utils.fetching import DEFAULT_ROOT, FOLDER_MAP, FILINGS_DIR_NAME, _parse_date, iter_filing_metadata

# --- CORE FUNCTIONS ---

def find_anchor_10k(ticker: str, fiscal_year: int, root: Path = DEFAULT_ROOT) -> Optional[Dict]:
    """
    2. Find the metadata of right 10-K filing for a given fiscal year.
    Handles cases with multiple 10-Ks by selecting the one with the LATEST filing date.
    """
    all_10ks = iter_filing_metadata(ticker, "10-K", root)
    candidates = []

    target_year = int(fiscal_year)

    for meta in all_10ks:
        # Check explicit fiscal_year
        meta_fy = meta.get("fiscal_year")
        
        # Safe Integer Conversion & Comparison
        try:
            if meta_fy and int(meta_fy) == target_year:
                candidates.append(meta)
        except ValueError:
            continue

    if not candidates:
        return None

    # Sort by filing_date descending (Latest first) to handle amendments or duplicates
    candidates.sort(key=lambda x: _parse_date(x.get("filing_date")), reverse=True)
    
    return candidates[0]

def find_secondary_anchor(ticker: str, k10_filing_date: str, root: Path = DEFAULT_ROOT) -> Optional[Dict]:
    """
    3. Find the first DEF 14A (Proxy) filed AFTER the 10-K filing date.
    """
    if not k10_filing_date:
        return None
        
    k10_date = _parse_date(k10_filing_date)
    all_proxies = iter_filing_metadata(ticker, "DEF 14A", root)
    
    candidates = []
    
    for meta in all_proxies:
        proxy_date = _parse_date(meta.get("filing_date"))
        
        # Strict logic: Must be AFTER the 10-K
        if proxy_date > k10_date:
            candidates.append(meta)
            
    if not candidates:
        return None
        
    # Sort by filing_date ascending (Earliest first) to find the IMMEDIATE next proxy
    candidates.sort(key=lambda x: _parse_date(x.get("filing_date")))
    
    return candidates[0]

def find_context_filings(ticker: str, proxy_filing_date: str, root: Path = DEFAULT_ROOT) -> Tuple[List[Dict], List[Dict]]:
    """
    4. Find context filings relative to the Proxy filing date.
       - 8-Ks: 1 year lookback
       - Form 4s: 6 month lookback
    """
    if not proxy_filing_date:
        return ([], [])

    anchor_date = _parse_date(proxy_filing_date)
    
    # --- 8-K Logic (1 Year Lookback) ---
    all_8ks = iter_filing_metadata(ticker, "8-K", root)
    valid_8ks = []
    start_8k = anchor_date - timedelta(days=365)
    
    for meta in all_8ks:
        f_date = _parse_date(meta.get("filing_date"))
        if start_8k <= f_date <= anchor_date:
            valid_8ks.append(meta)

    # --- Form 4 Logic (6 Month Lookback) ---
    all_4s = iter_filing_metadata(ticker, "4", root)
    valid_4s = []
    start_4 = anchor_date - timedelta(days=180)
    
    for meta in all_4s:
        f_date = _parse_date(meta.get("filing_date"))
        if start_4 <= f_date <= anchor_date:
            valid_4s.append(meta)
            
    return (valid_8ks, valid_4s)


# --- TEST SUITE ---
if __name__ == "__main__":
    # Settings for the test
    TEST_TICKER = "AAPL"
    TEST_YEAR = 2018
    ROOT_PATH = Path("./") 

    print(f"=== STARTING TESTS FOR {TEST_TICKER} (FY{TEST_YEAR}) ===\n")

    # 1. Test iter_filing_metadata
    print("--- Test 1: Iterating 10-K Metadata ---")
    print("Why: Verify we can read the file system and JSONs correctly.")
    print("Expected: A list with > 0 items (if data exists).")
    
    k10_list = iter_filing_metadata(TEST_TICKER, "10-K", ROOT_PATH)
    print(f"Actual: Found {len(k10_list)} metadata files.")
    if len(k10_list) > 0:
        print(f"Sample: {k10_list[0].get('accession_number')}")
    print("\n")

    # 2. Test find_anchor_10k
    print("--- Test 2: Finding Anchor 10-K ---")
    print(f"Why: specific retrieval of FY{TEST_YEAR} 10-K.")
    print(f"Expected: A dict with 'fiscal_year': {TEST_YEAR}.")
    
    anchor_10k = find_anchor_10k(TEST_TICKER, TEST_YEAR, ROOT_PATH)
    
    if anchor_10k:
        k10_date = anchor_10k.get('filing_date')
        print(f"Actual: Found 10-K! ID: {anchor_10k.get('accession_number')}")
        print(f"        Filing Date: {k10_date}")
        print(f"        Fiscal Year: {anchor_10k.get('fiscal_year')}")
    else:
        print("Actual: None (Check if FY exists in metadata or if patch was run)")
        k10_date = None
    print("\n")

    # 3. Test find_secondary_anchor
    print("--- Test 3: Finding Secondary Anchor (Proxy) ---")
    if k10_date:
        print(f"Why: Find first Proxy after 10-K date ({k10_date}).")
        print(f"Expected: A Proxy metadata dict with date > {k10_date}.")
        
        anchor_proxy = find_secondary_anchor(TEST_TICKER, k10_date, ROOT_PATH)
        
        if anchor_proxy:
            proxy_date = anchor_proxy.get('filing_date')
            print(f"Actual: Found Proxy! ID: {anchor_proxy.get('accession_number')}")
            print(f"        Filing Date: {proxy_date}")
            is_after = _parse_date(proxy_date) > _parse_date(k10_date)
            print(f"        Is After 10-K? {is_after}")
        else:
            print("Actual: None (No proxy found after 10-K date)")
            proxy_date = None
    else:
        print("Skipped (No 10-K found in Step 2)")
        proxy_date = None
    print("\n")

    # 4. Test find_context_filings
    print("--- Test 4: Finding Context Filings ---")
    if proxy_date:
        print(f"Why: Gathering 8-Ks (1yr) and Form 4s (6mo) before Proxy ({proxy_date}).")
        print("Expected: Two lists of metadata.")
        
        ctx_8k, ctx_4 = find_context_filings(TEST_TICKER, proxy_date, ROOT_PATH)
        
        print(f"Actual: {len(ctx_8k)} 8-Ks found.")
        print(f"Actual: {len(ctx_4)} Form 4s found.")
        
        if ctx_8k:
            print(f"        Last 8-K Date: {ctx_8k[-1].get('filing_date')}")
    else:
        print("Skipped (No Proxy found in Step 3)")
    
    print("\n=== TESTS COMPLETE ===")