import os
from pathlib import Path
from datetime import datetime
import json
from typing import List, Union, Optional, Dict

# --- CONFIGURATION ---
# --- CONFIGURATION ---
DEFAULT_ROOT = Path("./")
FILINGS_DIR_NAME = "SnP500_filings"

# Map simple form names to actual folder names
FOLDER_MAP = {
    "10-K": "10-K",
    "10-Q": "10-Q",
    "8-K": "8-K",
    "DEF 14A": "Proxy_Statement",
    "4": "Insider_Trading"
}

# --- HELPER FUNCTIONS ---

def _parse_date(date_str: Optional[str]) -> datetime:
    """Parses YYYY-MM-DD string to datetime object."""
    if not date_str:
        return datetime.min
    try:
        return datetime.strptime(str(date_str), "%Y-%m-%d")
    except (ValueError, TypeError):
        return datetime.min

def _load_json(path: Path) -> Optional[Dict]:
    """Safely loads a JSON file."""
    if not path.exists():
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None
    

# --- CORE FETCHING ---

def get_filing_paths(
    ticker: str, 
    form: str, 
    start_date: Union[str, datetime], 
    end_date: Union[str, datetime],
    root: Union[str, Path] = DEFAULT_ROOT
) -> List[Path]:
    """
    Retrieves a list of folder paths for a specific ticker and form type 
    within a given date range.

    Args:
        ticker (str): Ticker symbol (e.g., "AAPL").
        form (str): Form type (e.g., "10-K", "DEF 14A", "4").
        start_date (str|datetime): Start of the search window (inclusive).
        end_date (str|datetime): End of the search window (inclusive).
        hsdb_root (str|Path): Path to the root directory containing 'SnP500_filings'.

    Returns:
        List[Path]: A list of valid Path objects to the filing folders.
    """
    
    # --- 1. INPUT VALIDATION ---
    if not isinstance(ticker, str) or not ticker:
        raise ValueError("Ticker must be a non-empty string.")
    
    if form not in FOLDER_MAP:
        raise ValueError(f"Invalid form type: '{form}'. Valid options: {list(FOLDER_MAP.keys())}")
    
    try:
        s_date = _parse_date(start_date)
        e_date = _parse_date(end_date)
    except ValueError as e:
        print(f"[ERROR] {e}")
        return []

    if s_date > e_date:
        raise ValueError("start_date cannot be after end_date.")

    # --- 2. PATH CONSTRUCTION ---
    root_path = Path(root)
    # Map input form to actual folder name (e.g., "DEF 14A" -> "Proxy_Statement")
    target_folder_name = FOLDER_MAP[form]
    
    base_dir = root_path / "SnP500_filings" / ticker / target_folder_name

    if not base_dir.exists():
        print(f"[WARN] Directory not found: {base_dir.resolve()}")
        return []

    valid_paths = []

    # --- 3. ITERATION & FILTERING ---
    # Structure: SnP500_filings/{ticker}/{form}/{YYYY-MM-DD}_{ACCESSION}/
    for entry in base_dir.iterdir():
        if entry.is_dir():
            try:
                # Extract date from folder name: "2025-12-12_000..." -> "2025-12-12"
                folder_date_str = entry.name.split('_')[0]
                folder_date = datetime.strptime(folder_date_str, "%Y-%m-%d")
                
                if s_date <= folder_date <= e_date:
                    valid_paths.append(entry)
                    
            except (IndexError, ValueError):
                # Skip folders that don't match the naming convention
                continue

    # Sort paths by date (optional, but usually helpful)
    valid_paths.sort(key=lambda p: p.name)
    
    return valid_paths

def iter_filing_metadata(
    ticker: str, 
    form: str, 
    start_date: Union[str, datetime, None] = None, 
    end_date: Union[str, datetime, None] = None, 
    root: Path = DEFAULT_ROOT
) -> List[Dict]:
    """
    Iterate and fetch metadata.json within SnP500_filings/{ticker}/{form_folder}
    
    """
    # 1. Setup Date Range (Defaults: Start of Time -> Now)
    if start_date is None:
        s_date = datetime.min
    else:
        s_date = _parse_date(start_date)

    if end_date is None:
        e_date = datetime.now()
    else:
        e_date = _parse_date(end_date)

    # 2. Resolve Directory
    base_dir = root / FILINGS_DIR_NAME / ticker / FOLDER_MAP.get(form, form)
    results = []

    if not base_dir.exists():
        return results

    # 3. Iterate & Filter by Name
    for filing_folder in base_dir.iterdir():
        if not filing_folder.is_dir():
            continue

        try:
            folder_date_str = filing_folder.name.split('_')[0]
            folder_date = datetime.strptime(folder_date_str, "%Y-%m-%d")
            
            # Check Date Range BEFORE reading file
            if s_date <= folder_date <= e_date:
                meta = _load_json(filing_folder / "metadata.json")
                if meta:
                    meta['_source_path'] = str(filing_folder)
                    results.append(meta)

        except (ValueError, IndexError):
            # Skip folders that don't match the "YYYY-MM-DD_Accession" naming convention
            continue
    
    return results