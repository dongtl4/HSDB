import xml.etree.ElementTree as ET
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Literal, Union, Any
from utils.fetching import get_filing_paths

# --- 1. DETAILED PARSER ---
def _parse_form4_details(xml_path: Path) -> List[Dict[str, Any]]:
    """
    Extracts individual open-market transactions from a Form 4 XML.
    Returns a list of dictionaries, one per transaction row.
    """
    transactions = []
    
    if not xml_path.exists():
        return transactions

    try:
        xml_content = xml_path.read_text(encoding='utf-8')
        root = ET.fromstring(xml_content)
        
        # A. Extract Reporting Owner Name (The "Who")
        # Structure: <reportingOwner><reportingOwnerId><rptOwnerName>...
        owner_name = "Unknown"
        owner_node = root.find(".//rptOwnerName")
        if owner_node is not None and owner_node.text:
            owner_name = owner_node.text.strip()
            
        # B. Iterate Transactions (The "What")
        for trans in root.findall(".//nonDerivativeTransaction"):
            try:
                # 1. Check Code (Filter for Open Market Buy/Sell only)
                code_node = trans.find(".//transactionCode")
                if code_node is None or not code_node.text: continue
                
                code = code_node.text.strip().upper()
                if code not in ['P', 'S']: continue # Skip Grants (A), Exercises (M), Tax (F)

                # 2. Extract Details
                date_node = trans.find(".//transactionDate/value")
                shares_node = trans.find(".//transactionShares/value")
                price_node = trans.find(".//transactionPricePerShare/value")
                
                if any(x is None for x in [date_node, shares_node, price_node]): continue
                
                # 3. Build Record
                qty = float(shares_node.text)
                price = float(price_node.text)
                val = qty * price
                date_str = date_node.text

                transactions.append({
                    "filing_date": date_str,  # Using transaction date from row
                    "insider": owner_name,
                    "type": "BUY" if code == 'P' else "SELL",
                    "shares": qty,
                    "price": price,
                    "value_usd": val
                })
                
            except (ValueError, AttributeError):
                continue
                
    except Exception as e:
        print(f"[ERR] Error parsing {xml_path.name}: {e}")

    return transactions

# --- 2. AGGREGATOR ---
def get_insider_activity_data(form4_folders: List[Path]) -> Dict[str, Any]:
    """
    Aggregates Form 4 data into a summary + detailed transaction list.
    """
    # Initialize Summary Buckets
    summary = {
        "total_buy_usd": 0.0,
        "total_sell_usd": 0.0,
        "net_activity_usd": 0.0,
        "transaction_count": 0,
        "unique_insiders": set()
    }
    
    all_transactions = []
    
    print(f"Scanning {len(form4_folders)} filing folders...")

    for folder in form4_folders:
        if not folder.is_dir(): continue

        # Find XML
        target_xml = folder / "4.xml"
        if not target_xml.exists():
            any_xmls = list(folder.glob("*.xml"))
            if any_xmls: target_xml = any_xmls[0]
            else: continue

        # Parse
        file_txs = _parse_form4_details(target_xml)
        
        # Aggregate
        for tx in file_txs:
            all_transactions.append(tx)
            
            summary["transaction_count"] += 1
            summary["unique_insiders"].add(tx["insider"])
            
            if tx["type"] == "BUY":
                summary["total_buy_usd"] += tx["value_usd"]
            else:
                summary["total_sell_usd"] += tx["value_usd"]

    # Finalize Calculations
    summary["net_activity_usd"] = summary["total_buy_usd"] - summary["total_sell_usd"]
    summary["unique_insiders"] = list(summary["unique_insiders"]) # Convert set to list for JSON
    
    # Sort transactions by date (newest first)
    all_transactions.sort(key=lambda x: x["filing_date"], reverse=True)

    return {
        "summary": summary,
        "raw_data": all_transactions
    }

def print_insider_activity(data, n=20):
    """Pretty prints the insider trading data dictionary."""
    summary = data.get("summary", {})
    raw_data = data.get("raw_data", [])

    print("\n" + "="*60)
    print(f"INSIDER TRADING REPORT ({len(raw_data)} Transactions)")
    print("="*60)
    
    # 1. Print Summary
    print(f"Total Bought:      ${summary.get('total_buy_usd', 0):>15,.2f}")
    print(f"Total Sold:        ${summary.get('total_sell_usd', 0):>15,.2f}")
    print(f"Net Activity:      ${summary.get('net_activity_usd', 0):>15,.2f}")
    print("-" * 60)
    print(f"Unique Insiders:   {len(summary.get('unique_insiders', []))}")
    print(f"Active Names:      {', '.join(summary.get('unique_insiders', [])[:5])}" + 
        ("..." if len(summary.get('unique_insiders', [])) > 5 else ""))
    print("="*60 + "\n")

    # 2. Print Recent Transactions Table
    print(f"{'DATE':<12} | {'INSIDER':<20} | {'TYPE':<4} | {'SHARES':>10} | {'VALUE ($)':>14}")
    print("-" * 75)
    
    # Show first Nshow transactions to keep it readable
    for tx in raw_data[:n]:
        print(f"{tx['filing_date']:<12} | "
            f"{tx['insider'][:n]:<20} | "
            f"{tx['type']:<4} | "
            f"{tx['shares']:>10,.0f} | "
            f"${tx['value_usd']:>13,.0f}")
    
    if len(raw_data) > n:
        print(f"... and {len(raw_data) - n} more transactions.")
    print("-" * 75)

# --- USAGE EXAMPLE ---
if __name__ == "__main__":
    example_paths = get_filing_paths('NVDA', '4', '2025-07-01', '2026-01-29')
    
    # 3. Calculate
    example = get_insider_activity_data(example_paths)

    # Usage
    print_insider_activity(example, 40)