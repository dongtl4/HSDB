import os
import re
import json
import psycopg2
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from openai import OpenAI 

load_dotenv()

# --- CONFIGURATION ---
DB_CONFIG = {
    "dbname": "hsdb_trading",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": "5432"
}

# Initialize the DeepSeek client directly
client = OpenAI(
    api_key=os.getenv('DEEPSEEK_API_KEY'), 
    base_url="https://api.deepseek.com"
)

# --- DEFINITIONS ---
NEEDED_CONCEPTS = {
    "income_statement": {
        "revenue": "Top-line total revenue or net sales. Do not pick 'Segment Revenue'.",
        "cogs": "Cost of goods sold or Cost of revenue.",
        "operating_income": "Operating income or Operating profit (EBIT).",
        "interest_expense": "Interest expense (gross or net).",
        "net_income": "Net income or Net loss attributable to the company (bottom line).",
    },
    "balance_sheet": {
        "assets_total": "Total Assets.",
        "assets_current": "Total Current Assets.",
        "cash_equivalents": "Cash and Cash Equivalents (Total).",
        "inventory": "Total Inventory (Net).",
        "receivables": "Net Accounts Receivable (Current).",
        "liabilities_current": "Total Current Liabilities.",
        "debt_long_term": "Long-term debt (excluding current portion).",
        "debt_short_term": "Short-term debt or current portion of long-term debt.",
        "equity_total": "Total Stockholders' Equity.",
    },
    "cash_flow": {
        "operating_cash_flow": "Net cash provided by (used in) operating activities.",
        "capex": "Payments to acquire property, plant, and equipment (Capital Expenditures).",
    }
}

# --- Database & Tree Helpers ---
def fetch_snapshot_from_db(ticker, date_str, filing_type="10-Q", db_config=DB_CONFIG):
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    try:
        cur.execute("SELECT cik FROM entities WHERE ticker = %s", (ticker,))
        res = cur.fetchone()
        if not res: return None
        cik = res[0]

        query = """
            SELECT data 
            FROM entity_facet_snapshots 
            WHERE entity_cik = %s 
            AND facet_name = 'FINANCIAL_HEALTH' 
            AND valid_from = %s
            AND trigger_event = %s
        """
        cur.execute(query, (cik, date_str, f"{filing_type} Filing"))
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()

def generate_composite_id(node):
    ids = node.get('identifiers', {})
    c, l = ids.get('concept', 'unknown'), ids.get('label', 'unknown')
    d = ids.get('dimension_member') or "NO_DIM"
    return f"{c}|{l}|{d}"

def simplify_tree_for_prompt(nodes, depth=0):
    lines = []
    for node in nodes:
        unique_id = generate_composite_id(node)
        lines.append(f"{'  ' * depth}- {node.get('label', 'Unknown')} (ID: {unique_id})")
        if node.get('children'):
            lines.extend(simplify_tree_for_prompt(node['children'], depth + 1))
    return lines

def find_node_by_composite_id(nodes, target_id):
    for node in nodes:
        if generate_composite_id(node) == target_id: return node
        if node.get('children'):
            found = find_node_by_composite_id(node['children'], target_id)
            if found: return found
    return None

# --- Prompt Creation ---
def create_prompt(statement_name, simplified_tree, specific_concepts):
    tree_text = "\n".join(simplified_tree)
    concepts_json = json.dumps(specific_concepts, indent=2)
    return f"""You are a financial data expert. Map TARGET CONCEPTS to the CANDIDATE STRUCTURE.
TARGET CONCEPTS: {concepts_json}
CANDIDATE STRUCTURE ({statement_name}):
{tree_text}
INSTRUCTIONS:
1. Return JSON: keys=TARGET CONCEPTS, values=ID from structure.
2. Prefer IDs ending in `|NO_DIM`.
3. If not found, set value to null."""

# --- HELPER: DIRECT DEEPSEEK CALL ---
def _query_deepseek_direct(prompt: str) -> str:
    """
    Replaces Agent.run() with a direct OpenAI client call.
    """
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "You are a financial data expert."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.0 
        )
        return response.choices[0].message.content or ""
    except Exception as e:
        print(f"[API ERROR] {e}")
        return ""

# --- Main Workflow ---
def process_fetch_request(
    ticker: str, 
    valid_from_date: str, 
    filing_type: str = '10-Q', 
    db_config: Dict = DB_CONFIG, 
    needed_concepts: Dict = NEEDED_CONCEPTS
) -> Dict[str, Dict[str, float]]:
    """
    Returns a dictionary of extracted time-series data using direct DeepSeek calls.
    """
    print(f"--- Fetching Data for {ticker} ({valid_from_date}) ---")
    
    # 1. Fetch Snapshot
    snapshot_result = fetch_snapshot_from_db(ticker, valid_from_date, filing_type, db_config)
    if not snapshot_result:
        print("Snapshot not found.")
        return {}
    
    snapshot_data = snapshot_result[0]
    financials = snapshot_data.get('financials', {})
    extracted_data = {}

    # 2. Iterate Statements
    for stmt_type, concepts_def in needed_concepts.items():
        print(f"Processing {stmt_type}...")
        root_nodes = financials.get(stmt_type)
        if not root_nodes: continue

        # 3. Simplify & Prompt
        simplified = simplify_tree_for_prompt(root_nodes)
        prompt = create_prompt(stmt_type, simplified, concepts_def)
        
        # 4. LLM Call (Directly via helper)
        content = _query_deepseek_direct(prompt)
        
        try:
            # DeepSeek JSON mode usually doesn't need regex cleaning if response_format is set, 
            # but keeping safety check for consistency.
            clean_json = re.sub(r"```json\n|```", "", content).strip()
            mapped_ids = json.loads(clean_json)
        except json.JSONDecodeError:
            print(f"  [ERROR] LLM JSON invalid for {stmt_type}")
            continue

        # 5. Extract Data
        for concept, composite_id in mapped_ids.items():
            print(f"{concept}: {composite_id}")
            if not composite_id:
                extracted_data[concept] = None
                continue
                
            node = find_node_by_composite_id(root_nodes, composite_id)
            if node and 'data' in node:
                clean_series = {}
                for d, v in node['data'].items():
                    try:
                        clean_series[d] = float(v)
                    except (ValueError, TypeError):
                        pass
                extracted_data[concept] = clean_series
            else:
                extracted_data[concept] = None

    return extracted_data

if __name__ == "__main__":
    results = process_fetch_request('AAPL', '2025-10-31', '10-K')
    print("\n--- Extracted Results ---")
    print(results)