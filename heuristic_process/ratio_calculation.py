import pandas as pd
from typing import Dict, Optional
from dotenv import load_dotenv

# 1. Import Schema
from schema.financial_health_schema import (
    EntityFinancialRatios, LiquidityRatios, SolvencyRatios, 
    ProfitabilityRatios, EfficiencyRatios, CashFlowRatios
)

# 2. Import Updated Logic
from heuristic_process.concept_fetching import (
    process_fetch_request, DB_CONFIG, NEEDED_CONCEPTS
)

load_dotenv()

def to_series(data_dict: Optional[Dict[str, float]]) -> Optional[pd.Series]:
    if not data_dict: return None
    s = pd.Series(data_dict)
    s.index = pd.to_datetime(s.index)
    return s

def safe_math(op, s1: Optional[pd.Series], s2: Optional[pd.Series]) -> Optional[Dict[str, float]]:
    if s1 is None or s2 is None: return None
    try:
        res = getattr(s1, op)(s2) if isinstance(op, str) else op(s1, s2)
        res = res.replace([float('inf'), -float('inf')], None).dropna()
        if not res.empty:
            res.index = res.index.strftime('%Y-%m-%d')
        return res.to_dict()
    except Exception as e:
        print(f"Math Error: {e}")
        return None

# --- Main Logic ---
# Removed process_agent parameter as concept_fetching now uses the internal OpenAI client
def calculate_ratios(ticker: str, valid_from: str, filing_type: str = '10-Q', 
                     db_config: Dict = DB_CONFIG, needed_concept: Dict = NEEDED_CONCEPTS):
    
    # 1. Fetch Data using the updated direct-client logic
    raw_data = process_fetch_request(ticker, valid_from, filing_type, db_config, needed_concept)
    
    if not raw_data:
        print("No data fetched.")
        return None

    D = {k: to_series(v) for k, v in raw_data.items()}
    
    # 3. Calculate Ratios
    # (Using safe_math wrapper for div/sub to handle date alignment automatically)
    
    liquidity = LiquidityRatios(
        current_ratio=safe_math('div', D['assets_current'], D['liabilities_current']),
        quick_ratio=safe_math('div', safe_math('sub', D['assets_current'], D['inventory']), D['liabilities_current']),
        cash_ratio=safe_math('div', D['cash_equivalents'], D['liabilities_current'])
    )

    debt = D['debt_long_term'].add(D['debt_short_term'], fill_value=0) if D['debt_long_term'] is not None else D['debt_long_term']
    
    solvency = SolvencyRatios(
        debt_to_equity=safe_math('div', debt, D['equity_total']),
        debt_to_assets=safe_math('div', debt, D['assets_total']),
        interest_coverage=safe_math('div', D['operating_income'], D['interest_expense']),
        equity_multiplier=safe_math('div', D['assets_total'], D['equity_total'])
    )

    profitability = ProfitabilityRatios(
        gross_profit_margin=safe_math('div', safe_math('sub', D['revenue'], D['cogs']), D['revenue']),
        operating_margin=safe_math('div', D['operating_income'], D['revenue']),
        net_profit_margin=safe_math('div', D['net_income'], D['revenue']),
        return_on_assets=safe_math('div', D['net_income'], D['assets_total']),
        return_on_equity=safe_math('div', D['net_income'], D['equity_total'])
    )

    efficiency = EfficiencyRatios(
        asset_turnover=safe_math('div', D['revenue'], D['assets_total']),
        inventory_turnover=safe_math('div', D['cogs'], D['inventory']),
        receivables_turnover=safe_math('div', D['revenue'], D['receivables'])
    )

    cash_flow = CashFlowRatios(
        operating_cash_flow_ratio=safe_math('div', D['operating_cash_flow'], D['liabilities_current']),
        free_cash_flow=safe_math('sub', D['operating_cash_flow'], D['capex'])
    )

    # 4. Final Object
    final_obj = EntityFinancialRatios(
        entity_id=ticker,
        liquidity=liquidity,
        solvency=solvency,
        profitability=profitability,
        efficiency=efficiency,
        cash_flow=cash_flow
    )

    print("\n--- Final Calculated Ratios ---")
    print(final_obj.model_dump_json(indent=2, exclude_none=True))
    return final_obj

if __name__ == "__main__":
    ratios = calculate_ratios('AAPL', '2025-10-31', '10-K')