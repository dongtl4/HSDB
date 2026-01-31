from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Union

# --- 1. INCOME STATEMENT (Profit & Loss) ---
class StandardIncomeStatement(BaseModel):
    # Top Line
    revenue_total: float = Field(..., description="Total Net Sales / Revenue")
    revenue_products: Optional[float] = Field(None, description="Revenue from tangible goods")
    revenue_services: Optional[float] = Field(None, description="Revenue from services/subscriptions")
    
    # Costs & Gross Profit
    cost_of_revenue_total: float = Field(..., description="Total Cost of Goods Sold (COGS)")
    gross_profit: float = Field(..., description="Gross profit is a company’s profit after subtracting the costs directly linked to making and delivering its products and services. Gross Profit = Revenue - COGS")
    
    # Operating Expenses (OpEx)
    research_and_development: Optional[float] = Field(None, description="R&D expenses")
    selling_general_admin: Optional[float] = Field(None, description="Selling, general and administrative (SG&A) is an operating expense. It involves various company expenses that are not related to production.")
    marketing_expense: Optional[float] = Field(None, description="Marketing/Advertising if reported separately")
    total_operating_expenses: float = Field(..., description="Operating expenses (OPEX) are a company's fixed costs that a company incurs during its ongoing business operations. It can include SG&A, R&D and other expenses.")
    
    # Profitability
    operating_income: float = Field(..., description="Operating Profit / EBIT")
    interest_expense: Optional[float] = Field(None, description="Interest paid on debt")
    interest_and_investment_income: Optional[float] = Field(None, description="Interest/investment income is the amount that the company received in interest or investment.")
    other_income_expense: Optional[float] = Field(None, description="Non-operating income/loss")
    
    # Bottom Line
    pretax_income: float = Field(..., description="Income before tax")
    income_tax_provision: float = Field(..., description="Taxes paid/owed")
    net_income: float = Field(..., description="Net Income available to shareholders")
    
    # Per Share
    eps_basic: float = Field(..., description="Basic Earnings Per Share")
    eps_diluted: float = Field(..., description="Diluted Earnings Per Share")

# --- 2. BALANCE SHEET (Snapshot) ---
class StandardBalanceSheetAssets(BaseModel):
    # Current Assets
    cash_and_equivalents: float = Field(..., description="The amount of money on the company's accounts held as straight cash, or very liquid assets that can be sold for cash at a very short notice.")
    short_term_investments: Optional[float] = Field(None, description="Liquidable assets like treasury bills, short-term bonds, money-market funds, marketable securities and other investments that can be sold for cash at a short notice.")
    cash_and_short_term_investments: float = Field(..., description="Cash and short term investments = Cash & Equivalents + Short-Term Investments")
    receivable: float = Field(..., description="The money owed to the company for products or services that have been delivered but not yet paid for.")
    inventory: float = Field(..., description="The value of product that is available for sale, as well as the value of purchased raw materials for making goods that will be sold.")
    restrict_cash: Optional[float] = Field(None, description="Cash and cash equivalents that can not be used or transferred, including funds held in escrow or cash restricted in use.")
    other_current_assets: Optional[float] = Field(None, description="Other current assets includes all current assets that do not fit into any of the above categories.")
    total_current_assets: float = Field(..., description="Total current assets includes all current assets, including cash and equivalents, short-term investments, receivables, inventory and others.")
    
    # Non-Current Assets
    property_plant_equipment_net: float = Field(..., description="Net property, plant and equipment represents the value of durable tangible assets used by the company, net of accumulated depreciation.")
    long_term_investments: Optional[float] = Field(None, description="Investments that the company plans to hold for more than one year")
    goodwill: Optional[float] = Field(None, description="Premium purchase price paid over the book value of net assets acquired")
    intangible_assets: Optional[float] = Field(None, description="Non-physical assets that provide future economic benefit to the company. E.g. patents, trademarks, licenses")
    total_assets: float = Field(..., description="Total Assets")

class StandardBalanceSheetLiabilities(BaseModel):
    # Current Liabilities
    accounts_payable: float = Field(..., description="Money owed to vendors or suppliers for goods and services received (trade payables).")
    accrued_expenses: Optional[float] = Field(None, description="Expenses incurred but not yet paid (e.g., accrued compensation, utilities, interest). Sometimes labeled 'Accrued Liabilities'.")
    short_term_debt: Optional[float] = Field(None, description="The portion of long-term debt or notes payable due within the next 12 months. Includes Commercial Paper.")    
    current_lease_liabilities: Optional[float] = Field(None, description="Lease payment obligations due within the next 12 months (Current portion of operating/finance leases).")
    income_taxes_payable: Optional[float] = Field(None, description="Unpaid income taxes due to government authorities within the current operating cycle.")    
    deferred_revenue_current: Optional[float] = Field(None, description="Cash received from customers for goods/services not yet delivered, to be recognized within 12 months. Also called 'Unearned Revenue'.")    
    other_current_liabilities: Optional[float] = Field(None, description="Any other short-term liabilities not included in the specific categories above.")    
    total_current_liabilities: float = Field(..., description="Total liabilities due within one year. Must equal the sum of all current liability fields.")
    
    # Non-Current Liabilities
    long_term_debt: float = Field(..., description="Interest-bearing debt or notes payable due after one year.")
    long_term_leases: Optional[float] = Field(None, description="Lease obligations due after more than one year.")
    long_term_unearned_revenue: Optional[float] = Field(None, description="Revenue to be recognized after more than 12 months.")
    pension_and_post_retirement_benefits: Optional[float] = Field(None, description="Liabilities related to employee pensions and post-retirement plans.")
    long_term_deferred_tax_liabilities: Optional[float] = Field(None, description="Taxes owed in the future due to timing differences.")
    other_long_term_liabilities: Optional[float] = Field(None, description="Any other non-current liabilities not categorized above.")
    
    # Total
    total_liabilities: float = Field(..., description="Total claims against assets (Sum of Current + Non-Current).")

class StandardBalanceSheetEquity(BaseModel):
    common_stock: float = Field(..., description="Par value of issued shares plus Additional Paid-In Capital (APIC).")
    retained_earnings: float = Field(..., description="Cumulative net earnings or profit retained by the company after dividend payments.")
    comprehensive_income_and_other: Optional[float] = Field(None, description="Gains/losses not included in net income (e.g., currency translation, unrealized gains).")
    total_shareholders_equity: float = Field(..., description="Total value of assets minus liabilities (Book Value).")
    total_liabilities_and_equity: float = Field(..., description="Sum of Total Liabilities and Total Shareholders' Equity (Must equal Total Assets).")

class BalanceSheetSupplemental(BaseModel):
    # --- Debt & Cash Position ---
    total_debt: float = Field(..., description="Sum of all interest-bearing short-term and long-term debt.")
    net_cash_debt: float = Field(..., description="Total Cash & Equivalents minus Total Debt (Positive = Net Cash, Negative = Net Debt).")
    net_cash_per_share: Optional[float] = Field(None, description="Net Cash (Debt) divided by Total Common Shares Outstanding.")
    
    # --- Share Counts ---
    filing_date_shares_outstanding: Optional[float] = Field(None, description="Number of shares outstanding as of the specific filing date (often differs from weighted average).")
    total_common_shares_outstanding: float = Field(..., description="Total number of common shares held by shareholders (issued minus treasury shares).")
    
    # --- Valuation & Liquidity Metrics ---
    working_capital: float = Field(..., description="Total Current Assets minus Total Current Liabilities.")
    book_value_per_share: float = Field(..., description="Total Shareholders' Equity divided by Total Common Shares Outstanding.")
    tangible_book_value: float = Field(..., description="Total Equity minus Intangible Assets and Goodwill.")
    tangible_book_value_per_share: float = Field(..., description="Tangible Book Value divided by Total Common Shares Outstanding.")

class PPEDetails(BaseModel):
    land: Optional[float] = Field(None, description="The carrying value of all land owned by the company.")
    machinery: Optional[float] = Field(None, description="All types of fixed assets not classified as land or building, at gross values, which are to be used in the production or sale of goods or services.")
    leasehold_improvements: Optional[float] = Field(None, description="Capital expenditure items related to leased assets that increase the value of the assets, which revert to the owner of the property upon termination of the lease.")

class StandardBalanceSheet(BaseModel):
    assets: StandardBalanceSheetAssets
    liabilities: StandardBalanceSheetLiabilities
    equity: StandardBalanceSheetEquity
    ppe_details: Optional[PPEDetails] = Field(None, description="Detailed breakdown of Property, Plant, and Equipment if reported.")
    supplemental_metrics: Optional[BalanceSheetSupplemental] = Field(None, description="Derived metrics and share counts found in the balance sheet summary.")

# --- 3. CASH FLOW STATEMENT (Liquidity) ---
class StandardCashFlowStatement(BaseModel):
    # Operating
    net_cash_from_operations: float
    depreciation_amortization: float = Field(..., description="D&A added back to cash flow")
    stock_based_compensation: Optional[float] = Field(None, description="SBC added back")
    
    # Investing
    capex: float = Field(..., description="Payments for property, plant, and equipment")
    acquisitions: Optional[float] = Field(None, description="Cash paid for acquisitions")
    investment_purchases_sales_net: Optional[float] = Field(None, description="Net buy/sell of marketable securities")
    
    # Financing
    dividends_paid: Optional[float] = None
    share_repurchases: Optional[float] = Field(None, description="Buybacks")
    debt_issued: Optional[float] = None
    debt_repaid: Optional[float] = None

# --- 4. MASTER FACET ---
class StandardFinancialHealthFacet(BaseModel):
    fiscal_year: int
    fiscal_period: str
    currency: str = "USD"
    scale: str = "millions"
    
    income_statement: StandardIncomeStatement
    balance_sheet: StandardBalanceSheet
    cash_flow: StandardCashFlowStatement
    
    # Verification
    source_quotes: List[str]

# --- 5. RAW STATEMENTS ---
# Already handled through edgartools statements

# --- 6. RATIOS ---
# --- Custom Type for Time Series Data ---
# Structure: { "YYYY-MM-DD": value, "YYYY-MM-DD": value }
# Maps directly to the "data" dictionary in your input JSON.
RatioTimeSeries = Dict[str, Optional[float]]

class LiquidityRatios(BaseModel):
    current_ratio: Optional[RatioTimeSeries] = Field(
        default=None, 
        description="Current Assets / Current Liabilities. Format: {'date': value}"
    )
    quick_ratio: Optional[RatioTimeSeries] = Field(
        default=None, 
        description="(Current Assets - Inventory) / Current Liabilities"
    )
    cash_ratio: Optional[RatioTimeSeries] = Field(
        default=None, 
        description="Cash & Equivalents / Current Liabilities"
    )

class SolvencyRatios(BaseModel):
    debt_to_equity: Optional[RatioTimeSeries] = Field(
        default=None, description="Total Debt / Total Equity"
    )
    debt_to_assets: Optional[RatioTimeSeries] = Field(
        default=None, description="Total Debt / Total Assets"
    )
    interest_coverage: Optional[RatioTimeSeries] = Field(
        default=None, description="EBIT / Interest Expense"
    )
    equity_multiplier: Optional[RatioTimeSeries] = Field(
        default=None, description="Total Assets / Total Equity"
    )

class ProfitabilityRatios(BaseModel):
    gross_profit_margin: Optional[RatioTimeSeries] = Field(
        default=None, description="(Revenue - COGS) / Revenue"
    )
    operating_margin: Optional[RatioTimeSeries] = Field(
        default=None, description="Operating Income / Revenue"
    )
    net_profit_margin: Optional[RatioTimeSeries] = Field(
        default=None, description="Net Income / Revenue"
    )
    return_on_assets: Optional[RatioTimeSeries] = Field(
        default=None, description="Net Income / Total Assets"
    )
    return_on_equity: Optional[RatioTimeSeries] = Field(
        default=None, description="Net Income / Total Equity"
    )
    return_on_capital_employed: Optional[RatioTimeSeries] = Field(
        default=None, description="EBIT / (Total Assets - Current Liabilities)"
    )

class EfficiencyRatios(BaseModel):
    asset_turnover: Optional[RatioTimeSeries] = Field(
        default=None, description="Revenue / Average Total Assets"
    )
    inventory_turnover: Optional[RatioTimeSeries] = Field(
        default=None, description="COGS / Average Inventory"
    )
    receivables_turnover: Optional[RatioTimeSeries] = Field(
        default=None, description="Revenue / Average Accounts Receivable"
    )
    days_sales_outstanding: Optional[RatioTimeSeries] = Field(
        default=None, description="(Accounts Receivable / Revenue) * 365"
    )

class CashFlowRatios(BaseModel):
    operating_cash_flow_ratio: Optional[RatioTimeSeries] = Field(
        default=None, description="Operating Cash Flow / Current Liabilities"
    )
    free_cash_flow: Optional[RatioTimeSeries] = Field(
        default=None, description="Operating Cash Flow - CapEx"
    )
    cash_flow_coverage: Optional[RatioTimeSeries] = Field(
        default=None, description="Operating Cash Flow / Total Debt"
    )

# --- Main Entity Model ---

class EntityFinancialRatios(BaseModel):
    """
    Master schema for multi-period financial ratios.
    """
    entity_id: str = Field(..., description="Unique identifier (e.g. CIK or Ticker)")
    source_accession: Optional[str] = Field(None, description="From meta.accession_number")
    
    # Categories
    liquidity: LiquidityRatios = Field(default_factory=LiquidityRatios)
    solvency: SolvencyRatios = Field(default_factory=SolvencyRatios)
    profitability: ProfitabilityRatios = Field(default_factory=ProfitabilityRatios)
    efficiency: EfficiencyRatios = Field(default_factory=EfficiencyRatios)
    cash_flow: CashFlowRatios = Field(default_factory=CashFlowRatios)

# --- Example of How the Ratio Data Will Look ---

if __name__ == "__main__":
    # Simulating the calculated data structure based on your input style
    example_data = {
        "entity_id": "AAPL",
        "source_accession": "0000320193-21-000105",
        "liquidity": {
            "current_ratio": {
                "2019-09-28": 1.54,
                "2020-09-26": 1.36,
                "2021-09-25": 1.07
            },
            # "quick_ratio" is missing/None here
        },
        "cash_flow": {
            "free_cash_flow": {
                "2019-09-28": 58896000000.0,
                "2020-09-26": 73365000000.0,
                "2021-09-25": 92953000000.0
            }
        }
    }

    # Validation
    ratios = EntityFinancialRatios(**example_data)
    
    # Accessing data
    print(f"Entity: {ratios.entity_id}")
    print(f"2021 Current Ratio: {ratios.liquidity.current_ratio['2021-09-25']}")