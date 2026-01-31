from pydantic import BaseModel, Field
from typing import Optional

# 1. GOVERNANCE RISK (Source: DEF 14A / Proxy Statement)
class BoardStructure(BaseModel):
    total_board_size: Optional[int] = Field(
        None, 
        description="Total number of directors on the board. Raw data."
    )
    independent_director_count: Optional[int] = Field(
        None, 
        description="Number of directors identified as independent. Raw data."
    )
    is_ceo_chairman_combined: Optional[bool] = Field(
        None, 
        description="True if CEO also holds Chairman title."
    )
    dual_class_structure: Optional[bool] = Field(
        None, 
        description="True if multiple share classes exist with different voting rights."
    )
    classified_board: Optional[bool] = Field(
        None, 
        description="True if directors serve staggered terms (anti-takeover defense)."
    )
    # We keep the ratio as it's a standard quick-reference metric, 
    # but it can now be re-verified using the raw counts above.
    independent_director_ratio: Optional[float] = Field(
        None, 
        ge=0, le=1, 
        description="Calculated: Independent Directors / Total Board Size."
    )

class InsiderAlignment(BaseModel):
    executive_ownership_percent: Optional[float] = Field(
        None, 
        description="Total % beneficial ownership by directors/executives."
    )
    pledged_shares_flag: Optional[bool] = Field(
        None, 
        description="True if executives have pledged shares as collateral."
    )

class GovernanceRisk(BaseModel):
    board_structure: BoardStructure
    insider_alignment: InsiderAlignment
    shareholder_rights_plan: bool = Field(
        False, 
        description="AKA 'Poison Pill'. True if active."
    )

# 2. EXECUTIVE STABILITY (Source: 8-K, 10-K, Form 4)
class CSuiteTurnover(BaseModel):
    last_12m_departures: int = Field(
        0, 
        ge=0, 
        description="Count of 8-K filings with Item 5.02 (Departure) in trailing 12 months."
    )
    cfo_tenure_years: Optional[float] = Field(
        None, 
        description="Years current CFO has held the role."
    )
    auditor_change_flag: bool = Field(
        False, 
        description="True if 8-K Item 4.01 filed (Auditor Resignation/Dismissal)."
    )

class InsiderTradingSummary(BaseModel):
    """
    Aggregated raw data from Form 4 filings over the lookback period (e.g., 6 months).
    Preserves magnitude and directionality.
    """
    total_buy_volume_usd: float = Field(
        0.0, 
        description="Total USD value of Open Market Purchases (Code 'P')."
    )
    total_sell_volume_usd: float = Field(
        0.0, 
        description="Total USD value of Open Market Sales (Code 'S')."
    )
    transaction_count: int = Field(
        0, 
        description="Total number of open market transactions processed."
    )
    unique_insiders_count: int = Field(
        0, 
        description="Number of distinct insiders who transacted."
    )
    net_activity_usd: float = Field(
        0.0, 
        description="Calculated: Buy Volume - Sell Volume. Positive = Net Buying."
    )

class CompensationSignals(BaseModel):
    pay_ratio_ceo_to_median: Optional[int] = Field(
        None, 
        description="CEO Pay Ratio."
    )
    # REPLACED: "recent_insider_selling" (Qualitative) -> "insider_trading" (Quantitative)
    insider_trading: InsiderTradingSummary = Field(
        default_factory=InsiderTradingSummary
    )

class ExecutiveStability(BaseModel):
    c_suite_turnover: CSuiteTurnover
    compensation_signals: CompensationSignals

# 3. OPERATIONAL EFFICIENCY (Source: 10-K & 10-Q Financials)
class WorkforceDynamics(BaseModel):
    total_employees: Optional[int] = Field(
        None, 
        description="Full-time employee count."
    )
    fiscal_year_revenue_mm: Optional[float] = Field(
        None, 
        description="Total Revenue (in Millions) used to calculate efficiency. Raw Data."
    )
    yoy_headcount_change: Optional[float] = Field(
        None, 
        description="Calculated % change from previous year."
    )
    revenue_per_employee: Optional[float] = Field(
        None, 
        description="Calculated: Total Revenue / Total Employees (in Millions)."
    )

class RestructuringActivity(BaseModel):
    active_restructuring_program: Optional[bool] = Field(
        None, 
        description="True if 'Restructuring charges' appear in financials."
    )
    last_charge_amount_mm: float = Field(
        0.0, 
        description="Amount of restructuring charges (in millions)."
    )

class OperationalEfficiency(BaseModel):
    workforce_dynamics: WorkforceDynamics
    restructuring_activity: RestructuringActivity

# 4. MASTER FACET SCHEMA
class LeadershipOrgFacet(BaseModel):
    governance_risk: GovernanceRisk
    executive_stability: ExecutiveStability
    operational_efficiency: OperationalEfficiency