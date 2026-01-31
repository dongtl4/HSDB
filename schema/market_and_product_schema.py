from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from datetime import datetime


# 1. MARKET POSITION (Source: 10-K Risk Factors/Business)
class ConcentrationRisk(BaseModel):
    top_customer_revenue_percent: Optional[float] = Field(
        None,
        description="Percentage of total revenue from single largest customer. Source: 10-K Note 1 or Risk Factors."
    )
    government_contract_dependency: bool = Field(
        ...,
        description="True if material revenue comes from gov contracts. Source: 10-K Business."
    )

class MarketPosition(BaseModel):
    reported_market_share: Optional[Literal["Leading", "Significant", "Minor", "N/A"]] = Field(
        "N/A",
        description="Self-reported position in primary market. Source: 10-K 'Competition' section."
    )
    competitor_names: List[str] = Field(
        default_factory=list,
        description="List of named competitors. Source: 10-K 'Competition' section."
    )
    concentration_risk: ConcentrationRisk
    gross_margin_trend: Literal["Expanding", "Stable", "Contracting"] = Field(
        ...,
        description="3-year trend of Gross Margin. Calculated from Financials."
    )


# 2. PRODUCT PORTFOLIO (Source: 10-K Segment Reporting)
class ProductSegment(BaseModel):
    segment_name: str = Field(..., description="Name of the operating segment.")
    revenue_amount_mm: float
    yoy_growth_percent: float
    operating_margin_percent: Optional[float]

class InnovationPipeline(BaseModel):
    rd_spending_ratio: float = Field(
        ...,
        description="R&D Expenses / Total Revenue. Source: Income Statement."
    )
    new_product_launches: bool = Field(
        False,
        description="Flag if 'new product launch' is mentioned in MD&A."
    )

class ProductPortfolio(BaseModel):
    revenue_mix: List[ProductSegment] = Field(
        ...,
        description="Breakdown of operating segments. Source: 10-K/10-Q Notes 'Segment Information'."
    )
    innovation_pipeline: InnovationPipeline

# 3. CUSTOMER HEALTH (Source: Balance Sheet/MD&A)
class DemandIndicators(BaseModel):
    backlog_amount_mm: Optional[float] = Field(
        None,
        description="Total value of unfilled orders. Source: 10-K 'Backlog' or MD&A."
    )
    deferred_revenue_growth: Optional[float] = Field(
        None,
        description="YoY growth of Deferred Revenue (Liability). Source: Balance Sheet."
    )
    inventory_turnover: Optional[float] = Field(
        None,
        description="Calculated: COGS / Average Inventory. Source: Financial Statements."
    )

class CustomerHealth(BaseModel):
    demand_indicators: DemandIndicators


# 4. MASTER FACET SCHEM
class MarketProductFacet(BaseModel):
    """
    The structured output for the 'MARKET_PRODUCT' facet in HSDB.
    Sources: 10-K/10-Q (Segment Notes, Risk Factors, MD&A).
    """
    market_position: MarketPosition
    product_portfolio: ProductPortfolio
    customer_health: CustomerHealth