from pydantic import BaseModel, Field, model_validator
from typing import List, Optional, Literal
from datetime import date, datetime


# 1. CORPORATE STRATEGY (Source: 10-K Item 1 "Business")
class BusinessModelEvolution(BaseModel):
    recurring_revenue_focus: bool = Field(
        False,
        description="True if MD&A emphasizes 'ARR', 'Subscription', or 'Recurring' revenue."
    )
    strategic_pivot_flag: bool = Field(
        False,
        description="True if 'Strategy' headers change significantly from previous 10-K (Agent determines)."
    )

class CorporateStrategy(BaseModel):
    stated_strategic_pillars: List[str] = Field(
        default_factory=list,
        description="List of top-level headers in 10-K 'Our Strategy' section."
    )
    business_model_evolution: BusinessModelEvolution


# 2. CAPITAL ALLOCATION (Source: Cash Flow & 8-K)
class AcquisitionEvent(BaseModel):
    target_name: str
    deal_value_mm: Optional[float]
    closing_date: date

class CapitalAllocation(BaseModel):
    # Calculated Fields (Hard Data)
    organic_investment_ratio: float = Field(
        ...,
        description="Calculated: Capex / Operating Cash Flow. High = Investing for growth."
    )
    shareholder_yield_focus: bool = Field(
        ...,
        description="True if (Buybacks + Dividends) > (Capex + M&A). Source: Cash Flow Statement."
    )
    
    # Event Driven Data
    recent_material_acquisitions: List[AcquisitionEvent] = Field(
        default_factory=list,
        description="List of acquisitions closed in the reporting period (Source: 8-K Item 2.01)."
    )
    divestiture_active: bool = Field(
        False,
        description="True if 8-K Item 2.01 'Completion of Disposition' is filed."
    )


# 3. FORWARD LOOKING (Source: 10-K Item 1A & 8-K Item 2.02)
class FutureOutlook(BaseModel):
    guidance_issued: bool = Field(
        False,
        description="True if 8-K Item 2.02 contains a 'Outlook' or 'Guidance' table."
    )
    risk_factor_growth_warning: bool = Field(
        False,
        description="True if 10-K Item 1A word count increased >10% YoY (Proxy for uncertainty)."
    )


# 4. MASTER FACET SCHEMA
class StrategicDirectionFacet(BaseModel):
    """
    The structured output for the 'STRATEGIC_DIRECTION' facet.
    Tracks 'Stated Strategy' vs 'Actual Spending'.
    """
    corporate_strategy: CorporateStrategy
    capital_allocation_framework: CapitalAllocation
    forward_looking_guidance: FutureOutlook