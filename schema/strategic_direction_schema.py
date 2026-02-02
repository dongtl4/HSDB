from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date

# 1. CORPORATE STRATEGY (Source: 10-K Item 1 "Business")
class CorporateStrategy(BaseModel):
    stated_strategic_pillars: List[str] = Field(
        default_factory=list,
        description="List of top-level headers identified in the 'Strategy' section."
    )
    business_overview_text: Optional[str] = Field(
        None,
        description="A concise extraction of the 'General' or 'Overview' subsection of Item 1, describing what the company actually does."
    )
    strategy_discussion_text: Optional[str] = Field(
        None,
        description="Raw text content extracted from the 'Our Strategy' or 'Strategic Objectives' subsection."
    )

# 2. CAPITAL ALLOCATION (Source: 10-K Item 7, Cash Flow, 8-K)
class AcquisitionEvent(BaseModel):
    target_name: str
    deal_value_mm: Optional[float]
    closing_date: Optional[date]
    description: Optional[str] = Field(None, description="Short text summary of the deal purpose.")

class DivestitureEvent(BaseModel):
    asset_name: str
    sale_price_mm: Optional[float]
    closing_date: Optional[date]
    description: Optional[str] = Field(None, description="Short text summary of the divestiture.")

class CapitalAllocation(BaseModel):
    # Textual Framework (The "Why" and "How")
    capital_allocation_priorities_text: Optional[str] = Field(
        None,
        description="Extracted text from MD&A 'Liquidity and Capital Resources' describing management's hierarchy of capital uses."
    )
    dividend_and_buyback_policy_text: Optional[str] = Field(
        None,
        description="Text describing the current share repurchase program authorization and dividend policy."
    )
    
    # Event Driven Data (The "What")
    recent_material_acquisitions: List[AcquisitionEvent] = Field(
        default_factory=list,
        description="List of acquisitions closed in the reporting period."
    )
    recent_material_divestitures: List[DivestitureEvent] = Field(
        default_factory=list,
        description="List of assets sold or spun off in the reporting period."
    )

# 3. FORWARD LOOKING (Source: 10-K Item 1A & Item 7)
class FutureOutlook(BaseModel):
    management_outlook_discussion: Optional[str] = Field(
        None,
        description="Extracted text from MD&A sections labeled 'Outlook', 'Trend Information', or 'Future Expectations'."
    )
    item_1a_risk_factors_word_count: Optional[int] = Field(
        None,
        description="Total word count of Item 1A. Useful for calculating uncertainty growth YoY."
    )
    top_risk_factors_summary: Optional[str] = Field(
        None,
        description="Text listing the headers or summary of the first 3-5 Risk Factors (often the most critical)."
    )

# 4. MASTER FACET SCHEMA
class StrategicDirectionFacet(BaseModel):
    """
    The structured output for the 'STRATEGIC_DIRECTION' facet.
    Focuses on textual evidence of strategy and raw event data.
    """
    corporate_strategy: CorporateStrategy
    capital_allocation_framework: CapitalAllocation
    forward_looking_guidance: FutureOutlook