from pydantic import BaseModel, Field, model_validator
from typing import Optional, Literal
from datetime import datetime


# 1. INVESTOR RELATIONS (Source: 8-K Item 5.07 & Financials)
class ShareholderSentiment(BaseModel):
    say_on_pay_support_percent: Optional[float] = Field(
        None,
        description="Percentage of 'For' votes on executive comp. <70% implies anger. Source: 8-K Item 5.07."
    )
    shareholder_proposals_count: int = Field(
        0,
        description="Number of proposals submitted by shareholders (proxy for activism). Source: DEF 14A."
    )
    activist_campaign_flag: bool = Field(
        False,
        description="True if 'settlement agreement' with activist investor is mentioned in 8-K."
    )

class CapitalReturn(BaseModel):
    dividend_status: Literal["Growing", "Stable", "Cut", "None"] = Field(
        "None",
        description="3-year trend. 'Cut' is a major negative stakeholder signal."
    )
    total_returned_cash_mm: float = Field(
        ...,
        description="Dividends + Buybacks (Trailing 12M). Source: Cash Flow Statement."
    )

class InvestorRelations(BaseModel):
    shareholder_sentiment: ShareholderSentiment
    capital_return: CapitalReturn


# 2. REGULATORY & COMMUNITY (Source: 10-K Item 3 & Notes)
class LegalFriction(BaseModel):
    active_class_actions: bool = Field(
        False,
        description="True if 'class action' is listed in Item 3 'Legal Proceedings'."
    )
    regulatory_investigation_flag: bool = Field(
        False,
        description="True if SEC/DOJ/FTC 'investigation' or 'subpoena' is disclosed."
    )
    loss_contingency_accrual_mm: Optional[float] = Field(
        None,
        description="Reserved cash for legal settlements. Source: Notes 'Commitments and Contingencies'."
    )

class CommunityImpact(BaseModel):
    environmental_fines_mm: float = Field(
        0.0,
        description="Total fines disclosed related to EPA/Environmental. Source: 10-K Item 1 or 3."
    )
    reputation_risk_disclosure: bool = Field(
        False,
        description="True if 'reputation' or 'public perception' is explicitly listed as a Risk Factor."
    )

class RegulatoryCommunity(BaseModel):
    legal_friction: LegalFriction
    community_impact: CommunityImpact


# 3. MASTER FACET SCHEMA
class StakeholderAnalysisFacet(BaseModel):
    """
    The structured output for the 'STAKEHOLDER_ANALYSIS' facet.
    Focuses on Friction (Lawsuits/Fines) and Support (Votes/Capital).
    """
    investor_relations: InvestorRelations
    regulatory_community: RegulatoryCommunity