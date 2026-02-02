from pydantic import BaseModel, Field
from typing import Optional

# 1. SHAREHOLDER DEMOCRACY (Source: 8-K Item 5.07, DEF 14A, SC 13D)
class ShareholderDemocracy(BaseModel):
    say_on_pay_support_percent: Optional[float] = Field(
        None,
        description="Percentage of 'For' votes on executive comp. Source: 8-K Item 5.07."
    )
    director_election_min_support_percent: Optional[float] = Field(
        None,
        description="The lowest percentage of 'For' votes received by any director standing for election. <90% often signals specific dissatisfaction."
    )
    shareholder_proposals_count: int = Field(
        0,
        description="Number of proposals submitted by shareholders. Source: DEF 14A."
    )
    activist_13d_filing_count: int = Field(
        0,
        description="Count of SC 13D (Activist Stake) filings submitted in the period. Source: File System."
    )

# 2. LEGAL & REGULATORY (Source: 10-K Item 3 & Notes)
class LegalAndRegulatory(BaseModel):
    active_class_actions_flag: bool = Field(
        False,
        description="True if 'class action' is explicitly listed in Item 3 'Legal Proceedings'."
    )
    loss_contingency_accrual_mm: Optional[float] = Field(
        None,
        description="Reserved cash for legal settlements (Loss Contingency). Source: Notes to Financials."
    )
    unrecognized_tax_benefits_mm: Optional[float] = Field(
        None,
        description="Amount of unrecognized tax benefits (FIN 48 liability). Measure of aggressive tax positions. Source: Notes (Income Taxes)."
    )
    environmental_fines_mm: float = Field(
        0.0,
        description="Total fines disclosed related to EPA/Environmental matters. Source: 10-K."
    )

# 3. LABOR RELATIONS (Source: 10-K Item 1 'Human Capital')
class LaborRelations(BaseModel):
    # Unionization (Power)
    unionized_workforce_percent: Optional[float] = Field(
        None,
        description="Percentage of employees represented by labor unions or collective bargaining agreements."
    )
    work_stoppage_flag: bool = Field(
        False,
        description="True if 'strikes', 'work stoppages', or 'labor disputes' are disclosed as active risks or events."
    )
    
    # Diversity (Demographics - Hard Data Only)
    female_employee_percent: Optional[float] = Field(
        None,
        description="Global percentage of women in the workforce."
    )
    minority_employee_percent: Optional[float] = Field(
        None,
        description="Percentage of employees identifying as racial/ethnic minorities (US usually)."
    )
    
    # Stability (Turnover)
    voluntary_turnover_percent: Optional[float] = Field(
        None,
        description="Reported voluntary turnover rate. High turnover indicates internal friction."
    )

# 4. CUSTOMER QUALITY (Source: 10-K Notes 'Product Warranty')
class CustomerQuality(BaseModel):
    warranty_provision_mm: Optional[float] = Field(
        None,
        description="New warranty accruals charged to expense during the period. High relative to revenue indicates quality issues."
    )
    warranty_liability_mm: Optional[float] = Field(
        None,
        description="Total ending balance of warranty liability."
    )

# 5. MASTER FACET SCHEMA
class StakeholderAnalysisFacet(BaseModel):
    """
    The structured output for the 'STAKEHOLDER_ANALYSIS' facet.
    Covers Shareholders, Government, Employees, and Customers.
    """
    shareholder_democracy: ShareholderDemocracy
    legal_and_regulatory: LegalAndRegulatory
    labor_relations: LaborRelations
    customer_quality: CustomerQuality