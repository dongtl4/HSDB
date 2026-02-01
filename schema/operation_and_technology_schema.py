from pydantic import BaseModel, Field
from typing import Optional, List

# 1. SUPPLY CHAIN (Source: 10-K Item 1A Risk Factors, Item 7 MD&A)
class SupplyChainData(BaseModel):
    major_suppliers: Optional[List[str]] = Field(
        default=None, 
        description="List of specific entity names identified as major or sole suppliers."
    )
    geographic_dependencies: Optional[List[str]] = Field(
        default=None,
        description="List of regions or countries identified as critical for supply chain or manufacturing (e.g., 'manufacturing in China')."
    )
    raw_material_volatility_snippet: Optional[str] = Field(
        default=None,
        description="Extracted snippet describing specific raw materials subject to price volatility or availability shortages."
    )

# 2. INVENTORY (Source: 10-K Notes to Financial Statements)
class InventoryBreakdown(BaseModel):
    raw_materials_value: Optional[float] = Field(
        None, 
        description="Reported value of Raw Materials inventory (in millions)."
    )
    work_in_process_value: Optional[float] = Field(
        None, 
        description="Reported value of Work-in-Process inventory (in millions)."
    )
    finished_goods_value: Optional[float] = Field(
        None, 
        description="Reported value of Finished Goods inventory (in millions)."
    )

# 3. OPERATIONAL INFRASTRUCTURE (Source: 10-K Item 2 Properties)
class PropertyMetrics(BaseModel):
    total_square_footage: Optional[float] = Field(
        None, 
        description="Total square footage of properties (office, manufacturing, retail, etc.)."
    )
    owned_square_footage: Optional[float] = Field(
        None, 
        description="Square footage of owned properties."
    )
    leased_square_footage: Optional[float] = Field(
        None, 
        description="Square footage of leased properties."
    )
    facilities_count: Optional[int] = Field(
        None, 
        description="Count of specific facilities (e.g., manufacturing plants, data centers, distribution centers)."
    )

# 4. TECHNOLOGY & IP (Source: 10-K Item 1 Business, Notes)
class IntellectualProperty(BaseModel):
    rd_expenses: Optional[float] = Field(
        None, 
        description="Research and development expenses for the period (in millions)."
    )
    patents_issued_count: Optional[int] = Field(
        None, 
        description="Total number of issued patents held."
    )
    patents_pending_count: Optional[int] = Field(
        None, 
        description="Total number of pending patent applications."
    )

# 5. CYBERSECURITY (Source: 10-K Item 1C, 8-K Item 1.05)
class SecurityIncident(BaseModel):
    date_reported: Optional[str] = Field(
        None, 
        description="Date of the incident or disclosure."
    )
    description: Optional[str] = Field(
        None, 
        description="Brief description of the nature of the breach or incident."
    )

class CybersecurityPosture(BaseModel):
    reported_incidents: Optional[List[SecurityIncident]] = Field(
        default=None, 
        description="List of material cybersecurity incidents disclosed in filings."
    )
    cyber_insurance_mentioned: bool = Field(
        False, 
        description="True if cyber insurance coverage is explicitly mentioned in Item 1C."
    )

# 6. MASTER FACET SCHEMA
class OpsTechnologyFacet(BaseModel):
    """
    The structured output for the 'OPS_TECHNOLOGY' facet.
    Focuses on extracting hard data points regarding supply chain, assets, IP, and security
    without inferring subjective risks or calculating derived ratios.
    """
    supply_chain: SupplyChainData
    inventory_breakdown: InventoryBreakdown
    operational_infrastructure: PropertyMetrics
    intellectual_property: IntellectualProperty
    cybersecurity: CybersecurityPosture