from pydantic import BaseModel, Field, model_validator
from typing import Optional, Literal
from datetime import datetime


# 1. SUPPLY CHAIN INTEGRITY (Source: 10-K Risk Factors)
class SupplierConcentration(BaseModel):
    single_source_dependency: Optional[bool] = Field(
        None,
        description="True if 'sole source' or 'single supplier' is mentioned in Risk Factors."
    )
    geographic_concentration: Optional[str] = Field(
        None,
        description="Region of high dependency if mentioned (e.g., 'manufacturing in China')."
    )

class InventoryHealth(BaseModel):
    days_sales_of_inventory: Optional[float] = Field(
        None,
        description="Calculated: (Average Inventory / COGS) * 365. Metric for efficiency."
    )
    inventory_build_up_flag: bool = Field(
        False,
        description="True if Inventory grew >20% faster than Revenue YoY (Sign of unsold goods)."
    )

class SupplyChainIntegrity(BaseModel):
    supplier_concentration: SupplierConcentration
    inventory_health: InventoryHealth

# 2. OPERATIONAL INFRASTRUCTURE (Source: 10-K Item 2 & Cash Flow)
class AssetProfile(BaseModel):
    properties_model: Literal["Owned", "Leased", "Mixed", "Asset-Light"] = Field(
        "Mixed",
        description="Dominant term found in 10-K Item 2 'Properties'."
    )
    manufacturing_footprint_change: Optional[Literal["Expansion", "Contraction", "Stable"]] = Field(
        None,
        description="Inferred from 'Restructuring' (closing plants) or 'Capital Projects' (opening plants)."
    )

class CapexEfficiency(BaseModel):
    capex_to_depreciation_ratio: Optional[float] = Field(
        None,
        description="Calculated: Capex / Depreciation & Amortization. >1.0 implies growth; <1.0 implies contraction."
    )

class OperationalInfrastructure(BaseModel):
    asset_profile: AssetProfile
    capex_efficiency: CapexEfficiency

# 3. TECH & SECURITY (Source: 10-K Item 1C & 8-K Item 1.05)
class CyberRiskPosture(BaseModel):
    material_breach_detected: bool = Field(
        False,
        description="True if 8-K Item 1.05 filed (post-2023) or 'data breach' keywords in Risk Factors."
    )
    last_breach_date: Optional[str] = Field(
        None,
        description="Date of last disclosed incident."
    )

class PatentPortfolio(BaseModel):
    total_issued_patents: Optional[int] = Field(
        None, 
        description="Total count of active patents held. Source: 10-K Item 1 'Intellectual Property'."
    )
    pending_applications: Optional[int] = Field(
        None, 
        description="Count of patents submitted/pending. A proxy for 'Future Innovation'. Source: 10-K Item 1."
    )
    expiration_risk_year: Optional[int] = Field(
        None, 
        description="The year when 'material' patents begin to expire. Critical for Pharma/Tech. Source: 10-K."
    )

class IPMoatStatus(BaseModel):
    portfolio_metrics: PatentPortfolio 
    patent_cliff_warning: bool = Field(
        False,
        description="True if 'expiration' is listed as a Key Risk in Item 1A."
    )
    active_ip_litigation: bool = Field(
        False,
        description="True if 'infringement' lawsuits are mentioned in Item 3 'Legal Proceedings'."
    )

class TechnologySecurity(BaseModel):
    cyber_risk_posture: CyberRiskPosture
    ip_moat_status: IPMoatStatus

# 4. MASTER FACET SCHEMA
class OpsTechnologyFacet(BaseModel):
    """
    The structured output for the 'OPS_TECHNOLOGY' facet.
    """
    supply_chain_integrity: SupplyChainIntegrity
    operational_infrastructure: OperationalInfrastructure
    technology_and_security: TechnologySecurity