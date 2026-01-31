from pydantic import BaseModel, Field
from typing import List, Optional

# --- 1. SEGMENTATION (Raw Data from Notes) ---
class ProductSegment(BaseModel):
    segment_name: str = Field(..., description="Name of the operating segment as reported.")
    revenue_amount: float = Field(..., description="Raw revenue assigned to this segment.")
    operating_income: Optional[float] = Field(None, description="Raw operating income or loss for this segment (if reported).")
    assets: Optional[float] = Field(None, description="Raw identifiable assets assigned to this segment (if reported).")

class GeographicSegment(BaseModel):
    region: str = Field(..., description="Geographic region name (e.g., 'North America', 'EMEA').")
    revenue_amount: float = Field(..., description="Raw revenue attributed to this region.")

# --- 2. MARKET POSITION & RISK (Text Extraction) ---
class MarketPosition(BaseModel):
    competitors: List[str] = Field(
        default_factory=list, 
        description="List of specific competitor names extracted from 'Competition' section."
    )
    major_customers: List[str] = Field(
        default_factory=list, 
        description="Names of significant customers if explicitly disclosed."
    )
    
    # Concentration Risk (Raw facts only)
    top_customer_revenue_percent: Optional[float] = Field(
        None, 
        description="Percentage of total revenue derived from the largest single customer (if stated)."
    )
    government_contract_dependency: Optional[bool] = Field(
        None, 
        description="True if the company explicitly states a material dependency on government contracts."
    )

# --- 3. BUSINESS CHARACTERISTICS (Structural Facts from Item 1) ---
class BusinessCharacteristics(BaseModel):
    # Seasonality
    is_seasonal: Optional[bool] = Field(
        None, 
        description="True if the business self-identifies as seasonal."
    )
    seasonality_desc: Optional[str] = Field(
        None, 
        description="Short excerpt describing the seasonal pattern (e.g., 'Sales generally higher in Q4')."
    )
    
    # Operational Scale
    employees_total: Optional[int] = Field(
        None, 
        description="Total number of full-time employees."
    )
    backlog_amount: Optional[float] = Field(
        None, 
        description="Total dollar value of backlog or remaining performance obligations (RPO)."
    )
    
    # Supply & Distribution
    significant_raw_materials: List[str] = Field(
        default_factory=list, 
        description="List of critical raw materials or components mentioned as potential supply chain risks."
    )
    distribution_channels: List[str] = Field(
        default_factory=list, 
        description="Primary methods of distribution extracted (e.g., 'Direct-to-consumer', 'Wholesale')."
    )

# --- 4. MASTER FACET ---
class MarketProductFacet(BaseModel):
    """
    HSDB Facet: Market & Product
    """
    product_segments: List[ProductSegment] = Field(default_factory=list)
    geographic_segments: List[GeographicSegment] = Field(default_factory=list)
    market_position: MarketPosition
    business_characteristics: BusinessCharacteristics