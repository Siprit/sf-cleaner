from pydantic import BaseModel, Field


class Lead(BaseModel):
    id: str
    first_name: str | None = Field(None, alias="FirstName")
    last_name: str | None = Field(None, alias="LastName")
    email: str | None = Field(None, alias="Email")
    phone: str | None = Field(None, alias="Phone")
    company: str | None = Field(None, alias="Company")
    title: str | None = Field(None, alias="Title")

    model_config = {"populate_by_name": True}

    @property
    def display_name(self) -> str:
        parts = filter(None, [self.first_name, self.last_name])
        return " ".join(parts) or self.id


class LeadUpdate(BaseModel):
    """Fields to write back to Salesforce."""
    id: str
    email: str | None = None
    phone: str | None = None
    lead_score: float | None = None       # Lead_Score__c
    company_size: str | None = None       # Company_Size__c
    industry: str | None = None           # Industry_Vertical__c
    annual_revenue: str | None = None     # Annual_Revenue__c
    tech_stack: str | None = None         # Tech_Stack__c
