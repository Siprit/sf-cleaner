from typing import Literal, TypedDict


class LeadState(TypedDict, total=False):
    lead_id: str
    raw_lead: dict                  # original fields from Salesforce
    linkedin_data: dict | None      # result from LinkedIn adapter
    zoominfo_data: dict | None      # result from ZoomInfo adapter
    reconciled: dict | None         # merged output {email, phone, ...}
    confidence: float               # 0.0 – 1.0
    cache_hit: bool
    action: Literal["update", "review", "skip"]
    error: str | None
