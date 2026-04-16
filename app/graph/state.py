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
    # Scoring
    lead_score: float | None        # 0–100 composite; written to Lead_Score__c
    score_breakdown: dict | None    # {completeness, activity, marketing} for observability
    # SF REST credentials — set by the task runner, consumed by score_lead node
    sf_access_token: str | None
    sf_instance_url: str | None
