"""Unit tests for the Salesforce Bulk API client."""

from unittest.mock import MagicMock, patch

import pytest

from app.salesforce.client import SalesforceClient
from app.salesforce.models import Lead, LeadUpdate


@pytest.fixture
def client():
    with patch("app.salesforce.client.SalesforceSettings") as MockSettings:
        MockSettings.return_value = MagicMock(
            sf_username="user@example.com",
            sf_consumer_key="key123",
            sf_private_key_path="/tmp/key.pem",
            sf_domain="login",
        )
        c = SalesforceClient()
        c._access_token = "fake_token"
        c._instance_url = "https://myorg.salesforce.com"
        return c


def test_to_csv():
    rows = [{"Id": "1", "Email": "a@b.com", "Phone": ""}]
    csv = SalesforceClient._to_csv(rows, ["Id", "Email", "Phone"])
    assert "a@b.com" in csv
    assert csv.startswith("Id,Email,Phone")


def test_lead_update_model():
    update = LeadUpdate(id="00Q1", email="new@acme.com", phone="+1234567890")
    assert update.id == "00Q1"
    assert update.email == "new@acme.com"


def test_lead_display_name():
    lead = Lead(id="001", FirstName="Jane", LastName="Doe")
    assert lead.display_name == "Jane Doe"


def test_lead_display_name_fallback():
    lead = Lead(id="001ID")
    assert lead.display_name == "001ID"
