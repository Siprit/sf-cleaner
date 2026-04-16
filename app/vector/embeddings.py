"""Lead identity → embedding using OpenAI text-embedding-3-small."""

import os

import openai

_client: openai.AsyncOpenAI | None = None


def _get_client() -> openai.AsyncOpenAI:
    global _client
    if _client is None:
        _client = openai.AsyncOpenAI(api_key=os.environ["OPENAI_API_KEY"])
    return _client


def lead_identity_text(lead: dict) -> str:
    """Build a short canonical string that identifies a person."""
    name = f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()
    company = lead.get("company", "")
    parts = filter(None, [name, company])
    return " at ".join(parts) or lead.get("id", "unknown")


async def embed(text: str) -> list[float]:
    resp = await _get_client().embeddings.create(
        model="text-embedding-3-small",
        input=text,
    )
    return resp.data[0].embedding
