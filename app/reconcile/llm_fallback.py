"""Ollama local LLM fallback for genuine data conflicts."""

import json
import os

import httpx

_PROMPT_TEMPLATE = """
You are a data quality expert. Two sources disagree on a contact's {field}.

Lead context:
- Name: {name}
- Company: {company}

LinkedIn says: {linkedin_val}
ZoomInfo says: {zoominfo_val}

Choose the most likely correct value. Respond ONLY with valid JSON:
{{"chosen_value": "<value or null>", "rationale": "<one sentence>", "confidence": <0.0-1.0>}}
""".strip()


async def llm_resolve(
    field: str,
    linkedin_val: str | None,
    zoominfo_val: str | None,
    lead: dict,
) -> tuple[str | None, float]:
    """
    Ask the local Ollama model to pick between two conflicting values.
    Returns (chosen_value, confidence).
    Falls back to (None, 0.0) if Ollama is unavailable.
    """
    base_url = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
    model = os.getenv("OLLAMA_MODEL", "llama3.1:8b")

    name = f"{lead.get('first_name', '')} {lead.get('last_name', '')}".strip()
    prompt = _PROMPT_TEMPLATE.format(
        field=field,
        name=name or "Unknown",
        company=lead.get("company", "Unknown"),
        linkedin_val=linkedin_val or "(none)",
        zoominfo_val=zoominfo_val or "(none)",
    )

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{base_url}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                    "format": "json",
                },
            )
            resp.raise_for_status()
            raw = resp.json().get("response", "{}")
            parsed = json.loads(raw)

        chosen = parsed.get("chosen_value") or None
        confidence = float(parsed.get("confidence", 0.5))
        return chosen, confidence

    except Exception:
        # Ollama unavailable or parse error — skip rather than crash
        return None, 0.0
