"""Advisors — where "autonomous" judgement enters the platform.

The optimisation loop is deterministic arithmetic: given a forecast and a
capacity ceiling it always produces the same schedule. What it cannot do is
*judge* — notice that the forecast is mostly unsettled, that a process failed to
schedule, or that a 2% saving is not worth the operational disruption of moving
a batch.

Two advisors implement the same interface:

* :class:`ClaudeAdvisor` — sends a structured summary to Claude and validates the
  JSON it returns.
* :class:`HeuristicAdvisor` — deterministic rules, no network, no key.

**The heuristic is not a placeholder.** It is what runs when there is no API key,
when the model call fails, or when the response cannot be validated. An
orchestrator that produces no advice because an LLM is unreachable would be
strictly worse than one that produces rule-based advice. Every caller falls back
to it, and every stored row records which advisor produced it via ``source``.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Protocol

import httpx
from pydantic import BaseModel, Field, ValidationError

from ecogrid.config import PlatformSettings

logger = logging.getLogger("ecogrid.orchestrator.advice")

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_VERSION = "2023-06-01"

SOURCE_CLAUDE = "claude"
SOURCE_HEURISTIC = "heuristic"


class Advice(BaseModel):
    """A recommendation, plus the reasoning that produced it."""

    headline: str = Field(min_length=1, max_length=280)
    rationale: str = ""
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    recommended_actions: list[str] = Field(default_factory=list)
    risk_flags: list[str] = Field(default_factory=list)
    #: ``claude`` | ``heuristic`` — set by the caller, never by the model.
    source: str = SOURCE_HEURISTIC


class Advisor(Protocol):
    """Anything that can turn platform state into advice."""

    async def advise(self, context: dict[str, Any]) -> Advice | None:
        """Return advice, or ``None`` if this advisor cannot produce any."""


# --------------------------------------------------------------------------- #
# Deterministic fallback
# --------------------------------------------------------------------------- #


class HeuristicAdvisor:
    """Rule-based advisor. Always available, always deterministic."""

    async def advise(self, context: dict[str, Any]) -> Advice | None:
        plan = context.get("plan") or {}
        grid = context.get("grid") or {}
        plant = context.get("plant") or {}

        saving_pct = float(plan.get("saving_pct") or 0.0)
        saved_kg = float(plan.get("carbon_saved_kg") or 0.0)
        run_id = plan.get("run_id")
        unscheduled = list(plan.get("unscheduled") or [])
        solver = plan.get("solver") or "none"

        window_count = int(grid.get("window_count") or 0)
        forecast_only = int(grid.get("forecast_only_count") or 0)
        avg_intensity = grid.get("avg_actual_intensity")

        actions: list[str] = []
        flags: list[str] = []
        confidence = 0.5

        if not run_id:
            return Advice(
                headline="No optimisation run available yet",
                rationale=(
                    "The optimizer needs at least two retained grid windows before it can "
                    "produce a schedule. Let ingestion run, then trigger a run."
                ),
                confidence=0.2,
                recommended_actions=[
                    "Confirm the ingestion worker is publishing",
                    "POST /api/v1/optimize/run once telemetry is retained",
                ],
                risk_flags=["no schedule to advise on"],
                source=SOURCE_HEURISTIC,
            )

        # --- operational risk flags ---------------------------------------- #
        if unscheduled:
            flags.append(
                f"{len(unscheduled)} process(es) could not be scheduled: {', '.join(unscheduled)}"
            )
            actions.append("Review flexible capacity or widen the optimizer horizon")
        if window_count and forecast_only > window_count / 2:
            flags.append("Advice is based mostly on forecast, not settled, intensity")
            confidence -= 0.15
        if avg_intensity is None:
            flags.append("No settled (actual) intensity available in the retained windows")
            confidence -= 0.1
        if not plant:
            flags.append("No plant telemetry — capacity ceiling unknown")
            confidence -= 0.1
        if solver != "local-greedy-v1":
            actions.append(f"Schedule produced by '{solver}' — verify before dispatch")

        # --- the actual recommendation -------------------------------------- #
        if saving_pct >= 10:
            headline = f"Strong arbitrage available — dispatch as scheduled ({saving_pct:.1f}% / {saved_kg:.0f} kg CO2e)"
            rationale = (
                f"Moving flexible load into the cleanest windows avoids roughly "
                f"{saved_kg:.0f} kg CO2e versus running as early as possible — a "
                f"{saving_pct:.1f}% reduction. That is well above the threshold where "
                f"the operational cost of shifting a batch is justified."
            )
            actions.insert(0, "Release the published schedule to plant control")
            confidence += 0.35
        elif saving_pct >= 2:
            headline = f"Modest arbitrage available ({saving_pct:.1f}% / {saved_kg:.0f} kg CO2e)"
            rationale = (
                "There is a real but small gain from shifting load. Worth taking if the "
                "affected processes tolerate rescheduling; skip it if a batch move risks "
                "product quality."
            )
            actions.insert(0, "Dispatch if the affected batches tolerate rescheduling")
            confidence += 0.2
        else:
            headline = "Grid is flat — little to gain from shifting load"
            rationale = (
                f"The spread between the cleanest and dirtiest retained windows is small "
                f"({saving_pct:.1f}% achievable). Holding the current schedule avoids "
                f"disruption for negligible carbon benefit."
            )
            actions.insert(0, "Hold the current plant schedule")
            confidence += 0.1

        return Advice(
            headline=headline,
            rationale=rationale,
            confidence=max(0.0, min(1.0, round(confidence, 2))),
            recommended_actions=actions,
            risk_flags=flags,
            source=SOURCE_HEURISTIC,
        )


# --------------------------------------------------------------------------- #
# Claude
# --------------------------------------------------------------------------- #

SYSTEM_PROMPT = """You are the AI Orchestrator for EcoGrid OS, an industrial energy
arbitrage and decarbonization platform. You review the output of a deterministic
scheduler and give a dispatcher a short, actionable recommendation.

Rules:
- Reason only about the data provided. Never invent plant names, prices, or figures.
- Be conservative: if the gain is small, say so and recommend holding.
- Flag risk explicitly when the decision rests on forecast rather than settled data.
- Respond with JSON ONLY — no prose, no code fences, no commentary.

The JSON object must have exactly these keys:
  "headline": string, <=280 chars, the recommendation in one line
  "rationale": string, 2-4 sentences of reasoning
  "confidence": number between 0 and 1
  "recommended_actions": array of strings, each an imperative action
  "risk_flags": array of strings, each a caveat (empty array if none)
"""

_JSON_BLOCK = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL)


def _build_user_prompt(context: dict[str, Any]) -> str:
    return (
        "Current platform state:\n\n"
        f"{json.dumps(context, indent=2, default=str)}\n\n"
        "Produce the JSON recommendation now."
    )


def _extract_json(text: str) -> Any:
    """Model output -> parsed JSON, tolerating a surrounding code fence."""
    stripped = text.strip()
    match = _JSON_BLOCK.search(stripped)
    if match:
        stripped = match.group(1).strip()
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        # Last resort: the object may be embedded in stray prose.
        start, end = stripped.find("{"), stripped.rfind("}")
        if start != -1 and end > start:
            return json.loads(stripped[start : end + 1])
        raise


class ClaudeAdvisor:
    """Calls Claude and validates the structured reply.

    Returns ``None`` on *any* failure — no key, transport error, non-JSON reply,
    schema mismatch — so the caller falls back to the heuristic advisor. Advice
    that cannot be validated is never stored, because unvalidated model output
    reaching a dispatcher is worse than no advice at all.
    """

    def __init__(self, settings: PlatformSettings, client: Any = None) -> None:
        self._settings = settings
        self._client = client

    @property
    def is_configured(self) -> bool:
        return bool(self._settings.anthropic_api_key)

    async def advise(self, context: dict[str, Any]) -> Advice | None:
        if not self.is_configured:
            logger.info("No ANTHROPIC key configured — using the heuristic advisor")
            return None

        body = {
            "model": self._settings.anthropic_model,
            "max_tokens": self._settings.anthropic_max_tokens,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": _build_user_prompt(context)}],
        }
        headers = {
            "x-api-key": self._settings.anthropic_api_key or "",
            "anthropic-version": ANTHROPIC_VERSION,
            "content-type": "application/json",
        }

        try:
            if self._client is not None:
                response = await self._client.post(ANTHROPIC_URL, headers=headers, json=body)
                status_code = response.status_code
                payload = response.json()
            else:
                async with httpx.AsyncClient(
                    timeout=self._settings.anthropic_timeout_seconds
                ) as client:
                    response = await client.post(ANTHROPIC_URL, headers=headers, json=body)
                status_code = response.status_code
                payload = response.json()
        except Exception as exc:  # noqa: BLE001 — any transport failure means fall back
            logger.warning("Claude request failed (%s) — falling back to heuristic", exc)
            return None

        if status_code >= 400:
            logger.warning(
                "Claude returned HTTP %s — falling back to heuristic", status_code
            )
            return None

        try:
            content = payload.get("content") or []
            text = "".join(block.get("text", "") for block in content if isinstance(block, dict))
            if not text.strip():
                raise ValueError("empty response content")
            data = _extract_json(text)
        except (ValidationError, ValueError, TypeError, json.JSONDecodeError) as exc:
            logger.warning("Could not parse Claude output (%s) — falling back", exc)
            return None

        try:
            advice = Advice.model_validate(data)
        except ValidationError as exc:
            logger.warning("Claude output failed schema validation (%s) — falling back", exc)
            return None

        advice.source = SOURCE_CLAUDE
        return advice


def build_advisor(settings: PlatformSettings, client: Any = None) -> ClaudeAdvisor:
    """The Claude advisor; callers fall back to the heuristic when it yields None."""
    return ClaudeAdvisor(settings, client=client)
