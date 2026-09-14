"""AI Orchestrator tests — advice, fallback behaviour, and tracking.

The critical property under test is the **fallback**: whenever the model is
unavailable or its reply cannot be validated, the advisor must return ``None`` so
the caller substitutes deterministic rule-based advice. Advice must never go
silent, and unvalidated model output must never reach a dispatcher.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ecogrid.config import PlatformSettings
from ecogrid.orchestrator.advice import (
    SOURCE_CLAUDE,
    SOURCE_HEURISTIC,
    ClaudeAdvisor,
    HeuristicAdvisor,
    _extract_json,
)
from ecogrid.orchestrator.tracking import FileTrackingSink, build_tracker


# --------------------------------------------------------------------------- #
# Fakes
# --------------------------------------------------------------------------- #


class FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


class FakeClient:
    """Records calls and returns a canned response (or raises)."""

    def __init__(self, response: Any = None, raise_exc: Exception | None = None) -> None:
        self._response = response
        self._raise = raise_exc
        self.calls: list[dict[str, Any]] = []

    async def post(self, url: str, headers: Any = None, json: Any = None) -> Any:
        self.calls.append({"url": url, "headers": headers, "json": json})
        if self._raise is not None:
            raise self._raise
        return self._response


def claude_payload(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}]}


def valid_advice_json() -> str:
    return json.dumps(
        {
            "headline": "Dispatch as scheduled — 18% saving available",
            "rationale": "The cleanest block is well below the daily average.",
            "confidence": 0.82,
            "recommended_actions": ["Release the schedule"],
            "risk_flags": ["Based on forecast"],
        }
    )


def base_context(**over: Any) -> dict[str, Any]:
    context: dict[str, Any] = {
        "grid": {
            "window_count": 24,
            "forecast_only_count": 2,
            "avg_actual_intensity": 210.0,
            "avg_renewable_percentage": 41.0,
            "cleanest_window": {"window_from": "2026-01-01T00:00:00+00:00", "intensity": 80.0},
            "dirtiest_window": {"window_from": "2026-01-01T12:00:00+00:00", "intensity": 390.0},
        },
        "plan": {
            "run_id": "run-123",
            "solver": "local-greedy-v1",
            "horizon_windows": 24,
            "decision_count": 72,
            "carbon_saved_kg": 4200.0,
            "saving_pct": 18.0,
            "unscheduled": [],
        },
        "plant": {"plant_count": 1, "total_flexible_mw": 14.0, "any_estimated": True},
    }
    context.update(over)  # shallow override is enough for these cases
    return context


# --------------------------------------------------------------------------- #
# Heuristic advisor
# --------------------------------------------------------------------------- #


async def test_heuristic_recommends_dispatch_on_strong_saving() -> None:
    advice = await HeuristicAdvisor().advise(base_context())
    assert advice is not None
    assert advice.source == SOURCE_HEURISTIC
    assert "Strong arbitrage" in advice.headline
    assert any("Release" in a for a in advice.recommended_actions)
    assert 0.0 <= advice.confidence <= 1.0


async def test_heuristic_holds_schedule_when_grid_is_flat() -> None:
    context = base_context(
        plan={"run_id": "r", "solver": "local-greedy-v1", "saving_pct": 0.4,
              "carbon_saved_kg": 12.0, "unscheduled": [], "horizon_windows": 24,
              "decision_count": 24}
    )
    advice = await HeuristicAdvisor().advise(context)
    assert advice is not None
    assert "flat" in advice.headline.lower()
    assert any("Hold" in a for a in advice.recommended_actions)


async def test_heuristic_warns_when_no_run_exists() -> None:
    advice = await HeuristicAdvisor().advise(base_context(plan={}))
    assert advice is not None
    assert "No optimisation run" in advice.headline
    assert advice.confidence <= 0.25
    assert any("no schedule" in f for f in advice.risk_flags)


async def test_heuristic_flags_unscheduled_processes() -> None:
    context = base_context(
        plan={"run_id": "r", "solver": "local-greedy-v1", "saving_pct": 5.0,
              "carbon_saved_kg": 900.0, "unscheduled": ["mill-01"],
              "horizon_windows": 24, "decision_count": 24}
    )
    advice = await HeuristicAdvisor().advise(context)
    assert advice is not None
    assert any("mill-01" in f for f in advice.risk_flags)


async def test_heuristic_flags_forecast_heavy_context() -> None:
    context = base_context(
        grid={"window_count": 24, "forecast_only_count": 20, "avg_actual_intensity": None,
              "avg_renewable_percentage": 30.0,
              "cleanest_window": None, "dirtiest_window": None}
    )
    advice = await HeuristicAdvisor().advise(context)
    assert advice is not None
    assert any("forecast" in f.lower() for f in advice.risk_flags)


async def test_heuristic_is_deterministic() -> None:
    first = await HeuristicAdvisor().advise(base_context())
    second = await HeuristicAdvisor().advise(base_context())
    assert first == second


# --------------------------------------------------------------------------- #
# Claude advisor + fallback
# --------------------------------------------------------------------------- #


async def test_claude_returns_none_without_api_key() -> None:
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key=None))
    assert advisor.is_configured is False
    assert await advisor.advise(base_context()) is None


async def test_claude_parses_valid_json() -> None:
    client = FakeClient(FakeResponse(200, claude_payload(valid_advice_json())))
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key="sk-test"), client=client)

    advice = await advisor.advise(base_context())

    assert advice is not None
    assert advice.source == SOURCE_CLAUDE
    assert advice.confidence == pytest.approx(0.82)
    assert advice.recommended_actions == ["Release the schedule"]
    # The key must never be logged into the request body.
    assert client.calls[0]["headers"]["x-api-key"] == "sk-test"


async def test_claude_parses_fenced_json() -> None:
    fenced = f"```json\n{valid_advice_json()}\n```"
    client = FakeClient(FakeResponse(200, claude_payload(fenced)))
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key="sk-test"), client=client)
    advice = await advisor.advise(base_context())
    assert advice is not None and advice.source == SOURCE_CLAUDE


async def test_claude_falls_back_on_http_error() -> None:
    client = FakeClient(FakeResponse(500, {"error": "internal"}))
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key="sk-test"), client=client)
    assert await advisor.advise(base_context()) is None


async def test_claude_falls_back_on_transport_error() -> None:
    client = FakeClient(raise_exc=RuntimeError("connection reset"))
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key="sk-test"), client=client)
    assert await advisor.advise(base_context()) is None


async def test_claude_falls_back_on_unparseable_output() -> None:
    client = FakeClient(FakeResponse(200, claude_payload("I think you should wait.")))
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key="sk-test"), client=client)
    assert await advisor.advise(base_context()) is None


async def test_claude_falls_back_on_schema_violation() -> None:
    # Missing the required `headline` field.
    bad = json.dumps({"rationale": "no headline here", "confidence": 0.9})
    client = FakeClient(FakeResponse(200, claude_payload(bad)))
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key="sk-test"), client=client)
    assert await advisor.advise(base_context()) is None


async def test_claude_falls_back_on_out_of_range_confidence() -> None:
    bad = json.dumps({"headline": "Do it", "confidence": 4.2})
    client = FakeClient(FakeResponse(200, claude_payload(bad)))
    advisor = ClaudeAdvisor(PlatformSettings(anthropic_api_key="sk-test"), client=client)
    assert await advisor.advise(base_context()) is None


# --------------------------------------------------------------------------- #
# JSON extraction + tracking
# --------------------------------------------------------------------------- #


def test_extract_json_handles_plain_fenced_and_embedded() -> None:
    assert _extract_json('{"a": 1}') == {"a": 1}
    assert _extract_json('```json\n{"a": 1}\n```') == {"a": 1}
    assert _extract_json('Sure!\n{"a": 1}\nHope that helps') == {"a": 1}


def test_file_tracking_sink_appends_runs(tmp_path: Path) -> None:
    sink = FileTrackingSink(tmp_path / "nested" / "runs.jsonl")
    sink.log_run("run-1", {"solver": "local"}, {"carbon_saved_kg": 12.5})
    sink.log_run("run-2", {"solver": "local"}, {"carbon_saved_kg": 30.0})

    lines = (tmp_path / "nested" / "runs.jsonl").read_text().strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["run_id"] == "run-1"
    assert first["metrics"]["carbon_saved_kg"] == 12.5


def test_build_tracker_defaults_to_file_sink() -> None:
    settings = PlatformSettings(mlflow_tracking_uri=None)
    assert isinstance(build_tracker(settings), FileTrackingSink)
