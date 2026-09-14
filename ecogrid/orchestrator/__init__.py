"""AI Orchestrator — Claude-backed judgement over the deterministic optimizer.

The optimizer does arithmetic; the orchestrator does judgement. It reads the same
platform state, asks Claude for a recommendation, and — critically — **falls back
to a deterministic heuristic advisor** whenever the model is unavailable or its
reply cannot be validated. Advice is persisted with the exact inputs it was
derived from and tracked in MLflow (or a local JSONL sink).
"""

from ecogrid.orchestrator.advice import (
    SOURCE_CLAUDE,
    SOURCE_HEURISTIC,
    Advice,
    Advisor,
    ClaudeAdvisor,
    HeuristicAdvisor,
    build_advisor,
)
from ecogrid.orchestrator.context import build_context
from ecogrid.orchestrator.loop import (
    OrchestrationResult,
    OrchestratorLoop,
    run_once,
)
from ecogrid.orchestrator.tracking import (
    FileTrackingSink,
    MlflowTrackingSink,
    TrackingSink,
    build_tracker,
)

__all__ = [
    "SOURCE_CLAUDE",
    "SOURCE_HEURISTIC",
    "Advice",
    "Advisor",
    "ClaudeAdvisor",
    "FileTrackingSink",
    "HeuristicAdvisor",
    "MlflowTrackingSink",
    "OrchestrationResult",
    "OrchestratorLoop",
    "TrackingSink",
    "build_advisor",
    "build_context",
    "build_tracker",
    "run_once",
]
