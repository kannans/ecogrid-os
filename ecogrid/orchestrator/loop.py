"""The orchestration loop — state in, judgement out.

Each pass:

1. Read the platform state (grid windows, latest schedule, plant capacity).
2. Ask Claude for a recommendation; **fall back to the deterministic advisor**
   if there is no key, the call fails, or the reply cannot be validated.
3. Persist the advice with the exact inputs it was derived from.
4. Publish it to ``ecogrid.decisions.advice``.
5. Log the run to MLflow (or the local JSONL sink).

The fallback is the important part: an orchestrator that goes silent when the
model is unreachable is worse than one that gives rule-based advice.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker

from ecogrid.config import PlatformSettings
from ecogrid.db import create_engine, create_session_factory
from ecogrid.logging_setup import configure_logging
from ecogrid.models import OrchestratorAdviceRow
from ecogrid.orchestrator.advice import (
    Advice,
    ClaudeAdvisor,
    HeuristicAdvisor,
    build_advisor,
)
from ecogrid.orchestrator.context import build_context
from ecogrid.orchestrator.tracking import build_tracker

logger = logging.getLogger("ecogrid.orchestrator.loop")

SCHEMA_VERSION = "1.0.0"


@dataclass
class OrchestrationResult:
    """Outcome of one pass."""

    advice: Advice
    context: dict[str, Any]
    published: bool


async def _persist(
    session_factory: async_sessionmaker,
    advice: Advice,
    context: dict[str, Any],
    run_id: str | None,
) -> None:
    async with session_factory() as session:
        session.add(
            OrchestratorAdviceRow(
                run_id=run_id,
                source=advice.source,
                headline=advice.headline,
                rationale=advice.rationale,
                confidence=advice.confidence,
                recommended_actions=list(advice.recommended_actions),
                risk_flags=list(advice.risk_flags),
                context=context,
            )
        )
        await session.commit()


async def _publish(
    producer: Any, settings: PlatformSettings, advice: Advice, run_id: str | None
) -> bool:
    if producer is None:
        return False
    payload = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "source": advice.source,
        "headline": advice.headline,
        "rationale": advice.rationale,
        "confidence": advice.confidence,
        "recommended_actions": list(advice.recommended_actions),
        "risk_flags": list(advice.risk_flags),
        "published_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        await asyncio.wait_for(
            producer.send_and_wait(
                settings.kafka_advice_topic,
                key=(run_id or "none").encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
            ),
            timeout=15.0,
        )
        return True
    except asyncio.TimeoutError:
        logger.warning("Publishing advice timed out (topic=%s)", settings.kafka_advice_topic)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Publishing advice failed: %s", exc)
    return False


async def run_once(
    session_factory: async_sessionmaker,
    settings: PlatformSettings,
    *,
    advisor: ClaudeAdvisor | None = None,
    producer: Any = None,
    tracker: Any = None,
) -> OrchestrationResult:
    """One orchestration pass. Always produces advice (heuristic at worst)."""
    context = await build_context(session_factory, settings.optimizer_horizon_windows)

    primary = advisor or build_advisor(settings)
    advice = await primary.advise(context)
    if advice is None:
        advice = await HeuristicAdvisor().advise(context)
    if advice is None:  # defensive: the heuristic always answers, but never assume
        advice = Advice(
            headline="No advice could be produced",
            rationale="Both the model and the heuristic advisor declined to advise.",
            confidence=0.0,
            source="heuristic",
        )

    run_id = (context.get("plan") or {}).get("run_id")
    await _persist(session_factory, advice, context, run_id)
    published = await _publish(producer, settings, advice, run_id)

    sink = tracker or build_tracker(settings)
    plan = context.get("plan") or {}
    grid = context.get("grid") or {}
    sink.log_run(
        run_id or "none",
        params={
            "solver": plan.get("solver", "none"),
            "advisor": advice.source,
            "model": settings.anthropic_model if advice.source == "claude" else "n/a",
            "horizon_windows": plan.get("horizon_windows", 0),
        },
        metrics={
            "carbon_saved_kg": float(plan.get("carbon_saved_kg") or 0.0),
            "saving_pct": float(plan.get("saving_pct") or 0.0),
            "decision_count": float(plan.get("decision_count") or 0.0),
            "confidence": float(advice.confidence),
            "grid_window_count": float(grid.get("window_count") or 0.0),
            "forecast_only_count": float(grid.get("forecast_only_count") or 0.0),
            "risk_flag_count": float(len(advice.risk_flags)),
        },
    )

    logger.info(
        "Orchestrator pass | source=%s confidence=%.2f run=%s | %s",
        advice.source,
        advice.confidence,
        run_id,
        advice.headline,
    )
    return OrchestrationResult(advice=advice, context=context, published=published)


class OrchestratorLoop:
    """Runs :func:`run_once` on a cadence."""

    def __init__(self, settings: PlatformSettings, producer: Any = None) -> None:
        self._settings = settings
        self._producer = producer
        self._own_producer = producer is None
        self._engine = None
        self._session_factory: async_sessionmaker | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._engine = create_engine(self._settings)
        self._session_factory = create_session_factory(self._engine)
        if self._producer is None:
            from aiokafka import AIOKafkaProducer

            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                client_id="ecogrid-orchestrator",
                acks="all",
                enable_idempotence=True,
                compression_type="gzip",
            )
            await self._producer.start()
        logger.info(
            "Orchestrator started | model=%s interval=%ss",
            self._settings.anthropic_model,
            self._settings.orchestrator_interval_seconds,
        )

    async def stop(self) -> None:
        if self._producer is not None and self._own_producer:
            await self._producer.stop()
        if self._engine is not None:
            await self._engine.dispose()
        logger.info("Orchestrator stopped")

    def request_stop(self, *_: Any) -> None:
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover
                signal.signal(sig, lambda s, _f: self.request_stop(s))

    async def run(self) -> int:
        self._install_signal_handlers()
        await self.start()
        try:
            while not self._stop.is_set():
                if not self._settings.orchestrator_enabled:
                    logger.info("Orchestrator disabled — idling")
                else:
                    try:
                        await run_once(
                            self._session_factory, self._settings, producer=self._producer
                        )
                    except Exception:  # noqa: BLE001 — one bad pass must not kill the loop
                        logger.exception("Orchestrator pass failed — continuing")
                await asyncio.sleep(self._settings.orchestrator_interval_seconds)
        finally:
            await self.stop()
        return 0


async def run() -> int:
    settings = PlatformSettings()
    configure_logging(settings.log_level)
    try:
        return await OrchestratorLoop(settings).run()
    except Exception:  # noqa: BLE001
        logger.critical("Orchestrator terminated with an unhandled error", exc_info=True)
        return 1


def main() -> int:
    try:
        return asyncio.run(run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
