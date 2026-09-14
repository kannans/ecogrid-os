"""Phase 1 ingestion worker — offline contract tests.

Run with:  pytest -q

These tests never touch the network or a broker: they pin the two things that
actually break in production — the merge contract between the two upstream
endpoints, and the resilience helpers.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from aiokafka.errors import KafkaError

from ingest_grid import (
    CarbonIndex,
    GenerationEnvelope,
    GridTelemetry,
    IngestionWorker,
    IntensityEnvelope,
    Settings,
    TelemetryPublisher,
    TelemetrySpool,
    backoff_delay,
    merge_telemetry,
)

INTENSITY_PAYLOAD = {
    "data": [
        {
            "from": "2026-09-14T17:00Z",
            "to": "2026-09-14T17:30Z",
            "intensity": {"forecast": 266, "actual": 263, "index": "moderate"},
        },
        {
            # Forecast-only window: `actual` is absent, not null.
            "from": "2026-09-14T17:30Z",
            "to": "2026-09-14T18:00Z",
            "intensity": {"forecast": 240, "index": "low"},
        },
        {
            # Unrated banding — must coerce to `unknown`, not crash.
            "from": "2026-09-14T18:00Z",
            "to": "2026-09-14T18:30Z",
            "intensity": {"forecast": 310, "actual": None, "index": "Very High"},
        },
    ]
}

GENERATION_PAYLOAD = {
    "data": [
        {
            "from": "2026-09-14T17:00Z",
            "to": "2026-09-14T17:30Z",
            "generationmix": [
                {"fuel": "wind", "perc": 35.5},
                {"fuel": "gas", "perc": 42.1},
                {"fuel": "nuclear", "perc": 14.4},
                {"fuel": "solar", "perc": 8.0},
            ],
        },
        {
            # Out-of-order on purpose: merge must key on the window, not the index.
            "from": "2026-09-14T18:00Z",
            "to": "2026-09-14T18:30Z",
            "generationmix": [
                {"fuel": "gas", "perc": 55.0},
                {"fuel": "wind", "perc": 45.0},
            ],
        },
    ]
}


def _merged() -> list[GridTelemetry]:
    return merge_telemetry(
        IntensityEnvelope.model_validate(INTENSITY_PAYLOAD),
        GenerationEnvelope.model_validate(GENERATION_PAYLOAD),
        ingested_at=datetime(2026, 9, 14, 17, 5, tzinfo=timezone.utc),
    )


def test_merges_on_window_not_array_position() -> None:
    records = _merged()
    assert len(records) == 3
    assert [r.window_from.hour for r in records] == [17, 17, 18]
    assert [r.window_from.minute for r in records] == [0, 30, 0]

    first = records[0]
    assert first.forecast_intensity == 266
    assert first.actual_intensity == 263
    assert first.carbon_index is CarbonIndex.MODERATE
    assert first.generation_mix["wind"] == pytest.approx(35.5)
    assert first.generation_mix["gas"] == pytest.approx(42.1)


def test_generation_window_matched_despite_reordering() -> None:
    third = _merged()[2]
    assert third.window_from.hour == 18
    assert third.generation_mix == {"gas": 55.0, "wind": 45.0}
    assert third.generation_mix_missing is False


def test_missing_actual_marks_forecast_only_and_falls_back() -> None:
    second = _merged()[1]
    assert second.actual_intensity is None
    assert second.is_forecast_only is True
    assert second.effective_intensity == 240  # falls back to forecast


def test_window_without_generation_mix_is_still_emitted() -> None:
    second = _merged()[1]
    assert second.generation_mix == {}
    assert second.generation_mix_missing is True
    assert second.renewable_percentage == 0.0


def test_carbon_index_is_normalised() -> None:
    assert _merged()[2].carbon_index is CarbonIndex.VERY_HIGH


def test_derived_percentage_aggregates() -> None:
    first = _merged()[0]
    assert first.renewable_percentage == pytest.approx(43.5)  # wind + solar
    assert first.low_carbon_percentage == pytest.approx(57.9)  # + nuclear
    assert first.fossil_percentage == pytest.approx(42.1)  # gas


def test_kafka_key_is_utc_window_start() -> None:
    assert _merged()[0].kafka_key == "2026-09-14T17:00:00Z"


def test_payload_hash_ignores_ingestion_time_but_tracks_content() -> None:
    a = merge_telemetry(
        IntensityEnvelope.model_validate(INTENSITY_PAYLOAD),
        GenerationEnvelope.model_validate(GENERATION_PAYLOAD),
        ingested_at=datetime(2026, 9, 14, 17, 5, tzinfo=timezone.utc),
    )
    b = merge_telemetry(
        IntensityEnvelope.model_validate(INTENSITY_PAYLOAD),
        GenerationEnvelope.model_validate(GENERATION_PAYLOAD),
        ingested_at=datetime(2026, 9, 14, 17, 10, tzinfo=timezone.utc),
    )
    assert a[0].payload_hash() == b[0].payload_hash()

    drifted = {**INTENSITY_PAYLOAD}
    drifted["data"] = [{**INTENSITY_PAYLOAD["data"][0], "intensity": {"forecast": 999, "actual": 999, "index": "high"}}]
    c = merge_telemetry(
        IntensityEnvelope.model_validate(drifted),
        GenerationEnvelope.model_validate(GENERATION_PAYLOAD),
    )
    assert c[0].payload_hash() != a[0].payload_hash()


def test_payload_serialises_to_utf8_json_bytes() -> None:
    raw = _merged()[0].to_kafka_value()
    assert isinstance(raw, bytes)
    assert b'"carbon_index":"moderate"' in raw
    assert b"2026-09-14T17:00:00Z" in raw


def test_generation_envelope_accepts_single_object() -> None:
    """Regression: the live /generation endpoint returns `data` as an OBJECT.

    Verified against api.carbonintensity.org.uk on 2026-09-14. The published
    spec claims an array; both shapes must validate.
    """
    envelope = GenerationEnvelope.model_validate(
        {
            "data": {
                "from": "2026-09-14T16:30Z",
                "to": "2026-09-14T17:00Z",
                "generationmix": [
                    {"fuel": "gas", "perc": 31.0},
                    {"perc": 0, "fuel": "coal"},
                    {"fuel": "imports", "perc": 3.9},
                ],
            }
        }
    )
    assert len(envelope.data) == 1
    assert envelope.data[0].generationmix[0].fuel == "gas"
    # Zero-percentage fuels are valid and must be retained.
    assert envelope.data[0].generationmix[1].perc == 0.0


def test_intensity_envelope_accepts_single_object() -> None:
    """Same normalisation applies to /intensity for forward compatibility."""
    envelope = IntensityEnvelope.model_validate(
        {
            "data": {
                "from": "2026-09-14T16:30Z",
                "to": "2026-09-14T17:00Z",
                "intensity": {"forecast": 130, "actual": 145, "index": "moderate"},
            }
        }
    )
    assert len(envelope.data) == 1
    assert envelope.data[0].intensity.actual == 145


def test_real_api_payload_merges_end_to_end() -> None:
    """Live-shaped payloads (1 intensity window + 1 generation object) merge."""
    records = merge_telemetry(
        IntensityEnvelope.model_validate(
            {"data": [{"from": "2026-09-14T16:30Z", "to": "2026-09-14T17:00Z",
                       "intensity": {"forecast": 130, "actual": 145, "index": "moderate"}}]}
        ),
        GenerationEnvelope.model_validate(
            {"data": {"from": "2026-09-14T16:30Z", "to": "2026-09-14T17:00Z",
                      "generationmix": [
                          {"fuel": "biomass", "perc": 7.1}, {"perc": 0, "fuel": "coal"},
                          {"fuel": "imports", "perc": 3.9}, {"fuel": "gas", "perc": 31.0},
                          {"perc": 9.2, "fuel": "nuclear"}, {"fuel": "other", "perc": 4.6},
                          {"fuel": "hydro", "perc": 3.7}, {"fuel": "wind", "perc": 33.8},
                          {"fuel": "solar", "perc": 6.7},
                      ]}}
        ),
    )
    assert len(records) == 1
    record = records[0]
    assert record.actual_intensity == 145
    assert record.is_forecast_only is False
    assert record.generation_mix_missing is False
    assert record.renewable_percentage == pytest.approx(51.3)   # wind + solar + hydro + biomass
    assert record.low_carbon_percentage == pytest.approx(60.5)  # + nuclear
    assert record.fossil_percentage == pytest.approx(31.0)      # gas + coal
    # imports/other are in neither bucket, so the three aggregates need not sum to 100.
    assert record.generation_mix["imports"] == pytest.approx(3.9)


def test_malformed_window_is_rejected() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        IntensityEnvelope.model_validate(
            {"data": [{"from": "2026-09-14T17:00Z", "to": "2026-09-14T16:00Z",
                       "intensity": {"forecast": 266}}]}
        )


def test_backoff_is_bounded_and_monotonic_in_ceiling() -> None:
    for attempt in (1, 2, 3, 4, 5, 6, 7, 8):
        delay = backoff_delay(attempt, base=1.0, cap=30.0)
        assert 0.0 <= delay <= 30.0
    # The ceiling must never exceed the cap even at extreme attempt counts.
    assert backoff_delay(50, base=1.0, cap=30.0) <= 30.0


def test_settings_read_env_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ECOGRID_KAFKA_TOPIC", "test.topic")
    monkeypatch.setenv("ECOGRID_POLL_INTERVAL_SECONDS", "120")
    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    assert settings.kafka_topic == "test.topic"
    assert settings.poll_interval_seconds == 120
    assert settings.kafka_bootstrap_servers == "localhost:9092"  # default preserved


# --------------------------------------------------------------------------- #
# Durability: spool + publisher retry / dedupe
# --------------------------------------------------------------------------- #


class _FakeProducer:
    """Stands in for AIOKafkaProducer so failure paths are testable offline."""

    def __init__(self, *, fail_first: int = 0) -> None:
        self.sent: list[dict[str, object]] = []
        self.fail_first = fail_first
        self.calls = 0

    async def send_and_wait(self, topic: str, key: bytes | None = None, value: bytes | None = None) -> None:
        self.calls += 1
        if self.calls <= self.fail_first:
            raise KafkaError("simulated broker failure")
        self.sent.append({"topic": topic, "key": key, "value": value})


def _fast_settings(tmp_path: Path) -> Settings:
    """Settings tuned for tests: no real I/O waits, temp spool."""
    return Settings(  # type: ignore[call-arg]
        _env_file=None,
        spool_path=tmp_path / "spool" / "telemetry-spool.jsonl",
        heartbeat_path=tmp_path / "heartbeat",
        kafka_max_attempts=2,
        kafka_backoff_base_seconds=0.001,
        kafka_backoff_max_seconds=0.002,
    )


def test_spool_round_trip(tmp_path: Path) -> None:
    spool = TelemetrySpool(_fast_settings(tmp_path))
    records = _merged()
    spool.append(records)

    loaded = spool.load()
    assert len(loaded) == len(records)
    assert [r.kafka_key for r in loaded] == [r.kafka_key for r in records]
    assert loaded[0].generation_mix == records[0].generation_mix


def test_spool_discards_corrupt_lines(tmp_path: Path) -> None:
    spool = TelemetrySpool(_fast_settings(tmp_path))
    spool.append(_merged()[:1])
    with spool.path.open("a", encoding="utf-8") as handle:
        handle.write("{not valid json at all\n")
    assert len(spool.load()) == 1  # corrupt line dropped, valid line kept


def test_spool_replace_truncates_instead_of_deleting(tmp_path: Path) -> None:
    """An empty spool is a zero-byte file, never a deleted file.

    Deleting requires a delete syscall, which some environments guard or
    forbid — in testing that terminated the worker mid-replay and left a stale
    spool behind.
    """
    spool = TelemetrySpool(_fast_settings(tmp_path))
    spool.append(_merged())
    assert spool.path.stat().st_size > 0

    spool.replace([])
    assert spool.path.exists(), "spool must not be unlinked"
    assert spool.path.stat().st_size == 0
    assert spool.load() == []


def test_spool_replace_on_missing_file_is_a_noop(tmp_path: Path) -> None:
    spool = TelemetrySpool(_fast_settings(tmp_path))
    assert not spool.path.exists()
    spool.replace([])
    assert not spool.path.exists()


async def test_worker_survives_a_failing_cycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """One bad cycle must never kill a long-running worker."""
    worker = IngestionWorker(_fast_settings(tmp_path), dry_run=True)
    calls = {"n": 0}

    async def exploding_cycle() -> None:
        calls["n"] += 1
        raise RuntimeError("simulated cycle explosion")

    monkeypatch.setattr(worker, "_run_cycle", exploding_cycle)
    await worker.run(once=True)  # must not raise
    assert calls["n"] == 1


def test_spool_is_bounded(tmp_path: Path) -> None:
    settings = _fast_settings(tmp_path)
    settings.spool_max_records = 3
    spool = TelemetrySpool(settings)
    spool.append(_merged())
    spool.append(_merged())
    assert len(spool.load()) == 3  # trimmed to the newest 3


async def test_publisher_suppresses_unchanged_windows(tmp_path: Path) -> None:
    publisher = TelemetryPublisher(_fast_settings(tmp_path), TelemetrySpool(_fast_settings(tmp_path)))
    publisher._producer = _FakeProducer()
    records = _merged()

    published, failed = await publisher.publish(records)
    assert (published, failed) == (len(records), 0)

    # Identical payloads on the next cycle must not be republished.
    published, failed = await publisher.publish(records)
    assert (published, failed) == (0, 0)


async def test_publisher_republishes_when_forecast_revised(tmp_path: Path) -> None:
    publisher = TelemetryPublisher(_fast_settings(tmp_path), TelemetrySpool(_fast_settings(tmp_path)))
    publisher._producer = _FakeProducer()

    await publisher.publish(_merged()[:1])

    revised = merge_telemetry(
        IntensityEnvelope.model_validate(
            {"data": [{**INTENSITY_PAYLOAD["data"][0], "intensity": {"forecast": 200, "actual": 201, "index": "high"}}]}
        ),
        GenerationEnvelope.model_validate(GENERATION_PAYLOAD),
    )
    published, failed = await publisher.publish(revised)
    assert (published, failed) == (1, 0)
    assert publisher._producer.calls == 2


async def test_failed_send_is_spooled_for_replay(tmp_path: Path) -> None:
    settings = _fast_settings(tmp_path)
    spool = TelemetrySpool(settings)
    publisher = TelemetryPublisher(settings, spool)
    # Every attempt fails -> the record must land in the spool, not vanish.
    publisher._producer = _FakeProducer(fail_first=99)

    published, failed = await publisher.publish(_merged())
    assert published == 0
    assert failed == 3
    assert len(spool.load()) == 3

    # Broker recovers -> replay drains the spool.
    publisher._producer = _FakeProducer()
    replayed = await publisher.replay_spool()
    assert replayed == 3
    assert spool.load() == []


async def test_replay_keeps_records_that_still_fail(tmp_path: Path) -> None:
    settings = _fast_settings(tmp_path)
    spool = TelemetrySpool(settings)
    publisher = TelemetryPublisher(settings, spool)
    publisher._producer = _FakeProducer(fail_first=99)
    await publisher.publish(_merged())

    # Broker still down -> spool must be preserved, not cleared.
    assert await publisher.replay_spool() == 0
    assert len(spool.load()) == 3


def test_publish_unchanged_windows_flag_disables_dedupe(tmp_path: Path) -> None:
    settings = _fast_settings(tmp_path)
    settings.publish_unchanged_windows = True
    publisher = TelemetryPublisher(settings, TelemetrySpool(settings))
    publisher._producer = _FakeProducer()

    asyncio.run(publisher.publish(_merged()))
    asyncio.run(publisher.publish(_merged()))
    assert publisher._producer.calls == 6  # 3 records × 2 cycles


class _HangingProducer:
    """Never resolves a send — simulates an unreachable/unknown topic."""

    def __init__(self) -> None:
        self.calls = 0

    async def send_and_wait(self, topic: str, key: bytes | None = None, value: bytes | None = None) -> None:
        self.calls += 1
        await asyncio.sleep(3600)


async def test_hanging_send_times_out_and_is_spooled(tmp_path: Path) -> None:
    """A send that never resolves must be bounded, not hang the cycle."""
    settings = _fast_settings(tmp_path)
    settings.kafka_send_timeout_seconds = 0.05
    settings.kafka_max_attempts = 2
    spool = TelemetrySpool(settings)
    publisher = TelemetryPublisher(settings, spool)
    publisher._producer = _HangingProducer()

    started = time.monotonic()
    published, failed = await publisher.publish(_merged()[:1])
    elapsed = time.monotonic() - started

    assert (published, failed) == (0, 1)
    assert elapsed < 2.0, "send must be bounded by kafka_send_timeout_seconds"
    assert publisher._producer.calls == 2  # both attempts made, then gave up
    assert len(spool.load()) == 1
