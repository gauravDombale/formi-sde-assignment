from unittest.mock import AsyncMock, patch

import pytest

from src.config import settings
from src.services.llm_scheduler import (
    CustomerBudget,
    LLMRequestScheduler,
    ProcessingLane,
    TokenBudgetManager,
    classify_processing_lane,
)
from src.services.post_call_processor import AnalysisResult
from src.services.recording import RecordingPollConfig, poll_and_upload_recording


class ManualClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


@pytest.mark.asyncio
async def test_scheduler_never_exceeds_global_rate_limits(make_post_call_context):
    clock = ManualClock()
    budget = TokenBudgetManager(
        total_tokens_per_minute=3_000,
        total_requests_per_minute=2,
        window_seconds=60,
        clock=clock,
    )
    processor = AsyncMock()
    processor.process_post_call = AsyncMock(
        return_value=AnalysisResult(
            call_stage="demo_booked",
            entities={},
            summary="ok",
            raw_response={"call_stage": "demo_booked"},
            tokens_used=1_500,
            latency_ms=10,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )
    )
    scheduler = LLMRequestScheduler(budget, processor=processor, estimated_tokens_per_call=1_500)

    results = []
    for i in range(5):
        ctx = make_post_call_context("demo_booked", interaction_id=f"interaction-{i}")
        results.append(await scheduler.process_when_budget_available(ctx))

    assert [result.status for result in results].count("completed") == 2
    assert [result.status for result in results].count("deferred") == 3
    assert processor.process_post_call.await_count == 2


def test_customer_a_cannot_consume_customer_b_reserved_budget():
    clock = ManualClock()
    budget = TokenBudgetManager(
        total_tokens_per_minute=100,
        total_requests_per_minute=100,
        customer_budgets=[
            CustomerBudget("customer-a", reserved_tokens_per_minute=20),
            CustomerBudget("customer-b", reserved_tokens_per_minute=80),
        ],
        clock=clock,
    )

    assert budget.acquire(customer_id="customer-a", estimated_tokens=20).allowed
    assert not budget.acquire(customer_id="customer-a", estimated_tokens=1).allowed
    assert budget.acquire(customer_id="customer-b", estimated_tokens=80).allowed


@pytest.mark.asyncio
async def test_short_transcripts_skip_llm(make_post_call_context):
    budget = TokenBudgetManager(total_tokens_per_minute=10_000, total_requests_per_minute=100)
    processor = AsyncMock()
    processor._update_interaction_metadata = AsyncMock()
    processor.process_post_call = AsyncMock()
    scheduler = LLMRequestScheduler(budget, processor=processor)

    ctx = make_post_call_context("short_call_hangup")
    result = await scheduler.process_when_budget_available(ctx)

    assert result.status == "completed"
    assert result.lane == ProcessingLane.SKIP
    assert result.result.tokens_used == 0
    processor.process_post_call.assert_not_called()
    processor._update_interaction_metadata.assert_awaited_once()


def test_processing_lane_uses_business_urgency(make_post_call_context):
    assert classify_processing_lane(make_post_call_context("rebook_confirmed")) == ProcessingLane.HOT
    assert classify_processing_lane(make_post_call_context("demo_booked")) == ProcessingLane.HOT
    assert classify_processing_lane(make_post_call_context("escalation_needed")) == ProcessingLane.HOT
    assert classify_processing_lane(make_post_call_context("not_interested")) == ProcessingLane.COLD
    assert classify_processing_lane(make_post_call_context("short_call_hangup")) == ProcessingLane.SKIP


@pytest.mark.asyncio
async def test_recording_poller_retries_until_recording_is_ready():
    attempts = 0

    async def fake_fetch(call_sid: str, account_id: str):
        nonlocal attempts
        attempts += 1
        if attempts == 3:
            return "https://recording.example/call.mp3"
        return None

    with patch("src.services.recording._fetch_exotel_recording_url", new=fake_fetch), patch(
        "src.services.recording._upload_to_s3", new=AsyncMock(return_value="recordings/abc.mp3")
    ) as upload, patch("src.services.recording.asyncio.sleep", new=AsyncMock()):
        s3_key = await poll_and_upload_recording(
            interaction_id="abc",
            call_sid="call-1",
            exotel_account_id="account-1",
            config=RecordingPollConfig(max_attempts=4, initial_delay_seconds=1, jitter_ratio=0),
        )

    assert attempts == 3
    assert s3_key == "recordings/abc.mp3"
    upload.assert_awaited_once()


@pytest.mark.asyncio
async def test_recording_poller_logs_terminal_failure(caplog):
    with patch("src.services.recording._fetch_exotel_recording_url", new=AsyncMock(return_value=None)), patch(
        "src.services.recording.asyncio.sleep", new=AsyncMock()
    ):
        result = await poll_and_upload_recording(
            interaction_id="missing-recording",
            call_sid="call-2",
            exotel_account_id="account-1",
            config=RecordingPollConfig(max_attempts=2, initial_delay_seconds=1, jitter_ratio=0),
        )

    assert result is None
    assert any(
        record.getMessage() == "recording_upload_failed"
        and getattr(record, "interaction_id", None) == "missing-recording"
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_audit_events_include_interaction_id(make_post_call_context, caplog):
    caplog.set_level("INFO", logger="postcall.audit")
    budget = TokenBudgetManager(total_tokens_per_minute=1, total_requests_per_minute=100)
    processor = AsyncMock()
    scheduler = LLMRequestScheduler(budget, processor=processor, estimated_tokens_per_call=1_500)

    ctx = make_post_call_context("demo_booked", interaction_id="audit-interaction")
    result = await scheduler.process_when_budget_available(ctx)

    assert result.status == "deferred"
    events = [record for record in caplog.records if record.name == "postcall.audit"]
    assert events
    assert all(getattr(record, "interaction_id", None) == "audit-interaction" for record in events)
