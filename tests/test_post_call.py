from unittest.mock import AsyncMock, patch

import pytest

from src.config import settings
from src.services.alerts import PostCallAlertEvaluator, PostCallHealthSnapshot
from src.services.circuit_breaker import PostCallCircuitBreaker
from src.services.customer_policy import CustomerProcessingPolicy
from src.services.llm_scheduler import (
    CustomerBudget,
    LLMRequestScheduler,
    ProcessingLane,
    TokenBudgetManager,
    classify_processing_lane,
)
from src.services.durable_tasks import DurableTask, DurableTaskStatus, PostgresDurableTaskStore
from src.services.post_call_processor import AnalysisResult
from src.services.recording import RecordingPollConfig, poll_and_upload_recording
from src.services.security import SensitiveDataProtector
from src.tasks.celery_tasks import process_durable_task


class ManualClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


class FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def one(self):
        return self._rows[0]


class FakeSession:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def begin(self):
        return self

    async def execute(self, statement, params=None):
        self.executed.append((str(statement), params or {}))
        return FakeResult(self.rows)


class FakeSessionFactory:
    def __init__(self, rows):
        self.session = FakeSession(rows)

    def __call__(self):
        return self.session


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


def test_customer_policy_can_override_hot_lane_keywords(make_post_call_context):
    ctx = make_post_call_context("hinglish_ambiguous")
    policy = CustomerProcessingPolicy(hot_phrases=("budget tight",), cold_phrases=())

    assert classify_processing_lane(ctx, policy) == ProcessingLane.HOT


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


@pytest.mark.asyncio
async def test_postgres_durable_store_claims_with_skip_locked():
    factory = FakeSessionFactory(
        [
            {
                "id": "analysis:interaction-1",
                "task_type": "llm_analysis",
                "interaction_id": "interaction-1",
                "customer_id": "customer-1",
                "campaign_id": "campaign-1",
                "payload": {"interaction_id": "interaction-1"},
                "status": "running",
                "attempts": 1,
                "max_attempts": 10,
                "last_error": None,
                "next_run_at_epoch": 123,
            }
        ]
    )
    store = PostgresDurableTaskStore(session_factory=factory, worker_id="worker-a")

    tasks = await store.claim_ready(limit=5)

    sql, params = factory.session.executed[0]
    assert "FOR UPDATE SKIP LOCKED" in sql
    assert "locked_until = NOW()" in sql
    assert params["worker_id"] == "worker-a"
    assert tasks[0].id == "analysis:interaction-1"
    assert tasks[0].status.value == "running"


@pytest.mark.asyncio
async def test_postgres_durable_store_enqueue_uses_idempotent_task_key():
    factory = FakeSessionFactory(
        [
            {
                "id": "analysis:interaction-2",
                "task_type": "llm_analysis",
                "interaction_id": "interaction-2",
                "customer_id": "customer-1",
                "campaign_id": "campaign-1",
                "payload": {"interaction_id": "interaction-2"},
                "status": "queued",
                "attempts": 0,
                "max_attempts": 10,
                "last_error": None,
                "next_run_at_epoch": 456,
            }
        ]
    )
    store = PostgresDurableTaskStore(session_factory=factory)

    task = await store.enqueue(
        task_id="analysis:interaction-2",
        task_type="llm_analysis",
        interaction_id="interaction-2",
        customer_id="customer-1",
        campaign_id="campaign-1",
        payload={"interaction_id": "interaction-2"},
        lane=ProcessingLane.HOT,
    )

    sql, params = factory.session.executed[0]
    assert "ON CONFLICT (id) DO UPDATE" in sql
    assert "CAST(:payload_json AS jsonb)" in sql
    assert params["task_id"] == "analysis:interaction-2"
    assert params["lane"] == "hot"
    assert task.id == "analysis:interaction-2"


def test_alert_evaluator_emits_threshold_alerts():
    evaluator = PostCallAlertEvaluator()
    alerts = evaluator.evaluate(
        PostCallHealthSnapshot(
            llm_tpm_utilization=0.90,
            llm_rpm_utilization=0.40,
            hot_lane_p95_wait_seconds=180,
            dead_lettered_tasks=1,
            recording_failure_rate=0.03,
            customer_budget_exhausted_minutes=11,
        )
    )

    assert {alert.name for alert in alerts} == {
        "llm_capacity_high",
        "hot_lane_sla_breached",
        "postcall_dead_letters_present",
        "recording_failure_rate_high",
        "customer_budget_exhaustion_sustained",
    }


@pytest.mark.asyncio
async def test_dialler_backpressure_is_gradual():
    breaker = PostCallCircuitBreaker()
    with patch("src.services.circuit_breaker.redis_client.get", new=AsyncMock(return_value="425")):
        decision = await breaker.get_backpressure("agent-1")

    assert decision.allowed is True
    assert decision.delay_seconds == 5
    assert decision.reason == "heavy_backpressure"


def test_sensitive_data_protector_marks_unencrypted_local_payload():
    protector = SensitiveDataProtector(key_b64="")
    protected = protector.protect_json({"transcript": [{"content": "PII"}]}, interaction_id="interaction-1")

    assert protected["encrypted"] is False
    assert protector.reveal_json(protected, interaction_id="interaction-1")["transcript"][0]["content"] == "PII"


@pytest.mark.asyncio
async def test_durable_task_processor_handles_downstream_jobs():
    task = DurableTask(
        id="signal_jobs:interaction-1",
        task_type="signal_jobs",
        interaction_id="interaction-1",
        customer_id="customer-1",
        campaign_id="campaign-1",
        payload={
            "interaction_id": "interaction-1",
            "session_id": "session-1",
            "campaign_id": "campaign-1",
            "analysis_result": {"call_stage": "demo_booked"},
        },
        status=DurableTaskStatus.RUNNING,
        attempts=1,
        next_run_at=0,
    )

    with patch("src.tasks.celery_tasks.trigger_signal_jobs", new=AsyncMock()) as trigger, patch(
        "src.tasks.celery_tasks.durable_task_store"
    ) as store:
        store.complete = AsyncMock()
        store.retry = AsyncMock()
        await process_durable_task(task)

    trigger.assert_awaited_once()
    store.complete.assert_awaited_once_with(task)
    store.retry.assert_not_called()
