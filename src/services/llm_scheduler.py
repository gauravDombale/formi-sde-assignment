import inspect
import math
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Iterable, Optional
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.config import settings
from src.services.audit import audit_logger
from src.services.customer_policy import CustomerProcessingPolicy, policy_from_additional_data
from src.services.post_call_processor import AnalysisResult, PostCallContext, PostCallProcessor
from src.utils.db import async_session_factory


class ProcessingLane(str, Enum):
    HOT = "hot"
    COLD = "cold"
    SKIP = "skip"


@dataclass
class CustomerBudget:
    customer_id: str
    reserved_tokens_per_minute: int = 0
    reserved_requests_per_minute: int = 0
    priority_weight: int = 1


@dataclass
class BudgetDecision:
    allowed: bool
    retry_after_seconds: float = 0
    reason: str = ""
    reservation_id: Optional[str] = None


@dataclass
class _Reservation:
    customer_id: str
    estimated_tokens: int
    estimated_requests: int
    window_id: int


class TokenBudgetManager:
    """
    In-process token/request scheduler used by local tests and workers.

    The production design persists these counters in Postgres/Redis with
    atomic updates; this implementation keeps the policy explicit and testable.
    """

    def __init__(
        self,
        total_tokens_per_minute: int,
        total_requests_per_minute: int,
        customer_budgets: Optional[Iterable[CustomerBudget]] = None,
        window_seconds: int = 60,
        clock=time.time,
    ):
        self.total_tokens_per_minute = total_tokens_per_minute
        self.total_requests_per_minute = total_requests_per_minute
        self.window_seconds = window_seconds
        self._clock = clock
        self._customer_budgets: Dict[str, CustomerBudget] = {
            budget.customer_id: budget for budget in customer_budgets or []
        }
        self._usage: Dict[int, Dict[str, Dict[str, int]]] = {}
        self._reservations: Dict[str, _Reservation] = {}

    def configure_customer(self, budget: CustomerBudget) -> None:
        self._customer_budgets[budget.customer_id] = budget

    def acquire(
        self,
        *,
        customer_id: str,
        estimated_tokens: int,
        estimated_requests: int = 1,
        campaign_id: Optional[str] = None,
        interaction_id: Optional[str] = None,
    ) -> BudgetDecision:
        window_id = self._window_id()
        self._ensure_window(window_id)

        if estimated_tokens > self.total_tokens_per_minute:
            return BudgetDecision(
                allowed=False,
                retry_after_seconds=self._seconds_until_next_window(),
                reason="request_exceeds_total_token_capacity",
            )

        usage = self._usage[window_id]
        total_tokens_used = sum(item["tokens"] for item in usage.values())
        total_requests_used = sum(item["requests"] for item in usage.values())

        if total_requests_used + estimated_requests > self.total_requests_per_minute:
            return BudgetDecision(False, self._seconds_until_next_window(), "global_rpm_exhausted")

        protected_tokens = self._protected_tokens_for_other_customers(customer_id, usage)
        available_tokens = self.total_tokens_per_minute - total_tokens_used - protected_tokens

        if estimated_tokens > available_tokens:
            return BudgetDecision(False, self._seconds_until_next_window(), "customer_or_global_tpm_exhausted")

        customer_usage = usage.setdefault(customer_id, {"tokens": 0, "requests": 0})
        customer_usage["tokens"] += estimated_tokens
        customer_usage["requests"] += estimated_requests
        reservation_id = str(uuid4())
        self._reservations[reservation_id] = _Reservation(
            customer_id=customer_id,
            estimated_tokens=estimated_tokens,
            estimated_requests=estimated_requests,
            window_id=window_id,
        )
        return BudgetDecision(True, reservation_id=reservation_id)

    def commit(self, reservation_id: str, *, actual_tokens: int) -> None:
        reservation = self._reservations.pop(reservation_id, None)
        if not reservation:
            return

        usage = self._usage.get(reservation.window_id, {})
        customer_usage = usage.get(reservation.customer_id)
        if not customer_usage:
            return

        delta = actual_tokens - reservation.estimated_tokens
        customer_usage["tokens"] = max(0, customer_usage["tokens"] + delta)

    def release(self, reservation_id: str) -> None:
        reservation = self._reservations.pop(reservation_id, None)
        if not reservation:
            return

        usage = self._usage.get(reservation.window_id, {})
        customer_usage = usage.get(reservation.customer_id)
        if not customer_usage:
            return

        customer_usage["tokens"] = max(0, customer_usage["tokens"] - reservation.estimated_tokens)
        customer_usage["requests"] = max(0, customer_usage["requests"] - reservation.estimated_requests)

    def usage_for_customer(self, customer_id: str) -> int:
        window = self._usage.get(self._window_id(), {})
        return window.get(customer_id, {}).get("tokens", 0)

    def _protected_tokens_for_other_customers(
        self, customer_id: str, usage: Dict[str, Dict[str, int]]
    ) -> int:
        protected = 0
        for other_customer_id, budget in self._customer_budgets.items():
            if other_customer_id == customer_id:
                continue
            used = usage.get(other_customer_id, {}).get("tokens", 0)
            protected += max(0, budget.reserved_tokens_per_minute - used)
        return protected

    def _window_id(self) -> int:
        return math.floor(self._clock() / self.window_seconds)

    def _ensure_window(self, window_id: int) -> None:
        self._usage.setdefault(window_id, {})
        for old_window_id in list(self._usage):
            if old_window_id < window_id - 1:
                del self._usage[old_window_id]

    def _seconds_until_next_window(self) -> float:
        return self.window_seconds - (self._clock() % self.window_seconds)


class PostgresTokenBudgetManager:
    """
    SQL-backed LLM budget manager.

    Reservations are serialized per provider/model/window using an advisory
    transaction lock, then written to llm_usage_ledger before the LLM request is
    allowed to run. This makes rate-limit admission durable and auditable.
    """

    def __init__(
        self,
        total_tokens_per_minute: int,
        total_requests_per_minute: int,
        session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
        provider: str = settings.LLM_PROVIDER,
        model: str = settings.LLM_MODEL,
    ):
        self.total_tokens_per_minute = total_tokens_per_minute
        self.total_requests_per_minute = total_requests_per_minute
        self._session_factory = session_factory
        self._provider = provider
        self._model = model

    async def acquire(
        self,
        *,
        customer_id: str,
        campaign_id: str,
        interaction_id: str,
        estimated_tokens: int,
        estimated_requests: int = 1,
    ) -> BudgetDecision:
        reservation_id = str(uuid4())
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        SELECT pg_advisory_xact_lock(
                            hashtext(:provider || ':' || :model || ':' || date_trunc('minute', NOW())::text)
                        )
                        """
                    ),
                    {"provider": self._provider, "model": self._model},
                )
                usage = (
                    await session.execute(
                        text(
                            """
                            WITH current_window AS (
                                SELECT date_trunc('minute', NOW()) AS window_start
                            ),
                            customer_usage AS (
                                SELECT
                                    customer_id,
                                    COALESCE(SUM(GREATEST(actual_tokens, estimated_tokens)), 0)::int AS tokens_used,
                                    COALESCE(SUM(request_count), 0)::int AS requests_used
                                FROM llm_usage_ledger, current_window
                                WHERE provider = :provider
                                  AND model = :model
                                  AND window_start = current_window.window_start
                                  AND status IN ('reserved', 'completed')
                                GROUP BY customer_id
                            ),
                            protected_budget AS (
                                SELECT COALESCE(
                                    SUM(GREATEST(b.reserved_tokens_per_minute - COALESCE(u.tokens_used, 0), 0)),
                                    0
                                )::int AS protected_tokens
                                FROM customer_llm_budgets b
                                LEFT JOIN customer_usage u ON u.customer_id = b.customer_id
                                WHERE b.customer_id <> CAST(:customer_id AS uuid)
                            )
                            SELECT
                                COALESCE((SELECT SUM(tokens_used) FROM customer_usage), 0)::int AS total_tokens_used,
                                COALESCE((SELECT SUM(requests_used) FROM customer_usage), 0)::int AS total_requests_used,
                                (SELECT protected_tokens FROM protected_budget) AS protected_tokens,
                                EXTRACT(EPOCH FROM (
                                    date_trunc('minute', NOW()) + INTERVAL '1 minute' - NOW()
                                ))::float AS retry_after_seconds,
                                (SELECT window_start FROM current_window) AS window_start
                            """
                        ),
                        {
                            "provider": self._provider,
                            "model": self._model,
                            "customer_id": customer_id,
                        },
                    )
                ).mappings().one()

                if estimated_tokens > self.total_tokens_per_minute:
                    return BudgetDecision(
                        False,
                        usage["retry_after_seconds"],
                        "request_exceeds_total_token_capacity",
                    )

                if usage["total_requests_used"] + estimated_requests > self.total_requests_per_minute:
                    return BudgetDecision(False, usage["retry_after_seconds"], "global_rpm_exhausted")

                available_tokens = (
                    self.total_tokens_per_minute
                    - usage["total_tokens_used"]
                    - usage["protected_tokens"]
                )
                if estimated_tokens > available_tokens:
                    return BudgetDecision(
                        False,
                        usage["retry_after_seconds"],
                        "customer_or_global_tpm_exhausted",
                    )

                await session.execute(
                    text(
                        """
                        INSERT INTO llm_usage_ledger (
                            interaction_id,
                            customer_id,
                            campaign_id,
                            provider,
                            model,
                            reservation_id,
                            estimated_tokens,
                            actual_tokens,
                            request_count,
                            status,
                            window_start
                        )
                        VALUES (
                            :interaction_id,
                            :customer_id,
                            :campaign_id,
                            :provider,
                            :model,
                            :reservation_id,
                            :estimated_tokens,
                            0,
                            :request_count,
                            'reserved',
                            :window_start
                        )
                        """
                    ),
                    {
                        "interaction_id": interaction_id,
                        "customer_id": customer_id,
                        "campaign_id": campaign_id,
                        "provider": self._provider,
                        "model": self._model,
                        "reservation_id": reservation_id,
                        "estimated_tokens": estimated_tokens,
                        "request_count": estimated_requests,
                        "window_start": usage["window_start"],
                    },
                )

        return BudgetDecision(True, reservation_id=reservation_id)

    async def commit(self, reservation_id: str, *, actual_tokens: int) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE llm_usage_ledger
                        SET actual_tokens = :actual_tokens,
                            status = 'completed',
                            completed_at = NOW()
                        WHERE reservation_id = CAST(:reservation_id AS uuid)
                        """
                    ),
                    {
                        "reservation_id": reservation_id,
                        "actual_tokens": actual_tokens,
                    },
                )

    async def release(self, reservation_id: str) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE llm_usage_ledger
                        SET status = 'released',
                            completed_at = NOW()
                        WHERE reservation_id = CAST(:reservation_id AS uuid)
                          AND status = 'reserved'
                        """
                    ),
                    {"reservation_id": reservation_id},
                )


@dataclass
class ScheduledAnalysis:
    interaction_id: str
    lane: ProcessingLane
    status: str
    retry_after_seconds: float = 0
    result: Optional[AnalysisResult] = None
    reason: str = ""


def classify_processing_lane(
    ctx: PostCallContext,
    policy: Optional[CustomerProcessingPolicy] = None,
) -> ProcessingLane:
    policy = policy or policy_from_additional_data(ctx.additional_data or {})
    transcript = (ctx.conversation_data or {}).get("transcript", [])
    if len(transcript) < policy.short_transcript_turns:
        return ProcessingLane.SKIP

    text = ctx.transcript_text.lower()
    if any(phrase in text for phrase in policy.hot_phrases):
        return ProcessingLane.HOT
    if any(phrase in text for phrase in policy.cold_phrases):
        return ProcessingLane.COLD
    return ProcessingLane.COLD


class LLMRequestScheduler:
    def __init__(
        self,
        budget_manager: TokenBudgetManager,
        processor: Optional[PostCallProcessor] = None,
        estimated_tokens_per_call: int = settings.LLM_AVG_TOKENS_PER_CALL,
    ):
        self.budget_manager = budget_manager
        self.processor = processor or PostCallProcessor()
        self.estimated_tokens_per_call = estimated_tokens_per_call

    async def process_when_budget_available(self, ctx: PostCallContext) -> ScheduledAnalysis:
        policy = policy_from_additional_data(ctx.additional_data or {})
        lane = classify_processing_lane(ctx, policy)
        audit_logger.emit(
            "analysis_classified",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            lane=lane.value,
        )

        if lane == ProcessingLane.SKIP:
            result = AnalysisResult(
                call_stage="short_call",
                entities={},
                summary="Short transcript skipped LLM analysis",
                raw_response={"call_stage": "short_call", "skipped_llm": True},
                tokens_used=0,
                latency_ms=0,
                provider=settings.LLM_PROVIDER,
                model=settings.LLM_MODEL,
            )
            await self.processor._update_interaction_metadata(ctx.interaction_id, result)
            audit_logger.emit(
                "analysis_skipped_short_transcript",
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                session_id=ctx.session_id,
                tokens_used=0,
            )
            return ScheduledAnalysis(ctx.interaction_id, lane, "completed", result=result)

        decision = await _maybe_await(
            self.budget_manager.acquire(
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                interaction_id=ctx.interaction_id,
                estimated_tokens=self.estimated_tokens_per_call,
            )
        )
        if not decision.allowed:
            audit_logger.emit(
                "analysis_deferred_budget_unavailable",
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                session_id=ctx.session_id,
                lane=lane.value,
                reason=decision.reason,
                retry_after_seconds=decision.retry_after_seconds,
            )
            return ScheduledAnalysis(
                ctx.interaction_id,
                lane,
                "deferred",
                retry_after_seconds=decision.retry_after_seconds,
                reason=decision.reason,
            )

        audit_logger.emit(
            "llm_budget_reserved",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            reservation_id=decision.reservation_id,
            estimated_tokens=self.estimated_tokens_per_call,
        )
        try:
            result = await self.processor.process_post_call(ctx, single_prompt=True)
        except Exception:
            await _maybe_await(self.budget_manager.release(decision.reservation_id or ""))
            audit_logger.emit(
                "analysis_failed",
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                session_id=ctx.session_id,
                severity="exception",
            )
            raise

        await _maybe_await(
            self.budget_manager.commit(
                decision.reservation_id or "",
                actual_tokens=result.tokens_used,
            )
        )
        audit_logger.emit(
            "analysis_completed",
            interaction_id=ctx.interaction_id,
            customer_id=ctx.customer_id,
            campaign_id=ctx.campaign_id,
            session_id=ctx.session_id,
            tokens_used=result.tokens_used,
            call_stage=result.call_stage,
            lane=lane.value,
        )
        return ScheduledAnalysis(ctx.interaction_id, lane, "completed", result=result)


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


default_budget_manager = PostgresTokenBudgetManager(
    total_tokens_per_minute=settings.LLM_TOKENS_PER_MINUTE,
    total_requests_per_minute=settings.LLM_REQUESTS_PER_MINUTE,
)
llm_scheduler = LLMRequestScheduler(default_budget_manager)
