"""
PostCallProcessor — Runs LLM analysis on a completed call transcript.

This is where the LLM quota gets spent. Every call that reaches this class
consumes ~1,500 tokens on average (see settings.LLM_AVG_TOKENS_PER_CALL).

The prompt extracts three things in a single LLM call (single_prompt=True):
  - call_stage: the outcome/disposition ("rebook_confirmed", "not_interested", etc.)
  - entities: structured data mentioned in the call (dates, amounts, names)
  - summary: a human-readable summary for the dashboard

One design observation: call_stage is usually detectable with high confidence
from just a few sentences of the transcript — sometimes from a single phrase.
Full entity extraction and summarisation are only useful if the call had a
meaningful outcome. Whether that distinction is worth acting on is a question
worth thinking about.

Another observation: the LLM response includes a `usage` field with the exact
token count. We log it per call. We don't aggregate it anywhere. We can't
currently answer "how many tokens did Customer X use this hour?" without
scanning logs.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional
from dataclasses import dataclass

from src.config import settings
from src.services.circuit_breaker import circuit_breaker
from src.services.audit import audit_logger

logger = logging.getLogger(__name__)


@dataclass
class PostCallContext:
    """Everything needed to process one completed call."""
    interaction_id: str
    session_id: str
    lead_id: str
    campaign_id: str
    customer_id: str  # The business using the platform (not the person called)
    agent_id: str
    call_sid: str     # Exotel's identifier for the call
    transcript_text: str
    conversation_data: dict
    additional_data: dict  # Arbitrary metadata from the dialler (campaign config, etc.)
    ended_at: datetime
    exotel_account_id: Optional[str] = None


@dataclass
class AnalysisResult:
    call_stage: str          # Disposition: rebook_confirmed, not_interested, etc.
    entities: Dict[str, Any] # Structured entities extracted from the transcript
    summary: str             # Human-readable summary for dashboard display
    raw_response: Dict[str, Any]
    tokens_used: int         # Actual tokens consumed — source of truth for billing
    latency_ms: float
    provider: str
    model: str


class PostCallProcessor:
    """
    Runs full LLM analysis on a transcript.

    Currently called for every interaction that isn't a short call.
    No pre-screening, no quota check before firing, no customer-level budgeting.

    The circuit_breaker.record_postcall_start() call increments a Redis counter
    used by the dialler's capacity check. But it tracks in-flight tasks, not
    actual tokens/minute. By the time the circuit breaker trips (at 90% RPM),
    we've already been 429-ing for a while.
    """

    async def process_post_call(
        self, ctx: PostCallContext, single_prompt: bool = True
    ) -> AnalysisResult:
        """
        Run LLM analysis and write result to interaction_metadata.

        single_prompt=True means we run entity extraction, classification, and
        summarisation in one LLM call. This was a cost optimisation over an
        earlier version that made three separate calls. It's the right trade-off.

        What this function does NOT do before calling the LLM:
          - Check whether we're near the tokens/minute limit
          - Check whether this customer has exceeded their allocated budget
          - Consider whether this call's outcome even warrants full analysis
        """

        # Tells the circuit breaker an LLM request is in flight.
        # Note: this increments llm:postcall:rpm but doesn't check it first.
        # The check happens in circuit_breaker.check_capacity(), which is
        # called by the dialler — not here, before spending the tokens.
        try:
            await circuit_breaker.record_postcall_start()
        except Exception as e:
            audit_logger.emit(
                "legacy_circuit_metric_failed",
                interaction_id=ctx.interaction_id,
                customer_id=ctx.customer_id,
                campaign_id=ctx.campaign_id,
                session_id=ctx.session_id,
                error=str(e),
                severity="warning",
            )

        try:
            prompt = self._build_analysis_prompt(
                ctx.transcript_text,
                ctx.additional_data,
                single_prompt,
            )

            start_time = datetime.utcnow()
            response = await self._call_llm(prompt)
            elapsed_ms = (datetime.utcnow() - start_time).total_seconds() * 1000

            result = self._parse_response(response, elapsed_ms)

            # Result written to interaction_metadata — the dashboard's hot cache.
            # There is no separate "analysis results" table. The JSONB column on
            # the interactions row is the only place this data lives.
            await self._update_interaction_metadata(ctx.interaction_id, result)

            logger.info(
                "postcall_analysis_complete",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "customer_id": ctx.customer_id,
                    "campaign_id": ctx.campaign_id,
                    "call_stage": result.call_stage,
                    "tokens_used": result.tokens_used,
                    "latency_ms": result.latency_ms,
                    # tokens_used is logged here but never written back to any
                    # counter that could enforce a per-customer budget.
                },
            )

            return result

        except Exception as e:
            logger.exception(
                "postcall_analysis_failed",
                extra={
                    "interaction_id": ctx.interaction_id,
                    "error": str(e),
                    # If this is a 429 from the LLM provider, the error message
                    # will say so. But the retry logic above doesn't distinguish
                    # "retry in 1 second" (rate limit) from "retry in 60 seconds"
                    # (transient failure) — it always waits 60 seconds.
                },
            )
            raise

        finally:
            try:
                await circuit_breaker.record_postcall_end()
            except Exception as e:
                audit_logger.emit(
                    "legacy_circuit_metric_failed",
                    interaction_id=ctx.interaction_id,
                    customer_id=ctx.customer_id,
                    campaign_id=ctx.campaign_id,
                    session_id=ctx.session_id,
                    error=str(e),
                    severity="warning",
                )

    def _build_analysis_prompt(
        self,
        transcript: str,
        additional_data: dict,
        single_prompt: bool,
    ) -> str:
        """
        Build the LLM prompt.

        The system prompt asks for three outputs in one JSON object.
        call_stage is the most important — everything downstream depends on it.
        entities and summary are useful but secondary.

        If you were thinking about a cheaper "just classify the call_stage" step
        before the full analysis, this is the prompt you'd be splitting.
        """
        system_prompt = """You are a call analysis assistant. Analyze the following
call transcript and extract:
1. call_stage: The outcome/disposition of the call
2. entities: Key information mentioned (dates, times, amounts, names, preferences)
3. summary: A brief summary of what happened in the call

Respond in JSON format:
{
    "call_stage": "...",
    "entities": {...},
    "summary": "..."
}"""

        return (
            f"{system_prompt}\n\n"
            f"Transcript:\n{transcript}\n\n"
            f"Additional context:\n{json.dumps(additional_data)}"
        )

    async def _call_llm(self, prompt: str) -> dict:
        """
        Call the configured LLM provider.

        In production this is an httpx POST to the provider's API.
        A 429 response raises an exception that propagates up to the Celery
        retry handler — which retries after a fixed 60-second delay regardless
        of the Retry-After header the provider sends back.

        Mock implementation for the assessment.
        """
        # The provider's response includes a `usage` block:
        # {"prompt_tokens": N, "completion_tokens": M, "total_tokens": N+M}
        # We surface total_tokens in AnalysisResult but don't write it back
        # anywhere that could be used for budget tracking or alerting.
        return {
            "call_stage": "unknown",
            "entities": {},
            "summary": "Mock analysis result",
            "usage": {"total_tokens": 1500},
        }

    def _parse_response(self, response: dict, latency_ms: float) -> AnalysisResult:
        return AnalysisResult(
            call_stage=response.get("call_stage", "unknown"),
            entities=response.get("entities", {}),
            summary=response.get("summary", ""),
            raw_response=response,
            tokens_used=response.get("usage", {}).get("total_tokens", 0),
            latency_ms=latency_ms,
            provider=settings.LLM_PROVIDER,
            model=settings.LLM_MODEL,
        )

    async def _update_interaction_metadata(
        self, interaction_id: str, result: AnalysisResult
    ) -> None:
        """
        Write analysis results into the interaction_metadata JSONB column.

        In production:
            UPDATE interactions
            SET interaction_metadata = interaction_metadata || $2::jsonb,
                updated_at = NOW()
            WHERE id = $1

        The dashboard reads interaction_metadata directly. There is no separate
        results table — this JSONB column is the only record of the analysis.
        If it gets overwritten by a retry, the previous result is gone.
        """
        logger.info(
            "metadata_updated",
            extra={
                "interaction_id": interaction_id,
                "call_stage": result.call_stage,
            },
        )


post_call_processor = PostCallProcessor()
