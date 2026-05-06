"""
PostCallCircuitBreaker — Tries to protect the LLM API from overload.

The idea was sound: if we're sending too many LLM requests, slow down new
calls before the provider starts returning 429s. The execution is too blunt.

Current behaviour:
  - Checks RPM usage against LLM_REQUESTS_PER_MINUTE
  - If usage >= 90%: freeze the dialler for the agent for 1800 seconds
  - The dialler checks this before dispatching new calls

Problems:
  1. Binary response. 89% capacity: full speed. 90%: complete stop for 30 minutes.
     There's no middle gear — no "slow down a bit", no "pause for 5 seconds".

  2. Wrong granularity. Freezes at agent_id level. If one campaign is consuming
     all the LLM quota, every agent — across all customers — hits the freeze.

  3. Measuring the wrong thing. This tracks in-flight Celery tasks via an RPM
     counter. But LLM providers rate-limit on tokens/minute, not requests/minute.
     A 10-turn conversation uses 3× the tokens of a 3-turn one. The circuit
     breaker doesn't know that.

  4. The counter is written by record_postcall_start(), which is called after
     deciding to fire the LLM request — not before. By the time the breaker
     could trip, the requests are already in flight.

  5. No visibility. The dialler just sees "circuit open". It doesn't know if it's
     because of a genuine quota issue or a transient Redis blip that made the
     counter stale.

Consider: what would a system look like where the dialler doesn't freeze at all,
but instead naturally dispatches fewer calls when LLM capacity is constrained?
The capacity signal already exists — it just needs to be used differently.
"""

import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


@dataclass
class CircuitState:
    agent_id: str
    is_open: bool = False
    opened_at: Optional[float] = None
    freeze_until: Optional[float] = None
    consecutive_failures: int = 0
    # consecutive_failures is tracked but never used in trip logic.
    # It was intended for a half-open state that never got implemented.


@dataclass(frozen=True)
class BackpressureDecision:
    allowed: bool
    delay_seconds: float
    capacity_ratio: float
    reason: str


class PostCallCircuitBreaker:
    """
    Checks whether the dialler should be allowed to make a new call.
    Called by the dialler before dispatching each outbound call.

    The Redis key `llm:postcall:rpm` is the shared state between this
    circuit breaker and the post-call workers. Workers increment it when
    they start an LLM request. The TTL of 60 seconds means it naturally
    decays — but if workers crash mid-request, they may not decrement it,
    causing the counter to be permanently inflated until TTL expires.
    """

    def __init__(self):
        self._states: Dict[str, CircuitState] = {}
        self._capacity_threshold = settings.CIRCUIT_BREAKER_CAPACITY_THRESHOLD
        self._freeze_seconds = settings.CIRCUIT_BREAKER_FREEZE_SECONDS

    async def check_capacity(self, agent_id: str) -> bool:
        """
        Returns True if the agent is allowed to make a new call.

        Called by the dialler. NOT called by the post-call workers —
        they fire LLM requests unconditionally.
        """
        decision = await self.get_backpressure(agent_id)
        return decision.allowed

    async def get_backpressure(self, agent_id: str) -> BackpressureDecision:
        """
        Return a proportional dispatch delay instead of a binary 30-minute freeze.
        Dialler callers can sleep for delay_seconds or reduce concurrency.
        """
        current_rpm = int(await redis_client.get("llm:postcall:rpm") or 0)
        max_rpm = settings.LLM_REQUESTS_PER_MINUTE

        usage_ratio = current_rpm / max_rpm if max_rpm > 0 else 0

        if usage_ratio < 0.70:
            return BackpressureDecision(True, 0, usage_ratio, "capacity_available")
        if usage_ratio < 0.85:
            return BackpressureDecision(True, 1, usage_ratio, "light_backpressure")
        if usage_ratio < 1.0:
            return BackpressureDecision(True, 5, usage_ratio, "heavy_backpressure")

        logger.warning(
            "dialler_backpressure_max",
            extra={
                "agent_id": agent_id,
                "current_rpm": current_rpm,
                "max_rpm": max_rpm,
                "capacity_ratio": usage_ratio,
                "delay_seconds": 15,
            },
        )
        return BackpressureDecision(False, 15, usage_ratio, "llm_capacity_exhausted")

    def _trip(self, agent_id: str):
        """
        Open the circuit breaker for agent_id for CIRCUIT_BREAKER_FREEZE_SECONDS.

        Logs an error but provides no context about WHY it tripped — the on-call
        engineer sees "circuit_breaker_tripped" and has to go dig in Redis to
        figure out whether it was LLM quota, a Celery backlog, or a Redis glitch.
        """
        now = time.time()
        state = CircuitState(
            agent_id=agent_id,
            is_open=True,
            opened_at=now,
            freeze_until=now + self._freeze_seconds,
        )
        self._states[agent_id] = state

        logger.error(
            "circuit_breaker_tripped",
            extra={
                "agent_id": agent_id,
                "freeze_seconds": self._freeze_seconds,
                "capacity_threshold": self._capacity_threshold,
                # Would be useful to also log: current_rpm, current_tpm,
                # queue_depth, and which customer's calls triggered the spike.
                # None of that is available here.
            },
        )

    async def record_postcall_start(self):
        """
        Increment the RPM counter when a post-call LLM request starts.

        This runs AFTER we've decided to fire the request, so it's a
        measurement, not a gate. The dialler reads this counter to make
        dispatch decisions — there's a lag between when requests go out
        and when the counter updates.
        """
        await redis_client.incr("llm:postcall:rpm")
        await redis_client.expire("llm:postcall:rpm", 60)

    async def record_postcall_end(self):
        """
        Decrement the RPM counter when the LLM request completes.

        If the worker crashes between start and end, the counter stays
        inflated until the 60-second TTL expires. During that window,
        the circuit breaker may trip unnecessarily.
        """
        await redis_client.decr("llm:postcall:rpm")


circuit_breaker = PostCallCircuitBreaker()
