import heapq
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from src.services.audit import audit_logger


class DurableTaskStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED_RETRYABLE = "failed_retryable"
    DEAD_LETTERED = "dead_lettered"


@dataclass(order=True)
class DurableTask:
    next_run_at: float
    id: str = field(compare=False)
    task_type: str = field(compare=False)
    interaction_id: str = field(compare=False)
    customer_id: str = field(compare=False)
    campaign_id: str = field(compare=False)
    payload: Dict[str, Any] = field(compare=False)
    status: DurableTaskStatus = field(default=DurableTaskStatus.QUEUED, compare=False)
    attempts: int = field(default=0, compare=False)
    max_attempts: int = field(default=10, compare=False)
    last_error: Optional[str] = field(default=None, compare=False)


class DurableTaskStore:
    """
    Minimal durable-task abstraction.

    Production implementation maps directly to the postcall_tasks table in
    schema.sql. The in-memory store keeps local tests deterministic.
    """

    def __init__(self, clock=time.time):
        self._clock = clock
        self._tasks: Dict[str, DurableTask] = {}
        self._ready_heap: List[DurableTask] = []

    async def enqueue(
        self,
        *,
        task_id: str,
        task_type: str,
        interaction_id: str,
        customer_id: str,
        campaign_id: str,
        payload: Dict[str, Any],
        run_after_seconds: float = 0,
    ) -> DurableTask:
        existing = self._tasks.get(task_id)
        if existing and existing.status not in {
            DurableTaskStatus.COMPLETED,
            DurableTaskStatus.DEAD_LETTERED,
        }:
            return existing

        task = DurableTask(
            id=task_id,
            task_type=task_type,
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            payload=payload,
            next_run_at=self._clock() + run_after_seconds,
        )
        self._tasks[task_id] = task
        heapq.heappush(self._ready_heap, task)
        audit_logger.emit(
            "durable_task_enqueued",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            task_id=task_id,
            task_type=task_type,
            next_run_at=task.next_run_at,
        )
        return task

    async def claim_ready(self, limit: int = 1) -> List[DurableTask]:
        now = self._clock()
        claimed: List[DurableTask] = []
        while self._ready_heap and len(claimed) < limit:
            task = heapq.heappop(self._ready_heap)
            current = self._tasks.get(task.id)
            if current is not task or task.next_run_at > now:
                if current is task:
                    heapq.heappush(self._ready_heap, task)
                break
            if task.status not in {DurableTaskStatus.QUEUED, DurableTaskStatus.FAILED_RETRYABLE}:
                continue
            task.status = DurableTaskStatus.RUNNING
            task.attempts += 1
            audit_logger.emit(
                "durable_task_claimed",
                interaction_id=task.interaction_id,
                customer_id=task.customer_id,
                campaign_id=task.campaign_id,
                task_id=task.id,
                task_type=task.task_type,
                attempt=task.attempts,
            )
            claimed.append(task)
        return claimed

    async def complete(self, task: DurableTask) -> None:
        task.status = DurableTaskStatus.COMPLETED
        audit_logger.emit(
            "durable_task_completed",
            interaction_id=task.interaction_id,
            customer_id=task.customer_id,
            campaign_id=task.campaign_id,
            task_id=task.id,
            task_type=task.task_type,
        )

    async def retry(self, task: DurableTask, *, error: str, delay_seconds: float) -> None:
        task.last_error = error
        if task.attempts >= task.max_attempts:
            task.status = DurableTaskStatus.DEAD_LETTERED
            audit_logger.emit(
                "durable_task_dead_lettered",
                interaction_id=task.interaction_id,
                customer_id=task.customer_id,
                campaign_id=task.campaign_id,
                task_id=task.id,
                task_type=task.task_type,
                error=error,
                severity="error",
            )
            return

        task.status = DurableTaskStatus.FAILED_RETRYABLE
        task.next_run_at = self._clock() + delay_seconds
        heapq.heappush(self._ready_heap, task)
        audit_logger.emit(
            "durable_task_retry_scheduled",
            interaction_id=task.interaction_id,
            customer_id=task.customer_id,
            campaign_id=task.campaign_id,
            task_id=task.id,
            task_type=task.task_type,
            error=error,
            next_run_at=task.next_run_at,
            severity="warning",
        )

    def get(self, task_id: str) -> Optional[DurableTask]:
        return self._tasks.get(task_id)


durable_task_store = DurableTaskStore()
