import heapq
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.services.llm_scheduler import ProcessingLane
from src.services.audit import audit_logger
from src.utils.db import async_session_factory


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
        lane: ProcessingLane = ProcessingLane.COLD,
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


class PostgresDurableTaskStore:
    """
    Durable task store backed by the postcall_tasks table.

    Workers claim due rows with FOR UPDATE SKIP LOCKED, then update them in the
    same transaction. If a worker dies after claiming, locked_until expires and
    another worker can reclaim the row.
    """

    CLAIM_SQL = """
        WITH candidate AS (
            SELECT id
            FROM postcall_tasks
            WHERE status IN ('queued', 'failed_retryable')
              AND next_run_at <= NOW()
              AND (locked_until IS NULL OR locked_until < NOW())
            ORDER BY
              CASE lane
                WHEN 'hot' THEN 0
                WHEN 'cold' THEN 1
                ELSE 2
              END,
              next_run_at ASC,
              created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
        )
        UPDATE postcall_tasks AS task
        SET status = 'running',
            attempts = task.attempts + 1,
            locked_by = :worker_id,
            locked_until = NOW() + (:lock_seconds * INTERVAL '1 second'),
            updated_at = NOW()
        FROM candidate
        WHERE task.id = candidate.id
        RETURNING
            task.id,
            task.task_type,
            task.interaction_id::text AS interaction_id,
            task.customer_id::text AS customer_id,
            task.campaign_id::text AS campaign_id,
            task.payload,
            task.status,
            task.attempts,
            task.max_attempts,
            task.last_error,
            EXTRACT(EPOCH FROM task.next_run_at) AS next_run_at_epoch;
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] = async_session_factory,
        worker_id: str = "postcall-worker",
        lock_seconds: int = 300,
    ):
        self._session_factory = session_factory
        self._worker_id = worker_id
        self._lock_seconds = lock_seconds

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
        lane: ProcessingLane = ProcessingLane.COLD,
    ) -> DurableTask:
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    text(
                        """
                        INSERT INTO postcall_tasks (
                            id,
                            interaction_id,
                            customer_id,
                            campaign_id,
                            task_type,
                            lane,
                            status,
                            payload,
                            next_run_at
                        )
                        VALUES (
                            :task_id,
                            :interaction_id,
                            :customer_id,
                            :campaign_id,
                            :task_type,
                            :lane,
                            'queued',
                            CAST(:payload_json AS jsonb),
                            NOW() + (:run_after_seconds * INTERVAL '1 second')
                        )
                        ON CONFLICT (id) DO UPDATE
                        SET next_run_at = CASE
                                WHEN postcall_tasks.status IN ('completed', 'dead_lettered')
                                THEN EXCLUDED.next_run_at
                                ELSE postcall_tasks.next_run_at
                            END,
                            payload = CASE
                                WHEN postcall_tasks.status IN ('completed', 'dead_lettered')
                                THEN EXCLUDED.payload
                                ELSE postcall_tasks.payload
                            END,
                            status = CASE
                                WHEN postcall_tasks.status IN ('completed', 'dead_lettered')
                                THEN 'queued'
                                ELSE postcall_tasks.status
                            END,
                            updated_at = NOW()
                        RETURNING
                            id,
                            task_type,
                            interaction_id::text AS interaction_id,
                            customer_id::text AS customer_id,
                            campaign_id::text AS campaign_id,
                            payload,
                            status,
                            attempts,
                            max_attempts,
                            last_error,
                            EXTRACT(EPOCH FROM next_run_at) AS next_run_at_epoch;
                        """
                    ),
                    {
                        "task_id": task_id,
                        "interaction_id": interaction_id,
                        "customer_id": customer_id,
                        "campaign_id": campaign_id,
                        "task_type": task_type,
                        "lane": lane.value,
                        "payload_json": json.dumps(payload),
                        "run_after_seconds": run_after_seconds,
                    },
                )
                row = result.mappings().one()

        task = self._row_to_task(row)
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
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    text(self.CLAIM_SQL),
                    {
                        "limit": limit,
                        "worker_id": self._worker_id,
                        "lock_seconds": self._lock_seconds,
                    },
                )
                rows = result.mappings().all()

        tasks = [self._row_to_task(row) for row in rows]
        for task in tasks:
            audit_logger.emit(
                "durable_task_claimed",
                interaction_id=task.interaction_id,
                customer_id=task.customer_id,
                campaign_id=task.campaign_id,
                task_id=task.id,
                task_type=task.task_type,
                attempt=task.attempts,
            )
        return tasks

    async def complete(self, task: DurableTask) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE postcall_tasks
                        SET status = 'completed',
                            completed_at = NOW(),
                            locked_by = NULL,
                            locked_until = NULL,
                            updated_at = NOW()
                        WHERE id = :task_id
                        """
                    ),
                    {"task_id": task.id},
                )
        audit_logger.emit(
            "durable_task_completed",
            interaction_id=task.interaction_id,
            customer_id=task.customer_id,
            campaign_id=task.campaign_id,
            task_id=task.id,
            task_type=task.task_type,
        )

    async def retry(self, task: DurableTask, *, error: str, delay_seconds: float) -> None:
        status = "dead_lettered" if task.attempts >= task.max_attempts else "failed_retryable"
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        UPDATE postcall_tasks
                        SET status = :status,
                            last_error = :error,
                            next_run_at = CASE
                                WHEN :status = 'dead_lettered'
                                THEN next_run_at
                                ELSE NOW() + (:delay_seconds * INTERVAL '1 second')
                            END,
                            locked_by = NULL,
                            locked_until = NULL,
                            updated_at = NOW()
                        WHERE id = :task_id
                        """
                    ),
                    {
                        "task_id": task.id,
                        "status": status,
                        "error": error,
                        "delay_seconds": delay_seconds,
                    },
                )

        event = "durable_task_dead_lettered" if status == "dead_lettered" else "durable_task_retry_scheduled"
        audit_logger.emit(
            event,
            interaction_id=task.interaction_id,
            customer_id=task.customer_id,
            campaign_id=task.campaign_id,
            task_id=task.id,
            task_type=task.task_type,
            error=error,
            severity="error" if status == "dead_lettered" else "warning",
        )

    @staticmethod
    def _row_to_task(row: Dict[str, Any]) -> DurableTask:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        return DurableTask(
            id=row["id"],
            task_type=row["task_type"],
            interaction_id=row["interaction_id"],
            customer_id=row["customer_id"],
            campaign_id=row["campaign_id"],
            payload=payload,
            status=DurableTaskStatus(row["status"]),
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            last_error=row["last_error"],
            next_run_at=float(row["next_run_at_epoch"]),
        )


durable_task_store = PostgresDurableTaskStore()
