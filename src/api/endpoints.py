"""
FastAPI endpoint for ending an interaction.

POST /session/{session_id}/interaction/{interaction_id}/end

Called by Exotel (telephony provider) when a call disconnects. This webhook
must respond fast — Exotel has a 5-second timeout and will retry if we don't.

Current design: respond immediately, hand off to Celery for the heavy work.
That part is fine. The problems are in what happens next.

A few things to notice as you read this file:

1. We check transcript length here (< 4 turns = "short"). Short calls skip
   the LLM entirely. That's the right idea. But the check only lives here —
   the Celery task doesn't know about it, so if a task gets requeued after
   a crash, it will re-run without the short-transcript gate.

2. For long transcripts, signal_jobs and lead_stage fire from asyncio.create_task
   BEFORE Celery has run the LLM. The analysis_result passed to signal_jobs
   is literally an empty dict {}. Downstream systems receive an empty payload
   and silently do nothing useful.

3. There is no correlation ID threaded through this endpoint. If something
   fails downstream, you know the interaction_id but not which step failed,
   when, or why.
"""

import logging
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from src.services.audit import audit_logger
from src.services.durable_tasks import durable_task_store
from src.services.llm_scheduler import ProcessingLane, classify_processing_lane
from src.services.post_call_processor import PostCallContext
from src.tasks.celery_tasks import process_interaction_end_background_task

logger = logging.getLogger(__name__)
router = APIRouter()


class InteractionEndRequest(BaseModel):
    call_sid: Optional[str] = None
    duration_seconds: Optional[int] = None
    call_status: Optional[str] = None
    # Arbitrary metadata from the telephony provider / dialler.
    # In practice contains things like: dialler_campaign_id, lead_phone,
    # call_attempt_number, agent_script_id. Passed through to the LLM prompt.
    additional_data: Optional[Dict[str, Any]] = None


class InteractionEndResponse(BaseModel):
    status: str
    interaction_id: str
    message: str


@router.post(
    "/session/{session_id}/interaction/{interaction_id}/end",
    response_model=InteractionEndResponse,
)
async def end_interaction(
    session_id: UUID,
    interaction_id: UUID,
    request: InteractionEndRequest,
    background_tasks: BackgroundTasks,
):
    """
    End an interaction and trigger post-call processing.

    Current flow:
    1. Load interaction from DB
    2. Mark status ENDED
    3. Decide short vs long transcript
       - Short (< 4 turns): fire signal jobs inline, skip LLM
       - Long: dump everything into Celery, fire signal jobs anyway (empty payload)
    4. Return 200 before anything actually processes
    """
    try:
        interaction = await _load_interaction(interaction_id)

        if not interaction:
            raise HTTPException(status_code=404, detail="Interaction not found")

        await _update_interaction_status(
            interaction_id=str(interaction_id),
            status="ENDED",
            ended_at=datetime.utcnow(),
            duration=request.duration_seconds,
            call_sid=request.call_sid,
        )

        transcript = interaction.get("conversation_data", {}).get("transcript", [])
        transcript_text = "\n".join(
            f"{turn.get('role', 'unknown')}: {turn.get('content', '')}"
            for turn in transcript
        )
        context = PostCallContext(
            interaction_id=str(interaction_id),
            session_id=str(session_id),
            lead_id=interaction["lead_id"],
            campaign_id=interaction["campaign_id"],
            customer_id=interaction["customer_id"],
            agent_id=interaction["agent_id"],
            call_sid=request.call_sid or "",
            transcript_text=transcript_text,
            conversation_data=interaction.get("conversation_data", {}),
            additional_data=request.additional_data or {},
            ended_at=datetime.utcnow(),
            exotel_account_id=interaction.get("exotel_account_id"),
        )
        lane = classify_processing_lane(context)

        celery_payload = {
            "interaction_id": str(interaction_id),
            "session_id": str(session_id),
            "lead_id": interaction["lead_id"],
            "campaign_id": interaction["campaign_id"],
            "customer_id": interaction["customer_id"],
            "agent_id": interaction["agent_id"],
            "call_sid": request.call_sid,
            "transcript_text": transcript_text,
            "conversation_data": interaction.get("conversation_data", {}),
            "additional_data": request.additional_data or {},
            "ended_at": datetime.utcnow().isoformat(),
            "exotel_account_id": interaction.get("exotel_account_id"),
        }

        await durable_task_store.enqueue(
            task_id=f"analysis:{interaction_id}",
            task_type="llm_analysis",
            interaction_id=str(interaction_id),
            customer_id=interaction["customer_id"],
            campaign_id=interaction["campaign_id"],
            payload=celery_payload,
            lane=lane,
        )
        await durable_task_store.enqueue(
            task_id=f"recording:{interaction_id}",
            task_type="recording_upload",
            interaction_id=str(interaction_id),
            customer_id=interaction["customer_id"],
            campaign_id=interaction["campaign_id"],
            payload={
                "interaction_id": str(interaction_id),
                "call_sid": request.call_sid or "",
                "exotel_account_id": interaction.get("exotel_account_id") or "",
                "customer_id": interaction["customer_id"],
                "campaign_id": interaction["campaign_id"],
            },
        )

        if lane == ProcessingLane.SKIP:
            # Fewer than 4 turns: wrong number, immediate hangup, network drop.
            # Skip LLM — there's nothing meaningful to extract.
            audit_logger.emit(
                "short_transcript_fast_path",
                interaction_id=str(interaction_id),
                customer_id=interaction["customer_id"],
                campaign_id=interaction["campaign_id"],
                session_id=str(session_id),
            )

        else:
            task = process_interaction_end_background_task.apply_async(
                args=[celery_payload],
                queue=f"postcall_{lane.value}",
                priority=9 if lane == ProcessingLane.HOT else 3,
            )

            audit_logger.emit(
                "postcall_enqueued",
                interaction_id=str(interaction_id),
                customer_id=interaction["customer_id"],
                campaign_id=interaction["campaign_id"],
                session_id=str(session_id),
                celery_task_id=task.id,
                lane=lane.value,
            )

        return InteractionEndResponse(
            status="ok",
            interaction_id=str(interaction_id),
            message="Interaction ended, processing enqueued",
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "end_interaction_failed",
            extra={"interaction_id": str(interaction_id), "error": str(e)},
        )
        raise HTTPException(status_code=500, detail="Internal server error")


async def _load_interaction(interaction_id: UUID) -> Optional[Dict[str, Any]]:
    """
    Load interaction from the database.

    In production: SELECT * FROM interactions WHERE id = $1.
    The conversation_data JSONB column holds the full transcript as:
        {"transcript": [{"role": "agent"|"customer", "content": "..."}]}

    The interaction_metadata column is the dashboard's hot cache —
    the UI reads from here, not from a separate analysis table.
    Worth thinking about whether that's the right separation of concerns.
    """
    # Mock — returns a realistic sample for local development
    return {
        "id": str(interaction_id),
        "lead_id": "a0000000-0000-0000-0000-000000000001",
        "campaign_id": "c0000000-0000-0000-0000-000000000001",
        "customer_id": "d0000000-0000-0000-0000-000000000001",
        "agent_id": "e0000000-0000-0000-0000-000000000001",
        "exotel_account_id": "mock-exotel-account",
        "conversation_data": {
            "transcript": [
                {"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"},
                {"role": "customer", "content": "Yes, speaking."},
                {"role": "agent", "content": "I'm calling from XYZ about your recent inquiry."},
                {"role": "customer", "content": "Oh yes, I was looking at the product."},
                {"role": "agent", "content": "Would you like to schedule a demo?"},
                {"role": "customer", "content": "Sure, let's do tomorrow at 3 PM."},
                {"role": "agent", "content": "Perfect, I've booked a demo for tomorrow at 3 PM."},
                {"role": "customer", "content": "Thank you, bye."},
            ]
        },
    }


async def _update_interaction_status(
    interaction_id: str,
    status: str,
    ended_at: datetime,
    duration: Optional[int],
    call_sid: Optional[str],
) -> None:
    """
    Update interaction status in the database.

    In production:
        UPDATE interactions
        SET status = $2, ended_at = $3, duration_seconds = $4, call_sid = $5
        WHERE id = $1
    """
    logger.info(
        "interaction_status_updated",
        extra={
            "interaction_id": interaction_id,
            "status": status,
            "ended_at": ended_at.isoformat(),
        },
    )
