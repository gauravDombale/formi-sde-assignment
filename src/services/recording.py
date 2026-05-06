"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

How Exotel works:
  After a call ends, Exotel processes the audio and makes a recording URL
  available via their REST API. The time between call-end and URL availability
  varies: typically 10–30 seconds, but can be 60–90s under load on their end.

  The URL is fetched via:
      GET /v1/Accounts/{account_sid}/Calls/{call_sid}/Recording
  Returns 200 + recording_url if ready, 404 if not yet available.

Current approach:
  Wait 45 seconds. Try once. If it's not there, give up silently.

This means:
  - Recordings ready in 10s: we waste 35 seconds of wall time
  - Recordings ready in 60s: we miss them entirely, no retry, no alert
  - We have no idea how many recordings we're silently missing

The Exotel API is poll-friendly — they don't rate-limit the status endpoint.
The information needed to fix this is already available: try, check, sleep
a bit, try again. How many times and with what interval is worth thinking about.

Note: recording upload and LLM analysis are completely independent. The LLM
reads the transcript text, not the audio. There's no reason they have to run
sequentially. What would need to change for them to run in parallel?
"""

import logging
import random
import asyncio
from dataclasses import dataclass
from typing import Optional

import httpx

from src.config import settings
from src.services.audit import audit_logger

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RecordingPollConfig:
    max_attempts: int = 8
    initial_delay_seconds: float = 2
    max_delay_seconds: float = 30
    jitter_ratio: float = 0.10


async def poll_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
    customer_id: Optional[str] = None,
    campaign_id: Optional[str] = None,
    config: RecordingPollConfig = RecordingPollConfig(),
) -> Optional[str]:
    """
    Poll Exotel until the recording is ready, then upload it to S3.

    Returns the S3 key on success, None on failure or timeout.
    """
    delay = config.initial_delay_seconds

    for attempt in range(1, config.max_attempts + 1):
        audit_logger.emit(
            "recording_poll_attempt",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            call_sid=call_sid,
            attempt=attempt,
        )

        try:
            recording_url = await _fetch_exotel_recording_url(call_sid, exotel_account_id)
            if recording_url:
                s3_key = await _upload_to_s3(recording_url, interaction_id)
                audit_logger.emit(
                    "recording_uploaded",
                    interaction_id=interaction_id,
                    customer_id=customer_id,
                    campaign_id=campaign_id,
                    call_sid=call_sid,
                    s3_key=s3_key,
                    attempt=attempt,
                )
                return s3_key

            audit_logger.emit(
                "recording_not_ready",
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                call_sid=call_sid,
                attempt=attempt,
                next_delay_seconds=delay if attempt < config.max_attempts else None,
            )
        except Exception as e:
            audit_logger.emit(
                "recording_poll_error",
                interaction_id=interaction_id,
                customer_id=customer_id,
                campaign_id=campaign_id,
                call_sid=call_sid,
                attempt=attempt,
                error=str(e),
                severity="error",
            )

        if attempt < config.max_attempts:
            await asyncio.sleep(_with_jitter(delay, config.jitter_ratio))
            delay = min(config.max_delay_seconds, delay * 2)

    audit_logger.emit(
        "recording_upload_failed",
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        call_sid=call_sid,
        attempts=config.max_attempts,
        severity="error",
    )
    return None


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> Optional[str]:
    """Backward-compatible wrapper for callers not yet migrated."""
    return await poll_and_upload_recording(
        interaction_id=interaction_id,
        call_sid=call_sid,
        exotel_account_id=exotel_account_id,
    )


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> Optional[str]:
    """
    Hit the Exotel API to get the recording URL for a completed call.

    Returns the recording URL if available, None if not yet ready.
    The 404 case (not yet ready) and the genuine error case (call had no
    recording, e.g., call was never connected) look the same from here —
    both return None. A retry loop would want to handle these differently.
    """
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("recording_url")
            return None
    except httpx.HTTPError:
        return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.

    In production: stream from recording_url → boto3 upload to S3_BUCKET.
    S3 key format: recordings/{interaction_id}.mp3

    The interaction's recording_s3_key column gets updated after this succeeds.
    If this crashes after the upload but before the DB write, the file is in S3
    but the interaction row doesn't know about it. Currently no reconciliation job.
    """
    s3_key = f"recordings/{interaction_id}.mp3"

    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key


def _with_jitter(delay: float, jitter_ratio: float) -> float:
    if jitter_ratio <= 0:
        return delay
    spread = delay * jitter_ratio
    return max(0, delay + random.uniform(-spread, spread))
