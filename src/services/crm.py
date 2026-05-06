import logging
from typing import Any, Dict, Optional

import httpx

from src.services.audit import audit_logger

logger = logging.getLogger(__name__)


class CRMDeliveryError(RuntimeError):
    pass


async def push_crm_update(
    *,
    interaction_id: str,
    customer_id: str,
    campaign_id: str,
    endpoint_url: Optional[str],
    payload: Dict[str, Any],
) -> None:
    if not endpoint_url:
        audit_logger.emit(
            "crm_push_skipped",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            reason="crm_not_configured",
        )
        return

    audit_logger.emit(
        "crm_push_started",
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        endpoint_url=endpoint_url,
    )
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(endpoint_url, json=payload)

    if response.status_code >= 500 or response.status_code == 429:
        raise CRMDeliveryError(f"retryable crm response: {response.status_code}")
    if response.status_code >= 400:
        audit_logger.emit(
            "crm_push_rejected",
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            status_code=response.status_code,
            severity="error",
        )
        return

    audit_logger.emit(
        "crm_push_completed",
        interaction_id=interaction_id,
        customer_id=customer_id,
        campaign_id=campaign_id,
        status_code=response.status_code,
    )
