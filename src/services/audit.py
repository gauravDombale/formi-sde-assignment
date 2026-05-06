import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional

logger = logging.getLogger("postcall.audit")


class AuditLogger:
    """Structured audit events with stable correlation fields."""

    def emit(
        self,
        event: str,
        *,
        interaction_id: str,
        customer_id: Optional[str] = None,
        campaign_id: Optional[str] = None,
        session_id: Optional[str] = None,
        severity: str = "info",
        **fields: Any,
    ) -> Dict[str, Any]:
        payload = {
            "event": event,
            "interaction_id": interaction_id,
            "customer_id": customer_id,
            "campaign_id": campaign_id,
            "session_id": session_id,
            "occurred_at": datetime.now(timezone.utc).isoformat(),
            **fields,
        }
        log = getattr(logger, severity, logger.info)
        log(event, extra=payload)
        return payload


audit_logger = AuditLogger()
