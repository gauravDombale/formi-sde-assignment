import logging
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.utils.db import async_session_factory

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

    async def emit_persisted(
        self,
        event: str,
        *,
        interaction_id: str,
        customer_id: Optional[str] = None,
        campaign_id: Optional[str] = None,
        session_id: Optional[str] = None,
        severity: str = "info",
        writer: Optional["PostgresAuditEventWriter"] = None,
        **fields: Any,
    ) -> Dict[str, Any]:
        payload = self.emit(
            event,
            interaction_id=interaction_id,
            customer_id=customer_id,
            campaign_id=campaign_id,
            session_id=session_id,
            severity=severity,
            **fields,
        )
        await (writer or audit_event_writer).write(payload, severity=severity)
        return payload


class PostgresAuditEventWriter:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession] = async_session_factory):
        self._session_factory = session_factory

    async def write(self, payload: Dict[str, Any], *, severity: str = "info") -> None:
        async with self._session_factory() as session:
            async with session.begin():
                await session.execute(
                    text(
                        """
                        INSERT INTO postcall_audit_events (
                            interaction_id,
                            customer_id,
                            campaign_id,
                            session_id,
                            event_name,
                            severity,
                            event_data,
                            occurred_at
                        )
                        VALUES (
                            :interaction_id,
                            NULLIF(:customer_id, '')::uuid,
                            NULLIF(:campaign_id, '')::uuid,
                            NULLIF(:session_id, '')::uuid,
                            :event_name,
                            :severity,
                            CAST(:event_data_json AS jsonb),
                            :occurred_at
                        )
                        """
                    ),
                    {
                        "interaction_id": payload["interaction_id"],
                        "customer_id": payload.get("customer_id") or "",
                        "campaign_id": payload.get("campaign_id") or "",
                        "session_id": payload.get("session_id") or "",
                        "event_name": payload["event"],
                        "severity": severity,
                        "event_data_json": json.dumps(payload, default=str),
                        "occurred_at": payload["occurred_at"],
                    },
                )


audit_logger = AuditLogger()
audit_event_writer = PostgresAuditEventWriter()
