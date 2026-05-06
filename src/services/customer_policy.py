from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Optional


DEFAULT_HOT_PHRASES = (
    "confirmed",
    "book",
    "booked",
    "demo",
    "appointment",
    "schedule",
    "reschedule",
    "manager",
    "escalate",
    "complaint",
    "unacceptable",
)

DEFAULT_COLD_PHRASES = (
    "not interested",
    "don't call",
    "dont call",
    "already booked",
    "already purchased",
    "wrong number",
)


@dataclass(frozen=True)
class CustomerProcessingPolicy:
    hot_phrases: tuple[str, ...] = field(default_factory=lambda: DEFAULT_HOT_PHRASES)
    cold_phrases: tuple[str, ...] = field(default_factory=lambda: DEFAULT_COLD_PHRASES)
    short_transcript_turns: int = 4
    crm_enabled: bool = False
    crm_endpoint: Optional[str] = None
    hot_lane_sla_seconds: int = 120
    max_downstream_attempts: int = 10

    @classmethod
    def from_mapping(cls, data: Optional[Dict[str, Any]]) -> "CustomerProcessingPolicy":
        if not data:
            return cls()

        return cls(
            hot_phrases=_tuple_or_default(data.get("hot_phrases"), DEFAULT_HOT_PHRASES),
            cold_phrases=_tuple_or_default(data.get("cold_phrases"), DEFAULT_COLD_PHRASES),
            short_transcript_turns=int(data.get("short_transcript_turns", 4)),
            crm_enabled=bool(data.get("crm_enabled", False)),
            crm_endpoint=data.get("crm_endpoint"),
            hot_lane_sla_seconds=int(data.get("hot_lane_sla_seconds", 120)),
            max_downstream_attempts=int(data.get("max_downstream_attempts", 10)),
        )


def policy_from_additional_data(additional_data: Dict[str, Any]) -> CustomerProcessingPolicy:
    config = (
        additional_data.get("processing_policy")
        or additional_data.get("customer_processing_policy")
        or additional_data.get("postcall_policy")
        or {}
    )
    return CustomerProcessingPolicy.from_mapping(config)


def _tuple_or_default(value: Any, default: Iterable[str]) -> tuple[str, ...]:
    if not value:
        return tuple(default)
    if isinstance(value, str):
        return (value.lower(),)
    return tuple(str(item).lower() for item in value)
