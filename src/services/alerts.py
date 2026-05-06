from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class PostCallHealthSnapshot:
    llm_tpm_utilization: float
    llm_rpm_utilization: float
    hot_lane_p95_wait_seconds: float
    dead_lettered_tasks: int
    recording_failure_rate: float
    customer_budget_exhausted_minutes: int


@dataclass(frozen=True)
class Alert:
    name: str
    severity: str
    message: str


class PostCallAlertEvaluator:
    def __init__(
        self,
        *,
        llm_utilization_threshold: float = 0.85,
        hot_lane_sla_seconds: int = 120,
        recording_failure_rate_threshold: float = 0.02,
        budget_exhaustion_minutes_threshold: int = 10,
    ):
        self.llm_utilization_threshold = llm_utilization_threshold
        self.hot_lane_sla_seconds = hot_lane_sla_seconds
        self.recording_failure_rate_threshold = recording_failure_rate_threshold
        self.budget_exhaustion_minutes_threshold = budget_exhaustion_minutes_threshold

    def evaluate(self, snapshot: PostCallHealthSnapshot) -> List[Alert]:
        alerts: List[Alert] = []
        if max(snapshot.llm_tpm_utilization, snapshot.llm_rpm_utilization) >= self.llm_utilization_threshold:
            alerts.append(
                Alert(
                    "llm_capacity_high",
                    "warning",
                    "LLM utilization is above configured threshold",
                )
            )
        if snapshot.hot_lane_p95_wait_seconds > self.hot_lane_sla_seconds:
            alerts.append(
                Alert(
                    "hot_lane_sla_breached",
                    "critical",
                    "Hot-lane post-call analysis wait time breached SLA",
                )
            )
        if snapshot.dead_lettered_tasks > 0:
            alerts.append(
                Alert(
                    "postcall_dead_letters_present",
                    "critical",
                    "One or more post-call tasks are dead-lettered",
                )
            )
        if snapshot.recording_failure_rate > self.recording_failure_rate_threshold:
            alerts.append(
                Alert(
                    "recording_failure_rate_high",
                    "warning",
                    "Recording upload failure rate is above threshold",
                )
            )
        if snapshot.customer_budget_exhausted_minutes >= self.budget_exhaustion_minutes_threshold:
            alerts.append(
                Alert(
                    "customer_budget_exhaustion_sustained",
                    "warning",
                    "Customer LLM budget has been exhausted for too long",
                )
            )
        return alerts
