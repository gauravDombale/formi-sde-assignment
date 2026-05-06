# Post-Call Processing Pipeline - Design Document

**Author:** Gaurav Dombale  
**Date:** 2026-05-06

## 1. Assumptions

1. The call-end webhook must remain fast and return within the telephony provider's timeout; all heavy work is asynchronous.
2. Transcripts are already available when `/session/{sid}/interaction/{iid}/end` is called. Recording availability is independent and may lag by minutes.
3. "Immediate" processing means outcomes where a human or automated workflow must act while the lead context is fresh: confirmed booking/rebooking, demo booked, callback with specific time, escalation/complaint, or other customer-configured hot outcomes.
4. Short transcripts under 4 turns are not worth LLM analysis and must not consume quota.
5. One LLM provider account is shared across customers, but customers may buy/reserve guaranteed token and request budgets.
6. Postgres is the durable source of truth. Redis/Celery can still be used as accelerators, but losing them must not lose work.

## 2. Problem Diagnosis

The root failure is that LLM calls are fired without admission control. A burst of 100K completed calls can produce thousands of requests per minute against hard provider limits like 500 RPM and 90K TPM. Once 429s start, Celery retries increase backlog pressure, Redis becomes a second point of loss, and the circuit breaker freezes dialling even though the real problem is unscheduled post-call analysis.

The current recording flow is also coupled incorrectly: it sleeps for 45 seconds inside the same worker before analysis starts, then silently skips recordings that arrive later.

## 3. Architecture Overview

```
POST /session/{sid}/interaction/{iid}/end
        |
        v
Update interaction ENDED + write audit event
        |
        +--> INSERT postcall_tasks: llm_analysis
        +--> INSERT recording_jobs / postcall_tasks: recording_upload
        +--> encrypt sensitive transcript payload when encryption key configured
        |
        v
Durable workers claim due rows with SKIP LOCKED
        |
        +--> Recording poller: retry/backoff until uploaded or dead-lettered
        |
        +--> LLM scheduler
              |
              +--> classify lane: skip / hot / cold
              +--> reserve global + customer budget
              +--> call LLM only after reservation
              +--> write usage ledger + analysis result + audit
              +--> enqueue signal, lead-stage, and CRM jobs durably
```

Key decisions:

1. Separate durable task creation from execution. The webhook records intent in Postgres before any worker/broker handoff.
2. Use admission control before calling the LLM. No worker calls the provider without a token/request reservation.
3. Split recording upload from LLM analysis. Recording delay must not block transcript analysis.
4. Keep Celery optional. It can wake workers, but Postgres rows are the replayable source of truth.

## 4. Rate Limit Management

The scheduler enforces both requests per minute and tokens per minute. Every LLM call starts with an estimated token reservation based on measured average usage and transcript size. The production default is `PostgresTokenBudgetManager`: it takes an advisory transaction lock per provider/model/minute window, checks global RPM/TPM and protected customer reservations, then writes a `reserved` row to `llm_usage_ledger` before the worker is allowed to call the provider. After the provider responds, the exact `usage.total_tokens` is committed back to the same ledger row.

If budget is unavailable, the task is not sent to the LLM. It is marked retryable with `next_run_at` set to the next budget window. This turns 429s into controlled queueing.

Hot-lane tasks are claimed first, then cold-lane tasks. Cold tasks are allowed to drain using unallocated headroom, but cannot consume another customer's reserved allocation.

The old binary circuit breaker is replaced by `get_backpressure`, which returns a proportional dialler delay based on utilization: no delay under 70%, light delay above 70%, heavier delay above 85%, and short refusal/delay only when capacity is exhausted. This avoids hardcoded 30-minute freezes.

## 5. Per-Customer Token Budgeting

Let total capacity be `N` tokens/minute. Each active customer may have a reserved budget `R_customer`. The scheduler protects the unused reserved budget of every other customer before allowing one customer to use shared headroom.

Example: total capacity is 100 tokens/min. Customer A reserves 20 and Customer B reserves 80. A can spend 20 immediately. A cannot spend token 21 while B has unused reservation, because that would consume B's guarantee. If B is idle and the policy allows opportunistic burst, A may use explicitly unallocated headroom, but not protected reserved capacity.

If a customer exceeds its budget, new analysis tasks for that customer are deferred. Other customers continue processing.

## 6. Differentiated Processing

The implementation uses deterministic transcript triage with per-customer overrides:

- `skip`: fewer than 4 turns, no LLM.
- `hot`: confirmed/rebooked/demo/scheduled/escalation/complaint signals.
- `cold`: not interested, already done, ambiguous follow-up, or no hot signal.

`CustomerProcessingPolicy` can override hot/cold phrases, short-transcript threshold, CRM settings, and hot-lane SLA through request/customer configuration. This keeps business urgency configurable without a deployment. A cheap classifier model can replace the phrase policy later, but the expensive full analysis still requires scheduler admission.

## 7. Recording Pipeline

The 45-second sleep is replaced by `poll_and_upload_recording`. It polls Exotel with exponential backoff and jitter, logs every attempt, and emits `recording_upload_failed` after terminal failure. Recording upload runs independently from analysis.

An on-call engineer can search audit events by `interaction_id` and see each `recording_poll_attempt`, `recording_not_ready`, `recording_uploaded`, or `recording_upload_failed` event.

## 8. Reliability & Durability

Durability comes from Postgres tables:

- `postcall_tasks` stores task type, payload, attempts, status, locks, and next retry time.
- `recording_jobs` stores recording-specific retry state.
- `llm_usage_ledger` stores reservations and actual token spend.
- `postcall_audit_events` stores traceable workflow events.

Workers use row locks with `FOR UPDATE SKIP LOCKED`. If a worker dies, `locked_until` expires and another worker can reclaim the task. Dead-lettered tasks retain payload and error for replay; they are not dropped.

The implementation includes `PostgresDurableTaskStore`, which performs idempotent enqueue by semantic task key, claims ready rows with `FOR UPDATE SKIP LOCKED`, sets `locked_by`/`locked_until` during claim, and exposes `complete`/`retry` transitions. Celery is retained only as a wake-up mechanism through `drain_due_postcall_tasks`; the replayable source of truth is Postgres.

## 9. Auditability & Observability

Every structured event includes:

- `interaction_id`
- `customer_id`
- `campaign_id`
- `session_id` when available
- event name, severity, timestamp
- task id, lane, attempt, retry time, token counts, or error where relevant

`AuditLogger` emits structured logs, and `PostgresAuditEventWriter` persists the same event shape into `postcall_audit_events` for replay/debugging days later. The code keeps logging and persistence separate so request paths can choose fail-open logging or fail-closed audit persistence depending on the criticality of the event.

Alerts:

- LLM utilization over 85% for 5 minutes.
- Hot-lane p95 wait over SLA.
- Any dead-lettered `llm_analysis` or `recording_upload` task.
- Recording failure rate over threshold.
- Customer budget exhaustion sustained for more than 10 minutes.

`PostCallAlertEvaluator` implements these thresholds as code so they can be wired to Prometheus/Grafana, a scheduled health check, or a paging integration.

## 10. Data Model

Implemented in `data/schema.sql`:

```sql
postcall_tasks(task_type, lane, status, payload, attempts, next_run_at, locked_until, last_error)
customer_llm_budgets(customer_id, reserved_tokens_per_minute, reserved_requests_per_minute, priority_weight)
llm_usage_ledger(interaction_id, customer_id, campaign_id, reservation_id, estimated_tokens, actual_tokens, status, window_start)
postcall_audit_events(interaction_id, event_name, severity, event_data, occurred_at)
recording_jobs(interaction_id, call_sid, status, attempts, next_poll_at, s3_key)
customer_processing_configs(customer_id, hot_phrases, cold_phrases, crm_enabled, crm_endpoint, encryption_required)
crm_delivery_status(interaction_id, customer_id, endpoint_url, status, attempts, next_retry_at)
```

## 11. Security

Sensitive data includes transcripts, extracted entities, lead PII, phone numbers, emails, provider call IDs, and recordings. Protections:

- TLS for provider, API, DB, Redis, and S3 traffic.
- S3 SSE-KMS for recordings, with short-lived signed URLs for playback.
- Column-level or application-layer encryption for transcript and PII fields where supported.
- `SensitiveDataProtector` provides optional AES-256-GCM application-layer encryption for sensitive JSON payloads when `POSTCALL_ENCRYPTION_KEY_B64` is configured.
- Strict tenant scoping on every query by `customer_id`.
- Redacted logs: audit events should contain identifiers and status, not full transcript text.
- Token ledger stores usage metadata, not prompt contents.

## 12. API Interface

The external webhook contract is unchanged. Internally, the endpoint now writes durable work records and lane/audit metadata. Keeping the API stable avoids telephony-provider changes and makes the fix deployable behind the current contract.

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Decision |
|--------|----------------|----------|
| Keep Celery/Redis only | Minimal code change | Rejected because Redis broker loss can lose in-flight work and retry state. |
| Use only provider Retry-After | Simple 429 recovery | Rejected because it still sends too many requests first. Admission control is required. |
| One queue per customer | Fairness | Not enough; token budgets still need a shared global scheduler. |
| Cheap classifier LLM before full analysis | Better triage | Deferred; it still consumes LLM quota and needs scheduling. Deterministic triage is safer first. |
| Binary dialler freeze | Existing protection | Replaced conceptually with proportional backlog/latency signals and budget-aware queueing. |

## 14. Known Weaknesses

The local tests still use an in-process budget manager where deterministic rate-limit simulation is clearer, but the production default budget manager is Postgres-backed and writes token reservations to `llm_usage_ledger`. The current hot/cold classifier is phrase-based and customer-configurable, but a trained classifier would handle ambiguous Hinglish and domain-specific outcomes better.

Implementation scope note: the repo now contains production-shaped, locally testable implementations for durable task claiming, recording retries, CRM webhook delivery, alert evaluation, encryption hooks, and dialler backpressure. The external integrations are intentionally generic mocks/adapters because this assessment must run locally without real CRM accounts, Grafana/Prometheus, S3/KMS, or live LLM credentials. In production I would wire these same interfaces to managed secrets, provider-specific CRM adapters, metrics exporters, and integration tests against real Postgres/Redis infrastructure.

## 15. What I Would Do With More Time

1. Add transcript-size-based token estimation instead of a flat average.
2. Add dashboards for queue depth, hot-lane SLA, token burn, and recording failures.
3. Add an integration test against real Postgres that kills a worker after claim and verifies stale-lock recovery.
4. Add real CRM provider adapters for Salesforce, HubSpot, and webhook-based customers.
5. Add a trained multilingual triage classifier behind the existing customer policy interface.
