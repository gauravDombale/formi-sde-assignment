# Post-Call Processing Pipeline - Design Document

**Author:** Gaurav Dombale  
**Date:** 2026-05-06

---

## 1. Assumptions

1. The call-end webhook must acknowledge quickly. Exotel-style webhooks should not wait for LLM analysis, recording upload, CRM push, or downstream workflow execution.
2. The transcript is available when the interaction-end webhook is received. The recording is not guaranteed to be available at that time and can arrive minutes later.
3. Not every call has the same business value. A confirmed booking, rebooking, demo, callback with a concrete time, or escalation needs faster processing than a clear “not interested” call.
4. Short calls with fewer than 4 turns are not useful enough to spend LLM quota on. They should still be recorded and auditable, but they should skip full analysis.
5. The platform uses a shared LLM provider account, but customers may have reserved budgets or priority entitlements.
6. Postgres is the durable system of record. Redis and Celery can be useful accelerators, but losing either must not lose work.
7. This assessment must run locally without real LLM, CRM, S3/KMS, or observability credentials, so external integrations are represented by production-shaped adapters and mockable interfaces.

---

## 2. Problem Diagnosis

The main issue is not Celery itself. The main issue is that the current system sends LLM requests without admission control.

At campaign scale, 100K completed calls can create thousands of post-call analysis requests per minute. If the provider limit is 500 requests/minute and 90K tokens/minute, the current implementation will exceed both limits, receive 429s, retry blindly, grow the backlog, and put more pressure on Redis. The retry system then amplifies the original problem instead of absorbing it.

There are three other important failures:

1. The recording path sleeps for 45 seconds, tries once, and silently gives up if the recording is not ready.
2. Redis-backed task and retry queues are not durable enough for “no result may be permanently lost.”
3. The circuit breaker freezes dialling for a fixed 30 minutes. That protects the LLM badly and hurts the business heavily.

The fix needs to turn uncontrolled work into scheduled, durable, auditable workflow execution.

---

## 3. Architecture Overview

```
POST /session/{sid}/interaction/{iid}/end
        |
        v
Mark interaction ENDED
Write audit event
Classify processing lane: skip / hot / cold
Protect sensitive payload when encryption key is configured
        |
        +--> postcall_tasks: llm_analysis
        +--> postcall_tasks: recording_upload
        |
        v
Durable workers claim rows with FOR UPDATE SKIP LOCKED
        |
        +--> Recording worker
        |       poll Exotel with backoff
        |       upload to S3
        |       log success/failure
        |
        +--> LLM worker
                reserve RPM/TPM budget in llm_usage_ledger
                call LLM only after reservation
                commit actual token usage
                update interaction metadata
                enqueue signal, lead-stage, and CRM tasks
```

### Key design decisions

1. **Postgres-backed workflow state:** work is first recorded in `postcall_tasks`, then workers claim it. Celery is a wake-up mechanism, not the source of truth.
2. **Admission control before provider calls:** no LLM call is made unless the request and token budget have been reserved.
3. **Recording and analysis are independent:** the LLM reads the transcript, not the audio. Recording delay should not block analysis.
4. **Business-aware prioritisation:** hot tasks are claimed before cold tasks, and customer budgets protect fairness.
5. **Every important transition is auditable:** interaction, customer, campaign, task, attempt, error, and token data are logged consistently.

---

## 4. Rate Limit Management

### How usage is tracked

The production path uses `PostgresTokenBudgetManager`. Before calling the LLM, it:

1. Takes a Postgres advisory transaction lock for the provider/model/minute window.
2. Reads current `llm_usage_ledger` reservations and completed usage for that minute.
3. Checks global request/minute and token/minute limits.
4. Protects unused reserved budget for other customers.
5. Inserts a `reserved` row into `llm_usage_ledger`.

After the LLM responds, the exact provider-reported token count is committed back to the same ledger row.

### How the system decides now vs. later

Calls are classified into lanes:

- `skip`: short calls, no LLM.
- `hot`: bookings, rebookings, demos, scheduled callbacks, escalations, complaints, or customer-configured hot phrases.
- `cold`: low urgency or ambiguous outcomes.

Hot tasks are claimed first. Cold tasks run when there is budget headroom and cannot consume another customer’s protected reserved capacity.

### What happens when the limit is hit

The system does not send the LLM request. The task remains durable and is rescheduled for the next budget window. This turns provider 429s into controlled queueing.

The old binary circuit breaker is replaced with proportional dialler backpressure:

- under 70% utilization: no delay
- 70-85%: light delay
- 85-100%: heavier delay
- exhausted: short refusal/delay, not a 30-minute freeze

---

## 5. Per-Customer Token Budgeting

The platform has total capacity `N` tokens/minute and `M` requests/minute. Each customer can have a reserved budget:

```text
customer_llm_budgets.reserved_tokens_per_minute
customer_llm_budgets.reserved_requests_per_minute
```

The scheduler protects unused reservation for other customers before allowing one customer to use shared headroom.

Example: total capacity is 100 tokens/minute. Customer A reserves 20 and Customer B reserves 80.

- Customer A is guaranteed 20 tokens/minute.
- Customer B is guaranteed 80 tokens/minute.
- If A spends its 20, A cannot consume B’s unused 80 unless the platform explicitly allows opportunistic burst beyond protected reservation.
- If A is exhausted, A’s tasks are deferred.
- B continues processing because A cannot starve B.

This is important operationally because one customer’s campaign burst should not break another customer’s SLA.

---

## 6. Differentiated Processing

I used deterministic triage first, with customer-configurable policy:

- `CustomerProcessingPolicy.hot_phrases`
- `CustomerProcessingPolicy.cold_phrases`
- `CustomerProcessingPolicy.short_transcript_turns`
- CRM and SLA-related customer settings

This is intentionally conservative. It is cheap, explainable, and does not spend another LLM call just to decide whether to spend the main LLM call. For ambiguous multilingual cases, the next step would be a small classifier behind the same policy interface.

The important design point is that differentiated processing is not hardcoded forever. Business rules can change without changing the core scheduler.

---

## 7. Recording Pipeline

The fixed `asyncio.sleep(45)` is replaced by `poll_and_upload_recording`.

The new flow:

1. Poll the recording provider.
2. If not ready, retry with exponential backoff and jitter.
3. Upload to S3 when available.
4. Emit structured events on every attempt.
5. Emit a terminal `recording_upload_failed` event if retries are exhausted.

For an on-call engineer, an interaction now has a visible trail:

```text
recording_poll_attempt
recording_not_ready
recording_uploaded
recording_upload_failed
```

Recording failures are no longer silent and can be alerted or replayed.

---

## 8. Reliability & Durability

Durable execution is handled through `postcall_tasks`.

Workers claim due work with:

```sql
FOR UPDATE SKIP LOCKED
```

When a worker claims a task, it sets:

- `status = running`
- `locked_by`
- `locked_until`
- `attempts = attempts + 1`

If the worker dies, the lock expires and another worker can reclaim the task. If a task repeatedly fails, it is moved to `dead_lettered` with the payload and last error retained.

This gives the system the properties the current Redis retry queue lacks:

- no silent drops
- replayable payloads
- visible attempts
- dead-letter inspection
- safe concurrent workers

Celery can still run `drain_due_postcall_tasks`, but Postgres is the workflow ledger.

---

## 9. Auditability & Observability

### What is logged

Every important event includes:

- `interaction_id`
- `customer_id`
- `campaign_id`
- `session_id` where available
- task id and task type where relevant
- attempt count
- lane
- reservation id
- estimated and actual tokens
- error and retry timing when relevant

`AuditLogger` emits structured logs. `PostgresAuditEventWriter` persists the same shape into `postcall_audit_events`, so an engineer can debug a failed interaction days later without reconstructing state from stdout.

### Alert conditions

`PostCallAlertEvaluator` implements alert thresholds for:

- LLM utilization above 85%
- hot-lane p95 wait above SLA
- any dead-lettered post-call task
- recording failure rate above threshold
- customer budget exhaustion sustained for more than 10 minutes

In production this evaluator would feed Prometheus/Grafana or the paging system.

---

## 10. Data Model

Implemented in `data/schema.sql`.

Important additions:

```sql
postcall_tasks(
    id,
    interaction_id,
    customer_id,
    campaign_id,
    task_type,
    lane,
    status,
    payload,
    attempts,
    max_attempts,
    next_run_at,
    locked_by,
    locked_until,
    last_error
);

customer_llm_budgets(
    customer_id,
    reserved_tokens_per_minute,
    reserved_requests_per_minute,
    priority_weight
);

llm_usage_ledger(
    interaction_id,
    customer_id,
    campaign_id,
    provider,
    model,
    reservation_id,
    estimated_tokens,
    actual_tokens,
    request_count,
    status,
    window_start
);

postcall_audit_events(
    interaction_id,
    customer_id,
    campaign_id,
    session_id,
    event_name,
    severity,
    event_data,
    occurred_at
);

recording_jobs(
    interaction_id,
    call_sid,
    customer_id,
    campaign_id,
    status,
    attempts,
    next_poll_at,
    last_error,
    s3_key
);

customer_processing_configs(
    customer_id,
    hot_phrases,
    cold_phrases,
    short_transcript_turns,
    crm_enabled,
    crm_endpoint,
    encryption_required
);

crm_delivery_status(
    interaction_id,
    customer_id,
    campaign_id,
    endpoint_url,
    status,
    attempts,
    next_retry_at,
    last_error
);
```

---

## 11. Security

Sensitive data:

- transcripts
- lead names, phone numbers, and emails
- extracted entities
- recording URLs and S3 keys
- CRM payloads
- provider call identifiers

Protections:

- TLS for API, database, Redis, CRM, provider, and storage traffic.
- S3 SSE-KMS for recordings in production.
- Optional AES-256-GCM application-layer encryption through `SensitiveDataProtector`.
- Tenant scoping by `customer_id`.
- Redacted logs: audit events contain identifiers and status, not full transcript text.
- Token ledger stores usage metadata, not prompt contents.
- Secrets should be provided through managed secret storage, not committed env files.

---

## 12. API Interface

I kept the external webhook contract unchanged:

```text
POST /session/{session_id}/interaction/{interaction_id}/end
```

I kept it stable because telephony provider contracts are expensive to change and the endpoint already contains enough identifiers to create durable work. The internal behavior changed: the endpoint now records durable tasks and returns quickly instead of depending on fragile fire-and-forget execution.

---

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What I Chose Instead |
|--------|----------------|--------------------------------------|
| Keep Celery + Redis as source of truth | Smallest change | Redis loss can lose broker state and retry state. I kept Celery only as a worker trigger and moved truth to Postgres. |
| Use Temporal | Strong workflow engine | Good long-term option, but heavy for this assignment and not already in the stack. Postgres task claiming gives most needed guarantees here. |
| Kafka/SQS-only queue | Scales well for events | Still needs idempotency, budgets, audit state, and dead-letter visibility. Postgres is simpler and queryable for this workflow. |
| Retry after provider 429 | Easy to implement | Too late. The system must avoid sending requests beyond provider limits. |
| LLM classifier before full analysis | Better triage for ambiguity | It still consumes quota. I started with deterministic policy and left room to plug in a classifier later. |
| Binary dialler freeze | Existing behavior | Too blunt. Proportional backpressure is less disruptive and more truthful. |

---

## 14. Known Weaknesses

The implementation is production-shaped and locally testable, but some external integrations are intentionally generic because this repo must run without real production credentials.

Current gaps I would call out honestly:

1. The CRM integration is a generic webhook adapter, not Salesforce/HubSpot-specific.
2. The alert evaluator is implemented, but dashboards and paging routes are not deployed.
3. S3/KMS behavior is represented through the storage/encryption interfaces, not a real AWS account.
4. Unit tests cover the durable SQL paths; I would still add an integration test that kills a worker after claim and verifies stale-lock recovery against a real Postgres instance.
5. Phrase-based triage is explainable and configurable, but a trained multilingual classifier would improve ambiguous Hinglish cases.

These are integration and operational rollout gaps, not core architecture gaps.

---

## 15. What I Would Do With More Time

1. Add transcript-size-based token estimation instead of a flat per-call average.
2. Add Grafana dashboards for queue depth, hot-lane SLA, token burn, dead letters, and recording failures.
3. Add a kill-worker integration test against real Postgres and Redis.
4. Add real CRM adapters for Salesforce, HubSpot, and customer webhooks.
5. Add a multilingual triage classifier behind the existing `CustomerProcessingPolicy` interface.
