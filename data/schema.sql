-- VoiceBot Post-Call Processing — Database Schema
-- This schema represents the CURRENT state of the system.
-- Candidates should propose schema changes as part of their solution.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE leads (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    name VARCHAR(255),
    phone VARCHAR(50),
    email VARCHAR(255),
    stage VARCHAR(100) DEFAULT 'new',
    lead_data JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_leads_campaign ON leads(campaign_id);
CREATE INDEX idx_leads_customer ON leads(customer_id);

CREATE TABLE sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_sessions_lead ON sessions(lead_id);
CREATE INDEX idx_sessions_campaign ON sessions(campaign_id);

CREATE TABLE interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID NOT NULL REFERENCES sessions(id),
    lead_id UUID NOT NULL REFERENCES leads(id),
    campaign_id UUID NOT NULL,
    customer_id UUID NOT NULL,
    agent_id UUID NOT NULL,

    status VARCHAR(20) DEFAULT 'INITIATED',
    call_sid VARCHAR(255),
    call_provider VARCHAR(50) DEFAULT 'exotel',

    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    duration_seconds INTEGER,

    -- Transcript stored here: conversation_data->'transcript' is a JSON array
    -- of {"role": "agent"|"customer", "content": "..."}
    conversation_data JSONB DEFAULT '{}',

    -- Hot cache for dashboard. Contains extracted entities, analysis status,
    -- call_stage, and other dashboard-facing fields.
    -- Structure: {"entities": {...}, "call_stage": "...", "analysis_status": "..."}
    interaction_metadata JSONB DEFAULT '{}',

    recording_url TEXT,
    recording_s3_key VARCHAR(512),

    -- Current Celery task tracking (no workflow visibility)
    postcall_celery_task_id VARCHAR(255),

    retry_count INTEGER DEFAULT 0,
    error_log JSONB DEFAULT '[]',

    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_interactions_session ON interactions(session_id);
CREATE INDEX idx_interactions_lead ON interactions(lead_id);
CREATE INDEX idx_interactions_campaign ON interactions(campaign_id);
CREATE INDEX idx_interactions_customer ON interactions(customer_id);
CREATE INDEX idx_interactions_call_sid ON interactions(call_sid);
CREATE INDEX idx_interactions_status ON interactions(status);

-- Durable post-call workflow state. Workers claim rows with SELECT ... FOR
-- UPDATE SKIP LOCKED, so a Redis/Celery restart cannot permanently lose work.
CREATE TABLE postcall_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    task_type VARCHAR(50) NOT NULL,
    lane VARCHAR(20) NOT NULL DEFAULT 'cold',
    status VARCHAR(30) NOT NULL DEFAULT 'queued',
    payload JSONB NOT NULL DEFAULT '{}',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 10,
    next_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_by VARCHAR(255),
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_postcall_tasks_unique_live
    ON postcall_tasks(interaction_id, task_type)
    WHERE status NOT IN ('completed', 'dead_lettered');
CREATE INDEX idx_postcall_tasks_ready
    ON postcall_tasks(status, next_run_at, lane);
CREATE INDEX idx_postcall_tasks_interaction
    ON postcall_tasks(interaction_id);

CREATE TABLE customer_llm_budgets (
    customer_id UUID PRIMARY KEY,
    reserved_tokens_per_minute INTEGER NOT NULL DEFAULT 0,
    reserved_requests_per_minute INTEGER NOT NULL DEFAULT 0,
    priority_weight INTEGER NOT NULL DEFAULT 1,
    hot_lane_sla_seconds INTEGER NOT NULL DEFAULT 120,
    config JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE llm_usage_ledger (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    provider VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL,
    reservation_id UUID,
    estimated_tokens INTEGER NOT NULL,
    actual_tokens INTEGER NOT NULL DEFAULT 0,
    request_count INTEGER NOT NULL DEFAULT 1,
    status VARCHAR(30) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_llm_usage_customer_window
    ON llm_usage_ledger(customer_id, window_start);
CREATE INDEX idx_llm_usage_interaction
    ON llm_usage_ledger(interaction_id);

CREATE TABLE postcall_audit_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    interaction_id UUID NOT NULL REFERENCES interactions(id),
    customer_id UUID,
    campaign_id UUID,
    session_id UUID,
    event_name VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL DEFAULT 'info',
    event_data JSONB NOT NULL DEFAULT '{}',
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_postcall_audit_interaction
    ON postcall_audit_events(interaction_id, occurred_at);
CREATE INDEX idx_postcall_audit_event
    ON postcall_audit_events(event_name, occurred_at);

CREATE TABLE recording_jobs (
    interaction_id UUID PRIMARY KEY REFERENCES interactions(id),
    call_sid VARCHAR(255) NOT NULL,
    customer_id UUID NOT NULL,
    campaign_id UUID NOT NULL,
    status VARCHAR(30) NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_poll_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    recording_url TEXT,
    s3_key VARCHAR(512),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_recording_jobs_ready
    ON recording_jobs(status, next_poll_at);

-- Seed data: sample interactions for testing
-- (Uses fixed UUIDs for reproducibility)

INSERT INTO leads (id, campaign_id, customer_id, name, phone, stage) VALUES
    ('a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Rahul Sharma', '+919876543210', 'contacted'),
    ('a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Priya Gupta', '+919876543211', 'new'),
    ('a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'Amit Verma', '+919876543212', 'contacted'),
    ('a0000000-0000-0000-0000-000000000004', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Neha Patel', '+919876543213', 'new'),
    ('a0000000-0000-0000-0000-000000000005', 'c0000000-0000-0000-0000-000000000002', 'd0000000-0000-0000-0000-000000000002', 'Rajesh Kumar', '+919876543214', 'contacted');

INSERT INTO sessions (id, lead_id, campaign_id, customer_id, agent_id, status) VALUES
    ('b0000000-0000-0000-0000-000000000001', 'a0000000-0000-0000-0000-000000000001', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000002', 'a0000000-0000-0000-0000-000000000002', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED'),
    ('b0000000-0000-0000-0000-000000000003', 'a0000000-0000-0000-0000-000000000003', 'c0000000-0000-0000-0000-000000000001', 'd0000000-0000-0000-0000-000000000001', 'e0000000-0000-0000-0000-000000000001', 'COMPLETED');

INSERT INTO interactions (id, session_id, lead_id, campaign_id, customer_id, agent_id, status, call_sid, duration_seconds, started_at, ended_at, conversation_data, interaction_metadata) VALUES
    (
        'f0000000-0000-0000-0000-000000000001',
        'b0000000-0000-0000-0000-000000000001',
        'a0000000-0000-0000-0000-000000000001',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-001',
        180,
        NOW() - INTERVAL '10 minutes',
        NOW() - INTERVAL '7 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Mr. Sharma?"}, {"role": "customer", "content": "Haan ji"}, {"role": "agent", "content": "I am calling from Cashify regarding your phone evaluation. Can we reschedule?"}, {"role": "customer", "content": "Tomorrow 3:30 PM works"}, {"role": "agent", "content": "Confirmed, our executive will visit tomorrow at 3:30 PM"}, {"role": "customer", "content": "Okay, confirmed. Bye."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000002',
        'b0000000-0000-0000-0000-000000000002',
        'a0000000-0000-0000-0000-000000000002',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-002',
        45,
        NOW() - INTERVAL '15 minutes',
        NOW() - INTERVAL '14 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello, am I speaking with Ms. Gupta?"}, {"role": "customer", "content": "Not interested, dont call again"}, {"role": "agent", "content": "Sorry for the inconvenience. Have a good day."}]}',
        '{"analysis_status": "pending"}'
    ),
    (
        'f0000000-0000-0000-0000-000000000003',
        'b0000000-0000-0000-0000-000000000003',
        'a0000000-0000-0000-0000-000000000003',
        'c0000000-0000-0000-0000-000000000001',
        'd0000000-0000-0000-0000-000000000001',
        'e0000000-0000-0000-0000-000000000001',
        'ENDED',
        'exotel-call-003',
        15,
        NOW() - INTERVAL '20 minutes',
        NOW() - INTERVAL '19 minutes',
        '{"transcript": [{"role": "agent", "content": "Hello—"}, {"role": "customer", "content": "Wrong number"}]}',
        '{"analysis_status": "pending"}'
    );
