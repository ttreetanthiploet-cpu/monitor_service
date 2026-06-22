-- ============================================================
-- n8n Monitor Database Schema
-- Project: n8n-monitor (Supabase)
-- Run this in: Supabase Dashboard → SQL Editor → New Query
-- ============================================================

-- ============================================================
-- TABLE 1: execution_log
-- One row per full conversation turn (one webhook call)
-- ============================================================
CREATE TABLE IF NOT EXISTS execution_log (
    id                          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id                TEXT NOT NULL UNIQUE,   -- n8n execution ID
    workflow_id                 TEXT,                   -- n8n workflow ID
    session_id                  TEXT,                   -- user session (groups turns)
    customer_id                 TEXT,                   -- customer identifier

    -- Timing
    started_at                  TIMESTAMPTZ,
    finished_at                 TIMESTAMPTZ,
    wall_time_ms                INTEGER,               -- total end-to-end latency

    -- Input
    user_message                TEXT,
    message_type                TEXT,                  -- text / button / confirmation

    -- Output
    ai_reply                    TEXT,
    reply_type                  TEXT,                  -- TEXT / OFFER / CONFIRM / REDIRECT

    -- Routing & classification
    route_to                    TEXT,                  -- advisor / education / summary / unknown
    narrative                   TEXT,                  -- running narrative from classification agent

    -- Guardrail flags
    input_guardrail_triggered   BOOLEAN DEFAULT FALSE,
    output_guardrail_triggered  BOOLEAN DEFAULT FALSE,
    output_guardrail_nsfw       BOOLEAN DEFAULT FALSE,
    output_guardrail_hallucination BOOLEAN DEFAULT FALSE,

    -- Escalation
    need_staff_contact          BOOLEAN DEFAULT FALSE,

    -- Status
    status                      TEXT DEFAULT 'success', -- success / guardrail_blocked / error
    error_message               TEXT,

    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_execution_log_session_id    ON execution_log(session_id);
CREATE INDEX IF NOT EXISTS idx_execution_log_customer_id   ON execution_log(customer_id);
CREATE INDEX IF NOT EXISTS idx_execution_log_started_at    ON execution_log(started_at);
CREATE INDEX IF NOT EXISTS idx_execution_log_route_to      ON execution_log(route_to);
CREATE INDEX IF NOT EXISTS idx_execution_log_status        ON execution_log(status);


-- ============================================================
-- TABLE 2: agent_call_log
-- One row per LLM / AI agent invocation across all sub-workflows
-- ============================================================
CREATE TABLE IF NOT EXISTS agent_call_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id        TEXT NOT NULL REFERENCES execution_log(execution_id) ON DELETE CASCADE,

    -- Identity
    agent_name          TEXT,   -- e.g. Classification Agent, Debt Solution Extractor
    workflow_name       TEXT,   -- e.g. Prototype_v1.2, AdvisorWorkFlow_v1.3
    model_name          TEXT,   -- e.g. gemini-2.5-pro, gemini-3.1-flash-lite

    -- Timing
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    processing_time_ms  INTEGER,

    -- Content (truncated to 2000 chars to save space)
    input_prompt        TEXT,
    output_text         TEXT,

    -- Token usage
    input_tokens        INTEGER DEFAULT 0,
    output_tokens       INTEGER DEFAULT 0,
    total_tokens        INTEGER DEFAULT 0,

    -- Cost
    input_cost_usd      NUMERIC(10, 6) DEFAULT 0,
    output_cost_usd     NUMERIC(10, 6) DEFAULT 0,
    total_cost_usd      NUMERIC(10, 6) DEFAULT 0,
    total_cost_thb      NUMERIC(10, 4) DEFAULT 0,

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_agent_call_log_execution_id  ON agent_call_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_agent_call_log_agent_name    ON agent_call_log(agent_name);
CREATE INDEX IF NOT EXISTS idx_agent_call_log_workflow_name ON agent_call_log(workflow_name);
CREATE INDEX IF NOT EXISTS idx_agent_call_log_started_at    ON agent_call_log(started_at);


-- ============================================================
-- TABLE 3: http_request_log
-- One row per external HTTP call (Offer Engine, Supabase Storage, etc.)
-- ============================================================
CREATE TABLE IF NOT EXISTS http_request_log (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id        TEXT NOT NULL REFERENCES execution_log(execution_id) ON DELETE CASCADE,

    -- Identity
    node_name           TEXT,   -- e.g. HTTP Request, Upload to Supabase Storage1
    workflow_name       TEXT,

    -- Request
    method              TEXT,   -- GET / POST
    url                 TEXT,
    request_body        JSONB,

    -- Response
    response_status     INTEGER,
    response_body       JSONB,

    -- Timing
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    processing_time_ms  INTEGER,

    -- Status
    success             BOOLEAN DEFAULT TRUE,
    error_message       TEXT,

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_http_request_log_execution_id ON http_request_log(execution_id);
CREATE INDEX IF NOT EXISTS idx_http_request_log_node_name    ON http_request_log(node_name);
CREATE INDEX IF NOT EXISTS idx_http_request_log_success      ON http_request_log(success);


-- ============================================================
-- TABLE 4: workflow_agent_flags
-- One row per execution — flat boolean flags for dashboarding
-- ============================================================
CREATE TABLE IF NOT EXISTS workflow_agent_flags (
    execution_id                TEXT PRIMARY KEY REFERENCES execution_log(execution_id) ON DELETE CASCADE,

    -- Sub-workflow flags
    used_input_guardrail        BOOLEAN DEFAULT FALSE,
    used_classification         BOOLEAN DEFAULT FALSE,
    used_advisor                BOOLEAN DEFAULT FALSE,
    used_education              BOOLEAN DEFAULT FALSE,
    used_summary                BOOLEAN DEFAULT FALSE,
    used_output_guardrail       BOOLEAN DEFAULT FALSE,

    -- Specific node flags
    advisor_http_call           BOOLEAN DEFAULT FALSE,  -- Offer Engine was called
    summary_storage_upload      BOOLEAN DEFAULT FALSE,  -- PDF uploaded to Supabase Storage
    education_embedding_used    BOOLEAN DEFAULT FALSE,  -- RAG retrieval ran

    created_at                  TIMESTAMPTZ DEFAULT NOW()
);


-- ============================================================
-- Confirm
-- ============================================================
SELECT 'Tables created successfully' AS result;
