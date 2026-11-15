-- Persistent daily batches. This migration never truncates legacy queues or Raw.
CREATE SCHEMA IF NOT EXISTS market_ops;
CREATE TABLE IF NOT EXISTS market_ops.crawl_batches (
    batch_id UUID PRIMARY KEY,
    batch_date DATE NOT NULL UNIQUE,
    expected_scopes INTEGER NOT NULL CHECK(expected_scopes>0),
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN','COMPLETE','PARTIAL','SOURCE_BLOCKED')),
    config_snapshot JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS market_ops.crawl_scope_queue (
    batch_id UUID NOT NULL REFERENCES market_ops.crawl_batches(batch_id),
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    keyword TEXT NOT NULL,
    location_label TEXT NOT NULL,
    scope_order INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN
       ('pending','listing_in_progress','detail_in_progress','partial_ready','retry_pending',
        'complete','exhausted','needs_review','blocked_by_host')),
    work_remaining BOOLEAN NOT NULL DEFAULT true,
    listing_complete BOOLEAN NOT NULL DEFAULT false,
    listing_cursor INTEGER NOT NULL DEFAULT 0 CHECK(listing_cursor>=0),
    detail_cursor TEXT,
    listing_pages_committed INTEGER NOT NULL DEFAULT 0,
    details_committed INTEGER NOT NULL DEFAULT 0,
    eligible_listing_jobs INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK(failure_count>=0),
    error_counts JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_error_class TEXT NOT NULL DEFAULT '',
    next_eligible_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    first_attempt_at TIMESTAMPTZ,
    last_served_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    slices_served INTEGER NOT NULL DEFAULT 0,
    lease_owner UUID,
    lease_expires_at TIMESTAMPTZ,
    current_raw_run_id UUID,
    current_raw_task_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(batch_id,position_id,location_id)
);
CREATE INDEX IF NOT EXISTS crawl_scope_queue_due ON market_ops.crawl_scope_queue
    (batch_id,work_remaining,next_eligible_at,last_served_at);
CREATE TABLE IF NOT EXISTS market_ops.crawl_listing_pages (
    batch_id UUID NOT NULL,
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    source_page_number INTEGER NOT NULL CHECK(source_page_number>0),
    raw_run_id UUID NOT NULL,
    raw_task_id BIGINT NOT NULL,
    raw_page_id BIGINT NOT NULL,
    raw_page_number INTEGER NOT NULL,
    page_url TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY(batch_id,position_id,location_id,source_page_number),
    FOREIGN KEY(batch_id,position_id,location_id)
        REFERENCES market_ops.crawl_scope_queue(batch_id,position_id,location_id)
);
CREATE TABLE IF NOT EXISTS market_ops.crawl_job_queue (
    batch_id UUID NOT NULL REFERENCES market_ops.crawl_batches(batch_id),
    source_job_id TEXT NOT NULL CHECK(source_job_id ~ '^[0-9]+$'),
    status TEXT NOT NULL DEFAULT 'detail_pending' CHECK(status IN
       ('detail_pending','detail_retry','detail_ok','detail_partial','not_found','quarantined','skipped_with_reason')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    recoverable_failures INTEGER NOT NULL DEFAULT 0,
    last_error_class TEXT NOT NULL DEFAULT '',
    next_eligible_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    first_seen TIMESTAMPTZ NOT NULL,
    last_attempt TIMESTAMPTZ,
    raw_observation_id BIGINT,
    observation_raw_run_id UUID,
    capture_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(batch_id,source_job_id)
);
CREATE INDEX IF NOT EXISTS crawl_job_queue_due ON market_ops.crawl_job_queue(batch_id,status,next_eligible_at);
CREATE TABLE IF NOT EXISTS market_ops.crawl_job_scopes (
    batch_id UUID NOT NULL,
    source_job_id TEXT NOT NULL,
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    job_url TEXT NOT NULL,
    listing_payload JSONB NOT NULL,
    original_discovered_at TIMESTAMPTZ NOT NULL,
    source_discovery_id BIGINT NOT NULL,
    source_discovery_run_id UUID NOT NULL,
    source_page_number INTEGER NOT NULL,
    PRIMARY KEY(batch_id,source_job_id,position_id,location_id),
    FOREIGN KEY(batch_id,source_job_id) REFERENCES market_ops.crawl_job_queue(batch_id,source_job_id),
    FOREIGN KEY(batch_id,position_id,location_id)
        REFERENCES market_ops.crawl_scope_queue(batch_id,position_id,location_id)
);
CREATE TABLE IF NOT EXISTS market_ops.host_access (
    host_key TEXT PRIMARY KEY,
    host_state TEXT NOT NULL DEFAULT 'AVAILABLE' CHECK(host_state IN
       ('AVAILABLE','RATE_LIMITED','ACCESS_DENIED','MANUAL_HOLD')),
    next_allowed_at TIMESTAMPTZ,
    denial_reason TEXT NOT NULL DEFAULT '',
    operator_review_required BOOLEAN NOT NULL DEFAULT false,
    signal_count INTEGER NOT NULL DEFAULT 0,
    safe_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_at TIMESTAMPTZ,
    review_reference TEXT
);
CREATE TABLE IF NOT EXISTS market_ops.host_access_reviews (
    review_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    host_key TEXT NOT NULL,
    previous_state TEXT NOT NULL,
    review_reference TEXT NOT NULL,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS market_ops.crawl_scope_errors (
    error_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_id UUID NOT NULL,
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    source_job_id TEXT,
    error_class TEXT NOT NULL,
    safe_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- A prior challenge is a review hold, not permission to retry after a timer.
INSERT INTO market_ops.host_access(host_key,host_state,next_allowed_at,denial_reason,
                                  operator_review_required,signal_count)
SELECT host,CASE WHEN reason='rate_limited' THEN 'RATE_LIMITED' ELSE 'ACCESS_DENIED' END,
       CASE WHEN reason='rate_limited' THEN next_retry_at ELSE NULL END,
       reason,reason<>'rate_limited',consecutive_signals
FROM market_ops.host_backoff
ON CONFLICT(host_key) DO NOTHING;
INSERT INTO market_ops.host_access(host_key) VALUES('www.topcv.vn') ON CONFLICT DO NOTHING;
