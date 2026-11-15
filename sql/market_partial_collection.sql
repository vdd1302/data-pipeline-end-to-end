-- Additive operational metadata for partial collection. Raw captures and the
-- analytical warehouse contract remain immutable and are not recreated here.
CREATE SCHEMA IF NOT EXISTS market_ops;

ALTER TABLE market_ops.scope_queue
    ADD COLUMN IF NOT EXISTS listing_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (listing_status IN ('pending','complete','partial','skipped'));
UPDATE market_ops.scope_queue SET listing_status = CASE
    WHEN status='success' THEN 'complete'
    WHEN status='partial' THEN 'partial'
    WHEN status IN ('blocked','exhausted','expired') THEN 'skipped'
    ELSE listing_status END
WHERE listing_status='pending'
  AND status IN ('success','partial','blocked','exhausted','expired');

CREATE TABLE IF NOT EXISTS market_ops.job_detail_queue (
    work_date DATE NOT NULL,
    source_job_id TEXT NOT NULL CHECK (source_job_id ~ '^[0-9]+$'),
    source_code TEXT NOT NULL DEFAULT 'topcv' CHECK (source_code='topcv'),
    job_url TEXT NOT NULL,
    listing_payload JSONB NOT NULL CHECK (jsonb_typeof(listing_payload)='object'),
    original_discovered_at TIMESTAMPTZ NOT NULL,
    cycle_started_on DATE NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','retry','success','skipped')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count>=0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count>=0),
    last_failure_attempt INTEGER NOT NULL DEFAULT 0 CHECK (last_failure_attempt>=0),
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    last_error TEXT NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,
    observation_raw_job_id BIGINT,
    observation_raw_run_id UUID,
    source_captured_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (work_date,source_job_id)
);
CREATE INDEX IF NOT EXISTS job_detail_queue_due
    ON market_ops.job_detail_queue(work_date,status,next_retry_at,last_attempt_at);

-- A single live capture may satisfy several verified scope discoveries. The
-- original discovery is retained; a reference is never a new listing capture.
CREATE TABLE IF NOT EXISTS market_ops.job_scope_links (
    work_date DATE NOT NULL,
    source_job_id TEXT NOT NULL,
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    job_url TEXT NOT NULL,
    listing_payload JSONB NOT NULL CHECK (jsonb_typeof(listing_payload)='object'),
    original_discovered_at TIMESTAMPTZ NOT NULL,
    source_discovery_id BIGINT,
    source_discovery_run_id UUID,
    discovery_page_number INTEGER,
    current_raw_run_id UUID,
    current_raw_task_id BIGINT,
    carried_forward BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (work_date,source_job_id,position_id,location_id),
    FOREIGN KEY (work_date,source_job_id)
        REFERENCES market_ops.job_detail_queue(work_date,source_job_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS job_scope_links_scope
    ON market_ops.job_scope_links(work_date,position_id,location_id);

CREATE TABLE IF NOT EXISTS market_ops.listing_card_rejections (
    rejection_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    work_date DATE NOT NULL,
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    page_number INTEGER NOT NULL CHECK (page_number>0),
    source_job_id TEXT,
    reason_code TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS market_ops.collection_batch_log (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL,
    details_committed INTEGER NOT NULL DEFAULT 0,
    listings_committed INTEGER NOT NULL DEFAULT 0,
    jobs_skipped INTEGER NOT NULL DEFAULT 0,
    report JSONB NOT NULL CHECK (jsonb_typeof(report)='object')
);
CREATE INDEX IF NOT EXISTS collection_batch_log_day
    ON market_ops.collection_batch_log(snapshot_date,batch_id DESC);
