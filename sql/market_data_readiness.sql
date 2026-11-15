-- Additive ingest accountability and BI publication metadata. No analytical
-- base table, historical Raw capture, role, or credential is recreated.
CREATE SCHEMA IF NOT EXISTS market_ops;
CREATE TABLE IF NOT EXISTS market_ops.raw_ingest_accounting (
    raw_job_id BIGINT NOT NULL,
    cleaning_version TEXT NOT NULL,
    disposition TEXT NOT NULL CHECK(disposition IN ('LOADED','SUPERSEDED','QUARANTINED','EXCLUDED')),
    source_job_id TEXT NOT NULL,
    crawl_run_id UUID NOT NULL,
    posting_natural_key TEXT,
    snapshot_date DATE NOT NULL,
    record_level TEXT NOT NULL,
    content_hash TEXT,
    reason_code TEXT NOT NULL DEFAULT '',
    publication_ready BOOLEAN NOT NULL DEFAULT false,
    accounted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(raw_job_id,cleaning_version)
);
CREATE INDEX IF NOT EXISTS raw_ingest_accounting_disposition
    ON market_ops.raw_ingest_accounting(cleaning_version,disposition,raw_job_id);
CREATE TABLE IF NOT EXISTS market_ops.raw_quarantine (
    raw_job_id BIGINT NOT NULL,
    cleaning_version TEXT NOT NULL,
    error_type TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN','RESOLVED')),
    attempts INTEGER NOT NULL DEFAULT 1,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    PRIMARY KEY(raw_job_id,cleaning_version)
);
CREATE INDEX IF NOT EXISTS raw_quarantine_due
    ON market_ops.raw_quarantine(cleaning_version,status,next_retry_at);

CREATE TABLE IF NOT EXISTS market_ops.warehouse_listing_observations (
    discovery_id BIGINT PRIMARY KEY,
    batch_id UUID,
    raw_page_id BIGINT NOT NULL,
    raw_run_id UUID NOT NULL,
    source_job_id TEXT NOT NULL,
    canonical_job_url TEXT NOT NULL,
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    listing_captured_at TIMESTAMPTZ NOT NULL,
    original_discovered_at TIMESTAMPTZ NOT NULL,
    source_page_number INTEGER NOT NULL,
    job_title TEXT NOT NULL,
    normalized_position TEXT,
    company_name TEXT,
    city TEXT,
    salary_label TEXT,
    record_level TEXT NOT NULL DEFAULT 'listing_only' CHECK(record_level='listing_only'),
    content_hash TEXT NOT NULL,
    cleaning_version TEXT NOT NULL,
    publication_ready BOOLEAN NOT NULL DEFAULT false,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS warehouse_listings_by_batch
    ON market_ops.warehouse_listing_observations(batch_id,snapshot_date,source_job_id);
CREATE TABLE IF NOT EXISTS market_ops.listing_ingest_rejections (
    discovery_id BIGINT NOT NULL,
    cleaning_version TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(discovery_id,cleaning_version)
);
CREATE TABLE IF NOT EXISTS market_ops.publication_state (
    id INTEGER PRIMARY KEY CHECK(id=1),
    cleaning_version TEXT NOT NULL,
    last_successful_refresh TIMESTAMPTZ NOT NULL,
    released_raw_watermark BIGINT NOT NULL DEFAULT 0,
    quality_status TEXT NOT NULL CHECK(quality_status='PASS')
);
CREATE TABLE IF NOT EXISTS market_ops.pipeline_runs (
    pipeline_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dag_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    batch_id UUID,
    business_status TEXT NOT NULL CHECK(business_status IN
      ('SUCCESS_FULL','SUCCESS_PARTIAL','NO_NEW_DATA','SOURCE_BLOCKED','FAILED_DQ','FAILED_INFRA')),
    details JSONB NOT NULL CHECK(jsonb_typeof(details)='object'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(dag_id,run_id,stage)
);

CREATE OR REPLACE VIEW market_dw.observed_listings AS
SELECT DISTINCT ON (l.source_job_id,l.position_id,l.location_id,l.snapshot_date)
    l.discovery_id,l.batch_id,l.source_job_id,l.canonical_job_url,l.position_id,
    l.location_id,l.snapshot_date,l.listing_captured_at,l.original_discovered_at,
    l.source_page_number,l.job_title,l.normalized_position,l.company_name,l.city,
    l.salary_label,l.record_level,
    NOT EXISTS(SELECT 1 FROM market_dw.fct_job_snapshot f
        WHERE f.source_job_id=l.source_job_id AND f.snapshot_date=l.snapshot_date) AS listing_only
FROM market_ops.warehouse_listing_observations l
WHERE l.publication_ready
ORDER BY l.source_job_id,l.position_id,l.location_id,l.snapshot_date,
         l.listing_captured_at DESC,l.discovery_id DESC;

CREATE OR REPLACE VIEW market_dw.coverage_freshness AS
SELECT b.batch_id,b.batch_date,b.expected_scopes,b.status AS batch_state,
    count(q.position_id) AS configured_scopes,
    count(q.position_id) FILTER(WHERE q.first_attempt_at IS NOT NULL) AS attempted_scopes,
    count(q.position_id) FILTER(WHERE q.first_attempt_at IS NULL) AS unattempted_scopes,
    count(*) FILTER(WHERE q.status='complete' AND NOT q.work_remaining) AS complete_scopes,
    count(*) FILTER(WHERE q.eligible_listing_jobs>0 OR q.details_committed>0) AS scopes_with_eligible_data,
    count(*) FILTER(WHERE q.work_remaining) AS scopes_with_work_remaining,
    count(*) FILTER(WHERE q.status IN ('exhausted','needs_review','blocked_by_host')) AS scopes_held,
    COALESCE(sum(q.listing_pages_committed),0) AS listing_pages_committed,
    COALESCE(sum(q.details_committed),0) AS details_committed,
    (count(q.position_id)=b.expected_scopes AND
        count(*) FILTER(WHERE q.status='complete' AND NOT q.work_remaining)=b.expected_scopes) AS coverage_complete,
    round(100.0*count(*) FILTER(WHERE q.first_attempt_at IS NOT NULL)/NULLIF(b.expected_scopes,0),2)
        AS scope_first_attempt_coverage,
    round(100.0*count(*) FILTER(WHERE q.eligible_listing_jobs>0 OR q.details_committed>0)/NULLIF(b.expected_scopes,0),2)
        AS scope_data_coverage,
    max(q.last_success_at) AS latest_scope_capture,
    (SELECT last_successful_refresh FROM market_ops.publication_state WHERE id=1) AS last_successful_refresh,
    (SELECT extract(epoch FROM(now()-last_successful_refresh))/60
        FROM market_ops.publication_state WHERE id=1) AS bi_freshness_minutes
FROM market_ops.crawl_batches b LEFT JOIN market_ops.crawl_scope_queue q USING(batch_id)
GROUP BY b.batch_id,b.batch_date,b.expected_scopes,b.status;

-- Only DQ-released detail facts are visible here. Unknown lifecycle is explicit;
-- a missing posting in a partial crawl never proves closure or market growth.
CREATE OR REPLACE VIEW market_dw.published_jobs AS
SELECT f.*,a.record_level,'UNKNOWN'::text AS observed_lifecycle_status
FROM market_dw.fct_job_snapshot f
JOIN market_ops.raw_ingest_accounting a ON a.raw_job_id=f.raw_job_id
    AND a.cleaning_version=f.cleaning_version AND a.disposition='LOADED' AND a.publication_ready
JOIN market_ops.publication_state p ON p.id=1 AND p.cleaning_version=a.cleaning_version;

CREATE OR REPLACE VIEW market_dw.latest_published_jobs AS
SELECT DISTINCT ON(posting_natural_key) * FROM market_dw.published_jobs
ORDER BY posting_natural_key,scraped_at DESC,raw_job_id DESC;

CREATE OR REPLACE VIEW market_dw.collection_overview AS
SELECT (SELECT count(DISTINCT source_job_id) FROM market_dw.observed_listings) AS observed_jobs,
    (SELECT count(*) FROM market_dw.latest_published_jobs) AS unique_observed_postings,
    (SELECT count(*) FROM market_dw.latest_published_jobs WHERE record_level='detail_partial') AS detail_partial_count,
    (SELECT count(*) FROM market_dw.latest_published_jobs WHERE record_level='detail_complete') AS detail_complete_count,
    (SELECT count(*) FROM market_dw.latest_published_jobs WHERE posted_date_key<>-1 AND is_temporal_eligible) AS posted_date_verified_count,
    (SELECT count(*) FROM market_dw.latest_published_jobs WHERE salary_midpoint_known) AS salary_known_count,
    (SELECT avg(salary_midpoint_vnd) FILTER(WHERE salary_midpoint_known) FROM market_dw.latest_published_jobs) AS average_salary_vnd,
    (SELECT 1.0*count(*) FILTER(WHERE listing_only)/NULLIF(count(*),0) FROM market_dw.observed_listings) AS listing_only_fraction,
    (SELECT max(listing_captured_at) FROM market_dw.observed_listings) AS latest_listing_capture,
    (SELECT max(scraped_at) FROM market_dw.published_jobs) AS latest_detail_capture,
    (SELECT last_successful_refresh FROM market_ops.publication_state WHERE id=1) AS last_successful_refresh,
    (SELECT count(*) FROM market_ops.raw_quarantine WHERE status='OPEN') AS quarantined_records,
    'Observed postings; coverage may be partial'::text AS observation_scope_label;

DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname='market_reader') THEN
        GRANT SELECT ON market_dw.observed_listings,market_dw.coverage_freshness,
            market_dw.published_jobs,market_dw.latest_published_jobs,market_dw.collection_overview TO market_reader;
    END IF;
END $$;
