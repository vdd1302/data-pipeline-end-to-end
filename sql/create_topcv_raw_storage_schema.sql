-- PostgreSQL 18; khởi tạo DB trống hoặc nâng cấp bổ sung schema raw hiện tại.
-- SỬA: giữ crawl_run_id/raw_job_id/source_code/raw_payload và 4 bảng cũ để
-- RawRepository, Raw Reader và các FK từ Staging tiếp tục hoạt động.
-- CREATE TABLE IF NOT EXISTS KHÔNG migrate bảng đã tồn tại; ALTER bên dưới
-- mới bổ sung cấu trúc. Không DROP bảng, đổi ID hoặc ghi đè payload lịch sử.
-- Chạy như một migration khi không có crawler/cleaning đang ghi; không chạy mỗi lịch.
-- Script chỉ tạo schema. Runner vẫn phải thực hiện retry, heartbeat, khóa chạy,
-- transaction checkpoint và đọc YAML; các cột metadata không tự bật các cơ chế này.

BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE SCHEMA IF NOT EXISTS raw;

-- SỬA: dừng rõ ràng nếu gặp schema đổi tên theo bản nháp run_id/observation_id.
-- Không tự đổi/copy khóa chính vì có thể làm hỏng FK và lịch sử Staging.
DO $raw_contract$
DECLARE
    expected RECORD;
    missing_columns TEXT;
BEGIN
    FOR expected IN
        SELECT * FROM (VALUES
            ('crawl_runs', ARRAY['crawl_run_id', 'source_code', 'started_at',
                'finished_at', 'snapshot_date', 'run_status', 'total_discovered',
                'total_fetched', 'total_failed', 'crawler_version', 'created_at']),
            ('job_discoveries', ARRAY['discovery_id', 'crawl_run_id',
                'source_job_id', 'job_url', 'query_city_raw', 'query_category_raw',
                'search_page_no', 'result_position', 'discovered_at']),
            ('job_observations', ARRAY['raw_job_id', 'crawl_run_id',
                'source_job_id', 'job_url', 'snapshot_date', 'scraped_at',
                'content_hash', 'fetch_status', 'http_status', 'parser_version',
                'raw_payload', 'ingested_at']),
            ('crawl_errors', ARRAY['error_id', 'crawl_run_id', 'source_job_id',
                'job_url', 'stage', 'error_type', 'error_message', 'occurred_at'])
        ) AS required(table_name, column_names)
    LOOP
        IF to_regclass('raw.' || expected.table_name) IS NOT NULL THEN
            SELECT string_agg(col, ', ' ORDER BY col)
            INTO missing_columns
            FROM unnest(expected.column_names) AS required_col(col)
            WHERE NOT EXISTS (
                SELECT 1 FROM information_schema.columns AS actual
                WHERE actual.table_schema = 'raw'
                  AND actual.table_name = expected.table_name
                  AND actual.column_name = col
            );
            IF missing_columns IS NOT NULL THEN
                RAISE EXCEPTION 'raw.% thiếu cột hợp đồng cũ: %. Cần migration riêng; không tự DROP/recreate.',
                    expected.table_name, missing_columns;
            END IF;
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1 FROM pg_class AS c
        JOIN pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = 'raw' AND c.relname = 'job_search_matches'
          AND c.relkind <> 'v'
    ) THEN
        RAISE EXCEPTION 'raw.job_search_matches đã là bảng/đối tượng khác; cần migrate dữ liệu trước khi chuyển thành view.';
    END IF;
END
$raw_contract$;

-- BEGIN BASELINE: hợp đồng tương thích với 4 bảng đã có trong PostgreSQL.
CREATE TABLE IF NOT EXISTS raw.crawl_runs (
    crawl_run_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_code       VARCHAR NOT NULL DEFAULT 'topcv',
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    snapshot_date     DATE NOT NULL,
    run_status        VARCHAR NOT NULL DEFAULT 'running',
    total_discovered  INTEGER NOT NULL DEFAULT 0,
    total_fetched     INTEGER NOT NULL DEFAULT 0,
    total_failed      INTEGER NOT NULL DEFAULT 0,
    crawler_version   VARCHAR,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_crawl_runs_status
        CHECK (run_status IN ('running', 'success', 'partial', 'failed')),
    CONSTRAINT chk_crawl_runs_counts
        CHECK (total_discovered >= 0 AND total_fetched >= 0 AND total_failed >= 0)
);

CREATE TABLE IF NOT EXISTS raw.job_discoveries (
    discovery_id       BIGSERIAL PRIMARY KEY,
    crawl_run_id       UUID NOT NULL,
    source_job_id      VARCHAR NOT NULL,
    job_url            TEXT NOT NULL,
    query_city_raw     VARCHAR,
    query_category_raw VARCHAR,
    search_page_no     INTEGER,
    result_position    INTEGER,
    discovered_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_discoveries_run FOREIGN KEY (crawl_run_id)
        REFERENCES raw.crawl_runs(crawl_run_id) ON DELETE RESTRICT,
    CONSTRAINT chk_discovery_page CHECK (search_page_no IS NULL OR search_page_no >= 1),
    CONSTRAINT chk_result_position CHECK (result_position IS NULL OR result_position >= 1)
);

CREATE TABLE IF NOT EXISTS raw.job_observations (
    raw_job_id      BIGSERIAL PRIMARY KEY,
    crawl_run_id    UUID NOT NULL,
    source_job_id   VARCHAR NOT NULL,
    job_url         TEXT NOT NULL,
    snapshot_date   DATE NOT NULL,
    scraped_at      TIMESTAMPTZ NOT NULL,
    content_hash    VARCHAR,
    fetch_status    VARCHAR NOT NULL DEFAULT 'success',
    http_status     INTEGER,
    parser_version  VARCHAR,
    raw_payload     JSONB NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_job_observations_run FOREIGN KEY (crawl_run_id)
        REFERENCES raw.crawl_runs(crawl_run_id) ON DELETE RESTRICT,
    CONSTRAINT uq_job_observation_run UNIQUE (crawl_run_id, source_job_id),
    CONSTRAINT chk_job_fetch_status CHECK (fetch_status IN ('success', 'partial', 'failed')),
    CONSTRAINT chk_http_status CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    CONSTRAINT chk_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object')
);

CREATE TABLE IF NOT EXISTS raw.crawl_errors (
    error_id       BIGSERIAL PRIMARY KEY,
    crawl_run_id   UUID NOT NULL,
    source_job_id  VARCHAR,
    job_url        TEXT,
    stage          VARCHAR NOT NULL,
    error_type     VARCHAR,
    error_message  TEXT NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_crawl_errors_run FOREIGN KEY (crawl_run_id)
        REFERENCES raw.crawl_runs(crawl_run_id) ON DELETE RESTRICT,
    CONSTRAINT chk_error_stage CHECK (stage IN ('discovery', 'fetch', 'parse', 'load'))
);
-- END BASELINE

-- 1. Metadata cấu hình/lịch: NULL nghĩa là legacy writer chưa ghi nhận.
-- SỬA: config_snapshot không NOT NULL/default {}; không giả mạo cấu hình cũ.
-- Tuyệt đối không lưu mật khẩu, DATABASE_URL hoặc cookies trong config_snapshot.
ALTER TABLE raw.crawl_runs
    ADD COLUMN IF NOT EXISTS config_version INTEGER,
    ADD COLUMN IF NOT EXISTS config_snapshot JSONB,
    ADD COLUMN IF NOT EXISTS config_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS run_purpose TEXT,
    ADD COLUMN IF NOT EXISTS observation_method TEXT,
    ADD COLUMN IF NOT EXISTS schedule_key TEXT,
    ADD COLUMN IF NOT EXISTS scheduled_for TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS lease_owner TEXT,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS retry_after_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS stop_reason TEXT,
    ADD COLUMN IF NOT EXISTS notes TEXT;

-- SỬA: giữ 'success' mà Python đang dùng; không đổi thành 'completed'.
-- Thay CHECK có chủ đích trong cùng transaction; dữ liệu vi phạm sẽ rollback.
ALTER TABLE raw.crawl_runs
    DROP CONSTRAINT IF EXISTS chk_crawl_runs_status,
    ADD CONSTRAINT chk_crawl_runs_status CHECK (run_status IN
        ('pending', 'running', 'success', 'partial', 'failed', 'interrupted', 'blocked')),
    DROP CONSTRAINT IF EXISTS ck_crawl_runs_config,
    ADD CONSTRAINT ck_crawl_runs_config CHECK (
        (config_version IS NULL OR config_version > 0)
        AND (config_snapshot IS NULL OR jsonb_typeof(config_snapshot) = 'object')
        AND (config_sha256 IS NULL OR
             (config_snapshot IS NOT NULL AND config_sha256 ~ '^[0-9a-f]{64}$'))),
    DROP CONSTRAINT IF EXISTS ck_crawl_runs_finish,
    ADD CONSTRAINT ck_crawl_runs_finish
        CHECK (finished_at IS NULL OR finished_at >= started_at),
    DROP CONSTRAINT IF EXISTS ck_crawl_runs_provenance,
    ADD CONSTRAINT ck_crawl_runs_provenance CHECK (
        (run_purpose IS NULL OR run_purpose IN ('production', 'test', 'manual', 'backfill', 'unknown'))
        AND (observation_method IS NULL OR observation_method IN
             ('selenium_live', 'manual_html', 'test', 'unknown'))),
    DROP CONSTRAINT IF EXISTS ck_crawl_runs_schedule,
    ADD CONSTRAINT ck_crawl_runs_schedule CHECK (
        (schedule_key IS NULL AND scheduled_for IS NULL)
        OR (schedule_key IS NOT NULL AND btrim(schedule_key) <> '' AND scheduled_for IS NOT NULL)),
    DROP CONSTRAINT IF EXISTS ck_crawl_runs_lease,
    ADD CONSTRAINT ck_crawl_runs_lease CHECK (
        (lease_owner IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner IS NOT NULL AND btrim(lease_owner) <> '' AND lease_expires_at IS NOT NULL));

-- Retry cùng lịch phải tìm lại run, không tạo UUID mới; lịch tiếp theo vẫn là run mới.
CREATE UNIQUE INDEX IF NOT EXISTS uq_crawl_runs_schedule
    ON raw.crawl_runs(source_code, schedule_key, scheduled_for)
    WHERE schedule_key IS NOT NULL;
-- Lease cần runner claim/renew/reclaim nguyên tử; không dùng CHECK với now()
-- để giả lập khóa tự hết hạn. Worker phải dừng nếu mất lease/kết nối giữ khóa.

COMMENT ON TABLE raw.crawl_runs IS
    'Crawl executions. Legacy API is preserved; nullable config/provenance means not recorded. Scheduling and leases are managed by the runner.';
COMMENT ON COLUMN raw.crawl_runs.snapshot_date IS
    'Vietnam business snapshot date. Existing cleaning requires run/observation dates to agree; define cross-midnight resume policy before automated scheduling.';
COMMENT ON COLUMN raw.crawl_runs.total_discovered IS
    'Legacy counter. Runner must distinguish search-match count from unique-job count; do not infer completeness from this value alone.';

-- 2. Một task = một vị trí x một thành phố trong run (20 task theo crawl.yaml).
CREATE TABLE IF NOT EXISTS raw.search_tasks (
    task_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crawl_run_id         UUID NOT NULL REFERENCES raw.crawl_runs(crawl_run_id) ON DELETE RESTRICT,
    position_id          TEXT NOT NULL CHECK (btrim(position_id) <> ''),
    position_keyword     TEXT NOT NULL CHECK (btrim(position_keyword) <> ''),
    location_id          TEXT NOT NULL CHECK (btrim(location_id) <> ''),
    location_label       TEXT NOT NULL CHECK (btrim(location_label) <> ''),
    search_url           TEXT,
    status               TEXT NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending', 'running', 'success', 'partial',
                                           'failed', 'skipped', 'blocked', 'interrupted')),
    last_completed_page  INTEGER NOT NULL DEFAULT 0 CHECK (last_completed_page >= 0),
    attempt_count        INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_retry_at        TIMESTAMPTZ,
    stop_reason          TEXT,
    started_at           TIMESTAMPTZ,
    finished_at          TIMESTAMPTZ,
    last_error           TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_search_tasks_scope UNIQUE (crawl_run_id, position_id, location_id),
    CONSTRAINT uq_search_tasks_id_run UNIQUE (task_id, crawl_run_id),
    CONSTRAINT ck_search_tasks_finish CHECK
        (finished_at IS NULL OR finished_at >= COALESCE(started_at, created_at))
);

-- 3. SỬA: lưu từng attempt của trang; retry thất bại không ghi đè trang thành công.
CREATE TABLE IF NOT EXISTS raw.search_pages (
    page_id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    crawl_run_id     UUID NOT NULL,
    task_id          BIGINT NOT NULL,
    page_number      INTEGER NOT NULL CHECK (page_number > 0),
    attempt_number   INTEGER NOT NULL DEFAULT 1 CHECK (attempt_number > 0),
    page_url         TEXT,
    -- Không mặc định success: writer phải xác nhận trang đã sẵn sàng.
    fetch_status     TEXT NOT NULL CHECK (fetch_status IN ('success', 'failed', 'blocked')),
    http_status      INTEGER CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    fetched_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    listing_count    INTEGER NOT NULL DEFAULT 0 CHECK (listing_count >= 0),
    listing_items    JSONB NOT NULL DEFAULT '[]'::jsonb,
    page_html        TEXT,
    parser_version   TEXT,
    CONSTRAINT ck_search_pages_items CHECK (CASE
        WHEN jsonb_typeof(listing_items) = 'array'
        THEN listing_count = jsonb_array_length(listing_items)
        ELSE FALSE END),
    CONSTRAINT uq_search_pages_attempt UNIQUE (task_id, page_number, attempt_number),
    CONSTRAINT uq_search_pages_reference UNIQUE (page_id, task_id, crawl_run_id, page_number),
    CONSTRAINT fk_search_pages_task FOREIGN KEY (task_id, crawl_run_id)
        REFERENCES raw.search_tasks(task_id, crawl_run_id) ON DELETE RESTRICT
);
COMMENT ON TABLE raw.search_pages IS
    'Captured page attempts. Retry delivery reuses the attempt number; a new navigation attempt uses a new number. listing_count counts stored items, not the advertised result total.';

-- 4. SỬA: job_discoveries là ledger search-match duy nhất, vẫn hỗ trợ writer cũ.
-- task_id/page_id/listing_payload NULL là discovery legacy chưa có metadata mới.
ALTER TABLE raw.job_discoveries
    ADD COLUMN IF NOT EXISTS task_id BIGINT,
    ADD COLUMN IF NOT EXISTS page_id BIGINT,
    ADD COLUMN IF NOT EXISTS listing_payload JSONB;
ALTER TABLE raw.job_discoveries
    DROP CONSTRAINT IF EXISTS ck_discoveries_listing,
    ADD CONSTRAINT ck_discoveries_listing
        CHECK (listing_payload IS NULL OR jsonb_typeof(listing_payload) = 'object'),
    DROP CONSTRAINT IF EXISTS ck_discoveries_page_reference,
    ADD CONSTRAINT ck_discoveries_page_reference CHECK (
        (task_id IS NULL AND page_id IS NULL)
        OR (task_id IS NOT NULL AND page_id IS NOT NULL AND search_page_no IS NOT NULL)),
    DROP CONSTRAINT IF EXISTS fk_discoveries_task,
    ADD CONSTRAINT fk_discoveries_task FOREIGN KEY (task_id, crawl_run_id)
        REFERENCES raw.search_tasks(task_id, crawl_run_id) ON DELETE RESTRICT,
    DROP CONSTRAINT IF EXISTS fk_discoveries_page,
    ADD CONSTRAINT fk_discoveries_page FOREIGN KEY (page_id, task_id, crawl_run_id, search_page_no)
        REFERENCES raw.search_pages(page_id, task_id, crawl_run_id, page_number) ON DELETE RESTRICT;

-- SỬA: chống replay theo TRANG LOGIC, không theo attempt hoặc rank.
-- Giữ được cùng job ở nhiều trang và nhiều keyword/thành phố.
-- Không ép unique lên legacy task_id=NULL vì có thể đã có discovery trùng.
CREATE UNIQUE INDEX IF NOT EXISTS uq_job_discoveries_task_page_job
    ON raw.job_discoveries(task_id, search_page_no, source_job_id)
    WHERE task_id IS NOT NULL;
-- Runner mới có thể INSERT ... ON CONFLICT (task_id, search_page_no, source_job_id)
-- WHERE task_id IS NOT NULL DO NOTHING.
-- Page + discoveries + last_completed_page phải COMMIT CÙNG transaction,
-- checkpoint chỉ tăng khi trang success và toàn bộ listing đã lưu.
-- DDL không tự thực hiện transaction này thay cho RawRepository/runner.

-- 5. Observation giữ business payload detail bất biến; pending detail ở discoveries.
-- SỬA: không tạo raw_job_id pending rồi UPDATE thành success: cleaning đang quét
-- theo raw_job_id và có thể bỏ sót/không clean lại ID cũ sau khi cursor đã đi qua.
-- In-run retry giữ nguyên observation; run mới tạo observation mới dù hash giống.
-- Retry fetch/parse trước khi ghi observation cuối cùng. Bản ghi failed/partial
-- đã lưu chiếm khóa cùng run; recovery bằng run mới, không UPDATE payload cũ.
-- source_job_id vẫn bắt buộc theo TopCV/parser hiện tại; chưa hỗ trợ fallback
-- URL hash/missing ID nếu chưa nâng cấp hợp đồng Python và cleaning tương ứng.
ALTER TABLE raw.job_observations
    ADD COLUMN IF NOT EXISTS detail_html TEXT,
    ADD COLUMN IF NOT EXISTS capture_source TEXT,
    ADD COLUMN IF NOT EXISTS source_captured_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS is_temporal_eligible BOOLEAN;
-- CHECK cũng xác thực hash legacy; nếu dữ liệu cũ vi phạm thì toàn migration
-- rollback để audit riêng, không tự sửa hash hoặc xoá bản ghi lịch sử.
ALTER TABLE raw.job_observations
    DROP CONSTRAINT IF EXISTS ck_job_observations_content_hash,
    ADD CONSTRAINT ck_job_observations_content_hash
        CHECK (content_hash IS NULL OR content_hash ~ '^[0-9a-f]{64}$');
-- content_hash = compute_content_hash(raw_payload) trong src/models.py.
-- Không hash raw_payload::text: cách serialize có thể khác Python.
-- Không đưa HTML/listing/timestamp/config vào business hash; không UNIQUE theo hash.
-- Hash và parser_version legacy có thể NULL; không backfill giá trị giả.

-- M4: NULL keeps old observations' provenance unknown, never rewrite legacy history.
-- Manual historical HTML has unknown source capture time and is ineligible for
-- date-based market-trend analysis. The legacy scraped_at column is retained
-- for compatibility, but for backfills it is the operator import timestamp.
ALTER TABLE raw.job_observations
    DROP CONSTRAINT IF EXISTS ck_job_observations_capture_source,
    ADD CONSTRAINT ck_job_observations_capture_source CHECK (
        capture_source IS NULL OR capture_source IN ('selenium_live', 'historical_saved_html')),
    DROP CONSTRAINT IF EXISTS ck_job_observations_historical_temporal,
    ADD CONSTRAINT ck_job_observations_historical_temporal CHECK (
        capture_source IS DISTINCT FROM 'historical_saved_html' OR
        (source_captured_at IS NULL AND is_temporal_eligible IS FALSE));

COMMENT ON TABLE raw.job_observations IS
    'Captured detail observations, append across runs. Preserve raw_job_id and payload for existing Staging references; pending work belongs in discoveries/tasks.';
COMMENT ON COLUMN raw.job_observations.detail_html IS
    'Optional source HTML for reparsing. Can be large; configure capture/retention in the runner, independently of the business payload hash.';

-- 6. Ghi lỗi: giữ error_message/error_type mà log_error() hiện tại đang INSERT.
ALTER TABLE raw.crawl_errors
    ADD COLUMN IF NOT EXISTS task_id BIGINT,
    ADD COLUMN IF NOT EXISTS page_id BIGINT,
    ADD COLUMN IF NOT EXISTS page_number INTEGER,
    ADD COLUMN IF NOT EXISTS error_kind TEXT,
    ADD COLUMN IF NOT EXISTS attempt_number INTEGER,
    ADD COLUMN IF NOT EXISTS retryable BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS retry_after_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS context JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE raw.crawl_errors
    DROP CONSTRAINT IF EXISTS chk_error_stage,
    ADD CONSTRAINT chk_error_stage CHECK (stage IN (
        'discovery', 'fetch', 'parse', 'load', 'navigate_search', 'parse_listing',
        'open_detail', 'parse_detail', 'persist', 'checkpoint', 'scheduler')),
    DROP CONSTRAINT IF EXISTS ck_crawl_errors_context,
    ADD CONSTRAINT ck_crawl_errors_context CHECK (jsonb_typeof(context) = 'object'),
    DROP CONSTRAINT IF EXISTS ck_crawl_errors_attempt,
    ADD CONSTRAINT ck_crawl_errors_attempt CHECK (
        (attempt_number IS NULL OR attempt_number > 0)
        AND (page_number IS NULL OR page_number > 0)
        AND (error_kind IS NULL OR btrim(error_kind) <> '')),
    DROP CONSTRAINT IF EXISTS ck_crawl_errors_page_reference,
    ADD CONSTRAINT ck_crawl_errors_page_reference
        CHECK (page_id IS NULL OR (task_id IS NOT NULL AND page_number IS NOT NULL)),
    DROP CONSTRAINT IF EXISTS fk_crawl_errors_task,
    ADD CONSTRAINT fk_crawl_errors_task FOREIGN KEY (task_id, crawl_run_id)
        REFERENCES raw.search_tasks(task_id, crawl_run_id) ON DELETE RESTRICT,
    DROP CONSTRAINT IF EXISTS fk_crawl_errors_page,
    ADD CONSTRAINT fk_crawl_errors_page FOREIGN KEY (page_id, task_id, crawl_run_id, page_number)
        REFERENCES raw.search_pages(page_id, task_id, crawl_run_id, page_number) ON DELETE RESTRICT;
-- Không ghi credentials/cookies/toàn bộ SQL params vào message/context.
-- Nếu DB mất kết nối thì cần log ứng dụng ngoài DB; bảng này không thể thay thế.
-- Error event không phải hàng đợi retry; pending detail lấy từ discoveries
-- chưa có observation. Runner áp dụng attempt limit/backoff và stop policy.

-- 7. Index hẹp; tái dùng tên index legacy để không tạo bản sao không cần thiết.
CREATE INDEX IF NOT EXISTS idx_job_discoveries_run ON raw.job_discoveries(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_job_discoveries_job ON raw.job_discoveries(source_job_id);
CREATE INDEX IF NOT EXISTS idx_job_observations_run ON raw.job_observations(crawl_run_id);
CREATE INDEX IF NOT EXISTS idx_job_observations_job ON raw.job_observations(source_job_id);
CREATE INDEX IF NOT EXISTS idx_job_observations_snapshot ON raw.job_observations(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_crawl_errors_run ON raw.crawl_errors(crawl_run_id);
CREATE INDEX IF NOT EXISTS ix_crawl_runs_status_started_at ON raw.crawl_runs(run_status, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_search_tasks_run_status ON raw.search_tasks(crawl_run_id, status);
CREATE INDEX IF NOT EXISTS ix_search_pages_run_fetched_at ON raw.search_pages(crawl_run_id, fetched_at DESC);
CREATE INDEX IF NOT EXISTS ix_job_discoveries_page ON raw.job_discoveries(page_id);
CREATE INDEX IF NOT EXISTS ix_job_observations_source_history ON raw.job_observations(source_job_id, scraped_at DESC);
CREATE INDEX IF NOT EXISTS ix_crawl_errors_task ON raw.crawl_errors(task_id) WHERE task_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_crawl_errors_page ON raw.crawl_errors(page_id) WHERE page_id IS NOT NULL;

-- 8. SỬA: job_search_matches là VIEW tổng hợp discovery, không phải bảng ghi thứ hai.
-- COUNT tính từ occurrence đã chống replay, không dùng occurrences_count += 1.
-- Mỗi dòng là một job/task; raw_job_id NULL nếu detail chưa thu thập thành công.
-- Writer ghi job_discoveries; không INSERT/UPDATE vào view này.
CREATE OR REPLACE VIEW raw.job_search_matches AS
SELECT
    MIN(d.discovery_id) AS match_id,
    d.crawl_run_id,
    d.task_id,
    d.source_job_id,
    o.raw_job_id,
    (array_agg(d.page_id ORDER BY d.search_page_no, d.result_position NULLS LAST, d.discovery_id))[1] AS first_page_id,
    (array_agg(d.result_position ORDER BY d.search_page_no, d.result_position NULLS LAST, d.discovery_id))[1] AS first_rank,
    MIN(d.discovered_at) AS first_matched_at,
    MAX(d.discovered_at) AS last_matched_at,
    COUNT(*) AS occurrences_count
FROM raw.job_discoveries AS d
LEFT JOIN raw.job_observations AS o
    ON o.crawl_run_id = d.crawl_run_id
   AND o.source_job_id = d.source_job_id
   AND o.fetch_status = 'success'
WHERE d.task_id IS NOT NULL
GROUP BY d.crawl_run_id, d.task_id, d.source_job_id, o.raw_job_id;

COMMENT ON VIEW raw.job_search_matches IS
    'Read-only logical matches derived from task-aware discoveries. Counts distinct logical pages, not retry attempts; legacy task-less discoveries remain in the base table.';

-- 9. SỬA: view trả đúng tên trường cleaning hiện tại, chỉ lấy detail success.
-- Reader hiện vẫn JOIN trực tiếp bảng; tạo view chưa tự thay đổi luồng Python.
CREATE OR REPLACE VIEW raw.v_job_observations_for_cleaning AS
SELECT
    o.raw_job_id,
    o.crawl_run_id,
    r.source_code,
    r.run_status,
    r.snapshot_date AS run_snapshot_date,
    r.crawler_version,
    o.source_job_id,
    o.job_url,
    o.snapshot_date,
    o.scraped_at,
    o.content_hash,
    o.fetch_status,
    o.raw_payload,
    o.parser_version,
    o.ingested_at,
    r.run_purpose,
    r.observation_method,
    o.capture_source,
    o.source_captured_at,
    o.is_temporal_eligible
FROM raw.job_observations AS o
JOIN raw.crawl_runs AS r ON r.crawl_run_id = o.crawl_run_id
WHERE o.fetch_status = 'success';

COMMENT ON VIEW raw.v_job_observations_for_cleaning IS
    'Successful raw detail observations with the existing cleaning field names. No normalization; provenance metadata is nullable for legacy writers.';

-- M4a: exclude unknown-time backfills and legacy-unknown provenance from
-- snapshot time-series analysis. Cleaning must opt into temporal eligibility.
CREATE OR REPLACE VIEW raw.v_job_observations_temporal_eligible AS
SELECT o.*
FROM raw.job_observations AS o
JOIN raw.crawl_runs AS r ON r.crawl_run_id = o.crawl_run_id
WHERE o.fetch_status = 'success'
  AND r.run_purpose = 'production'
  AND r.observation_method = 'selenium_live'
  AND o.capture_source = 'selenium_live'
  AND o.is_temporal_eligible IS TRUE
  AND o.source_captured_at IS NOT NULL;

COMMIT;

