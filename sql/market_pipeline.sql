-- Operational metadata is separate from the non-null analytical contract.
CREATE SCHEMA IF NOT EXISTS market_ops;
CREATE TABLE IF NOT EXISTS market_ops.scope_queue (
    snapshot_date DATE NOT NULL,
    position_id TEXT NOT NULL,
    location_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
      CHECK (status IN ('pending','running','success','partial','retry','blocked','exhausted','expired')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00Z',
    last_error TEXT NOT NULL DEFAULT '',
    raw_run_id UUID,
    raw_task_id BIGINT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date,position_id,location_id)
);
ALTER TABLE market_ops.scope_queue ALTER COLUMN next_retry_at SET DEFAULT '1970-01-01T00:00:00Z';
UPDATE market_ops.scope_queue SET next_retry_at='1970-01-01T00:00:00Z' WHERE next_retry_at='-infinity';
ALTER TABLE market_ops.scope_queue ADD COLUMN IF NOT EXISTS failure_count INTEGER;
UPDATE market_ops.scope_queue SET failure_count=CASE
    WHEN status IN ('retry','exhausted','blocked') THEN attempts ELSE 0 END
    WHERE failure_count IS NULL;
ALTER TABLE market_ops.scope_queue ALTER COLUMN failure_count SET DEFAULT 0;
ALTER TABLE market_ops.scope_queue ALTER COLUMN failure_count SET NOT NULL;
CREATE TABLE IF NOT EXISTS market_ops.host_backoff (
    host TEXT PRIMARY KEY,
    next_retry_at TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL
);
ALTER TABLE market_ops.host_backoff ADD COLUMN IF NOT EXISTS consecutive_signals INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS market_ops.refresh_checkpoint (
    id INTEGER PRIMARY KEY CHECK (id=1),
    last_raw_job_id BIGINT NOT NULL DEFAULT 0 CHECK (last_raw_job_id >= 0),
    cleaning_version TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS market_ops.refresh_log (
    refresh_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    selected INTEGER NOT NULL,
    loaded INTEGER NOT NULL,
    rejected INTEGER NOT NULL,
    nonnull_verified BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS market_ops.mapping_errors (
    raw_job_id BIGINT PRIMARY KEY,
    error_type TEXT NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================================
-- DATA WAREHOUSE B3 STAR SCHEMA (7 DIMENSIONS + 1 FACT + 2 BRIDGES)
-- All 7 Dimensions and 1 Fact strictly follow the NON-NULL analytical contract.
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS market_dw;

-- ----------------------------------------------------------------------------
-- DIMENSION 1: DIM_JOB_POSTING (Bản ghi bài đăng tuyển dụng)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.dim_job_posting (
    posting_key TEXT PRIMARY KEY,
    posting_natural_key TEXT NOT NULL DEFAULT '',
    source_job_id TEXT NOT NULL,
    canonical_job_url TEXT NOT NULL,
    job_title TEXT NOT NULL,
    job_level TEXT NOT NULL,
    job_description TEXT NOT NULL,
    job_requirements TEXT NOT NULL,
    benefits TEXT NOT NULL
);
ALTER TABLE market_dw.dim_job_posting ADD COLUMN IF NOT EXISTS posting_natural_key TEXT NOT NULL DEFAULT '';

-- ----------------------------------------------------------------------------
-- DIMENSION 2: DIM_DATE (Chiều thời gian chuẩn hóa YYYYMMDD)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31),
    week_of_year INTEGER NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    quarter INTEGER NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year INTEGER NOT NULL,
    year_month INTEGER NOT NULL,
    year_week INTEGER NOT NULL
);
INSERT INTO market_dw.dim_date (date_key, full_date, day, week_of_year, month, quarter, year, year_month, year_week)
VALUES (-1, '1970-01-01', 1, 1, 1, 1, 1970, 197001, 197001)
ON CONFLICT (date_key) DO NOTHING;

-- ----------------------------------------------------------------------------
-- DIMENSION 3: DIM_POSITION (10 nhóm vị trí IT mục tiêu & nhóm khác)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.dim_position (
    position_id TEXT PRIMARY KEY,
    position_name TEXT NOT NULL,
    in_scope BOOLEAN NOT NULL,
    role_family TEXT NOT NULL DEFAULT 'Other IT',
    classification_version TEXT NOT NULL DEFAULT 'topcv-cleaning-v1'
);
ALTER TABLE market_dw.dim_position ADD COLUMN IF NOT EXISTS role_family TEXT NOT NULL DEFAULT 'Other IT';
ALTER TABLE market_dw.dim_position ADD COLUMN IF NOT EXISTS classification_version TEXT NOT NULL DEFAULT 'topcv-cleaning-v1';

-- ----------------------------------------------------------------------------
-- DIMENSION 4: DIM_COMPANY (Doanh nghiệp tuyển dụng)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.dim_company (
    company_key TEXT PRIMARY KEY,
    company_natural_key TEXT NOT NULL DEFAULT '',
    company_name TEXT NOT NULL,
    company_url TEXT NOT NULL,
    company_size TEXT NOT NULL,
    industry TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE market_dw.dim_company ADD COLUMN IF NOT EXISTS company_natural_key TEXT NOT NULL DEFAULT '';

-- ----------------------------------------------------------------------------
-- DIMENSION 5: DIM_EXPERIENCE (Khung năm kinh nghiệm)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.dim_experience (
    experience_key TEXT PRIMARY KEY,
    experience_bucket TEXT NOT NULL UNIQUE,
    min_years_boundary NUMERIC NOT NULL CHECK (min_years_boundary >= -1),
    max_years_boundary NUMERIC NOT NULL CHECK (max_years_boundary >= -1),
    sort_order INTEGER NOT NULL
);

-- ----------------------------------------------------------------------------
-- DIMENSION 6: DIM_LOCATION (Địa điểm làm việc: Thành phố / Quận huyện)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.dim_location (
    location_key TEXT PRIMARY KEY,
    city TEXT NOT NULL,
    district TEXT NOT NULL,
    location_name TEXT NOT NULL
);

-- ----------------------------------------------------------------------------
-- DIMENSION 7: DIM_SKILL (Từ điển kỹ năng công nghệ IT)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.dim_skill (
    skill_key TEXT PRIMARY KEY,
    skill_name TEXT NOT NULL,
    skill_category TEXT NOT NULL DEFAULT 'General',
    is_known BOOLEAN NOT NULL,
    normalization_version TEXT NOT NULL DEFAULT 'topcv-skill-rules-v2'
);
ALTER TABLE market_dw.dim_skill ADD COLUMN IF NOT EXISTS skill_category TEXT NOT NULL DEFAULT 'General';
ALTER TABLE market_dw.dim_skill ADD COLUMN IF NOT EXISTS normalization_version TEXT NOT NULL DEFAULT 'topcv-skill-rules-v2';

-- ----------------------------------------------------------------------------
-- FACT TABLE: FCT_JOB_SNAPSHOT (1 Fact Table kết nối đầy đủ 7 Dimensions)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.fct_job_snapshot (
    posting_natural_key TEXT NOT NULL,
    crawl_run_id UUID NOT NULL,
    raw_job_id BIGINT NOT NULL UNIQUE,
    source_job_id TEXT NOT NULL,
    canonical_job_url TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    scraped_at TIMESTAMPTZ NOT NULL,
    -- Foreign Keys to Dimensions (7 Dimension FK roles)
    posting_key TEXT NOT NULL REFERENCES market_dw.dim_job_posting(posting_key),
    snapshot_date_key INTEGER NOT NULL REFERENCES market_dw.dim_date(date_key),
    posted_date_key INTEGER NOT NULL DEFAULT -1 REFERENCES market_dw.dim_date(date_key),
    deadline_date_key INTEGER NOT NULL DEFAULT -1 REFERENCES market_dw.dim_date(date_key),
    position_id TEXT NOT NULL REFERENCES market_dw.dim_position(position_id),
    company_key TEXT NOT NULL REFERENCES market_dw.dim_company(company_key),
    experience_key TEXT NOT NULL REFERENCES market_dw.dim_experience(experience_key),
    -- Business attributes & measures
    position_status TEXT NOT NULL,
    job_title TEXT NOT NULL,
    job_level TEXT NOT NULL,
    job_description TEXT NOT NULL,
    job_requirements TEXT NOT NULL,
    benefits TEXT NOT NULL,
    posting_count INTEGER NOT NULL DEFAULT 1 CHECK (posting_count = 1),
    salary_min_vnd NUMERIC NOT NULL CHECK (salary_min_vnd >= -1),
    salary_max_vnd NUMERIC NOT NULL CHECK (salary_max_vnd >= -1),
    salary_midpoint_vnd NUMERIC NOT NULL CHECK (salary_midpoint_vnd >= -1),
    salary_min_known BOOLEAN NOT NULL,
    salary_max_known BOOLEAN NOT NULL,
    salary_midpoint_known BOOLEAN NOT NULL,
    salary_disclosed_flag BOOLEAN NOT NULL DEFAULT FALSE,
    salary_negotiable_flag BOOLEAN NOT NULL DEFAULT FALSE,
    salary_status TEXT NOT NULL,
    salary_currency TEXT NOT NULL,
    salary_period TEXT NOT NULL,
    salary_label TEXT NOT NULL,
    experience_min_years NUMERIC NOT NULL CHECK (experience_min_years >= -1),
    experience_max_years NUMERIC NOT NULL CHECK (experience_max_years >= -1),
    experience_min_known BOOLEAN NOT NULL,
    experience_max_known BOOLEAN NOT NULL,
    experience_bucket TEXT NOT NULL,
    posted_date_label TEXT NOT NULL,
    deadline_date_label TEXT NOT NULL,
    is_new BOOLEAN NOT NULL DEFAULT FALSE,
    is_continuing BOOLEAN NOT NULL DEFAULT FALSE,
    is_reappeared BOOLEAN NOT NULL DEFAULT FALSE,
    -- Legacy boolean lifecycle flags default FALSE; unknown is explicitly
    -- recorded separately so FALSE cannot be interpreted as verified absence.
    lifecycle_status TEXT NOT NULL DEFAULT 'unknown'
      CHECK (lifecycle_status IN ('unknown','new','continuing','reappeared')),
    content_changed_flag BOOLEAN NOT NULL DEFAULT FALSE,
    observation_method TEXT NOT NULL,
    is_temporal_eligible BOOLEAN NOT NULL,
    cleaning_status TEXT NOT NULL,
    quality_flags JSONB NOT NULL CHECK (jsonb_typeof(quality_flags) = 'array'),
    content_hash TEXT NOT NULL,
    cleaning_version TEXT NOT NULL,
    PRIMARY KEY (posting_natural_key,crawl_run_id),
    CHECK (salary_min_known = (salary_min_vnd >= 0)),
    CHECK (salary_max_known = (salary_max_vnd >= 0)),
    CHECK (salary_midpoint_known = (salary_midpoint_vnd >= 0)),
    CHECK (experience_min_known = (experience_min_years >= 0)),
    CHECK (experience_max_known = (experience_max_years >= 0)),
    CHECK (NOT (salary_min_known AND salary_max_known) OR salary_min_vnd <= salary_max_vnd)
);
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS posted_date_key INTEGER NOT NULL DEFAULT -1 REFERENCES market_dw.dim_date(date_key);
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS deadline_date_key INTEGER NOT NULL DEFAULT -1 REFERENCES market_dw.dim_date(date_key);
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS salary_disclosed_flag BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS salary_negotiable_flag BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS is_new BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS is_continuing BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS is_reappeared BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS content_changed_flag BOOLEAN NOT NULL DEFAULT FALSE;

-- ----------------------------------------------------------------------------
-- BRIDGE 1: BRIDGE_JOB_LOCATION (Liên kết nhiều-nhiều Tin ↔ Địa điểm)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.bridge_job_location (
    posting_natural_key TEXT NOT NULL,
    crawl_run_id UUID NOT NULL,
    location_key TEXT NOT NULL REFERENCES market_dw.dim_location(location_key),
    allocation_weight NUMERIC NOT NULL CHECK (allocation_weight > 0 AND allocation_weight <= 1),
    PRIMARY KEY (posting_natural_key,crawl_run_id,location_key),
    FOREIGN KEY (posting_natural_key,crawl_run_id) REFERENCES market_dw.fct_job_snapshot
);

-- ----------------------------------------------------------------------------
-- BRIDGE 2: BRIDGE_JOB_SKILL (Liên kết nhiều-nhiều Tin ↔ Kỹ năng)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS market_dw.bridge_job_skill (
    posting_natural_key TEXT NOT NULL,
    crawl_run_id UUID NOT NULL,
    skill_key TEXT NOT NULL REFERENCES market_dw.dim_skill(skill_key),
    PRIMARY KEY (posting_natural_key,crawl_run_id,skill_key),
    FOREIGN KEY (posting_natural_key,crawl_run_id) REFERENCES market_dw.fct_job_snapshot
);

ALTER TABLE market_dw.fct_job_snapshot ADD COLUMN IF NOT EXISTS lifecycle_status TEXT NOT NULL DEFAULT 'unknown';

-- ============================================================================
-- ANALYTICAL VIEWS FOR BI & APACHE SUPERSET
-- ============================================================================
-- Views are stable BI contracts. Do not CASCADE-drop dependent datasets on
-- initialization/refresh. Incompatible changes require reviewed migration.
CREATE OR REPLACE VIEW market_dw.jobs AS
SELECT f.posting_natural_key, f.crawl_run_id, f.raw_job_id, f.source_job_id, f.canonical_job_url, f.snapshot_date, f.scraped_at, f.posting_key, f.snapshot_date_key, f.posted_date_key, f.deadline_date_key, f.position_id, f.company_key, f.experience_key, f.position_status, f.job_title, f.job_level, f.job_description, f.job_requirements, f.benefits, f.posting_count, f.salary_min_vnd, f.salary_max_vnd, f.salary_midpoint_vnd, f.salary_min_known, f.salary_max_known, f.salary_midpoint_known, f.salary_disclosed_flag, f.salary_negotiable_flag, f.salary_status, f.salary_currency, f.salary_period, f.salary_label, f.experience_min_years, f.experience_max_years, f.experience_min_known, f.experience_max_known, f.experience_bucket, f.posted_date_label, f.deadline_date_label, f.is_new, f.is_continuing, f.is_reappeared, f.content_changed_flag, f.observation_method, f.is_temporal_eligible, f.cleaning_status, f.quality_flags, f.content_hash, f.cleaning_version, p.position_name, p.in_scope, c.company_name, c.company_size, c.industry,
       COALESCE(l.cities,'Chưa xác định') AS cities,
       COALESCE(l.locations,'Chưa xác định') AS locations,
       COALESCE(s.skills,'Chưa công bố') AS skills,
       f.lifecycle_status
FROM market_dw.fct_job_snapshot f
JOIN market_dw.dim_position p USING (position_id)
JOIN market_dw.dim_company c USING (company_key)
LEFT JOIN LATERAL (
    SELECT string_agg(DISTINCT d.city, ', ' ORDER BY d.city) AS cities,
           string_agg(DISTINCT d.location_name, '; ' ORDER BY d.location_name) AS locations
    FROM market_dw.bridge_job_location b JOIN market_dw.dim_location d USING (location_key)
    WHERE b.posting_natural_key=f.posting_natural_key AND b.crawl_run_id=f.crawl_run_id
) l ON TRUE
LEFT JOIN LATERAL (
    SELECT string_agg(d.skill_name, ', ' ORDER BY d.skill_name) AS skills
    FROM market_dw.bridge_job_skill b JOIN market_dw.dim_skill d USING (skill_key)
    WHERE b.posting_natural_key=f.posting_natural_key AND b.crawl_run_id=f.crawl_run_id
) s ON TRUE;

-- One latest observation PER posting, including partial but usable records.
CREATE OR REPLACE VIEW market_dw.latest_jobs AS
SELECT DISTINCT ON (posting_natural_key) * FROM market_dw.jobs
ORDER BY posting_natural_key,scraped_at DESC,raw_job_id DESC;

CREATE OR REPLACE VIEW market_dw.overview AS
SELECT count(*) AS total_jobs,
       count(DISTINCT company_key) FILTER (WHERE company_key <> 'unknown') AS total_companies,
       count(*) FILTER (WHERE in_scope) AS in_scope_jobs,
       count(*) FILTER (WHERE position_status='unclassified') AS unclassified_jobs,
       count(*) FILTER (WHERE salary_midpoint_known) AS salary_sample_count,
       COALESCE(round(avg(salary_midpoint_vnd) FILTER (WHERE salary_midpoint_known),0),0) AS average_salary_vnd,
       COALESCE(round(100.0*count(*) FILTER (WHERE salary_midpoint_known)/NULLIF(count(*),0),2),0) AS salary_coverage_pct,
       count(*) FILTER (WHERE cleaning_status='partial') AS partial_jobs,
       count(*) FILTER (WHERE is_temporal_eligible) AS live_observations,
       COALESCE(max(snapshot_date)::text,'Chưa có dữ liệu') AS latest_snapshot
FROM market_dw.latest_jobs;

-- Include positions with zero observations. Missing salary remains labelled.
CREATE OR REPLACE VIEW market_dw.by_position AS
SELECT p.position_id,p.position_name,count(j.posting_natural_key) AS job_count,
       count(j.posting_natural_key) FILTER (WHERE j.salary_midpoint_known) AS salary_sample_count,
       COALESCE(round(avg(j.salary_midpoint_vnd) FILTER (WHERE j.salary_midpoint_known),0),0) AS average_salary_vnd,
       CASE WHEN count(j.posting_natural_key) FILTER (WHERE j.salary_midpoint_known)>0
            THEN 'Có mẫu lương VND/tháng' ELSE 'Chưa có mẫu lương VND/tháng' END AS salary_availability,
       CASE WHEN count(j.posting_natural_key) FILTER (WHERE j.salary_midpoint_known)>0
            THEN to_char(round(avg(j.salary_midpoint_vnd) FILTER (WHERE j.salary_midpoint_known),0),'FM999,999,999,999') || ' VND/tháng'
            ELSE 'Chưa có mẫu lương' END AS average_salary_label
FROM market_dw.dim_position p LEFT JOIN market_dw.latest_jobs j USING (position_id)
WHERE p.in_scope GROUP BY p.position_id,p.position_name;

CREATE OR REPLACE VIEW market_dw.by_city AS
SELECT d.city,count(DISTINCT b.posting_natural_key) AS job_count,
       sum(b.allocation_weight) AS allocated_jobs
FROM market_dw.latest_jobs j JOIN market_dw.bridge_job_location b
 ON b.posting_natural_key=j.posting_natural_key AND b.crawl_run_id=j.crawl_run_id
JOIN market_dw.dim_location d USING (location_key) GROUP BY d.city;

CREATE OR REPLACE VIEW market_dw.top_skills AS
SELECT d.skill_name,count(*) AS job_count
FROM market_dw.latest_jobs j JOIN market_dw.bridge_job_skill b
 ON b.posting_natural_key=j.posting_natural_key AND b.crawl_run_id=j.crawl_run_id
JOIN market_dw.dim_skill d USING (skill_key)
WHERE d.is_known GROUP BY d.skill_name;

CREATE OR REPLACE VIEW market_dw.by_experience AS
SELECT experience_bucket,count(*) AS job_count FROM market_dw.latest_jobs GROUP BY experience_bucket;

CREATE OR REPLACE VIEW market_dw.data_quality AS
SELECT cleaning_status,position_status,salary_status,is_temporal_eligible,count(*) AS job_count
FROM market_dw.latest_jobs GROUP BY 1,2,3,4;

-- Observed jobs are not evidence of market coverage or source posting dates.
CREATE OR REPLACE VIEW market_dw.daily_observations AS
SELECT snapshot_date,count(*) AS observation_count,count(DISTINCT posting_natural_key) AS unique_jobs
FROM market_dw.fct_job_snapshot WHERE is_temporal_eligible GROUP BY snapshot_date;

CREATE OR REPLACE VIEW market_dw.scope_status AS
SELECT q.snapshot_date,q.position_id,q.location_id,q.status,q.attempts,
       CASE WHEN q.status IN ('success','partial','blocked','exhausted','expired') THEN 'Không tự thử lại'
            WHEN q.attempts=0 THEN 'Chờ lượt chạy'
            ELSE to_char(q.next_retry_at AT TIME ZONE 'Asia/Ho_Chi_Minh','YYYY-MM-DD HH24:MI') END AS next_retry_at,
       COALESCE(NULLIF(q.last_error,''),'Không có') AS last_error,q.updated_at,
       p.position_name,
       CASE q.location_id WHEN 'ha_noi' THEN 'Hà Nội' WHEN 'ho_chi_minh' THEN 'TP. Hồ Chí Minh' ELSE 'Chưa xác định' END AS city,
       CASE q.status WHEN 'pending' THEN 'Chờ chạy' WHEN 'running' THEN 'Đang thu thập'
         WHEN 'retry' THEN 'Chờ thử lại' WHEN 'success' THEN 'Đã xác minh hết trang'
         WHEN 'partial' THEN 'Chưa đủ phạm vi / giới hạn trang' WHEN 'blocked' THEN 'Bị từ chối truy cập'
         WHEN 'exhausted' THEN 'Đã hết số lần thử trong ngày' ELSE 'Đã qua ngày snapshot' END AS status_label,
       q.failure_count
FROM market_ops.scope_queue q JOIN market_dw.dim_position p USING (position_id);

-- Posted-date view for strictly eligible captures only. Missing/uncertain dates
-- are excluded: never infer a posting date from the crawl snapshot.
CREATE OR REPLACE VIEW market_dw.verified_posted_dates AS
SELECT d.full_date AS posted_date,
       count(*) AS observation_count,
       count(DISTINCT f.posting_natural_key) AS unique_postings
FROM market_dw.fct_job_snapshot f
JOIN market_dw.dim_date d ON d.date_key=f.posted_date_key
WHERE f.posted_date_key <> -1 AND f.is_temporal_eligible
GROUP BY d.full_date;
