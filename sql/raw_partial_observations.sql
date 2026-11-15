-- Capture metadata only: keep Raw payloads and their original timestamps immutable.
ALTER TABLE raw.search_pages
    ADD COLUMN IF NOT EXISTS source_page_number INTEGER CHECK (source_page_number > 0);

ALTER TABLE raw.job_observations
    ADD COLUMN IF NOT EXISTS record_level TEXT NOT NULL DEFAULT 'detail_complete'
        CHECK (record_level IN ('detail_complete','detail_partial')),
    ADD COLUMN IF NOT EXISTS missing_fields TEXT[] NOT NULL DEFAULT '{}';

-- Backfill quality metadata honestly; do not manufacture absent business fields.
UPDATE raw.job_observations o SET
    missing_fields = quality.missing,
    record_level = CASE WHEN cardinality(quality.missing)=0
        THEN 'detail_complete' ELSE 'detail_partial' END
FROM (
    SELECT raw_job_id,array_agg(field ORDER BY field) FILTER (
        WHERE nullif(btrim(raw_payload->>field),'') IS NULL) AS absent
    FROM raw.job_observations
    CROSS JOIN unnest(ARRAY['job_title_raw','company_name_raw','location_raw',
        'job_description_raw','job_requirements_raw']) AS fields(field)
    GROUP BY raw_job_id
) missing
CROSS JOIN LATERAL (SELECT COALESCE(missing.absent,ARRAY[]::text[]) AS missing) quality
WHERE o.raw_job_id=missing.raw_job_id
  AND (o.missing_fields IS DISTINCT FROM quality.missing OR
       o.record_level IS DISTINCT FROM CASE WHEN cardinality(quality.missing)=0
           THEN 'detail_complete' ELSE 'detail_partial' END);
