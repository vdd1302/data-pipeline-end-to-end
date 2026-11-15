"""M4.4.1 - read-only, incremental TopCV raw observation batch selector.

Uses a caller-provided psycopg 3 connection, with dict_row configured by caller.
No DDL, DML, or DB configuration changes. No dependency on data volume.
M4.4.2 will implement the separate staging UPSERT writer.
"""
from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from src.cleaning.staging_mapper import CLEANING_VERSION


# M4.4.3.2c: Narrow the TopCV *posting* source contract to actual detail URLs.
# Excludes M2_TEST identifiers even when older test runs used source_code='topcv'.
# All conditions MUST match in candidate selection AND upper-watermark query.
# This is a compatibility guard for existing Raw records, not a substitute for
# an explicit provenance/run_purpose field on future crawl runs.
# Constant SQL only; any caller-supplied values remain bound parameters.
_TOPCV_POSTING_ELIGIBILITY_SQL = """
r.source_code = 'topcv'
AND o.source_job_id ~ '^[0-9]+$'
AND (
  (o.job_url ~* '^https?://(www[.])?topcv[.]vn/viec-lam/[^/?#]+/[0-9]+[.]html([?#].*)?$'
   AND substring(o.job_url FROM '/([0-9]+)[.]html([?#]|$)') = o.source_job_id)
  OR
  (o.job_url ~* '^https?://(www[.])?topcv[.]vn/brand/[^/?#]+/tuyen-dung/[^/?#]+-j[0-9]+[.]html([?#].*)?$'
   AND substring(o.job_url FROM '-j([0-9]+)[.]html([?#]|$)') = o.source_job_id)
)
""".strip()

_RUN_SELECTION_SQL = """
(%(crawl_run_ids)s::uuid[] IS NULL OR (
    r.crawl_run_id = ANY(%(crawl_run_ids)s::uuid[])
    AND r.run_purpose = 'production'
    AND r.run_status IN ('success', 'partial')
))
""".strip()


def validate_crawl_run_ids(values: tuple[str, ...] | None) -> tuple[str, ...] | None:
    """Validate an explicit run scope; an empty scope must never mean all runs."""
    if values is None:
        return None
    if not isinstance(values, tuple) or not values or any(not isinstance(v, str) for v in values):
        raise ValueError('crawl_run_ids must be a non-empty tuple of UUID strings or None')
    try:
        normalized = tuple(str(UUID(v)) for v in values)
    except (ValueError, AttributeError) as exc:
        raise ValueError('crawl_run_ids must contain UUID strings') from exc
    if len(set(normalized)) != len(normalized):
        raise ValueError('crawl_run_ids cannot contain duplicates')
    return normalized

# Always use parameter bindings for VALUES. Identifiers here are constant code.
_SELECT_CANDIDATES = f"""
SELECT
    o.raw_job_id,
    o.crawl_run_id,
    o.source_job_id,
    o.job_url,
    o.snapshot_date,
    o.scraped_at,
    o.content_hash,
    o.fetch_status,
    o.raw_payload,
    r.source_code,
    r.crawler_version,
    r.run_status,
    r.snapshot_date AS run_snapshot_date,
    r.run_purpose, r.observation_method, o.capture_source,
    o.source_captured_at, o.is_temporal_eligible AS raw_temporal_eligible
FROM raw.job_observations AS o
JOIN raw.crawl_runs AS r ON r.crawl_run_id = o.crawl_run_id
LEFT JOIN staging.job_observations_clean AS s ON s.raw_job_id = o.raw_job_id
WHERE o.raw_job_id > %(after_id)s
  AND o.raw_job_id <= %(upper_id)s
  AND o.fetch_status = 'success'
  AND {_TOPCV_POSTING_ELIGIBILITY_SQL}
  AND {_RUN_SELECTION_SQL}
  AND (
        s.raw_job_id IS NULL
        OR s.cleaning_version IS DISTINCT FROM %(cleaning_version)s
        OR s.cleaning_status = 'pending'
        OR (%(retry_failed)s AND s.cleaning_status = 'failed')
      )
  AND (%(raw_job_ids)s::bigint[] IS NULL OR o.raw_job_id = ANY(%(raw_job_ids)s::bigint[]))
ORDER BY o.raw_job_id ASC
LIMIT %(batch_size)s
""".strip()

_WATERMARK = f"""
SELECT COALESCE(MAX(o.raw_job_id), 0) AS upper_raw_job_id
FROM raw.job_observations AS o
JOIN raw.crawl_runs AS r ON r.crawl_run_id = o.crawl_run_id
WHERE o.fetch_status = 'success' AND {_TOPCV_POSTING_ELIGIBILITY_SQL}
  AND {_RUN_SELECTION_SQL}
""".strip()


@dataclass(frozen=True)
class RawSelectionOptions:
    """Stable bounded scan for a single preview or pipeline attempt."""
    batch_size: int = 100
    after_raw_job_id: int = 0
    upper_raw_job_id: int | None = None
    cleaning_version: str = CLEANING_VERSION
    raw_job_ids: tuple[int, ...] | None = None
    retry_failed: bool = False
    crawl_run_ids: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        validate_crawl_run_ids(self.crawl_run_ids)
        if type(self.batch_size) is not int or not 1 <= self.batch_size <= 1000:
            raise ValueError('batch_size must be 1..1000')
        if type(self.after_raw_job_id) is not int or self.after_raw_job_id < 0:
            raise ValueError('after_raw_job_id must be a non-negative integer')
        if self.upper_raw_job_id is not None and (
            type(self.upper_raw_job_id) is not int or self.upper_raw_job_id < 0
        ):
            raise ValueError('upper_raw_job_id must be a non-negative integer or None')
        if not isinstance(self.cleaning_version, str) or not self.cleaning_version.strip():
            raise ValueError('cleaning_version must be a nonempty string')
        if type(self.retry_failed) is not bool:
            raise ValueError('retry_failed must be boolean')
        if self.raw_job_ids is not None:
            if not isinstance(self.raw_job_ids, tuple) or not self.raw_job_ids:
                raise ValueError('raw_job_ids must be a non-empty tuple or None')
            if any(type(n) is not int or n <= 0 for n in self.raw_job_ids):
                raise ValueError('raw_job_ids must contain positive integers')
            if len(set(self.raw_job_ids)) != len(self.raw_job_ids):
                raise ValueError('raw_job_ids cannot contain duplicates')


def _row_as_mapping(row: Any) -> Mapping[str, Any]:
    if not isinstance(row, Mapping):
        raise TypeError('Database connection must use psycopg.rows.dict_row')
    return row


def get_raw_watermark(conn: Any, crawl_run_ids: tuple[str, ...] | None = None) -> int:
    """Capture a maximum raw id to bound an incremental scan."""
    with conn.cursor() as cur:
        run_ids = validate_crawl_run_ids(crawl_run_ids)
        cur.execute(_WATERMARK, {'crawl_run_ids': list(run_ids) if run_ids is not None else None})
        row = cur.fetchone()
    if row is None:
        return 0
    value = _row_as_mapping(row)['upper_raw_job_id']
    if type(value) is not int or value < 0:
        raise TypeError('Unexpected raw watermark type')
    return value


def select_raw_candidates(conn: Any, opts: RawSelectionOptions) -> list[Mapping[str, Any]]:
    """Read one page, JOINing metadata required by map_raw_observation().

    Caller provides upper_raw_job_id for stable pagination across multiple pages.
    If omitted, it is captured from DB before the SELECT.
    """
    upper_id = opts.upper_raw_job_id
    if upper_id is None:
        upper_id = get_raw_watermark(conn, opts.crawl_run_ids)
    if upper_id <= opts.after_raw_job_id:
        return []
    params = {
        'after_id': opts.after_raw_job_id,
        'upper_id': upper_id,
        'cleaning_version': opts.cleaning_version,
        'raw_job_ids': list(opts.raw_job_ids) if opts.raw_job_ids is not None else None,
        'retry_failed': opts.retry_failed,
        'batch_size': opts.batch_size,
        'crawl_run_ids': (list(validate_crawl_run_ids(opts.crawl_run_ids))
                          if opts.crawl_run_ids is not None else None),
    }
    with conn.cursor() as cur:
        cur.execute(_SELECT_CANDIDATES, params)
        results = cur.fetchall()
    records = [_row_as_mapping(item) for item in results]
    ids = [item['raw_job_id'] for item in records]
    if any(type(i) is not int for i in ids) or ids != sorted(set(ids)):
        raise ValueError('Expected unique, ascending raw_job_id results')
    if any(not (opts.after_raw_job_id < i <= upper_id) for i in ids):
        raise ValueError('Returned raw_job_id outside requested keyset bounds')
    return records


def iter_raw_candidate_batches(conn: Any, opts: RawSelectionOptions) -> Iterator[list[Mapping[str, Any]]]:
    """Lazy keyset scan, preserving one upper watermark per iteration.

    No database writes. A crash does not alter any DB state.
    """
    upper = get_raw_watermark(conn, opts.crawl_run_ids) if opts.upper_raw_job_id is None else opts.upper_raw_job_id
    after = opts.after_raw_job_id
    while after < upper:
        page_opts = RawSelectionOptions(
            batch_size=opts.batch_size,
            after_raw_job_id=after,
            upper_raw_job_id=upper,
            cleaning_version=opts.cleaning_version,
            raw_job_ids=opts.raw_job_ids,
            retry_failed=opts.retry_failed,
            crawl_run_ids=opts.crawl_run_ids,
        )
        batch = select_raw_candidates(conn, page_opts)
        if not batch:
            return
        yield batch
        after = batch[-1]['raw_job_id']
