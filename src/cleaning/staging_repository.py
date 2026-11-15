"""M4.4.2 - transactional, idempotent TopCV staging batch UPSERT.

Contract:
* Accept exactly the dicts emitted by staging_mapper.map_raw_observation().
* Do not modify Raw Storage, crawl runs, or Data Warehouse.
* Use psycopg 3 connection supplied by caller; caller owns connection lifetime.
* One batch is atomic. ON CONFLICT(raw_job_id) updates only changed rows.
* The database, not Python, sets cleaned_at on inserts/meaningful updates.

No connection is opened at import time; no dependency on Selenium/Airflow.
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from src.cleaning.staging_mapper import MAPPER_COLUMNS

MAX_BATCH_SIZE = 1000
_JSONB_COLUMNS = frozenset({"quality_flags", "locations_clean", "skill_evidence_clean"})
_REQUIRED_STATUS = frozenset({"pending", "success", "partial", "failed"})
_EXPECTED_INSERTABLE_COUNT = 45

if len(MAPPER_COLUMNS) != _EXPECTED_INSERTABLE_COUNT or len(set(MAPPER_COLUMNS)) != len(MAPPER_COLUMNS):
    raise RuntimeError("Mapper contract changed: expected 45 unique insertable staging columns")

# Identifiers come solely from trusted, version-controlled MAPPER_COLUMNS, never user input.
_INSERT_COLUMNS = ", ".join(MAPPER_COLUMNS)
_VALUE_PLACEHOLDERS = ", ".join(
    f"%({col})s::jsonb" if col in _JSONB_COLUMNS else f"%({col})s"
    for col in MAPPER_COLUMNS
)
_MUTABLE_COLUMNS = tuple(col for col in MAPPER_COLUMNS if col != "raw_job_id")
_UPDATE_SETTERS = ",\n    ".join(
    f"{col} = EXCLUDED.{col}" for col in _MUTABLE_COLUMNS
)
_CHANGE_OLD = ", ".join(f"s.{col}" for col in _MUTABLE_COLUMNS)
_CHANGE_NEW = ", ".join(f"EXCLUDED.{col}" for col in _MUTABLE_COLUMNS)

UPSERT_STAGING_SQL = f"""
INSERT INTO staging.job_observations_clean AS s
    ({_INSERT_COLUMNS})
VALUES
    ({_VALUE_PLACEHOLDERS})
ON CONFLICT (raw_job_id) DO UPDATE SET
    {_UPDATE_SETTERS},
    cleaned_at = clock_timestamp()
WHERE ROW({_CHANGE_OLD}) IS DISTINCT FROM ROW({_CHANGE_NEW})
""".strip()


@dataclass(frozen=True)
class BatchUpsertResult:
    submitted: int
    affected: int
    unchanged: int


def _validate_row(record: Mapping[str, Any]) -> None:
    if not isinstance(record, Mapping):
        raise TypeError("Staging record must be a mapping")
    actual = set(record.keys())
    expected = set(MAPPER_COLUMNS)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise ValueError(f"Staging column mismatch; missing={missing}, unexpected={unexpected}")
    if type(record["raw_job_id"]) is not int or record["raw_job_id"] <= 0:
        raise ValueError("raw_job_id must be a positive integer")
    if not isinstance(record["cleaning_version"], str) or not record["cleaning_version"].strip():
        raise ValueError("cleaning_version must be a nonempty string")
    if record["cleaning_status"] not in _REQUIRED_STATUS:
        raise ValueError("cleaning_status must be pending, success, partial or failed")
    if type(record["is_temporal_eligible"]) is not bool:
        raise ValueError("is_temporal_eligible must be boolean")
    if record["observation_method"] not in {"selenium_live", "manual_html", "unknown"}:
        raise ValueError("unexpected observation_method")
    flags = record["quality_flags"]
    if not isinstance(flags, list) or not all(isinstance(flag, str) for flag in flags):
        raise ValueError("quality_flags must be a list of strings")
    for field in ("locations_clean", "skill_evidence_clean"):
        value = record[field]
        if value is not None and not isinstance(value, list):
            raise ValueError(f"{field} must be a list or None")
    skills = record["skills_clean"]
    if skills is not None and (not isinstance(skills, list) or not all(isinstance(s, str) for s in skills)):
        raise ValueError("skills_clean must be a list of strings or None")


def _bind_record(record: Mapping[str, Any]) -> dict[str, Any]:
    params = dict(record)
    for column in _JSONB_COLUMNS:
        value = params[column]
        if value is not None:
            # Parameterized text explicitly cast to JSONB in SQL; no ad-hoc interpolation.
            params[column] = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    return params


def upsert_staging_batch(conn: Any, records: Sequence[Mapping[str, Any]]) -> BatchUpsertResult:
    """Write up to 1000 Mapper records atomically using a psycopg 3 connection.

    - Empty batch: no transaction, no SQL.
    - Validate and JSON-encode *before* any DB writes.
    - INSERT new rows; UPDATE changed rows; leave identical rows untouched.
    - On SQL error, conn.transaction() rolls back this batch/savepoint.
    - Caller commits its enclosing transaction (for example, by exiting
      `with psycopg.connect(...) as conn:` normally).

    `affected` counts inserted + actually updated rows. It deliberately does
    not report separate inserted/updated counts without a further SQL query.
    """
    if not isinstance(records, Sequence) or isinstance(records, (str, bytes)):
        raise TypeError("records must be a finite sequence")
    if len(records) > MAX_BATCH_SIZE:
        raise ValueError(f"batch too large: maximum {MAX_BATCH_SIZE}")
    if not records:
        return BatchUpsertResult(submitted=0, affected=0, unchanged=0)

    seen_ids: set[int] = set()
    versions: set[str] = set()
    params_list: list[dict[str, Any]] = []
    for record in records:
        _validate_row(record)
        raw_id = record["raw_job_id"]
        if raw_id in seen_ids:
            raise ValueError(f"duplicate raw_job_id={raw_id} within batch")
        seen_ids.add(raw_id)
        versions.add(record["cleaning_version"])
        params_list.append(_bind_record(record))
    if len(versions) != 1:
        raise ValueError("mixed cleaning_version values in same batch")

    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(UPSERT_STAGING_SQL, params_list)
            affected = cur.rowcount
    if type(affected) is not int or not 0 <= affected <= len(records):
        raise RuntimeError(f"unexpected cursor.rowcount={affected!r} after batch UPSERT")
    return BatchUpsertResult(
        submitted=len(records),
        affected=affected,
        unchanged=len(records) - affected,
    )
