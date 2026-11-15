"""SQL for crawler -> Raw storage; the caller owns the connection/transaction.

No connections, commits, migrations, Selenium calls, or automatic retries here.
All identifiers are fixed; data is always passed through bound parameters.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta, timezone
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from .schedule import normalize_schedule_slot, scheduled_run_id


class RawStorageConflictError(ValueError):
    """A replay changed captured data, or a write targets finished work."""


@dataclass(frozen=True)
class PageWriteResult:
    page_id: int
    inserted: bool
    discoveries_inserted: int
    last_completed_page: int


@dataclass(frozen=True)
class DetailWriteResult:
    raw_job_id: int
    inserted: bool


@dataclass(frozen=True)
class RunSummary:
    crawl_run_id: UUID
    status: str
    total_discovered: int
    total_fetched: int
    total_failed: int


def _run(conn, run_id: UUID, *, open_only: bool = True) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM raw.crawl_runs WHERE crawl_run_id = %s FOR UPDATE", (run_id,)
    ).fetchone()
    if row is None:
        raise LookupError("Crawl run không tồn tại.")
    if open_only and row["run_status"] != "running":
        raise RawStorageConflictError("Không ghi thêm dữ liệu vào crawl run đã kết thúc.")
    return row


def _task(conn, run_id: UUID, task_id: int) -> dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM raw.search_tasks WHERE task_id = %s AND crawl_run_id = %s FOR UPDATE",
        (task_id, run_id),
    ).fetchone()
    if row is None:
        raise LookupError("Search task không tồn tại hoặc thuộc run khác.")
    return row


def check_schema(conn, *, require_schedule=False) -> None:
    """Fail before writes if the reviewed additive SQL migration is missing."""
    required = {
        "crawl_runs": {"crawl_run_id", "snapshot_date", "config_snapshot", "config_sha256", "run_purpose", "observation_method"},
        "search_tasks": {"task_id", "crawl_run_id", "last_completed_page", "status"},
        "search_pages": {"page_id", "attempt_number", "listing_items", "fetch_status"},
        "job_discoveries": {"discovery_id", "task_id", "page_id", "listing_payload"},
        "job_observations": {"raw_job_id", "raw_payload", "content_hash", "detail_html", "capture_source", "source_captured_at", "is_temporal_eligible"},
        "crawl_errors": {"error_id", "error_kind", "task_id"},
    }
    if require_schedule:
        required['crawl_runs'] |= {'schedule_key', 'scheduled_for'}
    actual: dict[str, set[str]] = {}
    for row in conn.execute(
        "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'raw'"
    ):
        actual.setdefault(row["table_name"], set()).add(row["column_name"])
    if any(not columns <= actual.get(table, set()) for table, columns in required.items()):
        raise RuntimeError(
            "Schema Raw chưa được nâng cấp. Chạy sql/create_topcv_raw_storage_schema.sql "
            "trên database Raw trước khi dùng RawIngestion."
        )


def create_run(conn, *, run_id, config, config_hash, snapshot_date,
               crawler_version, run_purpose, observation_method,
               schedule_key=None, scheduled_for=None) -> UUID:
    schedule_key, scheduled_for = normalize_schedule_slot(schedule_key, scheduled_for)
    if schedule_key is not None:
        return _create_scheduled_run(conn, run_id=run_id, config=config,
            config_hash=config_hash, snapshot_date=snapshot_date,
            crawler_version=crawler_version, run_purpose=run_purpose,
            observation_method=observation_method, schedule_key=schedule_key,
            scheduled_for=scheduled_for)
    check_schema(conn)
    row = conn.execute(
        """INSERT INTO raw.crawl_runs
            (crawl_run_id, source_code, started_at, snapshot_date, run_status,
             config_version, config_snapshot, config_sha256, crawler_version,
             run_purpose, observation_method)
        VALUES (%s, 'topcv', now(), %s, 'running', %s, %s, %s, %s, %s, %s)
        ON CONFLICT (crawl_run_id) DO NOTHING RETURNING crawl_run_id""",
        (run_id, snapshot_date, config["version"], Jsonb(config), config_hash,
         crawler_version, run_purpose, observation_method),
    ).fetchone()
    if row:
        return row["crawl_run_id"]
    existing = _run(conn, run_id, open_only=False)
    if (existing["config_sha256"], existing["snapshot_date"], existing["crawler_version"],
        existing["run_purpose"], existing["observation_method"],
        existing.get('schedule_key'), existing.get('scheduled_for')) != (
        config_hash, snapshot_date, crawler_version, run_purpose, observation_method,
        None, None
    ):
        raise RawStorageConflictError("run_id đã tồn tại với cấu hình/metadata khác.")
    return run_id


def _create_scheduled_run(conn, *, run_id, config, config_hash, snapshot_date,
                          crawler_version, run_purpose, observation_method,
                          schedule_key, scheduled_for) -> UUID:
    """Serialize one slot and reuse its existing run without mutating its data.

    The SQL unique slot index also protects against external writers. A replay
    may name another UUID, but it must agree on all immutable run metadata.
    """
    check_schema(conn, require_schedule=True)
    lock_id = scheduled_run_id(schedule_key, scheduled_for).int % (2**63 - 1)
    conn.execute('SELECT pg_advisory_xact_lock(%s)', (lock_id,))

    def find_slot():
        return conn.execute(
            """SELECT * FROM raw.crawl_runs
               WHERE source_code = 'topcv' AND schedule_key = %s
                 AND scheduled_for = %s FOR UPDATE""",
            (schedule_key, scheduled_for),
        ).fetchone()

    existing = find_slot()
    if existing is None:
        inserted = conn.execute(
            """INSERT INTO raw.crawl_runs
                (crawl_run_id, source_code, started_at, snapshot_date, run_status,
                 config_version, config_snapshot, config_sha256, crawler_version,
                 run_purpose, observation_method, schedule_key, scheduled_for)
            VALUES (%s, 'topcv', now(), %s, 'running', %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING RETURNING crawl_run_id""",
            (run_id, snapshot_date, config['version'], Jsonb(config), config_hash,
             crawler_version, run_purpose, observation_method, schedule_key, scheduled_for),
        ).fetchone()
        if inserted is not None:
            return inserted['crawl_run_id']
        existing = find_slot()
        if existing is None:
            # The requested UUID was already used for another slot/manual run.
            raise RawStorageConflictError('run_id is already assigned to different work.')
    actual = (
        existing['source_code'], existing['config_sha256'], existing['snapshot_date'],
        existing['crawler_version'], existing['run_purpose'], existing['observation_method'],
        existing.get('schedule_key'), existing.get('scheduled_for'),
    )
    expected = ('topcv', config_hash, snapshot_date, crawler_version, run_purpose,
                observation_method, schedule_key, scheduled_for)
    if actual != expected:
        raise RawStorageConflictError('Schedule slot already exists with different configuration or metadata.')
    return existing['crawl_run_id']


def create_task(conn, run_id, position, location, search_url=None) -> int:
    _run(conn, run_id)
    row = conn.execute(
        """INSERT INTO raw.search_tasks
            (crawl_run_id, position_id, position_keyword, location_id, location_label,
             search_url, status, started_at, attempt_count)
        VALUES (%s, %s, %s, %s, %s, %s, 'running', now(), 1)
        ON CONFLICT (crawl_run_id, position_id, location_id) DO NOTHING RETURNING task_id""",
        (run_id, position["id"], position["keyword"], location["id"], location["search_label"], search_url),
    ).fetchone()
    if row:
        return row["task_id"]
    existing = conn.execute(
        """SELECT * FROM raw.search_tasks
        WHERE crawl_run_id = %s AND position_id = %s AND location_id = %s FOR UPDATE""",
        (run_id, position["id"], location["id"]),
    ).fetchone()
    if (existing["position_keyword"], existing["location_label"], existing["search_url"]) != (
        position["keyword"], location["search_label"], search_url
    ):
        raise RawStorageConflictError("Task đã tồn tại với keyword/location/URL khác.")
    return existing["task_id"]


def save_search_page(conn, *, run_id, task_id, page_number, page_url,
                     items, discoveries, attempt_number, fetch_status,
                     fetched_at, page_html, parser_version, http_status,
                     source_page_number=None) -> PageWriteResult:
    _run(conn, run_id)
    task = _task(conn, run_id, task_id)
    existing = conn.execute(
        "SELECT * FROM raw.search_pages WHERE task_id = %s AND page_number = %s AND attempt_number = %s",
        (task_id, page_number, attempt_number),
    ).fetchone()
    capture = (page_url, fetch_status, items, page_html, parser_version, http_status)
    if existing:
        original = tuple(existing[key] for key in
                         ("page_url", "fetch_status", "listing_items", "page_html", "parser_version", "http_status"))
        if original != capture:
            raise RawStorageConflictError("Attempt đã có capture khác; dùng attempt_number mới cho lần tải mới.")
        if source_page_number is not None and (existing.get('source_page_number') or page_number) != source_page_number:
            raise RawStorageConflictError('Source page number differs from the saved capture')
        return PageWriteResult(existing["page_id"], False, 0, task["last_completed_page"])
    if task["status"] != "running":
        raise RawStorageConflictError("Task đã kết thúc; không thêm page attempt mới.")
    checkpoint = task["last_completed_page"]
    if fetch_status == "success" and page_number > checkpoint + 1:
        raise ValueError("Không được bỏ qua trang khi cập nhật checkpoint.")
    page_id = conn.execute(
        """INSERT INTO raw.search_pages
            (crawl_run_id, task_id, page_number, attempt_number, page_url, fetch_status,
             fetched_at, listing_count, listing_items, page_html, parser_version, http_status,
             source_page_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING page_id""",
        (run_id, task_id, page_number, attempt_number, page_url, fetch_status,
         fetched_at, len(items), Jsonb(items), page_html, parser_version, http_status,
         source_page_number if source_page_number is not None else page_number),
    ).fetchone()["page_id"]
    inserted = 0
    if fetch_status == "success":
        for item in discoveries:
            row = conn.execute(
                """INSERT INTO raw.job_discoveries
                    (crawl_run_id, source_job_id, job_url, query_city_raw, query_category_raw,
                     search_page_no, result_position, task_id, page_id, listing_payload, discovered_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id, search_page_no, source_job_id) WHERE task_id IS NOT NULL
                DO NOTHING RETURNING discovery_id""",
                (run_id, item["source_job_id"], item["job_url"], task["location_label"],
                 task["position_keyword"], page_number, item["rank"], task_id,
                 page_id, Jsonb(item["listing_payload"]), fetched_at),
            ).fetchone()
            inserted += row is not None
        checkpoint = max(checkpoint, page_number)
        conn.execute("UPDATE raw.search_tasks SET last_completed_page = %s WHERE task_id = %s",
                     (checkpoint, task_id))
    return PageWriteResult(page_id, True, inserted, checkpoint)


def register_carried_discovery(conn, *, run_id, source_job_id, job_url, listing_payload,
                               discovered_at, source_discovery_id=None) -> int:
    """Register original discovery evidence, with no artificial page/checkpoint."""
    _run(conn, run_id)
    existing = conn.execute('SELECT discovery_id,job_url FROM raw.job_discoveries '
        'WHERE crawl_run_id=%s AND source_job_id=%s ORDER BY discovery_id LIMIT 1',
        (run_id,source_job_id)).fetchone()
    if existing:
        if existing['job_url'] != job_url:
            raise RawStorageConflictError('Carried discovery URL conflicts with this run')
        return existing['discovery_id']
    origin = conn.execute('''SELECT d.* FROM raw.job_discoveries d
        JOIN raw.crawl_runs r USING(crawl_run_id)
        WHERE d.source_job_id=%s AND d.job_url=%s AND d.discovered_at=%s
          AND d.listing_payload=%s AND r.source_code='topcv'
          AND r.run_purpose='production' AND r.observation_method='selenium_live'
          AND (%s::bigint IS NULL OR d.discovery_id=%s)
        ORDER BY d.discovery_id LIMIT 1''',
        (source_job_id,job_url,discovered_at,Jsonb(listing_payload),
         source_discovery_id,source_discovery_id)).fetchone()
    if origin is None:
        raise RawStorageConflictError('Original discovery evidence is unavailable')
    return conn.execute('''INSERT INTO raw.job_discoveries
        (crawl_run_id,source_job_id,job_url,query_city_raw,query_category_raw,
         listing_payload,discovered_at) VALUES(%s,%s,%s,%s,%s,%s,%s)
        RETURNING discovery_id''',
        (run_id,source_job_id,job_url,origin['query_city_raw'],origin['query_category_raw'],
         Jsonb(listing_payload),discovered_at)).fetchone()['discovery_id']


def save_job_detail(conn, *, run_id, source_job_id, job_url, detail, content_hash,
                    scraped_at, parser_version, detail_html, http_status,
                    capture_source, source_captured_at, is_temporal_eligible,
                    record_level='detail_complete', missing_fields=None) -> DetailWriteResult:
    run = _run(conn, run_id)
    # Không tạo observation pending. Discovery của run là nguồn xác định job.
    discovery = conn.execute(
        """SELECT job_url FROM raw.job_discoveries
        WHERE crawl_run_id = %s AND source_job_id = %s ORDER BY discovery_id LIMIT 1""",
        (run_id, source_job_id),
    ).fetchone()
    if discovery is None:
        raise LookupError("Phải lưu discovery trước khi lưu detail của job.")
    job_url = job_url or discovery["job_url"]
    if scraped_at.astimezone(timezone(timedelta(hours=7))).date() != run["snapshot_date"]:
        raise ValueError("Ngày capture khác snapshot_date của run; tạo run mới khi chuyển ngày.")
    row = conn.execute(
        """INSERT INTO raw.job_observations
            (crawl_run_id, source_job_id, job_url, snapshot_date, scraped_at,
             content_hash, fetch_status, parser_version, raw_payload, detail_html, http_status,
             capture_source, source_captured_at, is_temporal_eligible,record_level,missing_fields)
        VALUES (%s, %s, %s, %s, %s, %s, 'success', %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (crawl_run_id, source_job_id) DO NOTHING RETURNING raw_job_id""",
        (run_id, source_job_id, job_url, run["snapshot_date"], scraped_at,
         content_hash, parser_version, Jsonb(detail), detail_html, http_status,
         capture_source, source_captured_at, is_temporal_eligible,record_level,missing_fields or []),
    ).fetchone()
    if row:
        return DetailWriteResult(row["raw_job_id"], True)
    existing = conn.execute(
        """SELECT raw_job_id, raw_payload, content_hash, fetch_status, capture_source,
               source_captured_at, is_temporal_eligible FROM raw.job_observations
        WHERE crawl_run_id = %s AND source_job_id = %s""", (run_id, source_job_id),
    ).fetchone()
    if (existing["fetch_status"] != "success" or existing["content_hash"] != content_hash
            or existing["raw_payload"] != detail
            or existing["capture_source"] != capture_source
            or existing["source_captured_at"] != source_captured_at
            or existing["is_temporal_eligible"] != is_temporal_eligible):
        raise RawStorageConflictError("Observation cùng run đã có dữ liệu khác; không ghi đè lịch sử.")
    return DetailWriteResult(existing["raw_job_id"], False)


def finish_task(conn, task_id, status, stop_reason=None) -> None:
    parent = conn.execute("SELECT crawl_run_id FROM raw.search_tasks WHERE task_id = %s", (task_id,)).fetchone()
    if parent is None:
        raise LookupError("Search task không tồn tại.")
    run_id = parent["crawl_run_id"]
    _run(conn, run_id)
    task = _task(conn, run_id, task_id)
    if task["status"] == status and task["finished_at"] is not None:
        return
    if task["status"] != "running":
        raise RawStorageConflictError("Không thay đổi trạng thái task đã kết thúc.")
    if status == "success" and task["last_completed_page"] == 0:
        raise ValueError("Task success phải có ít nhất một trang listing đã lưu thành công.")
    conn.execute(
        "UPDATE raw.search_tasks SET status = %s, finished_at = now(), stop_reason = %s WHERE task_id = %s",
        (status, stop_reason, task_id),
    )


def finish_run(conn, run_id, status, stop_reason=None) -> RunSummary:
    run = _run(conn, run_id, open_only=False)
    if run["run_status"] != "running":
        if run["run_status"] != status:
            raise RawStorageConflictError("Không thay đổi trạng thái run đã kết thúc.")
        return RunSummary(run_id, status, run["total_discovered"], run["total_fetched"], run["total_failed"])
    counts = conn.execute(
        """WITH discovered AS (
            SELECT DISTINCT source_job_id FROM raw.job_discoveries WHERE crawl_run_id = %(run)s
        ), captured AS (
            SELECT source_job_id FROM raw.job_observations
            WHERE crawl_run_id = %(run)s AND fetch_status = 'success'
        ), failed AS (
            SELECT source_job_id FROM raw.crawl_errors
            WHERE crawl_run_id = %(run)s AND source_job_id IS NOT NULL
              AND stage IN ('fetch', 'parse', 'load', 'open_detail', 'parse_detail', 'persist')
            UNION SELECT source_job_id FROM raw.job_observations
            WHERE crawl_run_id = %(run)s AND fetch_status IN ('failed', 'partial')
        ) SELECT
            (SELECT count(*) FROM discovered) AS discovered,
            (SELECT count(*) FROM captured) AS fetched,
            (SELECT count(*) FROM failed f WHERE NOT EXISTS
                (SELECT 1 FROM captured c WHERE c.source_job_id = f.source_job_id)) AS failed,
            (SELECT count(*) FROM discovered d WHERE NOT EXISTS
                (SELECT 1 FROM captured c WHERE c.source_job_id = d.source_job_id)) AS uncaptured,
            (SELECT count(*) FROM raw.search_tasks WHERE crawl_run_id = %(run)s) AS tasks,
            (SELECT count(*) FROM raw.search_tasks WHERE crawl_run_id = %(run)s
                AND status NOT IN ('success', 'skipped')) AS unfinished_tasks""", {"run": run_id},
    ).fetchone()
    if status == "success" and (not counts["tasks"] or counts["unfinished_tasks"]
                                or counts["uncaptured"] or counts["failed"]):
        raise ValueError("Run còn task/job chưa hoàn tất; không thể đánh dấu success.")
    conn.execute(
        """UPDATE raw.crawl_runs SET run_status = %s, finished_at = now(), stop_reason = %s,
            total_discovered = %s, total_fetched = %s, total_failed = %s WHERE crawl_run_id = %s""",
        (status, stop_reason, counts["discovered"], counts["fetched"], counts["failed"], run_id),
    )
    return RunSummary(run_id, status, counts["discovered"], counts["fetched"], counts["failed"])


def log_error(conn, *, run_id, stage, message, error_type, source_job_id=None,
              job_url=None, task_id=None, page_number=None, attempt_number=None,
              retryable=False, error_kind=None, retry_after_at=None, context=None) -> int:
    _run(conn, run_id)
    if task_id is not None:
        _task(conn, run_id, task_id)
    return conn.execute(
        """INSERT INTO raw.crawl_errors
            (crawl_run_id, stage, error_message, error_type, source_job_id, job_url,
             task_id, page_number, attempt_number, retryable, error_kind, retry_after_at, context)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING error_id""",
        (run_id, stage, message, error_type, source_job_id, job_url,
         task_id, page_number, attempt_number, retryable, error_kind, retry_after_at, Jsonb(context or {})),
    ).fetchone()["error_id"]
