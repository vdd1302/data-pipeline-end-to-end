"""Crawler-facing API: validate captures, then commit short Raw transactions.

Selenium/parser calls happen in the caller, outside these methods. This module
does not start a browser, execute migrations, schedule work, or clean payloads.
"""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import re
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

import yaml

from src.raw_storage.contracts import RawJobPayload, compute_content_hash
from .db import PROJECT_ROOT, get_connection
from . import repository
from .schedule import normalize_schedule_slot, scheduled_run_id


VIETNAM_TZ = timezone(timedelta(hours=7))
_REQUIRED_DETAIL = (
    "job_title_raw", "company_name_raw", "location_raw",
    "job_description_raw", "job_requirements_raw",
)
_TASK_TERMINAL = {"success", "partial", "failed", "skipped", "blocked", "interrupted"}
_RUN_TERMINAL = _TASK_TERMINAL - {"skipped"}
_ERROR_STAGES = {
    "discovery", "fetch", "parse", "load", "navigate_search", "parse_listing",
    "open_detail", "parse_detail", "persist", "checkpoint", "scheduler",
}


def _object(value, label: str) -> dict:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} phải là object/dict.")
    try:
        # Strict JSON copy also rejects NaN, dates, and non-JSON objects before SQL.
        return json.loads(json.dumps(dict(value), ensure_ascii=False, allow_nan=False))
    except (ValueError, TypeError):
        raise ValueError(f"{label} phải chứa dữ liệu JSON hợp lệ.") from None


def _config(value) -> dict:
    cfg = _object(value, "config")
    storage = cfg.get("raw_storage")
    if not isinstance(storage, dict) or (
        storage.get("engine"), storage.get("schema"), storage.get("connection_url_env")
    ) != ("postgresql", "raw", "TOPCV_RAW_DATABASE_URL"):
        raise ValueError("Cấu hình phải dùng PostgreSQL/raw/TOPCV_RAW_DATABASE_URL.")
    expected_tables = {"runs": "crawl_runs", "discoveries": "job_discoveries",
                       "observations": "job_observations", "errors": "crawl_errors"}
    if not isinstance(storage.get("tables"), dict) or any(
        storage["tables"].get(key) != name for key, name in expected_tables.items()
    ):
        raise ValueError("Tên bảng Raw không khớp hợp đồng repository.")
    if not isinstance(cfg.get("source"), dict) or cfg["source"].get("id") != "topcv":
        raise ValueError("RawIngestion hiện chỉ hỗ trợ source topcv.")
    if type(cfg.get("version")) is not int or cfg["version"] < 1:
        raise ValueError("config.version phải là số nguyên dương.")
    if not isinstance(cfg.get("project"), dict) or cfg["project"].get("timezone") not in (
        "Asia/Ho_Chi_Minh", "Asia/Bangkok"
    ):
        raise ValueError("Cần timezone Việt Nam (UTC+07:00) cho snapshot_date.")
    # Snapshot is public configuration only; connection URLs stay in .env.
    def check_public(obj):
        if isinstance(obj, dict):
            for key, child in obj.items():
                if key.casefold() in {"password", "passwd", "token", "secret", "cookies",
                                       "cookie", "database_url", "topcv_raw_database_url",
                                       "topcv_dw_database_url", "api_key", "authorization"}:
                    raise ValueError("Không lưu thông tin bí mật vào config_snapshot.")
                check_public(child)
        elif isinstance(obj, list):
            for child in obj:
                check_public(child)
    check_public(cfg)
    return cfg


def load_crawl_config(path: str | Path = "config/crawl.yaml") -> dict:
    """Read public YAML relative to the project root, independent of caller cwd."""
    target = Path(path)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    with target.open(encoding="utf-8") as stream:
        cfg = yaml.safe_load(stream)
    return _config(cfg)


def _positive(value, name: str) -> int:
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} phải là số nguyên dương.")
    return value


def _text(value, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} phải là chuỗi không rỗng.")
    return value


def _timestamp(value: datetime | None) -> datetime:
    value = datetime.now(timezone.utc) if value is None else value
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Thời điểm capture phải là datetime có timezone.")
    return value.astimezone(timezone.utc)


def _identity(key, url: str | None = None) -> tuple[str, str | None]:
    source_id = str(key) if type(key) is int else key
    if isinstance(source_id, str) and source_id.startswith("topcv:id:"):
        source_id = source_id[len("topcv:id:"):]
    if not isinstance(source_id, str) or not re.fullmatch(r"[0-9]+", source_id):
        raise ValueError("source_job_id phải là ID số của TopCV.")
    if url is None:
        return source_id, None
    parsed = urlsplit(_text(url, "job_url"))
    # SỬA: Brand có đường dẫn riêng nhưng vẫn dùng cùng ID số của TopCV.
    # Giữ nguyên đường dẫn đã quan sát; không tự dựng URL alias /viec-lam/.
    standard_path = re.fullmatch(
        r"/viec-lam/[^?#]+/" + re.escape(source_id) + r"\.html", parsed.path
    )
    brand_path = re.fullmatch(
        r"/brand/[^/?#]+/tuyen-dung/[^/?#]+-j" + re.escape(source_id) + r"\.html",
        parsed.path,
    )
    if (parsed.scheme not in {"http", "https"} or parsed.hostname not in {"topcv.vn", "www.topcv.vn"}
        or parsed.username is not None or parsed.password is not None
        or parsed.port not in {None, 80, 443}
        or (standard_path is None and brand_path is None)):
        raise ValueError("job_url phải là URL việc làm TopCV khớp source_job_id.")
    return source_id, urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _http_status(value):
    if value is not None and (type(value) is not int or not 100 <= value <= 599):
        raise ValueError("http_status phải là None hoặc số nguyên 100..599.")
    return value


class RawIngestion:
    """Crawler API; one atomic transaction per operation, no browser work inside.

    Default connections commit and close on return, roll back and close on error.
    A custom connection_factory may provide a caller-owned transaction for tests.
    SQL migration is explicit; start_run reports a missing schema before writes.
    """

    def __init__(self, *, connection_factory=None):
        self._connect = get_connection if connection_factory is None else connection_factory

    @contextmanager
    def _operation(self):
        with self._connect() as conn:
            with conn.transaction():
                yield conn

    def start_run(self, config: dict | None = None, *, run_id=None,
                  snapshot_date: date | None = None, crawler_version="topcv-selenium-v1",
                  run_purpose="production", observation_method="selenium_live",
                  schedule_key=None, scheduled_for=None) -> UUID:
        cfg = load_crawl_config() if config is None else _config(config)
        schedule_key, scheduled_for = normalize_schedule_slot(schedule_key, scheduled_for)
        if run_id is None:
            run_id = (scheduled_run_id(schedule_key, scheduled_for)
                      if schedule_key is not None else uuid4())
        else:
            run_id = UUID(str(run_id))
        if snapshot_date is None:
            snapshot_time = scheduled_for if scheduled_for is not None else _timestamp(None)
            snapshot_date = snapshot_time.astimezone(VIETNAM_TZ).date()
        if type(snapshot_date) is not date:
            raise ValueError("snapshot_date phải là datetime.date.")
        if run_purpose not in {"production", "test", "manual", "backfill", "unknown"}:
            raise ValueError("run_purpose không hợp lệ.")
        if observation_method not in {"selenium_live", "manual_html", "test", "unknown"}:
            raise ValueError("observation_method không hợp lệ.")
        _text(crawler_version, "crawler_version")
        schedule = ({"schedule_key": schedule_key, "scheduled_for": scheduled_for}
                    if schedule_key is not None else {})
        with self._operation() as conn:
            return repository.create_run(conn, run_id=run_id, config=cfg,
                config_hash=compute_content_hash(cfg), snapshot_date=snapshot_date,
                crawler_version=crawler_version, run_purpose=run_purpose,
                observation_method=observation_method, **schedule)

    def start_task(self, run_id, position: dict, location: dict, search_url: str | None = None) -> int:
        position, location = _object(position, "position"), _object(location, "location")
        for key in ("id", "keyword"):
            _text(position.get(key), "position." + key)
        for key in ("id", "search_label"):
            _text(location.get(key), "location." + key)
        if search_url is not None:
            _text(search_url, "search_url")
        run_id = UUID(str(run_id))
        with self._operation() as conn:
            return repository.create_task(conn, run_id, position, location, search_url)

    def register_carried_discovery(self, run_id, source_job_id, job_url, listing_payload,
                                   discovered_at, *, source_discovery_id=None,
                                   query_city_raw=None, query_category_raw=None):
        """Carry original evidence without fabricating a new listing page."""
        source_id, url = _identity(source_job_id, job_url)
        payload = _object(listing_payload, 'listing_payload')
        if _identity(payload.get('source_job_id'), payload.get('job_url')) != (source_id, url):
            raise ValueError('Carried listing identity does not match its discovery')
        if source_discovery_id is not None:
            _positive(source_discovery_id, 'source_discovery_id')
        if discovered_at is None:
            raise ValueError('The original discovery timestamp is required')
        with self._operation() as conn:
            return repository.register_carried_discovery(conn, run_id=UUID(str(run_id)),
                source_job_id=source_id, job_url=url, listing_payload=payload,
                discovered_at=_timestamp(discovered_at), source_discovery_id=source_discovery_id)

    def save_search_page(self, run_id, task_id, page_number: int, page_url: str | None, items: list,
                         *, attempt_number=1, fetch_status="success", fetched_at=None,
                         page_html=None, parser_version="topcv-listing-v1", http_status=None,
                         source_page_number=None
                         ) -> repository.PageWriteResult:
        """Accept listing dictionaries from src.crawler.parser (or compatible mappings).

        Preserve raw listing items (including duplicate cards); store one discovery
        per logical page/job. Reusing an attempt requires identical capture data.
        """
        _positive(task_id, "task_id")
        _positive(page_number, "page_number")
        _positive(attempt_number, "attempt_number")
        if source_page_number is not None:
            _positive(source_page_number, 'source_page_number')
        if fetch_status not in {"success", "failed", "blocked"}:
            raise ValueError("fetch_status của trang không hợp lệ.")
        if not isinstance(items, list) or (fetch_status != "success" and items):
            raise ValueError("items phải là list; trang failed/blocked không có listing đã xác minh.")
        if page_url is not None:
            _text(page_url, "page_url")
        if page_html is not None and not isinstance(page_html, str):
            raise ValueError("page_html phải là str hoặc None.")
        _text(parser_version, "parser_version")
        capture_time = _timestamp(fetched_at)
        run_id, http_status = UUID(str(run_id)), _http_status(http_status)
        raw_items, discoveries, seen = [], [], set()
        for rank, item in enumerate(items, 1):
            raw = _object(asdict(item) if is_dataclass(item) and not isinstance(item, type) else item, "listing item")
            source_id, url = _identity(raw.get("source_job_id"), raw.get("job_url"))
            if url is None:
                raise ValueError("Listing item phải có job_url.")
            raw_items.append(raw)
            if source_id not in seen:
                discoveries.append({"source_job_id": source_id, "job_url": url,
                                    "rank": rank, "listing_payload": raw})
                seen.add(source_id)
        with self._operation() as conn:
            return repository.save_search_page(conn, run_id=run_id, task_id=task_id,
                page_number=page_number, page_url=page_url, items=raw_items, discoveries=discoveries,
                attempt_number=attempt_number, fetch_status=fetch_status, fetched_at=capture_time,
                page_html=page_html, parser_version=parser_version, http_status=http_status,
                source_page_number=source_page_number)

    def save_job_detail(self, run_id, key: str, detail: RawJobPayload | Mapping,
                        *, job_url: str | None = None, scraped_at=None,
                        parser_version="topcv-detail-parser-v1", detail_html=None,
                        http_status=None, capture_source="selenium_live",
                        source_captured_at=None, is_temporal_eligible=True, allow_partial=False
                        ) -> repository.DetailWriteResult:
        """Save parsed business fields only; identity/HTML/timestamps stay outside.

        key is source_job_id (or topcv:id:<id>). If URL is omitted it is resolved
        from a discovery in this run. Duplicate business content returns the old
        ID with inserted=False and keeps all original capture metadata unchanged.
        """
        source_id, url = _identity(key, job_url)
        payload = _object(detail.to_dict() if isinstance(detail, RawJobPayload) else detail, "detail")
        metadata_fields = {
            "source_job_id", "job_url", "source_code", "crawl_run_id", "run_id",
            "scraped_at", "snapshot_date", "content_hash", "fetch_status",
            "parser_version", "detail_html", "listing_payload",
        }
        if metadata_fields & payload.keys():
            raise ValueError("detail chỉ chứa business fields; truyền identity/capture metadata bằng tham số riêng.")
        if type(allow_partial) is not bool:
            raise ValueError('allow_partial must be boolean')
        for field in (('job_title_raw',) if allow_partial else _REQUIRED_DETAIL):
            _text(payload.get(field), field)
        for field, value in payload.items():
            if field == 'skill_tags_raw':
                if value is not None and (not isinstance(value, list) or any(not isinstance(x, str) for x in value)):
                    raise ValueError('skill_tags_raw must contain text values')
            elif value is not None and not isinstance(value, str):
                raise ValueError('Raw business values must be text or null')
        missing_fields = tuple(field for field in _REQUIRED_DETAIL
                               if not isinstance(payload.get(field), str) or not payload[field].strip())
        _text(parser_version, "parser_version")
        if detail_html is not None and not isinstance(detail_html, str):
            raise ValueError("detail_html phải là str hoặc None.")
        capture_time = _timestamp(scraped_at)
        if type(is_temporal_eligible) is not bool:
            raise ValueError("is_temporal_eligible must be boolean.")
        if capture_source == "selenium_live":
            if not is_temporal_eligible:
                raise ValueError("Live capture must use temporal_eligible=True in this API.")
            source_capture = capture_time if source_captured_at is None else _timestamp(source_captured_at)
            if source_capture != capture_time:
                raise ValueError("Live source_captured_at must match scraped_at.")
        elif capture_source == "historical_saved_html":
            if is_temporal_eligible or source_captured_at is not None:
                raise ValueError("Historical HTML must have unknown capture time and temporal_eligible=False.")
            source_capture = None
        else:
            raise ValueError("Unsupported capture_source.")
        run_id, http_status = UUID(str(run_id)), _http_status(http_status)
        with self._operation() as conn:
            return repository.save_job_detail(conn, run_id=run_id, source_job_id=source_id,
                job_url=url, detail=payload, content_hash=compute_content_hash(payload),
                scraped_at=capture_time, parser_version=parser_version,
                detail_html=detail_html, http_status=http_status,
                capture_source=capture_source, source_captured_at=source_capture,
                is_temporal_eligible=is_temporal_eligible,
                record_level='detail_partial' if missing_fields else 'detail_complete',
                missing_fields=list(missing_fields))

    def finish_task(self, task_id, status="success", *, stop_reason=None) -> None:
        _positive(task_id, "task_id")
        if status not in _TASK_TERMINAL:
            raise ValueError("Trạng thái kết thúc task không hợp lệ.")
        if stop_reason is not None:
            _text(stop_reason, "stop_reason")
        with self._operation() as conn:
            repository.finish_task(conn, task_id, status, stop_reason)

    def finish_run(self, run_id, status="success", *, stop_reason=None) -> repository.RunSummary:
        """Counts reflect registered work, not exhaustive coverage of the market."""
        if status not in _RUN_TERMINAL:
            raise ValueError("Trạng thái kết thúc run không hợp lệ.")
        if stop_reason is not None:
            _text(stop_reason, "stop_reason")
        run_id = UUID(str(run_id))
        with self._operation() as conn:
            return repository.finish_run(conn, run_id, status, stop_reason)

    def log_error(self, run_id, stage: str, error: Exception, *, source_job_id=None,
                  job_url=None, task_id=None, page_number=None, attempt_number=None,
                  retryable=False, error_kind=None, retry_after_at=None, context=None) -> int:
        """Record error type/SQLSTATE, not str(error), SQL params, or credentials."""
        if stage not in _ERROR_STAGES or not isinstance(error, Exception):
            raise ValueError("Cần stage hợp lệ và Exception để ghi lỗi.")
        if type(retryable) is not bool:
            raise ValueError("retryable phải là bool.")
        if error_kind is not None and (not isinstance(error_kind, str) or not
                re.fullmatch(r'[a-z][a-z0-9_]{0,63}', error_kind)):
            raise ValueError('error_kind must be a public reason code')
        if retry_after_at is not None and (not isinstance(retry_after_at, datetime) or
                retry_after_at.tzinfo is None):
            raise ValueError('retry_after_at must be timezone aware')
        public_context = _object(context or {}, 'error context')
        allowed = {'phase', 'position_id', 'location_id', 'checkpoint_page', 'reason_code', 'http_status',
                   'cf_ray', 'cf_mitigated', 'body_sha256', 'response_class'}
        if set(public_context) - allowed or any(
                type(value) not in (str, int, bool, type(None)) or
                (isinstance(value, str) and (len(value) > 100 or
                 not re.fullmatch(r'[A-Za-z0-9_ .-]*', value)))
                for value in public_context.values()):
            raise ValueError('Error context only accepts bounded public diagnostic codes')
        for label, value in (("task_id", task_id), ("page_number", page_number), ("attempt_number", attempt_number)):
            if value is not None:
                _positive(value, label)
        if source_job_id is not None:
            source_job_id, job_url = _identity(source_job_id, job_url)
        elif job_url is not None:
            raise ValueError("Cần source_job_id khi ghi job_url của lỗi detail.")
        run_id = UUID(str(run_id))
        error_type = type(error).__name__
        state = getattr(error, "sqlstate", None)
        message = error_type + (f" (SQLSTATE {state})" if isinstance(state, str) and re.fullmatch(r"[A-Z0-9]{5}", state) else "")
        with self._operation() as conn:
            return repository.log_error(conn, run_id=run_id, stage=stage,
                message=message, error_type=error_type, source_job_id=source_job_id,
                job_url=job_url, task_id=task_id, page_number=page_number,
                attempt_number=attempt_number, retryable=retryable,
                error_kind=error_kind, retry_after_at=retry_after_at, context=public_context)
