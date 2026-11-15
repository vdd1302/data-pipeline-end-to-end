"""M5c: import a locally verified M5b discovery into Raw PostgreSQL.

No Selenium, no HTTP, no detail writes. Local checkpoint is structurally checked,
not cryptographically authenticated. One DB transaction per explicit commit.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
from uuid import UUID, uuid5
import hashlib
import json

from src.crawler.pipeline.detail import load_discovery_manifest
from src.crawler.pipeline.discovery import _fold, verified_route_for
from src.raw_storage.contracts import compute_content_hash
from src.raw_storage.ingestion import _config
from src.raw_storage import repository
from src.raw_storage.db import get_connection
from src.raw_storage.schedule import normalize_schedule_slot, scheduled_run_id

NAMESPACE = UUID('2d2cc696-18d3-4056-81e9-1ad2581add2f')
MAX_FILE = 4 * 1024 * 1024


class CheckpointImportError(ValueError):
    """Untrusted/ambiguous checkpoint or conflicting already-committed DB run."""


def _load(path: Path) -> dict:
    if not path.is_file() or not 0 < path.stat().st_size <= MAX_FILE:
        raise CheckpointImportError('Missing, empty, or oversized JSON checkpoint (>4 MiB).')
    try:
        data = json.loads(path.read_text(encoding='utf-8-sig'))
    except (UnicodeError, ValueError) as exc:
        raise CheckpointImportError('Invalid UTF-8 JSON checkpoint.') from exc
    if not isinstance(data, dict):
        raise CheckpointImportError('Checkpoint must be a JSON object.')
    return data


def _when(value: str) -> datetime:
    if not isinstance(value, str):
        raise CheckpointImportError('Missing source capture timestamp.')
    try:
        date = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError as exc:
        raise CheckpointImportError('Invalid source timestamp.') from exc
    if date.tzinfo is None or date.utcoffset() is None:
        raise CheckpointImportError('Source timestamp must have a timezone.')
    return date.astimezone(timezone.utc)


@dataclass(frozen=True)
class ImportPlan:
    run_id: UUID
    route: str
    snapshot_date: object
    captured_at: datetime
    items: list[dict]
    discoveries: list[dict]
    scope: str
    content_sha256: str
    source_report: str
    source_discovery: str
    run_purpose: str = 'manual'
    schedule_key: str | None = None
    scheduled_for: datetime | None = None


def with_schedule(plan: ImportPlan, *, schedule_key, scheduled_for,
                  run_purpose='production') -> ImportPlan:
    """Bind validated evidence to its scheduled slot, refusing stale captures.

    A checkpoint digest identifies historical imports. A scheduled run instead
    uses the scope-qualified schedule key and due time, so clearing a task cannot
    silently turn the same period into another Raw observation.
    """
    key, due = normalize_schedule_slot(schedule_key, scheduled_for)
    if key is None or run_purpose not in {'production', 'test'}:
        raise CheckpointImportError('Scheduled discovery requires a slot and production/test purpose.')
    if not due <= plan.captured_at < due + timedelta(days=1):
        raise CheckpointImportError('Discovery capture is outside the scheduled daily slot.')
    return replace(plan, run_id=scheduled_run_id(key, due), run_purpose=run_purpose,
                   schedule_key=key, scheduled_for=due)


def build_import_plan(*, cfg, task, m5b_report: Path, discovery_path: Path) -> ImportPlan:
    """No database/network I/O; validate one completed M5b listing capture."""
    m5b_report, discovery_path = Path(m5b_report).resolve(), Path(discovery_path).resolve()
    report = _load(m5b_report)
    discovery, jobs, fingerprint = load_discovery_manifest(discovery_path, task)
    if (report.get('mode') != 'M5b_unified_calibration_discovery_no_db'
            or report.get('status') != 'discovery_captured'
            or report.get('first_page_reused') is not True
            or report.get('discovery_status') != discovery['status']
            or report.get('pages_captured') != len(discovery['pages'])
            or report.get('unique_job_ids') != len(jobs)):
        raise CheckpointImportError('M5b report and discovery metadata disagree.')
    if (not isinstance(report.get('discovery_path'), str)
            or Path(report['discovery_path']).name != discovery_path.name):
        raise CheckpointImportError('M5b report points to a different discovery checkpoint.')
    if report.get('database_writes') != 0 or report.get('detail_requests') != 0:
        raise CheckpointImportError('Expected a no-database, no-detail M5b source.')
    calibration = report.get('calibration')
    route = verified_route_for(task.position_id, task.location_id, config_path=cfg.config_path)
    if (not isinstance(calibration, dict) or calibration.get('status') != 'live_verified'
            or any(calibration.get(k) is not True for k in (
                'route_exact', 'scope_verified', 'all_cards_city_verified'))
            or type(calibration.get('parsed_cards')) is not int
            or calibration.get('parsed_cards') != len(jobs)
            or calibration.get('expected_route') != route
            or discovery.get('verified_entry_url') != route):
        raise CheckpointImportError('Live calibration proof or registered route mismatch.')
    meta = report.get('task')
    if not isinstance(meta, dict) or any((meta.get(k) != v) for k, v in (
        ('position_id', task.position_id), ('location_id', task.location_id),
        ('keyword', task.keyword), ('city', task.search_label))):
        raise CheckpointImportError('Wrong position/city in M5b report.')
    if (discovery['status'] not in ('no_verified_next_link', 'max_pages_reached')
            or len(discovery['pages']) != 1 or not jobs):
        raise CheckpointImportError('M5c supports one fully verified listing page only.')
    page = discovery['pages'][0]
    if page.get('page') != 1 or page.get('url') != route or type(page.get('cards')) is not int:
        raise CheckpointImportError('First page audit missing or wrong URL.')
    if (page['cards'] != len(jobs)
            or page.get('unique_ids_on_page') != len(jobs)
            or page.get('new_unique_jobs') != len(jobs)
            or discovery.get('duplicate_cards_within_run') != 0):
        raise CheckpointImportError('Cannot reconstruct original card count: first page mismatch.')
    if page.get('pagination_status') != discovery['status'] and discovery['status'] != 'max_pages_reached':
        raise CheckpointImportError('Discovery stop status conflicts with observed pagination.')
    captured_at = _when(page.get('captured_at_utc'))
    start, end = _when(report.get('started_at_utc')), _when(report.get('updated_at_utc'))
    if not (start <= captured_at <= end):
        raise CheckpointImportError('Capture timestamp outside the browser session.')
    if _when(calibration.get('audited_at_utc')) < captured_at:
        raise CheckpointImportError('Calibration audit predates page capture unexpectedly.')

    ranks = set()
    ordered = []
    for job in jobs:
        rank = next((j.get('first_position') for j in discovery['jobs']
                     if j.get('source_job_id') == job['source_job_id']), None)
        if type(rank) is not int or rank < 1 or rank > len(jobs) or rank in ranks:
            raise CheckpointImportError('Missing or inconsistent original card position.')
        ranks.add(rank)
        listing = job.get('listing') or {}
        if not isinstance(listing, dict) or not listing:
            raise CheckpointImportError('M5c needs M5b structured listing metadata.')
        if _fold(task.search_label) not in _fold(listing.get('location_raw') or ''):
            raise CheckpointImportError('Listing city does not match verified scope.')
        item = dict(listing)
        item.update({'source_job_id': job['source_job_id'], 'job_url': job['job_url'],
                     'result_position': rank, 'checkpoint_capture_origin': 'm5b_verified_listing',
                     'checkpoint_captured_at_utc': captured_at.isoformat()})
        ordered.append((rank, item))
    ordered.sort(key=lambda entry: entry[0])
    items = [item for _, item in ordered]
    discoveries = [{'source_job_id': item['source_job_id'], 'job_url': item['job_url'],
                    'rank': item['result_position'], 'listing_payload': item} for item in items]
    hash_value = hashlib.sha256(json.dumps({
        'manifest_sha256': fingerprint, 'captured_at_utc': captured_at.isoformat(),
        'items': items, 'route': route,
    }, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode('utf-8')).hexdigest()
    run_id = uuid5(NAMESPACE, f'm5c_listing_checkpoint:{hash_value}')
    return ImportPlan(run_id, route, captured_at.astimezone(cfg.timezone).date(),
                      captured_at, items, discoveries, f'{task.position_id}/{task.location_id}',
                      hash_value, str(m5b_report), str(discovery_path))


def _read_existing(conn, plan: ImportPlan, *, cfg, task) -> dict:
    run = conn.execute('SELECT * FROM raw.crawl_runs WHERE crawl_run_id=%s', (plan.run_id,)).fetchone()
    if run is None:
        return {'state': 'not_imported', 'existing_discoveries': 0}
    if (run['run_status'] != 'partial' or run['run_purpose'] != plan.run_purpose
            or run['observation_method'] != 'selenium_live'
            or run['snapshot_date'] != plan.snapshot_date
            or run['config_sha256'] != compute_content_hash(_config(cfg.snapshot))
            or run['crawler_version'] != 'topcv-m5c-checkpoint-import-v1'
            or run['total_discovered'] != len(plan.items)
            or run['total_fetched'] != 0 or run['total_failed'] != 0
            or run.get('schedule_key') != plan.schedule_key
            or run.get('scheduled_for') != plan.scheduled_for):
        raise CheckpointImportError('Existing run_id has conflicting state/metadata.')
    tasks = conn.execute('SELECT * FROM raw.search_tasks WHERE crawl_run_id=%s', (plan.run_id,)).fetchall()
    if (len(tasks) != 1 or tasks[0]['position_id'] != task.position_id
            or tasks[0]['location_id'] != task.location_id or tasks[0]['search_url'] != plan.route
            or tasks[0]['status'] != 'success'):
        raise CheckpointImportError('Existing Raw task conflicts with checkpoint.')
    pages = conn.execute('SELECT * FROM raw.search_pages WHERE crawl_run_id=%s', (plan.run_id,)).fetchall()
    if (len(pages) != 1 or pages[0]['page_number'] != 1
            or pages[0]['attempt_number'] != 1 or pages[0]['page_url'] != plan.route
            or pages[0]['fetch_status'] != 'success' or pages[0]['fetched_at'] != plan.captured_at
            or pages[0]['listing_items'] != plan.items or pages[0]['page_html'] is not None
            or pages[0]['parser_version'] != 'topcv-m5b-reconstructed-listing-v1'):
        raise CheckpointImportError('Existing Raw page differs from verified checkpoint.')
    discoveries = conn.execute('''SELECT source_job_id, job_url, result_position, listing_payload
                               FROM raw.job_discoveries WHERE crawl_run_id=%s''', (plan.run_id,)).fetchall()
    expected = {d['source_job_id']: d for d in plan.discoveries}
    if (len(discoveries) != len(expected) or any(
            d['source_job_id'] not in expected or
            d['job_url'] != expected[d['source_job_id']]['job_url'] or
            d['result_position'] != expected[d['source_job_id']]['rank'] or
            d['listing_payload'] != expected[d['source_job_id']]['listing_payload']
            for d in discoveries)):
        raise CheckpointImportError('Existing Raw discoveries differ from checkpoint.')
    obs = conn.execute('SELECT count(*) AS n FROM raw.job_observations WHERE crawl_run_id=%s', (plan.run_id,)).fetchone()['n']
    if obs:
        raise CheckpointImportError('Discovery-only run unexpectedly contains detail observations.')
    return {'state': 'already_imported_identical', 'existing_discoveries': len(expected)}


def check_database(plan: ImportPlan, *, cfg, task, connection_factory=get_connection) -> dict:
    """Read-only inspection; never start or alter any run."""
    with connection_factory(read_only=True) as conn:
        return _read_existing(conn, plan, cfg=cfg, task=task)


def commit_checkpoint(plan: ImportPlan, *, cfg, task, connection_factory=get_connection) -> dict:
    """One PostgreSQL transaction; atomic or full rollback. No HTTP/browser."""
    with connection_factory() as conn:
        with conn.transaction():
            # Lock is scoped to this transaction; concurrent same run waits.
            conn.execute('SELECT pg_advisory_xact_lock(%s)', (plan.run_id.int % (2**63 - 1),))
            existing = _read_existing(conn, plan, cfg=cfg, task=task)
            if existing['state'] == 'already_imported_identical':
                return existing
            schedule = ({'schedule_key': plan.schedule_key, 'scheduled_for': plan.scheduled_for}
                        if plan.schedule_key is not None else {})
            actual_run = repository.create_run(conn, run_id=plan.run_id,
                config=_config(cfg.snapshot), config_hash=compute_content_hash(_config(cfg.snapshot)),
                snapshot_date=plan.snapshot_date, crawler_version='topcv-m5c-checkpoint-import-v1',
                run_purpose=plan.run_purpose, observation_method='selenium_live', **schedule)
            if plan.schedule_key is not None and actual_run != plan.run_id:
                raise CheckpointImportError('Scheduled slot belongs to another Raw run; reconcile before import.')
            task_id = repository.create_task(conn, plan.run_id, task.position, task.location, plan.route)
            page = repository.save_search_page(conn, run_id=plan.run_id, task_id=task_id,
                page_number=1, page_url=plan.route, items=plan.items, discoveries=plan.discoveries,
                attempt_number=1, fetch_status='success', fetched_at=plan.captured_at,
                page_html=None, parser_version='topcv-m5b-reconstructed-listing-v1', http_status=None)
            if page.discoveries_inserted != len(plan.discoveries):
                raise CheckpointImportError('DB inserted unexpected number of discovery identities.')
            repository.finish_task(conn, task_id, 'success', 'discovery_only_no_detail')
            summary = repository.finish_run(conn, plan.run_id, 'partial',
                                            'verified_listing_checkpoint_import_no_detail')
            if (summary.total_discovered != len(plan.items) or summary.total_fetched != 0
                    or summary.total_failed != 0):
                raise CheckpointImportError('DB summary mismatch; transaction rolled back.')
            return {'state': 'committed_discovery_only', 'existing_discoveries': len(plan.items)}
