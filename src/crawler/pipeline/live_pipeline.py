"""M4b: single-session verified TopCV listing -> detail -> Raw PostgreSQL.

Only the exact audited public route is allowed. This is a LIVE Selenium
runner, independent of M4a's historical saved-HTML writer. Each verified
listing page commits atomically using RawIngestion; each verified detail
commits separately. No challenge retry, invented URLs or implied coverage.

A new invocation produces a new run ID. Crash recovery reads the database's
committed rows; automatic same-run resume is NOT implemented in M4b.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import time
from uuid import UUID, uuid4

from selenium.common.exceptions import WebDriverException

from src.crawler.parser import AccessChallenge, LayoutChanged
from src.crawler.pipeline.t1_topcv_adapter import adapt_topcv_detail
from src.crawler.topcv import NavigationError
from src.crawler.pipeline.discovery import verified_route_for, run_discovery, VerifiedPageSinkError, CalibratedFirstPage
from src.crawler.pipeline.detail import load_discovery_manifest, DiscoveryValidationError, _write_json
from src.raw_storage.ingestion import RawIngestion
from src.raw_storage.schedule import normalize_schedule_slot, scheduled_run_id


class LivePipelineError(RuntimeError):
    """The run cannot safely accept more live data."""


def validate_live_plan(task, cfg, *, max_pages: int, max_jobs: int) -> str:
    """No browser, network or database access. Refuse unverified or mismatched scope.

    A registry route is evidence for a URL, not permission to mix a task
    from one city/position with a different runtime configuration. Verify
    the complete task contract before allowing any live Raw operations.
    """
    if task not in cfg.tasks:
        raise LivePipelineError('Task is outside runtime config scope (position, city, keyword or label mismatch).')
    route = verified_route_for(task.position_id, task.location_id, config_path=cfg.config_path)
    if route is None:
        raise LivePipelineError('This position/city has no audited public TopCV result route.')
    cap = min(10, int(cfg.crawl['max_pages_per_query']))
    if type(max_pages) is not int or not 1 <= max_pages <= cap:
        raise LivePipelineError(f'max_pages must be 1..{cap} according to crawl.yaml.')
    if type(max_jobs) is not int or not 1 <= max_jobs <= 100:
        raise LivePipelineError('max_jobs must be 1..100 for a bounded live run.')
    configured_max_jobs = cfg.crawl.get('max_jobs_per_query')
    if configured_max_jobs is not None and max_jobs > configured_max_jobs:
        raise LivePipelineError('max_jobs exceeds crawl.yaml safety limit.')
    if cfg.selenium.headless and cfg.selenium.on_challenge == 'pause_for_manual_verification':
        raise LivePipelineError('Manual verification cannot work headlessly.')
    return route


def run_live_pipeline(*, crawler, task, cfg, store: RawIngestion,
                      report_path: Path, max_pages: int = 1, max_jobs: int = 3,
                      sleep_fn=time.sleep, now_fn=None, run_id=None,
                      run_purpose: str = 'test',
                      schedule_key=None, scheduled_for=None,
                      first_page_evidence: CalibratedFirstPage | None = None) -> dict:
    """Opt-in DB writer: listing first, then details within one browser session.

    The caller must explicitly enable live writes. A DB failure is fatal,
    without attempting an unsafe replay of a previously committed page.
    """
    route = validate_live_plan(task, cfg, max_pages=max_pages, max_jobs=max_jobs)
    schedule_key, scheduled_for = normalize_schedule_slot(schedule_key, scheduled_for)
    if first_page_evidence is not None:
        if not isinstance(first_page_evidence, CalibratedFirstPage) or first_page_evidence.route != route:
            raise LivePipelineError('First-page evidence must match the verified scoped route.')
    if run_purpose not in ('test', 'production'):
        raise LivePipelineError('Only test and production live run purposes are supported.')
    report_path = Path(report_path).resolve()
    now_fn = now_fn or (lambda: datetime.now(timezone.utc))
    started = now_fn()
    if started.tzinfo is None or started.utcoffset() is None:
        raise LivePipelineError('Clock must return timezone-aware time.')
    snapshot_date = started.astimezone(cfg.timezone).date()
    if run_id is None:
        run_id = (scheduled_run_id(schedule_key, scheduled_for)
                  if schedule_key is not None else uuid4())
    else:
        run_id = UUID(str(run_id))
    # A unique report for each run; never overwrite an old operator report.
    if report_path.exists():
        raise LivePipelineError('Output report already exists; choose a fresh run-specific path.')
    report = {
        'version': 1, 'mode': 'M4b_live_selenium_raw',
        'status': 'starting', 'run_id': str(run_id),
        'position_id': task.position_id, 'location_id': task.location_id,
        'verified_route': route, 'snapshot_date': str(snapshot_date),
        'started_at_utc': started.astimezone(timezone.utc).isoformat(),
        'run_purpose': run_purpose, 'observation_method': 'selenium_live',
        'capture_source': 'selenium_live', 'is_temporal_eligible': True,
        'max_pages': max_pages, 'max_jobs': max_jobs,
        'pages_committed': 0, 'discovered': 0, 'details_requested': 0,
        'new_observations': 0, 'detail_successes': 0,
        'raw_job_ids': [], 'failures': [],
        'coverage_confirmed': False, 'auto_resume_supported': False,
        'historical_checkpoint_used': False,
        'first_page_reused_from_calibration': first_page_evidence is not None,
    }
    schedule = ({'schedule_key': schedule_key, 'scheduled_for': scheduled_for}
                if schedule_key is not None else {})
    if schedule:
        report.update(schedule_key=schedule_key, scheduled_for=scheduled_for.isoformat())

    def persist():
        report['updated_at_utc'] = now_fn().astimezone(timezone.utc).isoformat()
        _write_json(report_path, report)

    def record_error(stage, exc, *, job_id=None, job_url=None):
        # Never put str(exc) in shared logs: URLs, tokens and credentials could
        # accidentally be included by third-party exception messages.
        report['failures'].append({'stage': stage, 'type': type(exc).__name__,
                                   'job_id': job_id})
        store.log_error(run_id, stage, exc, source_job_id=job_id,
                        job_url=job_url, task_id=task_id, retryable=False)
        persist()

    run_started = False
    task_started = False
    task_id = None
    try:
        # RawIngestion checks schema, opens and closes an independent short
        # transaction for each operation. Selenium is never inside a DB tx.
        stored_run_id = store.start_run(cfg.snapshot, run_id=run_id, snapshot_date=snapshot_date,
                        crawler_version='topcv-m4b-live-v1', run_purpose=run_purpose,
                        observation_method='selenium_live', **schedule)
        if schedule:
            run_id = UUID(str(stored_run_id))
            report['run_id'] = str(run_id)
        run_started = True
        task_id = store.start_task(run_id, task.position, task.location, route)
        task_started = True
        report['status'] = 'discovering'
        persist()

        def commit_page(page, capture, items):
            if capture.captured_at.astimezone(cfg.timezone).date() != snapshot_date:
                raise LivePipelineError('Date boundary crossed; no cross-date live capture accepted.')
            result = store.save_search_page(run_id, task_id, page, capture.page_url,
                items, fetched_at=capture.captured_at, page_html=None,
                parser_version='topcv-live-listing-v1')
            report['pages_committed'] += int(result.inserted)
            report['discovered'] += result.discoveries_inserted
            persist()
            print('RAW PAGE COMMITTED:', page, '| new discoveries:', result.discoveries_inserted)

        # Preserve separate M2-format journal for diagnostic and validation;
        # the PostgreSQL page write happens BEFORE M2 advances its checkpoint.
        discovery_path = report_path.with_name(report_path.stem + '_discovery.json')
        discovery = run_discovery(crawler, task, cfg, max_pages=max_pages,
            report_path=discovery_path, sleep_fn=sleep_fn, on_verified_page=commit_page,
            first_page_evidence=first_page_evidence)
        report['discovery_status'] = discovery['status']
        # M8.5I: propagate the exact M2 failure stage without changing the
        # fail-closed discovery -> detail gate or the database write ordering.
        if 'blocked_stage' in discovery:
            report['discovery_blocked_stage'] = discovery['blocked_stage']
        if 'failed_stage' in discovery:
            report['discovery_failed_stage'] = discovery['failed_stage']
        report['discovery_report'] = discovery_path.name
        report['discovered'] = discovery['unique_job_count']
        persist()

        acceptable = {'no_verified_next_link', 'max_pages_reached'}
        if discovery['status'] not in acceptable:
            stop = ('blocked' if discovery['status'] == 'stopped_on_access_challenge'
                    else 'failed' if report['pages_committed'] == 0 else 'partial')
            report['status'] = stop
            # A stopped discovery must never lead to detail requests.
            report['stop_reason'] = 'discovery_' + discovery['status']
        else:
            _, jobs, fingerprint = load_discovery_manifest(discovery_path, task)
            report['discovery_manifest_sha256'] = fingerprint
            report['selected_jobs'] = min(max_jobs, len(jobs))
            report['status'] = 'fetching_details'
            persist()
            consecutive_parse_failures = 0
            for job in jobs[:max_jobs]:
                sid = job['source_job_id']
                url = job['job_url']
                if report['details_requested']:
                    sleep_fn(cfg.selenium.delay_between_jobs_seconds)
                report['details_requested'] += 1
                persist()
                try:
                    capture = crawler.fetch_detail_capture(url)
                    prepared = adapt_topcv_detail(
                        source_job_id=sid, expected_job_url=url,
                        captured_job_url=capture.job_url,
                        parsed_detail=capture.parsed, listing=job['listing'],
                        expected_city=task.search_label)
                    if capture.captured_at.astimezone(cfg.timezone).date() != snapshot_date:
                        raise LivePipelineError('Date boundary crossed; detail not written to old snapshot.')
                    saved = store.save_job_detail(run_id, sid, prepared.payload,
                        job_url=prepared.job_url, scraped_at=capture.captured_at,
                        parser_version=prepared.parser_version,
                        detail_html=None, capture_source='selenium_live',
                        source_captured_at=capture.captured_at, is_temporal_eligible=True)
                    report['detail_successes'] += 1
                    report['new_observations'] += int(saved.inserted)
                    report['raw_job_ids'].append(saved.raw_job_id)
                    consecutive_parse_failures = 0
                    persist()
                    print('RAW DETAIL COMMITTED:', sid, '| raw_job_id:', saved.raw_job_id)
                except AccessChallenge as exc:
                    record_error('open_detail', exc, job_id=sid, job_url=url)
                    report['status'] = 'blocked'
                    report['stop_reason'] = 'detail_access_challenge'
                    break
                except LayoutChanged as exc:
                    record_error('parse_detail', exc, job_id=sid, job_url=url)
                    consecutive_parse_failures += 1
                    if consecutive_parse_failures >= 2:
                        report['status'] = 'partial'
                        report['stop_reason'] = 'repeated_detail_layout_error'
                        break
                except (NavigationError, WebDriverException, TimeoutError, OSError) as exc:
                    record_error('open_detail', exc, job_id=sid, job_url=url)
                    report['status'] = 'interrupted'
                    report['stop_reason'] = 'detail_navigation_error'
                    break
                except LivePipelineError as exc:
                    record_error('fetch', exc, job_id=sid, job_url=url)
                    report['status'] = 'interrupted'
                    report['stop_reason'] = 'snapshot_date_boundary'
                    break
            if report['status'] == 'fetching_details':
                if (report['detail_successes'] == len(jobs)
                        and discovery['status'] == 'no_verified_next_link'):
                    report['status'] = 'success'
                    report['stop_reason'] = 'observed_pagination_end_not_market_coverage'
                else:
                    report['status'] = 'partial'
                    report['stop_reason'] = ('sample_limit_or_unverified_coverage'
                        if len(jobs) > max_jobs or discovery['status'] == 'max_pages_reached'
                        else 'detail_parse_failures')
        # Status is only about the bounded run, never market completeness.
        final_status = report['status']
        store.finish_task(task_id, status=final_status,
                          stop_reason=report['stop_reason'])
        summary = store.finish_run(run_id, status=final_status,
                                   stop_reason=report['stop_reason'])
        report['db_summary'] = {
            'total_discovered': summary.total_discovered,
            'total_fetched': summary.total_fetched,
            'total_failed': summary.total_failed,
        }
        persist()
        return report
    except BaseException as exc:
        # On failures to write/checkpoint, avoid pretending the DB is clean.
        # An interrupted process may leave an open 'running' run requiring
        # operator reconciliation; do not automatically retry a failed write.
        report['status'] = 'interrupted_unreconciled'
        report['fatal_error_type'] = type(exc).__name__
        report['operator_action'] = 'Inspect Raw run and checkpoints; no automatic replay.'
        try:
            persist()
        except Exception:
            pass
        if run_started and task_started:
            try:
                store.finish_task(task_id, status='interrupted', stop_reason='unreconciled_failure')
                store.finish_run(run_id, status='interrupted', stop_reason='unreconciled_failure')
            except Exception:
                pass
        raise
