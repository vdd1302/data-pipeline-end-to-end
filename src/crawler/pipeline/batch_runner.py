"""M6: one-command, conservative 20-scope TopCV -> Raw orchestration.

Two explicit modes:
* discovery: reuse M5d -> M5c verified one-page checkpoints + atomic Raw import.
* full: calibrate first page in one Selenium session, use its capture in M4b
  to ingest verified listing pages AND attempt live detail in Raw.

Plan: no browser, network or DB. All writes are individually opt-in.
Stop the sweep on access challenge, ambiguous readiness, DB failure or a
running/unreconciled prior scope. No challenge retry, proxy, captcha bypass.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import json
import os
import re
import time
import unicodedata
from uuid import uuid4

from src.crawler.pipeline.discovery import _atomic_json, verified_route_for
from src.crawler.pipeline.m5_calibration import candidate_for, with_timestamp, promote_route
from src.crawler.pipeline.m5d_orchestrator import capture_scope, raw_action, plan_scopes, browser_run_lock


class BatchError(RuntimeError):
    pass


ALLOWED_MODES = {'discovery', 'full'}
STOP_STATUSES = {
    'stopped_on_access_challenge', 'calibration_not_verified',
    'browser_or_driver_error', 'discovery_incomplete_or_blocked',
    'stopped_on_navigation_or_parse_error', 'interrupted_unreconciled',
    'blocked', 'interrupted', 'failed', 'm5c_preflight_rejected',
    'prior_run_requires_review',
}


def _utc():
    return datetime.now(timezone.utc).isoformat()


def _root(cfg):
    return cfg.config_path.parent.parent


def state_path(cfg):
    return _root(cfg) / 'artifacts' / 'crawl_pipeline' / 'state.json'


def read_state(cfg):
    path = state_path(cfg)
    if not path.exists():
        return {'version': 1, 'scopes': {}}
    if path.stat().st_size > 2_000_000:
        raise BatchError('Batch state exceeds 2 MiB; refusing overwrite.')
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, ValueError) as exc:
        raise BatchError('Batch state cannot be read; do not overwrite.') from exc
    if not isinstance(value, dict) or value.get('version') != 1 or not isinstance(value.get('scopes'), dict):
        raise BatchError('Invalid batch state format; refusing overwrite.')
    return value


def _record(cfg, key, entry):
    data = read_state(cfg)
    previous = data['scopes'].get(key) or {}
    data['scopes'][key] = {**previous, **entry, 'updated_at_utc': _utc()}
    _atomic_json(state_path(cfg), data)


def _read_candidates(path, *, required=False):
    """Read a local public-page candidate manifest; never marks it verified."""
    if not path.exists():
        if required:
            raise BatchError(f'Missing candidate catalog: {path.name}')
        return {}
    if path.stat().st_size > 2_000_000:
        raise BatchError('Candidate catalog unexpectedly large.')
    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError, UnicodeError) as exc:
        raise BatchError(f'Invalid candidate catalog: {path.name}') from exc
    if not isinstance(raw, dict) or raw.get('version') != 1 or not isinstance(raw.get('candidates'), list):
        raise BatchError('Candidate manifest requires version 1 and candidates list.')
    result = {}
    for obj in raw['candidates']:
        if not isinstance(obj, dict):
            raise BatchError('Malformed candidate entry.')
        key = (obj.get('position_id'), obj.get('location_id'))
        if not all(isinstance(x, str) and x for x in key):
            raise BatchError('Candidate scope ID must be a nonempty string.')
        if key in result:
            raise BatchError('Duplicate candidate scope; resolve before running.')
        if not isinstance(obj.get('url'), str) or not obj['url']:
            raise BatchError('Candidate route must be a nonempty string.')
        result[key] = obj['url']
    return result


def _candidate_map(cfg):
    """Operator-supplied candidates override bundled PUBLICLY OBSERVED candidates.

    All candidates remain UNVERIFIED until Selenium's strict live calibration.
    If an operator file already contains an entry, preserve that decision.
    """
    bundled_path = cfg.config_path.parent / 'observed_candidate_catalog.json'
    if not bundled_path.exists():
        bundled_path = cfg.config_path.parent / 'm6_observed_candidate_catalog.json'
    manual_path = cfg.config_path.parent / 'route_candidates.json'
    if not manual_path.exists():
        manual_path = cfg.config_path.parent / 'm5_route_candidates.json'
    bundled = _read_candidates(bundled_path)
    manual = _read_candidates(manual_path)
    return {**bundled, **manual}


def _calibration_diagnostic(report_path):
    """Bounded safe reason-only extraction from the saved M5b report (no HTML)."""
    if not report_path:
        return {}
    path = Path(report_path)
    try:
        if not path.is_file() or path.stat().st_size > 2_000_000:
            return {}
        report = json.loads(path.read_text(encoding='utf-8'))
        cal = report.get('calibration') or {}
        if not isinstance(cal, dict):
            return {}
        fields = ('status', 'reason', 'route_exact', 'scope_verified',
                  'all_cards_city_verified', 'parsed_cards')
        return {k: cal[k] for k in fields if k in cal and
                isinstance(cal[k], (str, bool, int, float, type(None)))}
    except (OSError, UnicodeError, ValueError, TypeError):
        return {}


def _slug(keyword):
    value = unicodedata.normalize('NFKD', keyword)
    value = ''.join(c for c in value if not unicodedata.combining(c))
    return re.sub(r'-+', '-', re.sub(r'[^a-z0-9]+', '-', value.lower())).strip('-')


def heuristic_candidate(task):
    """An UNVERIFIED public route hypothesis, NEVER a verified registry entry.

    The live browser must pass exact URL, title, city, and every-card identity
    checks before discovery. If the TopCV path convention differs, it is skipped.
    """
    loc = {'ha_noi': ('ha-noi', 'l1'), 'ho_chi_minh': ('ho-chi-minh', 'l2')}.get(task.location_id)
    if not loc:
        raise BatchError('Unknown city code in candidate generator.')
    return 'https://www.topcv.vn/tim-viec-lam-' + _slug(task.keyword) + '-tai-' + loc[0] + '-' + loc[1] + 'cr257'


def route_for(cfg, task, *, candidate_map=None, auto_candidates=False):
    verified = verified_route_for(task.position_id, task.location_id, config_path=cfg.config_path)
    if verified:
        return verified, 'live_verified'
    candidates = _candidate_map(cfg) if candidate_map is None else candidate_map
    key = (task.position_id, task.location_id)
    if key in candidates:
        value = candidate_for(cfg, task, candidates[key])
        return value, 'supplied_unverified_candidate'
    if auto_candidates:
        value = candidate_for(cfg, task, heuristic_candidate(task))
        return value, 'heuristic_unverified_candidate'
    return None, 'missing_route_candidate'


def plan_batch(cfg, *, mode='discovery', auto_candidates=False):
    if mode not in ALLOWED_MODES:
        raise BatchError('Invalid batch mode.')
    candidates = _candidate_map(cfg)
    existing = plan_scopes(cfg)
    state = read_state(cfg)
    rows = []
    for task in cfg.tasks:
        key = f'{task.position_id}/{task.location_id}'
        try:
            url, kind = route_for(cfg, task, candidate_map=candidates, auto_candidates=auto_candidates)
            err = None
        except ValueError as exc:
            url, kind, err = None, 'bad_candidate', type(exc).__name__
        entry = state['scopes'].get(mode + ':' + key) or {}
        rows.append({'scope': key, 'route_kind': kind, 'candidate_url': url,
                     'existing_state': entry.get('status'), 'error_type': err})
    return {'version': 1, 'mode': mode, 'configured_scopes': len(rows),
            'live_verified': existing['verified_routes'],
            'ready_to_attempt': sum(bool(x['candidate_url']) for x in rows),
            'missing_candidates': sum(not x['candidate_url'] for x in rows),
            'candidate_mode': 'with_heuristic_unverified_routes' if auto_candidates else 'configured_only',
            'scopes': rows, 'chrome_sessions': 0, 'network_requests': 0, 'db_writes': 0}


@contextmanager
def sweep_lock(cfg):
    path = _root(cfg) / 'artifacts' / 'crawl_pipeline' / 'sweep.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise BatchError('Another sweep may be running. Review sweep.lock, do not remove blindly.') from exc
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write('Crawl sweep active. Check process before removing.\n')
        yield
    finally:
        path.unlink(missing_ok=True)


def ensure_database(expected='topcv_raw_storage', *, factory=None):
    if expected != 'topcv_raw_storage':
        raise BatchError('Only database topcv_raw_storage can be selected for this pipeline.')
    if factory is None:
        from src.raw_storage.db import get_connection
        factory = get_connection
    with factory(read_only=True) as conn:
        row = conn.execute('SELECT current_database() AS name').fetchone()
        name = row['name']
    if name != expected:
        raise BatchError('Database verification failed; no write attempted.')
    return name


def _execute_discovery(cfg, task, route, kind, *, new_capture=False, write_db=False,
                       schedule_key=None, scheduled_for=None, run_purpose='manual'):
    scheduled = ({'schedule_key': schedule_key, 'scheduled_for': scheduled_for,
                  'run_purpose': run_purpose} if schedule_key is not None else {})
    result = capture_scope(cfg, task, candidate_url=route,
                           promote=(kind != 'live_verified'), new_capture=new_capture, **scheduled)
    status = result['status']
    out = {'status': status, 'job_count': result.get('job_count', 0),
           'run_id': result.get('run_id'), 'report': result.get('report'),
           'fresh_capture': result.get('fresh_capture', False)}
    if status not in ('checkpoint_ready', 'already_captured'):
        out['reason'] = result.get('reason') or result.get('error_type')
        audit = _calibration_diagnostic(result.get('report'))
        if audit:
            out['calibration_diagnostic'] = audit
        return out
    if write_db:
        # Exactly the M5c writer, with read-back idempotency verification.
        checked = raw_action(cfg, task, mode='check', **scheduled)
        if checked['state'] == 'not_imported':
            raw_action(cfg, task, mode='commit', **scheduled)
        readback = raw_action(cfg, task, mode='check', **scheduled)
        if readback['state'] != 'already_imported_identical':
            raise BatchError('DB readback mismatch after discovery import.')
        out['database_status'] = readback['state']
        out['db_discoveries'] = readback['existing_discoveries']
        out['pages_committed'] = 1
    else:
        out['database_status'] = 'not_checked'
    return out


def _execute_full(cfg, task, route, kind, *, run_id, max_pages, max_jobs, run_purpose,
                  sleep_fn=time.sleep, schedule_key=None, scheduled_for=None):
    # NO extra listing navigation: the calibrated first page is handed directly
    # to the multi-page M4b Raw writer and optional detail loop.
    from src.crawler.topcv import TopCVCrawler
    from src.crawler.pipeline.m5_calibration import calibrate_browser
    from src.crawler.pipeline.live_pipeline import run_live_pipeline
    from src.raw_storage.ingestion import RawIngestion
    from src.crawler.parser import AccessChallenge

    root = _root(cfg) / 'artifacts' / 'crawl_pipeline' / 'full' / task.position_id / task.location_id
    root.mkdir(parents=True, exist_ok=True)
    report_path = root / f'live_{run_id}.json'
    with browser_run_lock(cfg), TopCVCrawler(cfg) as crawler:
        try:
            calibration, first = calibrate_browser(crawler, task, cfg, route, retain_capture=True)
        except AccessChallenge:
            return {'status': 'stopped_on_access_challenge', 'job_count': 0, 'report': None}
        if calibration.get('status') != 'live_verified' or first is None:
            return {'status': 'calibration_not_verified', 'reason': calibration.get('status'),
                    'job_count': 0, 'report': None}
        if kind != 'live_verified':
            promote_route(cfg, task, with_timestamp(calibration))
        # Fail closed if the registry was not promoted successfully.
        outcome = run_live_pipeline(crawler=crawler, task=task, cfg=cfg,
                                    store=RawIngestion(), report_path=report_path,
                                    max_pages=max_pages, max_jobs=max_jobs,
                                    run_id=run_id, run_purpose=run_purpose,
                                    first_page_evidence=first, sleep_fn=sleep_fn,
                                    **({'schedule_key': schedule_key, 'scheduled_for': scheduled_for}
                                       if schedule_key is not None else {}))
    return {'status': outcome['status'], 'job_count': outcome['discovered'],
            'run_id': outcome['run_id'], 'details_ok': outcome['detail_successes'],
            'observation_rows': outcome['new_observations'],
            'pages_committed': outcome['pages_committed'],
            'reason': outcome.get('stop_reason'), 'report': str(report_path)}


def run_batch(cfg, *, mode='discovery', write_db=False, confirm_database=None,
              auto_candidates=False, max_scopes=None, min_gap_seconds=20,
              max_pages=1, max_jobs=100, new_snapshot=False,
              run_purpose='production', sleep_fn=time.sleep,
              discovery_fn=None, full_fn=None, database_verifier=None,
              schedule_key=None, scheduled_for=None, scheduled_reader=None):
    """Run a bounded sequential sweep, preserving every scope outcome locally.

    Previously accepted M5d checkpoints are reused in discovery mode. Full
    runs have a separate durable ledger to prevent unsafe silent reruns; use
    --new-snapshot only after reviewing prior full run state/DB reconciliation.
    """
    if mode not in ALLOWED_MODES:
        raise BatchError('Invalid mode.')
    if not isinstance(write_db, bool) or not isinstance(auto_candidates, bool):
        raise BatchError('Invalid boolean controls.')
    if mode == 'full' and not write_db:
        raise BatchError('Full live mode requires explicit Raw database write authorization.')
    if write_db and confirm_database != 'topcv_raw_storage':
        raise BatchError('Database write requires --confirm-database topcv_raw_storage.')
    if type(max_pages) is not int or not 1 <= max_pages <= min(10, cfg.crawl['max_pages_per_query']):
        raise BatchError('Invalid max_pages for configured crawl limits.')
    if mode == 'discovery' and max_pages != 1:
        raise BatchError('M5c checkpoint importer only supports one fully verified page. Use full mode for multi-page Raw.')
    if type(max_jobs) is not int or not 1 <= max_jobs <= 100:
        raise BatchError('max_jobs must be 1..100.')
    if run_purpose not in ('test', 'production'):
        raise BatchError('Unknown run purpose.')
    from src.raw_storage.schedule import normalize_schedule_slot, scheduled_run_id
    schedule_key, scheduled_for = normalize_schedule_slot(schedule_key, scheduled_for)
    if schedule_key is not None and (not write_db or run_purpose != 'production'):
        raise BatchError('Scheduled batches require production Raw writes.')
    if scheduled_reader is None and schedule_key is not None:
        from src.crawler.pipeline.scheduled_audit import read_scheduled_scope
        scheduled_reader = read_scheduled_scope
    if type(min_gap_seconds) not in (int, float) or min_gap_seconds < 8:
        raise BatchError('A minimum 8-second gap between scopes is required.')
    if max_scopes is not None and (type(max_scopes) is not int or max_scopes < 1):
        raise BatchError('max_scopes must be a positive integer or omitted.')
    if cfg.crawl['enabled'] is not True:
        raise BatchError('Crawling disabled in crawl.yaml.')
    if mode == 'full' and run_purpose == 'production' and max_jobs < 100:
        raise BatchError('Production full runs must allow 100 jobs per scope (not a tiny test sample).')

    runner = discovery_fn or _execute_discovery
    live_runner = full_fn or _execute_full
    verify_db = database_verifier or ensure_database
    candidates = _candidate_map(cfg)
    if write_db:
        verify_db(confirm_database)
    batch_id = str(uuid4())
    filename = _root(cfg) / 'artifacts' / 'crawl_pipeline' / f'batch_{batch_id}.json'
    summary = {'version': 1, 'batch_id': batch_id, 'mode': mode,
               'started_at_utc': _utc(), 'status': 'running',
               'write_db': write_db, 'run_purpose': run_purpose if mode == 'full' else None,
               'configured_scopes': len(cfg.tasks), 'results': [], 'coverage_confirmed': False}
    if schedule_key is not None:
        summary.update(schedule_key=schedule_key, scheduled_for=scheduled_for.isoformat())
    _atomic_json(filename, summary)

    with sweep_lock(cfg):
        attempted = 0
        for task in cfg.tasks:
            if max_scopes is not None and attempted >= max_scopes:
                break
            key = f'{task.position_id}/{task.location_id}'
            state_key = mode + ':' + key
            scoped_schedule_key = f'{schedule_key}/{mode}/{key}' if schedule_key is not None else None
            scheduled = ({'schedule_key': scoped_schedule_key, 'scheduled_for': scheduled_for,
                          'run_purpose': run_purpose} if scoped_schedule_key is not None else {})
            if schedule_key is not None:
                state_key += ':' + scheduled_for.isoformat()
            try:
                route, kind = route_for(cfg, task, candidate_map=candidates,
                                        auto_candidates=auto_candidates)
                if not route:
                    result = {'status': 'missing_route_candidate', 'job_count': 0}
                    summary['results'].append({'scope': key, **result})
                    _atomic_json(filename, summary)
                    continue
            except ValueError as exc:
                summary['results'].append({'scope': key, 'status': 'invalid_route_candidate',
                                           'error_type': type(exc).__name__})
                _atomic_json(filename, summary)
                continue

            entry = read_state(cfg)['scopes'].get(state_key)
            # M8.2C: discovery checkpoints are also durable state. Never
            # automatically revisit a previously blocked/interrupted scope
            # by invoking a lower-level batch entry point directly. The
            # M8 scheduler has its own V5 hold guard; this closes the
            # additional direct batch-runner replay path.
            if (entry is not None and entry.get('status') in
                    (STOP_STATUSES | {'started', 'partial'})):
                summary['results'].append({'scope': key, 'route_kind': kind,
                    'status': 'prior_run_requires_review',
                    'previous_status': entry.get('status')})
                summary.update(status='stopped_for_review', stop_scope=key,
                    stop_status='prior_run_requires_review')
                _atomic_json(filename, summary)
                break
            if schedule_key is not None:
                committed = scheduled_reader(cfg, task, mode=mode, schedule_key=scoped_schedule_key,
                                             scheduled_for=scheduled_for)
                if committed is not None:
                    summary['results'].append({'scope': key, 'route_kind': kind, **committed})
                    _atomic_json(filename, summary)
                    if committed['status'] == 'prior_run_requires_review':
                        summary.update(status='stopped_for_review', stop_scope=key,
                                       stop_status='prior_run_requires_review')
                        break
                    continue
                if entry is not None:
                    summary['results'].append({'scope': key, 'status': 'prior_run_requires_review',
                                               'previous_status': entry.get('status')})
                    summary.update(status='stopped_for_review', stop_scope=key,
                                   stop_status='prior_run_requires_review')
                    break
            # A proved-wrong exact candidate is not revisited just because
            # the operator runs another batch. A new URL or explicit fresh
            # snapshot is required to reconsider it.
            if (mode == 'discovery' and kind != 'live_verified'
                    and not new_snapshot and entry
                    and entry.get('status') == 'candidate_rejected_scope_mismatch'
                    and entry.get('candidate_url') == route):
                summary['results'].append({'scope': key,
                    'route_kind': kind, 'status': 'previously_rejected_candidate', 'job_count': 0})
                _atomic_json(filename, summary)
                continue
            if mode == 'full' and entry and not new_snapshot:
                # Do not silently redo a previously started/committed Full run.
                summary['results'].append({'scope': key, 'status': 'prior_run_requires_review',
                                           'previous_status': entry.get('status')})
                summary['status'] = 'stopped_for_review'
                summary['stop_scope'] = key
                summary['stop_status'] = 'prior_run_requires_review'
                _atomic_json(filename, summary)
                break
            if attempted:
                sleep_fn(min_gap_seconds)
            attempted += 1
            prepared_run_id = (str(scheduled_run_id(scoped_schedule_key, scheduled_for))
                               if scoped_schedule_key is not None else
                               str(uuid4()) if mode == 'full' else None)
            _record(cfg, state_key, {'status': 'started', 'batch_id': batch_id,
                                     'mode': mode, 'run_id': prepared_run_id,
                                     'candidate_url': route, 'route_kind': kind,
                                     **({'schedule_key': scoped_schedule_key,
                                         'scheduled_for': scheduled_for.isoformat()}
                                        if schedule_key is not None else {})})
            try:
                if mode == 'discovery':
                    result = runner(cfg, task, route, kind, new_capture=new_snapshot,
                                    write_db=write_db, **scheduled)
                else:
                    result = live_runner(cfg, task, route, kind, run_id=prepared_run_id,
                                         max_pages=max_pages, max_jobs=max_jobs, run_purpose=run_purpose,
                                         **({k: v for k, v in scheduled.items() if k != 'run_purpose'}))
                if schedule_key is not None and result.get('run_id'):
                    committed = scheduled_reader(cfg, task, mode=mode,
                        schedule_key=scoped_schedule_key, scheduled_for=scheduled_for)
                    if committed is not None:
                        result = {**result, **committed, 'reused_slot': False}
                    else:
                        result = {**result, 'status': 'prior_run_requires_review',
                                  'database_status': 'requires_review', 'fresh_capture': False,
                                  'reason': 'scheduled_raw_run_missing_after_crawl'}
            except (KeyboardInterrupt, SystemExit):
                _record(cfg, state_key, {'status': 'interrupted_unreconciled', 'batch_id': batch_id})
                summary['status'] = 'interrupted_unreconciled'
                _atomic_json(filename, summary)
                raise
            except Exception as exc:
                result = {'status': 'interrupted_unreconciled', 'error_type': type(exc).__name__,
                          'run_id': prepared_run_id}
            _record(cfg, state_key, {**result, 'batch_id': batch_id, 'mode': mode})
            summary['results'].append({'scope': key, 'route_kind': kind, **result})
            _atomic_json(filename, summary)
            status = result['status']
            audit = result.get('calibration_diagnostic') or {}
            # A candidate that rendered a parseable, nonempty listing of the
            # WRONG scope is not a challenge: quarantine and move on. Never
            # skip results_not_ready, challenge, or a previously verified URL.
            rejected_candidate = (
                mode == 'discovery' and kind != 'live_verified'
                and status == 'calibration_not_verified'
                and audit.get('status') == 'scope_or_listing_unverified'
                and (audit.get('parsed_cards', 0) > 0 or audit.get('route_exact') is False)
            )
            if rejected_candidate:
                result['status'] = 'candidate_rejected_scope_mismatch'
                summary['results'][-1]['status'] = 'candidate_rejected_scope_mismatch'
                _record(cfg, state_key, {'status': 'candidate_rejected_scope_mismatch'})
                _atomic_json(filename, summary)
                continue
            if status in STOP_STATUSES:
                summary['status'] = 'stopped_for_review'
                summary['stop_scope'] = key
                summary['stop_status'] = status
                summary['stop_reason'] = result.get('reason')
                summary['stop_calibration'] = audit
                break
        else:
            summary['status'] = 'batch_processed_all_scopes'
        if summary['status'] == 'running':
            summary['status'] = 'batch_limit_reached' if max_scopes else 'batch_processed_all_scopes'
        if summary['status'] == 'batch_processed_all_scopes' and any(
                r['status'] in ('missing_route_candidate', 'invalid_route_candidate', 'candidate_rejected_scope_mismatch', 'previously_rejected_candidate')
                for r in summary['results']):
            summary['status'] = 'batch_completed_with_skips'
        summary['finished_at_utc'] = _utc()
        summary['attempted_scopes'] = attempted
        summary['discovered_job_mentions'] = sum(r.get('job_count', 0) or 0 for r in summary['results'])
        summary['successful_scope_results'] = sum(r.get('status') in
            ('checkpoint_ready', 'already_captured', 'success', 'partial') for r in summary['results'])
        _atomic_json(filename, summary)
    return filename, summary
