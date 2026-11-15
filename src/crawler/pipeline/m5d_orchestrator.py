"""M5d: guarded per-scope discovery ledger atop verified M5b + M5c.

No automated scope sweep. A browser is launched only for an explicitly
selected scope, and only with an already verified route or operator-supplied
candidate. Default plan and adopt are offline. DB access is separately opted in.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import os
from uuid import uuid4

from src.crawler.pipeline.discovery import _atomic_json, verified_route_for
from src.crawler.pipeline.m5_calibration import candidate_for
from src.crawler.pipeline.m5c_checkpoint_raw import (
    build_import_plan, check_database, commit_checkpoint, with_schedule,
)
from src.crawler.pipeline.m5b_unified import run_m5b
from src.raw_storage.schedule import normalize_schedule_slot


class OrchestrationError(ValueError):
    pass


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def scope_key(task):
    return f'{task.position_id}/{task.location_id}'


def _size_sha256(path):
    path = Path(path).resolve()
    if not path.is_file() or not 0 < path.stat().st_size <= 4 * 1024 * 1024:
        raise OrchestrationError('Missing, empty, or oversized checkpoint (4 MiB limit).')
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ledger_path(cfg):
    return cfg.config_path.parent.parent / 'artifacts' / 'm5d' / 'ledger.json'


def load_ledger(cfg):
    path = ledger_path(cfg)
    if not path.exists():
        return {'version': 1, 'scopes': {}}
    if path.stat().st_size > 2_000_000:
        raise OrchestrationError('Unexpected oversized M5d ledger.')
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except (ValueError, UnicodeError) as exc:
        raise OrchestrationError('Invalid M5d ledger; do not overwrite.') from exc
    if (not isinstance(value, dict) or value.get('version') != 1 or
            not isinstance(value.get('scopes'), dict)):
        raise OrchestrationError('Invalid M5d ledger structure; do not overwrite.')
    return value


def _write_ledger(cfg, ledger):
    _atomic_json(ledger_path(cfg), ledger)


def _events(ledger, task):
    value = ledger['scopes'].setdefault(scope_key(task), [])
    if not isinstance(value, list):
        raise OrchestrationError('Bad scope entries in ledger.')
    return value


def _last_success(ledger, task):
    for item in reversed(_events(ledger, task)):
        if item.get('status') == 'checkpoint_ready':
            return item
    return None


def _success_for_slot(ledger, task, schedule_key, scheduled_for):
    """Only the exact period may be reused by a scheduled invocation."""
    if schedule_key is None:
        return _last_success(ledger, task)
    for item in reversed(_events(ledger, task)):
        if item.get('status') != 'checkpoint_ready' or item.get('schedule_key') != schedule_key:
            continue
        try:
            _, due = normalize_schedule_slot(item['schedule_key'],
                datetime.fromisoformat(item['scheduled_for']))
        except (KeyError, TypeError, ValueError):
            raise OrchestrationError('Scheduled ledger entry has invalid slot metadata.') from None
        if due == scheduled_for:
            return item
    return None


def _verify_success(cfg, task, event):
    """Re-verify source file hashes + full M5c import plan on EVERY reuse."""
    if event is None:
        raise OrchestrationError('No accepted M5d checkpoint. Capture or adopt first.')
    report = Path(event['report']).resolve()
    discovery = Path(event['discovery']).resolve()
    if _size_sha256(report) != event['report_sha256'] or _size_sha256(discovery) != event['discovery_sha256']:
        raise OrchestrationError('Checkpoint changed since registration; refusing reuse.')
    plan = build_import_plan(cfg=cfg, task=task, m5b_report=report, discovery_path=discovery)
    if event.get('schedule_key') is not None:
        try:
            plan = with_schedule(plan, schedule_key=event['schedule_key'],
                scheduled_for=datetime.fromisoformat(event['scheduled_for']),
                run_purpose=event['run_purpose'])
        except (KeyError, TypeError, ValueError):
            raise OrchestrationError('Scheduled checkpoint metadata is invalid.') from None
    if (str(plan.run_id) != event['run_id'] or len(plan.items) != event['job_count'] or
            plan.scope != scope_key(task)):
        raise OrchestrationError('Checkpoint identity differs from ledger.')
    return plan


def record_checkpoint(cfg, task, *, report_path, discovery_path, origin,
                      schedule_key=None, scheduled_for=None, run_purpose='manual'):
    """Accept M5b2 capture only after M5c's full preflight; no DB/network."""
    report_path = Path(report_path).resolve()
    discovery_path = Path(discovery_path).resolve()
    plan = build_import_plan(cfg=cfg, task=task, m5b_report=report_path,
                             discovery_path=discovery_path)
    key, due = normalize_schedule_slot(schedule_key, scheduled_for)
    if key is not None:
        plan = with_schedule(plan, schedule_key=key, scheduled_for=due, run_purpose=run_purpose)
    ledger = load_ledger(cfg)
    events = _events(ledger, task)
    record = {'status': 'checkpoint_ready', 'origin': origin,
              'registered_at_utc': utc_now(), 'report': str(report_path),
              'discovery': str(discovery_path),
              'report_sha256': _size_sha256(report_path),
              'discovery_sha256': _size_sha256(discovery_path),
              'run_id': str(plan.run_id), 'job_count': len(plan.items),
              'captured_at_utc': plan.captured_at.isoformat(),
              'database_status': 'not_checked'}
    if key is not None:
        record.update(schedule_key=key, scheduled_for=due.isoformat(), run_purpose=run_purpose)
    # Repeated adopt or identical capture never appends a duplicate event.
    if any(e.get('status') == 'checkpoint_ready' and e.get('run_id') == record['run_id']
           for e in events):
        old = next(e for e in events if e.get('status') == 'checkpoint_ready' and
                   e.get('run_id') == record['run_id'])
        _verify_success(cfg, task, old)
        if key is not None and (old['report_sha256'] != record['report_sha256'] or
                                old['discovery_sha256'] != record['discovery_sha256']):
            raise OrchestrationError('The scheduled period already has different evidence; reconcile before replacing it.')
        return {'action': 'already_registered', 'record': old}
    events.append(record)
    _write_ledger(cfg, ledger)
    return {'action': 'registered', 'record': record}


def plan_scopes(cfg):
    """Local, no-browser, no-DB report for ALL configured scopes."""
    ledger = load_ledger(cfg)
    rows = []
    for task in cfg.tasks:
        verified = verified_route_for(task.position_id, task.location_id,
                                      config_path=cfg.config_path)
        success = _last_success(ledger, task)
        last = _events(ledger, task)[-1] if _events(ledger, task) else None
        rows.append({'position_id': task.position_id, 'location_id': task.location_id,
                     'keyword': task.keyword, 'city': task.search_label,
                     'verified_route': verified,
                     'status': ('checkpoint_ready' if success else
                                'verified_pending_capture' if verified else
                                'needs_independent_live_calibration'),
                     'job_count': success['job_count'] if success else None,
                     'run_id': success['run_id'] if success else None,
                     'last_attempt_status': last['status'] if last else None,
                     'database_status': success.get('database_status') if success else None})
    return {'version': 1, 'mode': 'M5d_local_scope_status_no_network_no_db',
            'configured_scopes': len(rows),
            'verified_routes': sum(bool(x['verified_route']) for x in rows),
            'checkpoint_ready': sum(x['status'] == 'checkpoint_ready' for x in rows),
            'pending_routes': sum(not x['verified_route'] for x in rows),
            'scopes': rows, 'chrome_sessions': 0, 'database_writes': 0}


def _record_failure(cfg, task, report_path, state):
    ledger = load_ledger(cfg)
    _events(ledger, task).append({'status': 'stopped', 'reason': state,
                                 'at_utc': utc_now(), 'report': str(report_path)})
    _write_ledger(cfg, ledger)


@contextmanager
def browser_run_lock(cfg):
    """Prevent accidental simultaneous M5d Chrome runs sharing one profile."""
    path = cfg.config_path.parent.parent / 'artifacts' / 'm5d' / 'browser.lock'
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError as exc:
        raise OrchestrationError('Another M5d Chrome run may be active. Inspect browser.lock manually.') from exc
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write('M5d browser run; remove only after confirming no active process.\n')
        yield
    finally:
        path.unlink(missing_ok=True)


def capture_scope(cfg, task, *, candidate_url=None, promote=False, new_capture=False,
                  schedule_key=None, scheduled_for=None, run_purpose='manual'):
    """One selected scope, one M5b session, never auto-retry or DB write."""
    if cfg.crawl['enabled'] is not True:
        raise OrchestrationError('Crawl disabled in config/crawl.yaml.')
    key, due = normalize_schedule_slot(schedule_key, scheduled_for)
    if key is not None and run_purpose not in {'production', 'test'}:
        raise OrchestrationError('Scheduled capture requires production/test purpose.')
    verified = verified_route_for(task.position_id, task.location_id,
                                  config_path=cfg.config_path)
    if not verified and not promote:
        raise OrchestrationError('New scope needs --candidate-url and --promote in one audited browser session.')
    if verified and candidate_url and candidate_url != verified:
        raise OrchestrationError('Candidate conflicts with a verified route; refusing to overwrite.')
    route = candidate_for(cfg, task, candidate_url or verified)
    with browser_run_lock(cfg):
        ledger = load_ledger(cfg)
        previous = _success_for_slot(ledger, task, key, due)
        if previous and (key is not None or not new_capture):
            _verify_success(cfg, task, previous)
            return {'status': 'already_captured', 'run_id': previous['run_id'],
                    'job_count': previous['job_count'], 'report': previous['report'],
                    'discovery': previous['discovery'], 'fresh_capture': key is not None}
        from src.crawler.topcv import TopCVCrawler
        folder = (cfg.config_path.parent.parent / 'artifacts' / 'm5d' /
                  task.position_id / task.location_id)
        folder.mkdir(parents=True, exist_ok=True)
        token = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid4().hex[:8]
        report_file = folder / f'm5b_unified_{task.position_id}_{task.location_id}_{token}.json'
        discovery_file = folder / f'm5b_discovery_{task.position_id}_{task.location_id}_{token}.json'
        # Only one navigation to first page. Strict one-page limit matches M5c.
        try:
            with TopCVCrawler(cfg) as crawler:
                report = run_m5b(crawler, task, cfg, route, max_pages=1,
                                 report_path=report_file, discovery_path=discovery_file,
                                 promote=promote)
        except Exception as exc:
            # No unsafe automatic retry. Do not expose URL, DB credentials, HTML.
            if not report_file.exists():
                _atomic_json(report_file, {'status': 'browser_or_driver_error',
                                         'error_type': type(exc).__name__})
            _record_failure(cfg, task, report_file, 'browser_or_driver_error')
            return {'status': 'browser_or_driver_error', 'error_type': type(exc).__name__,
                    'report': str(report_file)}
        if report.get('status') != 'discovery_captured':
            _record_failure(cfg, task, report_file, report.get('status', 'unknown'))
            return {'status': report.get('status'),
                    'reason': (report.get('calibration') or {}).get('status') or report.get('discovery_status'),
                    'report': str(report_file)}
        try:
            registered = record_checkpoint(cfg, task, report_path=report_file,
                                           discovery_path=discovery_file, origin='m5d_live',
                                           schedule_key=key, scheduled_for=due, run_purpose=run_purpose)
        except (ValueError, OSError, KeyError) as exc:
            _record_failure(cfg, task, report_file, 'm5c_preflight_rejected')
            return {'status': 'm5c_preflight_rejected', 'error_type': type(exc).__name__,
                    'report': str(report_file), 'discovery': str(discovery_file)}
        return {'status': 'checkpoint_ready', 'run_id': registered['record']['run_id'],
                'job_count': registered['record']['job_count'],
                'report': registered['record']['report'],
                'discovery': registered['record']['discovery'], 'fresh_capture': True}


def raw_action(cfg, task, *, mode, schedule_key=None, scheduled_for=None, run_purpose='manual'):
    """Explicit DB check/commit only; reuse exact M5c atomic writer."""
    if mode not in ('check', 'commit'):
        raise OrchestrationError('Invalid DB operation.')
    ledger = load_ledger(cfg)
    key, due = normalize_schedule_slot(schedule_key, scheduled_for)
    event = _success_for_slot(ledger, task, key, due)
    plan = _verify_success(cfg, task, event)
    if key is not None and plan.run_purpose != run_purpose:
        raise OrchestrationError('Scheduled Raw import purpose differs from its capture ledger.')
    # The DB can be inspected or committed only for the scope of this capture.
    if mode == 'check':
        result = check_database(plan, cfg=cfg, task=task)
    else:
        # DB name verification happens in CLI before invoking this writer.
        result = commit_checkpoint(plan, cfg=cfg, task=task)
    # Update the matching event after successful DB operation. DB truth is never
    # presumed from local ledger alone; next --check-db re-reads PostgreSQL.
    event['database_status'] = result['state']
    event['database_checked_at_utc'] = utc_now()
    _write_ledger(cfg, ledger)
    return {'run_id': str(plan.run_id), 'job_count': len(plan.items), **result}
