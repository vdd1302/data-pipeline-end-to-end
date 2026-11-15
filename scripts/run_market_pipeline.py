"""Daily coordination, bounded recovery and data-driven warehouse stages."""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import replace
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
from uuid import uuid4

from src.crawler.config import load_runtime_config
from src.market_pipeline.warehouse import target_connection, initialize, initialize_raw, refresh, verify_nonnull
from src.raw_storage.db import get_connection

EXIT_DEFERRED = 75
EXIT_REVIEW_REQUIRED = 4
BUSINESS_STATUSES = frozenset({'SUCCESS_FULL', 'SUCCESS_PARTIAL', 'NO_NEW_DATA',
                              'SOURCE_BLOCKED', 'FAILED_DQ', 'FAILED_INFRA'})
FATAL_SOURCE_STATES = frozenset({'fatal_error', 'browser_unavailable', 'schema_error', 'database_error'})
DENIED_HOST_STATES = frozenset({'ACCESS_DENIED', 'MANUAL_HOLD'})
READ_ONLY_ACTIONS = frozenset({'status', 'coverage', 'progress', 'health'})


class ManualCrawlOnly(Exception):
    """Raised when an Airflow-triggered run asks to touch the source while crawling is manual."""


def airflow_crawl_enabled(environ=None):
    return (os.environ if environ is None else environ).get('TOPCV_AIRFLOW_CRAWL_ENABLED') == 'YES'


def fatal_result(error, stage):
    """Never render exception strings containing connection or browser secrets."""
    import traceback
    traceback.print_exc()
    status = getattr(error, 'business_status', None)
    if status not in {'FAILED_DQ', 'FAILED_INFRA'}:
        status = 'FAILED_DQ' if type(error).__name__ in {'DataQualityError', 'WarehouseQualityError'} else 'FAILED_INFRA'
    result = {'business_status': status, 'error_type': type(error).__name__, 'error_stage': stage, 'error_detail': str(error)[:400]}
    if getattr(error, 'operator_hint', None):
        # Written by this project for the operator (see src/crawler/undetected.py).
        result['error_detail'] = str(error.operator_hint)[:400]
    elif type(error).__module__.startswith('selenium.'):
        # Browser startup/driver errors carry no credentials; keep the first line so the
        # operator can tell a missing driver from a locked profile or a version mismatch.
        first = (str(error).strip().splitlines() or [''])[0]
        result['error_detail'] = re.sub(r'\s+', ' ', first)[:200]
    state = getattr(error, 'sqlstate', None)
    if isinstance(state, str) and re.fullmatch(r'[A-Z0-9]{5}', state):
        result['sqlstate'] = state
    return result


def crawl_exit_code(result):
    if result.get('business_status') in {'FAILED_DQ', 'FAILED_INFRA'} or result.get('status') in FATAL_SOURCE_STATES:
        return 1
    return 0 if result.get('details_committed', 0) or result.get('listings_committed', 0) else EXIT_DEFERRED


def batch_outcome(result):
    if result.get('business_status') in BUSINESS_STATUSES:
        return result['business_status']
    if result.get('status') in FATAL_SOURCE_STATES:
        return 'FAILED_INFRA'
    if (result.get('source_status') == 'SOURCE_BLOCKED' or
            result.get('status') in {'SOURCE_BLOCKED', 'source_blocked', 'host_access_denied', 'needs_review'} or
            result.get('host_state') in DENIED_HOST_STATES):
        return 'SOURCE_BLOCKED'
    if result.get('details_committed', 0) or result.get('listings_committed', 0):
        return 'SUCCESS_PARTIAL'
    return 'NO_NEW_DATA'


def read_report(path, *, required=False):
    if path is None:
        return {} if not required else {'business_status': 'FAILED_INFRA', 'error_type': 'MissingStageReport'}
    try:
        data = json.loads(Path(path).read_text(encoding='utf-8-sig'))
        if not isinstance(data, dict) or (required and not data):
            raise ValueError('Stage report must be an object')
        return data
    except (OSError, ValueError, TypeError):
        return {'business_status': 'FAILED_INFRA', 'error_type': 'MissingOrInvalidStageReport'}


def read_source_report(path):
    result = read_report(path, required=True)
    source = result.get('crawl', result)
    if not isinstance(source, dict) or not (
            'status' in source or 'source_status' in source or
            source.get('business_status') in {'FAILED_DQ', 'FAILED_INFRA'}):
        return {'business_status': 'FAILED_INFRA', 'error_type': 'MissingOrInvalidSourceOutcome'}
    return source


def write_report(path, result):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
    temporary.replace(path)


def runtime_config(path):
    cfg = load_runtime_config(path)
    if os.environ.get('TOPCV_CONTAINER_BROWSER') == '1':
        display = os.environ.get('DISPLAY', '')
        x_socket = Path(f"/tmp/.X11-unix/X{display[1:].split('.')[0]}") if display.startswith(':') else None
        use_xvfb = os.environ.get('TOPCV_CONTAINER_XVFB') == '1' and bool(x_socket and x_socket.exists())
        cfg = replace(cfg, selenium=replace(cfg.selenium, browser_profile_dir=Path('/opt/airflow/browser-profile'),
                                            headless=not use_xvfb, on_challenge='stop_and_log'))
    return cfg


def _bounded_seconds(name, default, maximum, environ):
    raw = (environ.get(name) or '').strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        value = -1.0
    if not 0 <= value <= maximum:
        raise ValueError(f'{name} must be a number of seconds between 0 and {maximum:g}')
    return value


def browser_driver_settings(driver, environ=None):
    """Validate the operator's transport choice before anything is started."""
    from src.crawler import undetected as U
    if driver not in U.DRIVERS:
        raise ValueError('The browser driver must be one of: ' + ', '.join(sorted(U.DRIVERS)))
    if driver != U.DRIVER_UNDETECTED:
        return None
    env = os.environ if environ is None else environ
    return {'driver': driver,
            'detached_navigation_seconds': _bounded_seconds(
                'TOPCV_UC_SETTLE_SECONDS', U.DEFAULT_SETTLE_SECONDS, U.MAX_SETTLE_SECONDS, env),
            'challenge_grace_seconds': _bounded_seconds(
                'TOPCV_UC_CHALLENGE_GRACE_SECONDS', U.DEFAULT_CHALLENGE_GRACE_SECONDS,
                U.MAX_CHALLENGE_GRACE_SECONDS, env)}


def apply_browser_driver(cfg, settings):
    """Select the browser transport for this run; YAML and the default stay plain Selenium."""
    if not settings:
        return cfg
    from src.crawler.undetected import PROFILE_SUBDIRECTORY
    # A profile of its own: it never mixes with the one earlier Selenium sessions used.
    return replace(cfg, selenium=replace(
        cfg.selenium, browser_profile_dir=cfg.selenium.browser_profile_dir / PROFILE_SUBDIRECTORY,
        **settings))


def configure_reader(conn):
    from urllib.parse import urlsplit
    from psycopg import sql
    secret = urlsplit(os.environ.get('MARKET_READER_URL', '')).password or os.environ.get('MARKET_READER_PASSWORD')
    if not secret:
        return
    with conn.transaction():
        role = conn.execute("SELECT 1 FROM pg_roles WHERE rolname='market_reader'").fetchone()
        priv = conn.execute('SELECT rolsuper, rolcreaterole FROM pg_roles WHERE rolname=current_user').fetchone()
        can_manage_roles = bool(priv and (priv['rolsuper'] or priv['rolcreaterole']))
        if not role:
            conn.execute('CREATE ROLE market_reader LOGIN')
        if can_manage_roles:
            conn.execute(sql.SQL('ALTER ROLE market_reader PASSWORD {}').format(sql.Literal(secret)))
            conn.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO market_reader').format(sql.Identifier(conn.info.dbname)))
        conn.execute('GRANT USAGE ON SCHEMA market_dw TO market_reader')
        conn.execute('GRANT SELECT ON ALL TABLES IN SCHEMA market_dw TO market_reader')
        conn.execute('ALTER DEFAULT PRIVILEGES IN SCHEMA market_dw GRANT SELECT ON TABLES TO market_reader')
        if can_manage_roles:
            conn.execute('ALTER ROLE market_reader SET default_transaction_read_only=on')


def scope_summary(cfg, scopes):
    expected = {(task.position_id, task.location_id) for task in cfg.tasks}
    actual = {(scope['position_id'], scope['location_id']) for scope in scopes}
    complete = bool(expected) and len(expected) == len(scopes) and expected == actual and all(
        scope['status'] in {'complete', 'success'} for scope in scopes)
    return {'expected_scopes': len(expected), 'scope_count': len(scopes),
            'scope_status_counts': dict(Counter(scope['status'] for scope in scopes)),
            'scope_coverage_complete': complete, 'scope_coverage_target_required': False}


def read_scope_progress(conn, cfg=None, *, batch_id=None, now=None):
    from src.market_pipeline.coordination import read_batch_summary, host_gate
    from src.market_pipeline.safety import require_source_approval
    summary = read_batch_summary(conn, batch_id=batch_id) or {}
    gate = host_gate(conn, now=now)
    try:
        require_source_approval()
        authorized = True
    except PermissionError:
        authorized = False
    return {**summary, 'host': gate, 'host_state': gate.get('host_state', gate.get('state')),
            'source_access_allowed': gate.get('allowed', False) and authorized,
            'source_authorization_configured': authorized, 'scope_coverage_target_required': False}


def _batch_id(args, source=None):
    if args.batch_id:
        return args.batch_id
    if args.batch_report:
        seed = read_report(args.batch_report, required=True)
        return seed.get('batch_id') or (seed.get('batch') or {}).get('batch_id')
    return (source or {}).get('batch_id')


def _status(details, default='NO_NEW_DATA'):
    value = details.get('business_status') or details.get('status')
    return value if value in BUSINESS_STATUSES else default


def evaluate_business_status(source, readiness, warehouse, quality, summary):
    stages = [source, readiness, warehouse, quality]
    if any(_status(stage) == 'FAILED_DQ' for stage in stages):
        return 'FAILED_DQ'
    if any(_status(stage) == 'FAILED_INFRA' for stage in stages) or source.get('status') in FATAL_SOURCE_STATES:
        return 'FAILED_INFRA'
    if quality.get('passed') is False or quality.get('quality_passed') is False:
        return 'FAILED_DQ'
    if quality and quality.get('quality_passed') is not True and quality.get('passed') is not True:
        return 'FAILED_DQ'
    loaded = int(warehouse.get('loaded', 0) or 0)
    listings = int(warehouse.get('listing_loaded', warehouse.get('listings_loaded', 0)) or 0)
    if loaded or listings or _status(warehouse) in {'SUCCESS_FULL', 'SUCCESS_PARTIAL'}:
        expected = int(summary.get('expected_scopes', summary.get('expected_scope_count', 0)) or 0)
        complete = int(summary.get('scope_complete', 0) or 0)
        full = bool(summary.get('scope_coverage_complete')) or (expected > 0 and complete == expected)
        return 'SUCCESS_FULL' if full else 'SUCCESS_PARTIAL'
    host_state = summary.get('host_state') or (summary.get('host') or {}).get('host_state') or (summary.get('host') or {}).get('state')
    if (batch_outcome(source) == 'SOURCE_BLOCKED' or readiness.get('source_blocked') or
            host_state in DENIED_HOST_STATES or summary.get('source_authorization_configured') is False):
        return 'SOURCE_BLOCKED'
    return 'NO_NEW_DATA'


def progress_exit_code(result, source_result=None):
    return 1 if result.get('business_status') in {'FAILED_DQ', 'FAILED_INFRA'} else 0


def _persist(conn, args, status, details, batch_id=None):
    from src.market_pipeline.warehouse import record_pipeline_run
    return record_pipeline_run(conn, args.dag_id, args.run_id, status, details,
                               stage=args.action, batch_id=batch_id)


def main(argv=None):
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    from dotenv import dotenv_values
    from src.raw_storage.db import PROJECT_ROOT
    for env_name in ('.env.market', '.env'):
        for key, value in dotenv_values(PROJECT_ROOT / env_name).items():
            if key not in os.environ and value is not None:
                os.environ[key] = value
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['plan', 'initialize', 'create-batch', 'crawl', 'readiness',
        'refresh', 'quality', 'evaluate', 'status', 'coverage', 'progress', 'health', 'review-host', 'e2e'])
    parser.add_argument('--config', default='config/crawl.airflow.yaml')
    parser.add_argument('--max-scopes', type=int, default=20)
    parser.add_argument('--batch-id')
    parser.add_argument('--batch-date')
    parser.add_argument('--dag-id', default=os.environ.get('PIPELINE_DAG_ID', 'manual'))
    parser.add_argument('--run-id', default=os.environ.get('PIPELINE_RUN_ID') or 'manual_' + str(uuid4()))
    parser.add_argument('--review-reference')
    parser.add_argument('--reprocess', action='store_true')
    parser.add_argument('--confirm-reprocess', action='store_true')
    parser.add_argument('--allow-manual-verification', action='store_true')
    priority_group = parser.add_mutually_exclusive_group()
    priority_group.add_argument('--detail-first', action='store_true',
                                help='Claim scopes with due detail_pending jobs before pending listing scopes')
    priority_group.add_argument('--detail-only', action='store_true',
                                help='Crawl only due detail jobs from collected listings without opening listing pages')
    parser.add_argument('--max-navigations', type=int, default=None,
                        help='Stop cleanly after at most N browser navigations in this session')
    parser.add_argument('--browser-driver', default=None,
                        help='selenium (default) or undetected; default comes from TOPCV_BROWSER_DRIVER')
    for option in ('report', 'batch-report', 'source-report', 'readiness-report', 'warehouse-report', 'quality-report'):
        parser.add_argument('--' + option, type=Path)
    args = parser.parse_args(argv)
    if args.max_navigations is not None and args.max_navigations < 1:
        parser.error('--max-navigations must be at least 1')
    if args.reprocess and (not args.confirm_reprocess or os.environ.get('TOPCV_ALLOW_REPROCESS') != 'YES'):
        parser.error('Reprocessing requires --confirm-reprocess and TOPCV_ALLOW_REPROCESS=YES after backup')
    if args.action == 'review-host' and not args.review_reference:
        parser.error('review-host requires --review-reference after verifying source access')
    try:
        driver_settings = browser_driver_settings(
            args.browser_driver or (os.environ.get('TOPCV_BROWSER_DRIVER') or '').strip() or 'selenium')
    except ValueError as exc:
        parser.error(str(exc))
    if args.report and args.action not in READ_ONLY_ACTIONS and args.report.exists():
        args.report.unlink()
    started = datetime.now(timezone.utc)
    result = {}
    code = 0
    selected_batch = args.batch_id
    try:
        cfg = apply_browser_driver(runtime_config(args.config), driver_settings)
        if args.detail_first or args.detail_only or args.max_navigations is not None:
            crawl_cfg = dict(cfg.crawl)
            if args.detail_first or args.detail_only:
                collect_stages = list(crawl_cfg.get('collect') or ('listing', 'detail'))
                if 'detail' not in collect_stages:
                    collect_stages.append('detail')
                crawl_cfg['collect'] = tuple(collect_stages)
            if args.detail_first:
                crawl_cfg['scope_priority'] = 'detail_first'
            elif args.detail_only:
                crawl_cfg['scope_priority'] = 'detail_only'
            if args.max_navigations is not None:
                crawl_cfg['max_navigations_per_session'] = args.max_navigations
            cfg = replace(cfg, crawl=crawl_cfg)
        if args.action == 'plan':
            from src.market_pipeline.crawl import plan
            result = plan(cfg)
        else:
            with target_connection(read_only=args.action in READ_ONLY_ACTIONS) as conn:
                if args.action == 'initialize':
                    with get_connection() as raw_conn:
                        initialize_raw(raw_conn)
                    initialize(conn)
                    configure_reader(conn)
                    result = {'business_status': 'NO_NEW_DATA', 'initialized': True, **verify_nonnull(conn)}
                elif args.action == 'create-batch':
                    from src.market_pipeline.crawl import create_daily_batch
                    value = args.batch_date or os.environ.get('PIPELINE_BATCH_DATE')
                    batch_day = date.fromisoformat(value) if value else started.astimezone(cfg.timezone).date()
                    batch = create_daily_batch(conn, cfg=cfg, batch_date=batch_day, now=started)
                    selected_batch = batch['batch_id']
                    result = {'business_status': 'NO_NEW_DATA', 'batch': batch, 'batch_id': selected_batch,
                              'stage_outcome': 'BATCH_READY'}
                elif args.action in {'crawl', 'e2e'}:
                    from src.market_pipeline.crawl import resume_due_batch
                    from src.market_pipeline.safety import require_source_approval
                    selected_batch = _batch_id(args)
                    if args.batch_report and not selected_batch:
                        raise ValueError('Daily worker requires its successful batch creation report')
                    try:
                        if not args.dag_id == 'manual' and not airflow_crawl_enabled():
                            raise ManualCrawlOnly()
                        require_source_approval()
                    except ManualCrawlOnly:
                        # Source collection is an operator-run step; Airflow only does the data stages.
                        source = {'status': 'manual_crawl_only', 'source_status': 'NO_NEW_DATA',
                                  'reason': 'airflow_crawl_disabled', 'batch_id': selected_batch,
                                  'details_committed': 0, 'listings_committed': 0}
                    except PermissionError:
                        source = {'status': 'source_blocked', 'source_status': 'SOURCE_BLOCKED',
                                  'host_state': 'MANUAL_HOLD', 'reason': 'source_authorization_missing',
                                  'batch_id': selected_batch, 'details_committed': 0, 'listings_committed': 0}
                    else:
                        from src.market_pipeline.coordination import session_gap_remaining
                        wait = session_gap_remaining(conn, cfg.crawl.get('min_session_gap_seconds', 0), started)
                        if wait > 0:
                            # Exit 75 (deferred): Airflow skips the task instead of failing it.
                            source = {'status': 'cooldown', 'source_status': 'NO_NEW_DATA',
                                      'reason': 'min_session_gap', 'retry_in_seconds': wait,
                                      'batch_id': selected_batch, 'details_committed': 0,
                                      'listings_committed': 0}
                        else:
                            source = resume_due_batch(
                                conn, cfg=cfg, batch_id=selected_batch, max_scopes=args.max_scopes,
                                allow_manual_verification=args.allow_manual_verification,
                            )
                    selected_batch = source.get('batch_id') or selected_batch
                    source['business_status'] = batch_outcome(source)
                    result = {'crawl': source, 'business_status': source['business_status'], 'batch_id': selected_batch}
                    code = crawl_exit_code(source)
                    if args.action == 'e2e':
                        from src.market_pipeline.warehouse import evaluate_quality
                        result['warehouse'] = refresh(target=conn, reprocess=args.reprocess, batch_id=selected_batch)
                        result['quality'] = evaluate_quality(conn)
                        result['quality'].setdefault('quality_passed', True)
                        result['batch_summary'] = read_scope_progress(conn, cfg, batch_id=selected_batch)
                        result['business_status'] = evaluate_business_status(source, {}, result['warehouse'],
                            result['quality'], result['batch_summary'])
                        code = progress_exit_code(result)
                elif args.action == 'readiness':
                    from src.market_pipeline.warehouse import evaluate_data_readiness
                    source = read_source_report(args.source_report) if args.source_report else {}
                    selected_batch = _batch_id(args, source)
                    result = evaluate_data_readiness(target=conn, batch_id=selected_batch)
                    result['batch_id'] = selected_batch
                    result.setdefault('business_status', 'SOURCE_BLOCKED' if result.get('source_blocked') else 'NO_NEW_DATA')
                elif args.action == 'refresh':
                    source = read_source_report(args.source_report) if args.source_report else {}
                    selected_batch = _batch_id(args, source)
                    readiness = read_report(args.readiness_report, required=True) if args.readiness_report else None
                    if readiness and _status(readiness) in {'FAILED_DQ', 'FAILED_INFRA'}:
                        result = readiness
                    elif readiness is not None and not readiness.get('ready', False):
                        result = {'business_status': 'NO_NEW_DATA', 'selected': 0, 'loaded': 0,
                                  'listings_loaded': 0, 'quarantined': 0, 'batch_id': selected_batch}
                    else:
                        result = refresh(target=conn, reprocess=args.reprocess, batch_id=selected_batch)
                        result['batch_id'] = selected_batch
                elif args.action == 'quality':
                    from src.market_pipeline.warehouse import evaluate_quality
                    source = read_source_report(args.source_report) if args.source_report else {}
                    selected_batch = _batch_id(args, source)
                    result = evaluate_quality(conn)
                    result.setdefault('quality_passed', True)
                    result['batch_id'] = selected_batch
                    result.setdefault('business_status', 'NO_NEW_DATA')
                elif args.action == 'evaluate':
                    source = read_source_report(args.source_report) if args.source_report else {}
                    readiness = read_report(args.readiness_report, required=bool(args.readiness_report))
                    warehouse = read_report(args.warehouse_report, required=bool(args.warehouse_report))
                    quality = read_report(args.quality_report, required=bool(args.quality_report))
                    selected_batch = _batch_id(args, source) or readiness.get('batch_id') or warehouse.get('batch_id')
                    summary = read_scope_progress(conn, cfg, batch_id=selected_batch)
                    status = evaluate_business_status(source, readiness, warehouse, quality, summary)
                    result = {'business_status': status, 'batch_id': selected_batch, 'batch_summary': summary,
                              'source': source, 'readiness': readiness, 'warehouse': warehouse, 'quality': quality,
                              'publish_allowed': status in {'SUCCESS_FULL', 'SUCCESS_PARTIAL'},
                              'coverage_label': 'observed_sample', 'market_total_claimed': False}
                elif args.action == 'health':
                    from src.market_pipeline.health import evaluate_health, notify, EXIT_UNHEALTHY
                    result = evaluate_health(conn)
                    if not result['healthy']:
                        result['alert_webhook_sent'] = notify(result)
                        code = EXIT_UNHEALTHY
                elif args.action == 'review-host':
                    from src.market_pipeline.coordination import set_host_available
                    from src.market_pipeline.safety import require_source_approval
                    require_source_approval()
                    set_host_available(conn, args.review_reference)
                    result = {'business_status': 'NO_NEW_DATA', 'host_state': 'AVAILABLE', 'operator_review_recorded': True}
                else:
                    result = read_scope_progress(conn, cfg, batch_id=selected_batch)
                    result['business_status'] = 'SOURCE_BLOCKED' if not result.get('source_access_allowed', False) else 'NO_NEW_DATA'
                if args.action not in READ_ONLY_ACTIONS:
                    status = result.get('business_status', 'NO_NEW_DATA')
                    result['pipeline_run_id'] = _persist(conn, args, status, result, selected_batch)
                    if status in {'FAILED_DQ', 'FAILED_INFRA'}:
                        code = 1
    except Exception as exc:
        result = fatal_result(exc, args.action)
        result['batch_id'] = selected_batch
        code = 1
        try:
            with target_connection() as conn:
                result['pipeline_run_id'] = _persist(conn, args, result['business_status'], result, selected_batch)
        except Exception:
            result['pipeline_status_persistence_failed'] = True
    result['dag_id'], result['run_id'] = args.dag_id, args.run_id
    result['started_at'], result['finished_at'] = started, datetime.now(timezone.utc)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    if args.report:
        write_report(args.report, result)
    return code


if __name__ == '__main__':
    raise SystemExit(main())
