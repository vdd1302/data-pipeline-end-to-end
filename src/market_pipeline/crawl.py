"""Scope-local listing/detail slices, persistent fair batches and fail-closed host access."""
from __future__ import annotations

from contextlib import ExitStack, nullcontext
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4, UUID
import json
import os
import re
import time
from urllib.parse import parse_qs, urlsplit, urlunsplit

from src.crawler.config import load_runtime_config, SearchTaskConfig
from src.crawler.parser import AccessChallenge, LayoutChanged, LISTING_PARSER_VERSION
from src.crawler.topcv import NavigationError, TopCVCrawler
from src.crawler.pipeline.batch_runner import _candidate_map, route_for
from src.crawler.pipeline.discovery import (_fold, _keyword_in_heading, is_keyword_route, validate_page_url,
    verify_listing_identity, pagination_from_html)
from src.crawler.pipeline.t1_topcv_adapter import adapt_topcv_detail
from src.raw_storage.schedule import scheduled_run_id
from src.raw_storage.db import get_connection, PROJECT_ROOT
from src.raw_storage.ingestion import RawIngestion
from .warehouse import verify_schema_contract
from .safety import require_source_approval
from . import coordination as C, job_queue as J

HOST = C.HOST
LOCK_ID = 72042702


class WorkerDeadline(RuntimeError):
    pass


class SourceHeld(RuntimeError):
    def __init__(self, gate):
        self.gate = gate
        super().__init__('Source access is held')


class JobWorkUnavailable(RuntimeError):
    pass


class _WorkerBudget:
    def __init__(self, seconds, minimum_interval, batch_size, batch_pause, sleep_fn, monotonic_fn,
                 max_navigations=None):
        self.clock, self.sleep = monotonic_fn, sleep_fn
        self.started = self.clock()
        self.deadline = self.started + seconds
        self.minimum_interval = minimum_interval
        self.batch_size, self.batch_pause = batch_size, batch_pause
        self.max_navigations = max_navigations
        self.last_navigation = None
        self.batch_details = 0
        self.navigation_attempts = self.details_committed = self.listings_committed = 0
        self.jobs_skipped = self.micro_batches_completed = 0

    def navigation_budget_exhausted(self):
        return self.max_navigations is not None and self.navigation_attempts >= self.max_navigations

    def check(self):
        if self.navigation_budget_exhausted():
            raise WorkerDeadline('Session navigation budget reached')
        if self.clock() >= self.deadline:
            raise WorkerDeadline('Worker time budget reached')

    def wait(self, seconds):
        self.check()
        if self.clock() + max(0, seconds) >= self.deadline:
            raise WorkerDeadline('Worker yields before a long wait')
        if seconds > 0:
            self.sleep(seconds)
        self.check()

    def before_navigation(self, detail=False, interval=0):
        self.check()
        if self.batch_details >= self.batch_size:
            self.wait(self.batch_pause)
            self.micro_batches_completed += 1
            self.batch_details = 0
        if self.last_navigation is not None:
            self.wait(max(0, max(self.minimum_interval, interval) -
                          (self.clock() - self.last_navigation)))
        self.check()

    def metrics(self):
        return {'navigation_attempts':self.navigation_attempts,
                'details_committed':self.details_committed,
                'listings_committed':self.listings_committed,
                'jobs_skipped':self.jobs_skipped,
                'micro_batches_completed':self.micro_batches_completed,
                'elapsed_seconds':round(self.clock()-self.started,2)}


class _PacedCrawler:
    def __init__(self, crawler, target, cfg, worker, now_fn):
        self.crawler, self.target, self.cfg, self.worker = crawler, target, cfg, worker
        self.now_fn, self.scope, self.before_detail_attempt = now_fn, None, None

    def __getattr__(self, name):
        method = getattr(self.crawler, name)
        if name not in {'open_public_listing','open_observed_result_page',
                        'submit_verified_search_form','fetch_detail_capture'}:
            return method
        def navigate(*args, **kwargs):
            detail = name == 'fetch_detail_capture'
            interval = (self.cfg.selenium.delay_between_jobs_seconds if detail
                        else self.cfg.selenium.delay_between_pages_seconds)
            self.worker.before_navigation(detail, interval)
            gate = C.host_gate(self.target, self.now_fn(), refresh=True)
            if not gate['allowed']:
                raise SourceHeld(gate)
            C.heartbeat(self.target, self.scope, self.now_fn(),
                        self.cfg.crawl.get('lease_seconds',180), navigation=True)
            if detail and self.before_detail_attempt and not self.before_detail_attempt():
                raise JobWorkUnavailable('Job already accounted')
            self.worker.navigation_attempts += 1
            if detail:
                self.worker.batch_details += 1
            try:
                return method(*args, **kwargs)
            finally:
                # Count cooldown after the page completes loading, not before navigation starts.
                self.worker.last_navigation = self.worker.clock()
        return navigate


def market_crawler(cfg):
    return TopCVCrawler(cfg, observe_http=True)


def create_daily_batch(target, cfg=None, batch_date=None, now=None):
    return C.create_daily_batch(target, cfg or load_runtime_config(),
                                batch_date=batch_date, now=now)


def retry_time(now, attempts, retry_after=None):
    result = now + timedelta(minutes=(5,15,30,60)[min(max(attempts-1,0),3)])
    return max(result,retry_after) if retry_after is not None else result


def plan(cfg=None):
    cfg = cfg or load_runtime_config()
    candidates = scope_routes(cfg)
    result = []
    for task in cfg.tasks:
        route, evidence = route_for(cfg, task, candidate_map=candidates)
        result.append({'position_id': task.position_id, 'location_id': task.location_id,
                       'keyword': task.keyword, 'route': route, 'route_evidence': evidence})
    return {'scope_count': len(result), 'scopes': result}


def scope_routes(cfg):
    """Use observed references, including the two category+keyword UI scopes.

    A reference is navigation input; every current page is verified again.
    Category proxies never label a job as Cybersecurity Specialist.
    """
    candidates = _candidate_map(cfg)
    catalog = json.loads((PROJECT_ROOT / 'tools/route_verifier/observed_topcv_routes_16.json').read_text(encoding='utf-8'))
    for item in catalog['scopes']:
        url = item.get('url') or item.get('category_proxy_reference_url')
        if url:
            candidates.setdefault((item['position_id'], item['location_id']), url)
    return candidates


def _diagnostic(error, reason, safe_response=None):
    error.reason_code = reason
    if safe_response is not None:
        error.safe_response = safe_response
    return error


def _capture(crawler, cfg, task, route, url, page):
    if page == 1:
        crawler.open_public_listing(url)
    else:
        crawler.open_observed_result_page(url, route, page=page)
    ready = crawler.wait_for_listing_ready()
    if not ready.get('ready'):
        raise _diagnostic(NavigationError('Listing DOM is not ready'), 'listing_not_ready')
    if page == 1 and task.position_id == 'cybersecurity_specialist' and not is_keyword_route(route):
        # A category route (broader IT security) needs the keyword applied through the search UI.
        # A keyword route (`...-kl<n>`) already is the site's own keyword search result page.
        crawler.fill_verified_keyword(task.keyword)
        baseline = crawler.inspect_listing_state()
        crawler.submit_verified_search_form()
        ready = crawler.wait_for_listing_ready(baseline=baseline)
        if not ready.get('ready'):
            raise _diagnostic(NavigationError('Keyword results did not become ready'), 'keyword_results_not_ready')
        route = crawler._driver().current_url
        if '-l' not in urlsplit(route).path or 'cr257' not in urlsplit(route).path or urlsplit(route).query:
            raise _diagnostic(NavigationError('Keyword search did not retain a verifiable IT category route'),
                              'keyword_route_unverified')
    state = crawler.inspect_navigation_controls(stage='search_results', keyword=task.keyword)
    capture = crawler.fetch_page_capture()
    checks = (
        (validate_page_url(capture.page_url, route, page=page), 'route_mismatch'),
        (_keyword_in_heading(task.keyword, state.get('page_title', '')), 'keyword_mismatch'),
        (_fold(task.search_label) in _fold(state.get('page_title', '')), 'title_city_mismatch'),
        (_fold(task.search_label) in _fold((state.get('observed_state') or {}).get('location_label') or ''),
         'filter_city_mismatch'),
    )
    for valid, reason in checks:
        if not valid:
            parsed_actual = urlsplit(capture.page_url)
            safe_info = {
                'reason_code': reason,
                'captured_path': urlunsplit((parsed_actual.scheme, parsed_actual.netloc, parsed_actual.path, '', ''))[:100],
                'expected_route': route[:100],
            }
            import logging
            logging.getLogger('topcv_market').warning(
                'Listing verification failed (%s) for %s/%s: actual_url=%s, title=%r, filter_loc=%r',
                reason, task.position_id, task.location_id, capture.page_url,
                state.get('page_title'), (state.get('observed_state') or {}).get('location_label')
            )
            raise _diagnostic(LayoutChanged(f'Listing scope could not be verified: {reason}'), reason, safe_response=safe_info)
    for item in capture.items:
        if not verify_listing_identity(item)[0]:
            raise _diagnostic(LayoutChanged('Listing identity could not be verified'), 'listing_identity_mismatch',
                              safe_response={'reason_code': 'listing_identity_mismatch'})
        if _fold(task.search_label) not in _fold(item.get('location_raw') or ''):
            import logging
            logging.getLogger('topcv_market').warning(
                'Listing city mismatch for %s/%s: search_label=%r, item_loc=%r',
                task.position_id, task.location_id, task.search_label, item.get('location_raw')
            )
            raise _diagnostic(LayoutChanged('Listing city disagrees with requested scope'), 'listing_city_mismatch',
                              safe_response={'reason_code': 'listing_city_mismatch'})
    return capture, route


def verified_pagination_end(html):
    """A missing/changed next href is not proof that the result set ended."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html,'html.parser')
    controls = []
    for node in soup.select('a[rel="next"], .pagination .next, .pagination [aria-label]'):
        label = _fold(' '.join((node.get_text(' ',strip=True),node.get('aria-label',''),node.get('title',''))))
        if node.get('rel') == ['next'] or 'next' in node.get('class',[]) or any(
                word in label for word in ('next','trang sau','trang tiep')):
            disabled = any(parent.has_attr('disabled') or parent.get('aria-disabled')=='true'
                or 'disabled' in parent.get('class',[]) for parent in (node,node.parent) if parent)
            controls.append(disabled)
    if bool(controls) and all(controls):
        return True
    pagers = soup.find_all(class_=lambda c: c and 'pagination' in c)
    if not pagers:
        cards = soup.select('.job-item-search-result')
        if len(cards) < 50:
            return True
    return False


def _raw_prefix(scope):
    return 'topcv-batch/'+str(scope['batch_id'])+'/'+scope['position_id']+'/'+scope['location_id']+'/'


def _ensure_raw(target, source, store, batch, scope, cfg, task, route, captured_at):
    """A Raw run belongs to the actual capture day, never the original batch day."""
    day = captured_at.astimezone(cfg.timezone).date()
    key = _raw_prefix(scope)+day.isoformat()
    slot = datetime.combine(day,datetime.min.time(),cfg.timezone)
    run_id = scheduled_run_id(key,slot)
    exists = source.execute('SELECT run_status FROM raw.crawl_runs WHERE crawl_run_id=%s',(run_id,)).fetchone()
    if exists is None:
        store.start_run(batch['config_snapshot'],snapshot_date=day,run_id=run_id,
            schedule_key=key,scheduled_for=slot,crawler_version='topcv-selenium-market-v4',run_purpose='production')
    elif exists['run_status'] != 'running':
        raise RuntimeError('A terminal capture run must never be reopened')
    raw_task = source.execute('SELECT task_id,last_completed_page FROM raw.search_tasks '
        'WHERE crawl_run_id=%s AND position_id=%s AND location_id=%s',
        (run_id,scope['position_id'],scope['location_id'])).fetchone()
    task_id = (raw_task['task_id'] if raw_task else
               store.start_task(run_id,task.position,task.location,route))
    scope['current_raw_run_id'],scope['current_raw_task_id'] = run_id,task_id
    target.execute('UPDATE market_ops.crawl_scope_queue SET current_raw_run_id=%s,current_raw_task_id=%s '
        'WHERE batch_id=%s AND position_id=%s AND location_id=%s',
        (run_id,task_id,scope['batch_id'],scope['position_id'],scope['location_id']))
    return run_id,task_id


def _adopt_page(target, source, scope, saved):
    logical = int(saved['source_page_number'] or saved['page_number'])
    with target.transaction():
        inserted = target.execute('''INSERT INTO market_ops.crawl_listing_pages
            (batch_id,position_id,location_id,source_page_number,raw_run_id,raw_task_id,
             raw_page_id,raw_page_number,page_url,captured_at)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING raw_page_id''',
            (scope['batch_id'],scope['position_id'],scope['location_id'],logical,saved['crawl_run_id'],
             saved['task_id'],saved['page_id'],saved['page_number'],saved['page_url'],saved['fetched_at'])).fetchone()
        J.enqueue_discoveries(target,source,scope,saved['crawl_run_id'],saved['task_id'],logical)
        target.execute('''UPDATE market_ops.crawl_scope_queue SET listing_cursor=GREATEST(listing_cursor,%s),
            listing_pages_committed=listing_pages_committed+%s,last_success_at=%s,updated_at=now()
            WHERE batch_id=%s AND position_id=%s AND location_id=%s''',
            (logical,int(inserted is not None),saved['fetched_at'],scope['batch_id'],scope['position_id'],scope['location_id']))
    scope['listing_cursor'] = max(scope['listing_cursor'],logical)
    if not saved['listing_items']:
        scope['listing_complete'] = True
        target.execute('UPDATE market_ops.crawl_scope_queue SET listing_complete=true '
            'WHERE batch_id=%s AND position_id=%s AND location_id=%s',
            (scope['batch_id'],scope['position_id'],scope['location_id']))


def _recover_pages(target, source, scope):
    """A committed Raw page is authoritative after a cross-database crash."""
    prefix = _raw_prefix(scope)
    pages = source.execute('''SELECT DISTINCT ON(COALESCE(p.source_page_number,p.page_number)) p.*
        FROM raw.search_pages p JOIN raw.crawl_runs r USING(crawl_run_id)
        WHERE left(r.schedule_key,%s)=%s AND p.fetch_status='success'
        ORDER BY COALESCE(p.source_page_number,p.page_number),p.page_id''',(len(prefix),prefix)).fetchall()
    for saved in pages:
        logical = int(saved['source_page_number'] or saved['page_number'])
        if logical <= scope['listing_cursor']:
            # A previous queue commit may have stopped before job links existed.
            J.enqueue_discoveries(target,source,scope,saved['crawl_run_id'],saved['task_id'],logical)
        elif logical == scope['listing_cursor']+1:
            _adopt_page(target,source,scope,saved)
        else:
            raise RuntimeError('Non-contiguous durable listing cursor')


def _last_page(target, source, scope):
    row = target.execute('''SELECT * FROM market_ops.crawl_listing_pages
        WHERE batch_id=%s AND position_id=%s AND location_id=%s ORDER BY source_page_number DESC LIMIT 1''',
        (scope['batch_id'],scope['position_id'],scope['location_id'])).fetchone()
    return (source.execute('SELECT * FROM raw.search_pages WHERE page_id=%s',(row['raw_page_id'],)).fetchone()
            if row else None)


def _record_failure(target, store, scope, error, phase, error_class, job_id=None):
    evidence = getattr(error, 'safe_response', {}) or {}
    C.record_error(target, scope, error_class, job_id=job_id, evidence=evidence)
    if not scope.get('current_raw_run_id'):
        return
    raw_reason = getattr(error, 'reason_code', error_class)
    clean_reason = raw_reason if (isinstance(raw_reason, str) and len(raw_reason) <= 100 and re.fullmatch(r'[A-Za-z0-9_ .-]*', raw_reason)) else error_class
    context = {'phase': phase, 'position_id': scope['position_id'], 'location_id': scope['location_id'],
               'checkpoint_page': scope['listing_cursor'], 'reason_code': clean_reason}
    for key in ('http_status', 'cf_mitigated', 'cf_ray', 'body_sha256', 'response_class'):
        value = evidence.get(key)
        if value is not None:
            if type(value) in (int, bool) or (isinstance(value, str) and len(value) <= 100 and re.fullmatch(r'[A-Za-z0-9_ .-]*', value)):
                context[key] = value
    try:
        store.log_error(scope['current_raw_run_id'], 'parse_detail' if phase == 'detail' and isinstance(error, LayoutChanged)
                        else 'open_detail' if phase == 'detail' else 'parse_listing' if isinstance(error, LayoutChanged)
                        else 'navigate_search', error, task_id=scope.get('current_raw_task_id'),
                        source_job_id=job_id, job_url=scope.get('_job_url') if job_id else None,
                        retryable=error_class in ('job_transient', 'scope_transient'),
                        error_kind=error_class, retry_after_at=getattr(error, 'retry_after', None), context=context)
    except Exception as log_err:
        import logging
        logging.getLogger('topcv_market').warning('Auxiliary error log failed: %s', log_err)


def _mark_listing_complete(target, scope):
    scope['listing_complete'] = True
    target.execute('UPDATE market_ops.crawl_scope_queue SET listing_complete=true '
        'WHERE batch_id=%s AND position_id=%s AND location_id=%s',
        (scope['batch_id'],scope['position_id'],scope['location_id']))


def _next_listing_target(target, scope, last, cfg, fallback_route):
    """Determine next listing URL or mark scope listing_complete when contract/pagination ends."""
    max_pages = cfg.crawl.get('max_pages_per_query')
    if max_pages is not None and scope['listing_cursor'] >= max_pages:
        _mark_listing_complete(target, scope)
        return None, fallback_route
    if not last:
        return fallback_route, fallback_route
    first = target.execute('''SELECT page_url FROM market_ops.crawl_listing_pages
        WHERE batch_id=%s AND position_id=%s AND location_id=%s ORDER BY source_page_number LIMIT 1''',
        (scope['batch_id'],scope['position_id'],scope['location_id'])).fetchone()
    actual = urlsplit(first['page_url'])
    route = first['page_url'] if is_keyword_route(first['page_url']) else urlunsplit((actual.scheme,actual.netloc,actual.path,'',''))
    pagination = pagination_from_html(last['page_html'],last['page_url'],route,scope['listing_cursor'])
    if pagination.next_url:
        return pagination.next_url, route
    if pagination.status=='no_verified_next_link' and verified_pagination_end(last['page_html']):
        _mark_listing_complete(target, scope)
        return None, route
    raise _diagnostic(LayoutChanged('Pagination completion cannot be verified'),'pagination_unverified')


def _slice(target, source, store, crawler, batch, scope, cfg, task, route, worker, now_fn):
    """One listing page followed immediately by at most five accountable jobs."""
    from selenium.common.exceptions import WebDriverException,InvalidSessionIdException,NoSuchWindowException
    _recover_pages(target,source,scope)
    J.reconcile(target,source,scope)
    last = _last_page(target,source,scope)
    collect_stages = set(cfg.crawl.get('collect') or ('listing', 'detail'))
    scope_priority = cfg.crawl.get('scope_priority', 'default')
    collect_listing = 'listing' in collect_stages and scope_priority != 'detail_only'
    collect_detail = 'detail' in collect_stages or scope_priority == 'detail_only'
    # Drain details before opening the next page when detail collection is enabled.
    # In listing-only mode, advance pages across scopes without fetching details.
    counts = J.scope_counts(target,scope,now_fn())
    max_pages = cfg.crawl.get('max_pages_per_query')
    if max_pages is not None and scope['listing_cursor'] >= max_pages and not scope['listing_complete']:
        _mark_listing_complete(target, scope)
    if (collect_listing and (not collect_detail or not counts['due'])
            and not scope['listing_complete'] and not worker.navigation_budget_exhausted()):
        logical = scope['listing_cursor']+1
        next_url, route = _next_listing_target(target, scope, last, cfg, route)
        if next_url and not scope['listing_complete']:
            scope['_phase'] = 'listing'
            scope.pop('_active_job',None)
            _ensure_raw(target,source,store,batch,scope,cfg,task,route,now_fn())
            capture,_ = _capture(crawler,cfg,task,route,next_url,logical)
            run_id,task_id = _ensure_raw(target,source,store,batch,scope,cfg,task,route,capture.captured_at)
            raw_task = source.execute('SELECT last_completed_page FROM raw.search_tasks WHERE task_id=%s',(task_id,)).fetchone()
            physical = raw_task['last_completed_page']+1
            saved = store.save_search_page(run_id,task_id,physical,capture.page_url,capture.items,
                page_html=capture.page_html,fetched_at=capture.captured_at,parser_version=LISTING_PARSER_VERSION,
                source_page_number=logical,http_status=getattr(capture,'http_status',None))
            page = source.execute('SELECT * FROM raw.search_pages WHERE page_id=%s',(saved.page_id,)).fetchone()
            _adopt_page(target,source,scope,page)
            worker.listings_committed += 1
            # Ensure full cooldown from the end of full listing capture/commit before first detail.
            worker.last_navigation = worker.clock()
            if max_pages is not None and scope['listing_cursor'] >= max_pages and not scope['listing_complete']:
                _mark_listing_complete(target, scope)
    if not collect_detail:
        if not scope['listing_complete']:
            last = _last_page(target,source,scope)
            if last:
                try:
                    _next_listing_target(target, scope, last, cfg, route)
                except LayoutChanged:
                    pass
        if scope['listing_complete']:
            return 'complete', False, None
        return 'partial_ready', True, now_fn()
    target.execute("UPDATE market_ops.crawl_scope_queue SET status='detail_in_progress' WHERE batch_id=%s AND position_id=%s AND location_id=%s",
                   (scope['batch_id'],scope['position_id'],scope['location_id']))
    pending = J.pending_for_scope(target,source,scope,now_fn(),cfg.crawl.get('scope_detail_quantum',5))
    for item in pending:
        if worker.navigation_budget_exhausted():
            break
        worker.check()
        scope['_phase'],scope['_active_job'],scope['_job_url'] = 'detail',item,item['job_url']
        _ensure_raw(target,source,store,batch,scope,cfg,task,route,now_fn())
        crawler.before_detail_attempt = lambda: J.mark_attempt(target,scope,item['source_job_id'],now_fn())
        try:
            capture = crawler.fetch_detail_capture(item['job_url'])
            adapted = adapt_topcv_detail(source_job_id=item['source_job_id'],expected_job_url=item['job_url'],
                captured_job_url=capture.job_url,parsed_detail=capture.parsed,listing=item['listing_payload'],
                expected_city=task.search_label,allow_partial=True)
            run_id,task_id = _ensure_raw(target,source,store,batch,scope,cfg,task,route,capture.captured_at)
            known = source.execute('SELECT 1 FROM raw.job_discoveries WHERE crawl_run_id=%s AND source_job_id=%s LIMIT 1',
                                   (run_id,item['source_job_id'])).fetchone()
            if not known:
                store.register_carried_discovery(run_id,item['source_job_id'],item['job_url'],item['listing_payload'],
                    item['discovered_at'],source_discovery_id=item['discovery_id'])
            saved = store.save_job_detail(run_id,item['source_job_id'],adapted.payload,job_url=adapted.job_url,
                scraped_at=capture.captured_at,parser_version=adapted.parser_version,detail_html=capture.detail_html,
                http_status=getattr(capture,'http_status',None),allow_partial=True)
            J.mark_success(target,scope,item['source_job_id'],run_id,saved.raw_job_id,capture.captured_at,
                           partial=adapted.record_level=='detail_partial')
            target.execute('''UPDATE market_ops.crawl_scope_queue SET details_committed=details_committed+1,
                detail_cursor=%s,last_success_at=%s,updated_at=now()
                WHERE batch_id=%s AND position_id=%s AND location_id=%s''',
                (item['source_job_id'],capture.captured_at,scope['batch_id'],scope['position_id'],scope['location_id']))
            worker.details_committed += 1
        except JobWorkUnavailable:
            continue
        except (AccessChallenge,SourceHeld,WorkerDeadline,InvalidSessionIdException,NoSuchWindowException):
            raise
        except (NavigationError,LayoutChanged,WebDriverException) as error:
            unavailable = getattr(error,'http_status',None) in (404,410)
            terminal = 'not_found' if unavailable else 'quarantined' if isinstance(error,LayoutChanged) else None
            error_class = 'job_not_found' if unavailable else 'job_layout_changed' if terminal else 'job_transient'
            status = J.mark_failure(target,scope,item['source_job_id'],error_class,now_fn(),
                max_attempts=cfg.crawl.get('job_retry_limit',2),
                retry_delay_seconds=cfg.crawl.get('scope_retry_delay_seconds',300),terminal=terminal)
            _record_failure(target,store,scope,error,'detail',error_class,item['source_job_id'])
            if status in ('not_found','quarantined','skipped_with_reason'):
                worker.jobs_skipped += 1
        finally:
            crawler.before_detail_attempt = None
        scope.pop('_active_job',None)
    counts = J.scope_counts(target,scope,now_fn())
    if not counts['pending'] and not scope['listing_complete']:
        last = _last_page(target,source,scope)
        if last:
            try:
                _next_listing_target(target, scope, last, cfg, route)
            except LayoutChanged:
                pass
    if scope['listing_complete'] and not counts['pending']:
        return 'complete',False,None
    next_time = (counts['next_eligible_at'] if scope['listing_complete'] and not counts['due']
                 and counts['pending'] else now_fn())
    return 'partial_ready',True,next_time


def resume_due_batch(target, **kwargs):
    kwargs['create_batch'] = False
    return run_queue(target,**kwargs)


def run_queue(target, *, cfg=None, max_scopes=20, create_batch=False, batch_id=None,
              crawler_factory=market_crawler, source=None, store=None, now=None,
              now_fn=None,sleep_fn=time.sleep,monotonic_fn=time.monotonic,
              allow_manual_verification=False,**legacy_options):
    from selenium.common.exceptions import InvalidSessionIdException,NoSuchWindowException,WebDriverException
    cfg = cfg or load_runtime_config()
    if type(max_scopes) is not int or not 1<=max_scopes<=100:
        raise ValueError('max_scopes must be a bounded slice budget in 1..100')
    if crawler_factory is market_crawler:
        require_source_approval()
    allow_manual = (
        bool(allow_manual_verification)
        and not cfg.selenium.headless
        and cfg.selenium.on_challenge == 'pause_for_manual_verification'
        and os.environ.get('TOPCV_CONTAINER_BROWSER') != '1'
    )
    if not allow_manual:
        cfg = replace(cfg,selenium=replace(cfg.selenium,on_challenge='stop_and_log'))
    now_fn = now_fn or (lambda:datetime.now(timezone.utc))
    now = C.utc_now(now or now_fn())
    if create_batch:
        created = C.create_daily_batch(target,cfg,now=now)
        batch_id = created['batch_id']
    verify_schema_contract(target)
    if not target.execute('SELECT pg_try_advisory_lock(%s) AS locked',(LOCK_ID,)).fetchone()['locked']:
        return {'status':'already_running','source_status':'NO_NEW_DATA','attempted':0,
                'snapshot_date':now.astimezone(cfg.timezone).date(),'batch_id':batch_id}
    try:
        batch = C.select_batch(target,batch_id,now=now)
        if batch is None:
            return {'status':'nothing_due','source_status':'NO_NEW_DATA','attempted':0,
                    'snapshot_date':now.astimezone(cfg.timezone).date(),'batch_id':batch_id,
                    'details_committed':0,'listings_committed':0,'worker_policy_version':'readme-v1'}
        def report(status,worker=None,attempted=0):
            summary = C.read_batch_summary(target,batch['batch_id'])
            result = {'status':status,'source_status':'SOURCE_BLOCKED' if status=='source_blocked' else 'SLICE_PASS',
                'batch_id':batch['batch_id'],'batch_date':batch['batch_date'],
                'snapshot_date':now_fn().astimezone(cfg.timezone).date(),'attempted':attempted,
                'worker_policy_version':'readme-v1','batch_summary':summary,
                'host_state':summary['host_state'],'work_remaining':summary['work_remaining'],
                'details_committed':0,'listings_committed':0,'jobs_skipped':0}
            if worker is not None:result.update(worker.metrics())
            return result
        gate = C.host_gate(target,now,refresh=True)
        if not gate['allowed']:
            return report('source_blocked')
        seconds = float(cfg.crawl.get('worker_time_budget_seconds',1500))
        if not 0<seconds<=1500:raise ValueError('Worker budget must be in 0..1500 seconds')
        max_navigations = cfg.crawl.get('max_navigations_per_session')
        if max_navigations is not None and (type(max_navigations) is not int or max_navigations < 1):
            raise ValueError('crawl.max_navigations_per_session must be a positive integer')
        scope_priority = cfg.crawl.get('scope_priority', 'default')
        if scope_priority not in {'default', 'detail_first', 'detail_only'}:
            raise ValueError(f'Invalid crawl.scope_priority: {scope_priority}')
        worker = _WorkerBudget(seconds,max(15,cfg.crawl.get('min_navigation_interval_seconds',15)),
            cfg.crawl.get('max_details_per_batch',20),cfg.crawl.get('micro_batch_pause_seconds',45),
            sleep_fn,monotonic_fn,max_navigations=max_navigations)
        owner = uuid4()
        slices = 0
        status = 'partial_success'
        with ExitStack() as stack:
            source = source or stack.enter_context(get_connection(read_only=True,autocommit=True))
            store = store or RawIngestion()
            real = stack.enter_context(crawler_factory(cfg))
            crawler = _PacedCrawler(real,target,cfg,worker,now_fn)
            def _keep_lease_alive():
                if crawler.scope is not None:
                    C.heartbeat(target,crawler.scope,now_fn(),cfg.crawl.get('lease_seconds',180),navigation=False)
            real._on_wait_tick = _keep_lease_alive
            candidates = scope_routes(cfg)
            while slices<max_scopes:
                try:worker.check()
                except WorkerDeadline:break
                gate = C.host_gate(target,now_fn(),refresh=True)
                if not gate['allowed']:
                    status = 'source_blocked'
                    break
                scope = C.claim_scope(target,batch['batch_id'],owner,now_fn(),
                                      cfg.crawl.get('lease_seconds',180),priority=scope_priority)
                if scope is None:break
                slices += 1
                crawler.scope = scope
                task = SearchTaskConfig(position_id=scope['position_id'],keyword=scope['keyword'],
                                        location_id=scope['location_id'],search_label=scope['location_label'])
                try:
                    route,_ = route_for(cfg,task,candidate_map=candidates)
                    if not route:raise _diagnostic(LayoutChanged('No verified route for scope'),'missing_observed_route')
                    _ensure_raw(target,source,store,batch,scope,cfg,task,route,now_fn())
                    state,remaining,next_time = _slice(target,source,store,crawler,batch,scope,cfg,task,route,worker,now_fn)
                    C.release_scope(target,scope,state,work_remaining=remaining,next_eligible_at=next_time,now=now_fn())
                except AccessChallenge as error:
                    gate = C.hold_host(target,error,now_fn(),cfg.crawl.get('rate_limit_backoff_seconds',900))
                    active = scope.get('_active_job')
                    if active:J.defer_host(target,scope,active['source_job_id'],'host_access_denied')
                    _record_failure(target,store,scope,error,scope.get('_phase','listing'),
                                    'host_rate_limit' if error.kind=='rate_limited' else 'host_access_denied',
                                    active['source_job_id'] if active else None)
                    C.release_scope(target,scope,'blocked_by_host',next_eligible_at=gate.get('next_allowed_at'),now=now_fn())
                    target.execute("UPDATE market_ops.crawl_batches SET status='SOURCE_BLOCKED',updated_at=now() WHERE batch_id=%s",(batch['batch_id'],))
                    status = 'source_blocked'
                    break
                except SourceHeld:
                    C.release_scope(target,scope,'blocked_by_host',now=now_fn())
                    status='source_blocked'
                    break
                except WorkerDeadline:
                    C.release_scope(target,scope,'partial_ready' if scope['listing_cursor'] else 'pending',now=now_fn())
                    break
                except (InvalidSessionIdException,NoSuchWindowException):
                    C.release_scope(target,scope,'partial_ready' if scope['listing_cursor'] else 'pending',now=now_fn())
                    status='browser_unavailable'
                    break
                except LayoutChanged as error:
                    _record_failure(target,store,scope,error,'listing','scope_layout_changed')
                    C.release_scope(target,scope,'needs_review',error_class='scope_layout_changed',now=now_fn())
                except (NavigationError,WebDriverException) as error:
                    _record_failure(target,store,scope,error,'listing','scope_transient')
                    failures = scope['failure_count']+1
                    exhausted = failures>=cfg.crawl.get('scope_retry_limit',3)
                    C.release_scope(target,scope,'exhausted' if exhausted else 'retry_pending',
                        work_remaining=not exhausted,error_class='scope_transient',recoverable=True,
                        next_eligible_at=now_fn()+timedelta(seconds=cfg.crawl.get('scope_retry_delay_seconds',300)),now=now_fn())
                except Exception:
                    C.release_scope(target,scope,'partial_ready' if scope['listing_cursor'] else 'pending',now=now_fn())
                    raise
        summary=C.read_batch_summary(target,batch['batch_id'])
        if summary['scope_coverage_complete']:
            target.execute("UPDATE market_ops.crawl_batches SET status='COMPLETE',updated_at=now() WHERE batch_id=%s",(batch['batch_id'],))
        elif not summary['work_remaining']:
            target.execute("UPDATE market_ops.crawl_batches SET status='PARTIAL',updated_at=now() WHERE batch_id=%s",(batch['batch_id'],))
        elif status != 'source_blocked' and C.host_gate(target, now_fn())['allowed']:
            target.execute("UPDATE market_ops.crawl_batches SET status='OPEN',updated_at=now() WHERE batch_id=%s",(batch['batch_id'],))
        if not worker.navigation_attempts and status=='partial_success':status='nothing_due'
        return report(status,worker,slices)
    finally:
        target.execute('SELECT pg_advisory_unlock(%s)',(LOCK_ID,))



