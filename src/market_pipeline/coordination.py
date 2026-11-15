"""Persistent batch coordination. No browser, source access, or automatic seeding."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid5, NAMESPACE_URL

from psycopg.types.json import Jsonb

HOST = 'www.topcv.vn'
ACTIVE_STATES = ('pending','listing_in_progress','detail_in_progress','partial_ready','retry_pending','blocked_by_host')
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def utc_now(value=None):
    value = value or datetime.now(timezone.utc)
    if value.tzinfo is None:
        raise ValueError('Coordination timestamps must include timezone')
    return value


def create_daily_batch(target, cfg, batch_date=None, now=None):
    """Only the daily coordinator calls this idempotent creation operation."""
    now = utc_now(now)
    batch_date = batch_date or now.astimezone(cfg.timezone).date()
    identities = {(task.position_id, task.location_id) for task in cfg.tasks}
    if not identities or len(identities) != len(cfg.tasks):
        raise ValueError('Daily batch requires unique configured scopes')
    batch_id = uuid5(NAMESPACE_URL, 'topcv-crawl-batch/' + batch_date.isoformat())
    with target.transaction():
        target.execute('''INSERT INTO market_ops.crawl_batches
            (batch_id,batch_date,expected_scopes,config_snapshot)
            VALUES(%s,%s,%s,%s) ON CONFLICT(batch_date) DO NOTHING''',
            (batch_id,batch_date,len(cfg.tasks),Jsonb(cfg.snapshot)))
        batch = target.execute('SELECT * FROM market_ops.crawl_batches WHERE batch_date=%s FOR UPDATE',
                               (batch_date,)).fetchone()
        if batch['expected_scopes'] != len(cfg.tasks):
            raise ValueError('Existing immutable batch has a different scope catalog')
        for order, task in enumerate(cfg.tasks):
            target.execute('''INSERT INTO market_ops.crawl_scope_queue
                (batch_id,position_id,location_id,keyword,location_label,scope_order)
                VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING''',
                (batch['batch_id'],task.position_id,task.location_id,task.keyword,task.search_label,order))
        actual = {(row['position_id'],row['location_id']) for row in target.execute(
            'SELECT position_id,location_id FROM market_ops.crawl_scope_queue WHERE batch_id=%s',
            (batch['batch_id'],))}
        if actual != identities:
            raise ValueError('Existing immutable batch scope identities differ')
    return dict(batch)


def select_batch(target, batch_id=None, now=None):
    if batch_id is not None:
        return target.execute('SELECT * FROM market_ops.crawl_batches WHERE batch_id=%s',
                              (UUID(str(batch_id)),)).fetchone()
    return target.execute('''SELECT b.* FROM market_ops.crawl_batches b
        WHERE EXISTS(SELECT 1 FROM market_ops.crawl_scope_queue q
          WHERE q.batch_id=b.batch_id AND q.work_remaining AND q.next_eligible_at<=%s
            AND (q.lease_owner IS NULL OR q.lease_expires_at<=%s)
            AND q.status IN ('pending','listing_in_progress','detail_in_progress','partial_ready','retry_pending','blocked_by_host'))
        ORDER BY b.batch_date,b.created_at LIMIT 1''',(utc_now(now),utc_now(now))).fetchone()


def host_gate(target, now=None, refresh=False):
    now = utc_now(now)
    row = target.execute('SELECT * FROM market_ops.host_access WHERE host_key=%s',(HOST,)).fetchone()
    if not row:
        return {'host_key':HOST,'host_state':'MANUAL_HOLD','allowed':False,
                'operator_review_required':True,'denial_reason':'missing_shared_host_gate','next_allowed_at':None}
    result = dict(row)
    timed = row['host_state'] == 'RATE_LIMITED'
    allowed = (row['host_state'] == 'AVAILABLE' or (timed and row['next_allowed_at'] is not None
               and row['next_allowed_at'] <= now)) and not row['operator_review_required']
    if allowed and timed and refresh:
        target.execute("UPDATE market_ops.host_access SET host_state='AVAILABLE',next_allowed_at=NULL,updated_at=now() WHERE host_key=%s",(HOST,))
        result['host_state'] = 'AVAILABLE'
    result['allowed'] = allowed
    return result


def session_gap_remaining(target, gap_seconds, now=None):
    """Seconds left before another browser session may touch the source.

    The per-session pacing in the worker resets with every process, so repeated manual or
    scheduled runs could otherwise hit the host back to back. The last claim time is
    persisted in the queue, which makes the gap hold across processes and restarts.
    """
    if not gap_seconds:
        return 0
    now = utc_now(now)
    row = target.execute('SELECT max(last_served_at) AS t FROM market_ops.crawl_scope_queue').fetchone()
    last = row['t'] if row else None
    if last is None:
        return 0
    return max(0, int(gap_seconds - (now - last).total_seconds()))


def hold_host(target, error, now=None, rate_limit_backoff_seconds=900):
    now = utc_now(now)
    rate_limited = getattr(error,'kind',None) == 'rate_limited'
    state = 'RATE_LIMITED' if rate_limited else 'ACCESS_DENIED'
    until = now + timedelta(seconds=max(1,rate_limit_backoff_seconds)) if rate_limited else None
    retry_after = getattr(error,'retry_after',None)
    if rate_limited and retry_after is not None:
        until = max(until,utc_now(retry_after))
    evidence = getattr(error,'safe_response',{}) or {}
    evidence = {key:value for key,value in evidence.items() if key in
                {'http_status','cf_mitigated','cf_ray','body_sha256','reason_code'}
                and type(value) in (str,int,bool,type(None))}
    reason = getattr(error,'kind','access_denied')
    with target.transaction():
        target.execute('''INSERT INTO market_ops.host_access
            (host_key,host_state,next_allowed_at,denial_reason,operator_review_required,signal_count,safe_evidence)
            VALUES(%s,%s,%s,%s,%s,1,%s) ON CONFLICT(host_key) DO UPDATE SET
            host_state=CASE WHEN market_ops.host_access.operator_review_required THEN market_ops.host_access.host_state ELSE EXCLUDED.host_state END,
            next_allowed_at=CASE WHEN market_ops.host_access.operator_review_required THEN NULL ELSE EXCLUDED.next_allowed_at END,
            denial_reason=EXCLUDED.denial_reason,
            operator_review_required=market_ops.host_access.operator_review_required OR EXCLUDED.operator_review_required,
            signal_count=market_ops.host_access.signal_count+1,safe_evidence=EXCLUDED.safe_evidence,updated_at=now()''',
            (HOST,state,until,reason,not rate_limited,Jsonb(evidence)))
    return host_gate(target,now)


def set_host_available(target, review_reference):
    """Explicit operator action, never invoked by a timer, recovery or seeding."""
    if not isinstance(review_reference,str) or not review_reference.strip() or len(review_reference)>200:
        raise ValueError('An operator review reference is required')
    with target.transaction():
        previous = target.execute('SELECT host_state FROM market_ops.host_access WHERE host_key=%s FOR UPDATE',(HOST,)).fetchone()
        if not previous:
            raise ValueError('Shared host gate is missing')
        target.execute('''INSERT INTO market_ops.host_access_reviews(host_key,previous_state,review_reference)
            VALUES(%s,%s,%s)''',(HOST,previous['host_state'],review_reference.strip()))
        target.execute("""UPDATE market_ops.host_access SET host_state='AVAILABLE',next_allowed_at=NULL,
            operator_review_required=false,denial_reason='',reviewed_at=now(),review_reference=%s,updated_at=now()
            WHERE host_key=%s""",(review_reference.strip(),HOST))
    return host_gate(target)


_SCOPE_PRIORITY_MODES = frozenset({'default', 'detail_first', 'detail_only'})
_DUE_DETAIL_EXISTS = '''EXISTS(
    SELECT 1 FROM market_ops.crawl_job_queue q
    JOIN market_ops.crawl_job_scopes l USING(batch_id,source_job_id)
    WHERE l.batch_id=s.batch_id AND l.position_id=s.position_id
      AND l.location_id=s.location_id
      AND q.status IN ('detail_pending','detail_retry')
      AND q.next_eligible_at<=%s)'''


def claim_scope(target, batch_id, owner, now=None, lease_seconds=180, *, priority='default'):
    if priority not in _SCOPE_PRIORITY_MODES:
        raise ValueError(f'Invalid scope priority mode: {priority}')
    now = utc_now(now)
    with target.transaction():
        # A dead lease becomes eligible; its committed page/job cursors survive.
        if priority == 'detail_first':
            sql = f'''SELECT * FROM market_ops.crawl_scope_queue s
                WHERE s.batch_id=%s AND s.work_remaining AND s.status=ANY(%s)
                  AND s.next_eligible_at<=%s AND (s.lease_owner IS NULL OR s.lease_expires_at<=%s)
                ORDER BY CASE WHEN {_DUE_DETAIL_EXISTS} THEN 0
                              WHEN s.first_attempt_at IS NULL THEN 1
                              WHEN s.status='retry_pending' THEN 3 ELSE 2 END,
                         s.last_served_at ASC NULLS FIRST,s.created_at,s.scope_order,s.position_id,s.location_id
                FOR UPDATE SKIP LOCKED LIMIT 1'''
            params = (batch_id, list(ACTIVE_STATES), now, now, now)
        elif priority == 'detail_only':
            sql = f'''SELECT * FROM market_ops.crawl_scope_queue s
                WHERE s.batch_id=%s AND s.work_remaining AND s.status=ANY(%s)
                  AND s.next_eligible_at<=%s AND (s.lease_owner IS NULL OR s.lease_expires_at<=%s)
                  AND {_DUE_DETAIL_EXISTS}
                ORDER BY CASE WHEN s.status='retry_pending' THEN 2 ELSE 1 END,
                         s.last_served_at ASC NULLS FIRST,s.created_at,s.scope_order,s.position_id,s.location_id
                FOR UPDATE SKIP LOCKED LIMIT 1'''
            params = (batch_id, list(ACTIVE_STATES), now, now, now)
        else:
            sql = '''SELECT * FROM market_ops.crawl_scope_queue
                WHERE batch_id=%s AND work_remaining AND status=ANY(%s)
                  AND next_eligible_at<=%s AND (lease_owner IS NULL OR lease_expires_at<=%s)
                ORDER BY CASE WHEN first_attempt_at IS NULL THEN 0
                              WHEN status='retry_pending' THEN 2 ELSE 1 END,
                         last_served_at ASC NULLS FIRST,created_at,scope_order,position_id,location_id
                FOR UPDATE SKIP LOCKED LIMIT 1'''
            params = (batch_id, list(ACTIVE_STATES), now, now)
        row = target.execute(sql, params).fetchone()
        if not row:
            return None
        expiry = now + timedelta(seconds=max(120,lease_seconds))
        target.execute('''UPDATE market_ops.crawl_scope_queue SET lease_owner=%s,lease_expires_at=%s,
            last_served_at=%s,slices_served=slices_served+1,updated_at=now()
            WHERE batch_id=%s AND position_id=%s AND location_id=%s''',
            (owner,expiry,now,batch_id,row['position_id'],row['location_id']))
    row.update(lease_owner=owner,lease_expires_at=expiry,last_served_at=now,
               slices_served=row['slices_served']+1)
    return row


def heartbeat(target, scope, now=None, lease_seconds=180, navigation=False):
    now = utc_now(now)
    row = target.execute('''UPDATE market_ops.crawl_scope_queue SET lease_expires_at=%s,
        first_attempt_at=CASE WHEN %s THEN COALESCE(first_attempt_at,%s) ELSE first_attempt_at END,
        updated_at=now() WHERE batch_id=%s AND position_id=%s AND location_id=%s AND lease_owner=%s
        RETURNING first_attempt_at''',
        (now+timedelta(seconds=max(120,lease_seconds)),navigation,now,scope['batch_id'],scope['position_id'],
         scope['location_id'],scope['lease_owner'])).fetchone()
    if not row:
        raise RuntimeError('Scope lease was lost; no source navigation is permitted')
    scope['first_attempt_at'] = row['first_attempt_at']


def release_scope(target, scope, status, *, work_remaining=True, next_eligible_at=None,
                  error_class=None, recoverable=False, now=None):
    now = utc_now(now)
    counts = dict(scope.get('error_counts') or {})
    if recoverable:
        counts[error_class or 'transient'] = int(counts.get(error_class or 'transient',0))+1
    row = target.execute('''UPDATE market_ops.crawl_scope_queue SET status=%s,work_remaining=%s,
        next_eligible_at=%s,last_error_class=%s,error_counts=%s,failure_count=failure_count+%s,
        lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
        WHERE batch_id=%s AND position_id=%s AND location_id=%s AND lease_owner=%s RETURNING failure_count''',
        (status,work_remaining,next_eligible_at or now,error_class or '',Jsonb(counts),int(recoverable),
         scope['batch_id'],scope['position_id'],scope['location_id'],scope['lease_owner'])).fetchone()
    if row:
        scope['failure_count']=row['failure_count']


def record_error(target, scope, error_class, *, job_id=None, evidence=None):
    safe = {k:v for k,v in (evidence or {}).items() if k in
            {'http_status','cf_mitigated','cf_ray','body_sha256','reason_code'}
            and type(v) in (str,int,bool,type(None))}
    target.execute('''INSERT INTO market_ops.crawl_scope_errors
        (batch_id,position_id,location_id,source_job_id,error_class,safe_evidence)
        VALUES(%s,%s,%s,%s,%s,%s)''',
        (scope['batch_id'],scope['position_id'],scope['location_id'],job_id,error_class,Jsonb(safe)))


def read_batch_summary(target, batch_id=None):
    batch = (target.execute('SELECT * FROM market_ops.crawl_batches WHERE batch_id=%s',(UUID(str(batch_id)),)).fetchone()
             if batch_id is not None else target.execute('SELECT * FROM market_ops.crawl_batches ORDER BY batch_date DESC LIMIT 1').fetchone())
    gate = host_gate(target)
    if not batch:
        return {'batch_id':None,'batch_date':None,'expected_scopes':0,'scope_count':0,
                'scope_complete':0,'scope_partial':0,'scope_unattempted':0,'scope_exhausted':0,
                'scopes_with_data':0,'status_counts':{},'scopes':[], 'host_state':gate['host_state'],
                'host_gate':gate,'work_remaining':False,'coverage_ratio':0,'first_attempt_coverage':0}
    scopes = list(target.execute('SELECT * FROM market_ops.crawl_scope_queue WHERE batch_id=%s ORDER BY scope_order', (batch['batch_id'],)))
    counts = Counter(row['status'] for row in scopes)
    expected = batch['expected_scopes']
    complete = counts['complete']
    first_attempts = sum(row['first_attempt_at'] is not None for row in scopes)
    jobs = target.execute('''SELECT count(*) AS discovered,
        count(*) FILTER(WHERE status='detail_ok') AS detail_ok,
        count(*) FILTER(WHERE status='detail_partial') AS detail_partial,
        count(*) FILTER(WHERE status IN ('detail_pending','detail_retry')) AS pending,
        count(*) FILTER(WHERE status='not_found') AS not_found,
        count(*) FILTER(WHERE status='quarantined') AS quarantined,
        count(*) FILTER(WHERE status='skipped_with_reason') AS skipped_with_reason
        FROM market_ops.crawl_job_queue WHERE batch_id=%s''',(batch['batch_id'],)).fetchone()
    work = any(row['work_remaining'] for row in scopes)
    return {'batch_id':batch['batch_id'],'batch_date':batch['batch_date'],'expected_scopes':expected,
        'scope_count':len(scopes),'status_counts':dict(counts),'scope_status_counts':dict(counts),
        'scope_complete':complete,'scope_partial':counts['partial_ready'],
        'scope_unattempted':expected-first_attempts,'scope_exhausted':counts['exhausted'],
        'scope_needs_review':counts['needs_review'],
        'scopes_with_data':sum(row['eligible_listing_jobs']>0 or row['details_committed']>0 for row in scopes),
        'scope_work_remaining':sum(row['work_remaining'] for row in scopes),'work_remaining':work,
        'scope_coverage_complete':complete==expected and len(scopes)==expected,
        'coverage_ratio':complete/expected if expected else 0,
        'first_attempt_coverage':first_attempts/expected if expected else 0,
        'unattempted_due_to_host_denial':expected-first_attempts if not gate['allowed'] else 0,
        'listing_pages_committed':sum(row['listing_pages_committed'] for row in scopes),
        'details_committed':sum(row['details_committed'] for row in scopes),
        'jobs':dict(jobs),'jobs_saved':jobs['detail_ok']+jobs['detail_partial'],
        'jobs_skipped':jobs['not_found']+jobs['quarantined']+jobs['skipped_with_reason'],
        'host_state':gate['host_state'],'host_gate':gate,'scopes':scopes,
        'status':'SOURCE_BLOCKED' if not gate['allowed'] else 'COMPLETE' if not work else 'IN_PROGRESS'}
