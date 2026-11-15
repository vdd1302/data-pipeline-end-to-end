"""Durable batch job accountability, shared captures and scope links."""
from datetime import datetime,timedelta,timezone
import json
from psycopg.types.json import Jsonb
from src.crawler.parser import normalize_job_link


def enqueue_discoveries(target,source,scope,raw_run_id,raw_task_id,source_page_number):
    items=source.execute('''SELECT DISTINCT ON(source_job_id) source_job_id,job_url,
        d.listing_payload,d.discovered_at,d.discovery_id,d.crawl_run_id FROM raw.job_discoveries d
        JOIN raw.search_pages p ON p.page_id=d.page_id
        WHERE d.crawl_run_id=%s AND d.task_id=%s AND d.listing_payload IS NOT NULL
        AND COALESCE(p.source_page_number,p.page_number)=%s
        ORDER BY source_job_id,d.discovery_id''',(raw_run_id,raw_task_id,source_page_number)).fetchall()
    inserted=0
    with target.transaction():
        for item in items:
            if normalize_job_link(item['job_url'],'https://www.topcv.vn')!=(item['job_url'],item['source_job_id']):
                raise ValueError('Committed discovery identity is not canonical')
            target.execute('''INSERT INTO market_ops.crawl_job_queue(batch_id,source_job_id,first_seen)
                VALUES(%s,%s,%s) ON CONFLICT DO NOTHING''',(scope['batch_id'],item['source_job_id'],item['discovered_at']))
            row=target.execute('''INSERT INTO market_ops.crawl_job_scopes
                (batch_id,source_job_id,position_id,location_id,job_url,listing_payload,
                 original_discovered_at,source_discovery_id,source_discovery_run_id,source_page_number)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING source_job_id''',
                (scope['batch_id'],item['source_job_id'],scope['position_id'],scope['location_id'],item['job_url'],
                 Jsonb(item['listing_payload']),item['discovered_at'],item['discovery_id'],item['crawl_run_id'],source_page_number)).fetchone()
            inserted+=row is not None
        target.execute('''UPDATE market_ops.crawl_scope_queue SET eligible_listing_jobs=
            (SELECT count(*) FROM market_ops.crawl_job_scopes WHERE batch_id=%s AND position_id=%s AND location_id=%s),
            updated_at=now() WHERE batch_id=%s AND position_id=%s AND location_id=%s''',
            (scope['batch_id'],scope['position_id'],scope['location_id'],scope['batch_id'],scope['position_id'],scope['location_id']))
    return inserted


def mark_success(target,scope,job_id,raw_run_id,raw_observation_id,capture_at,partial=False):
    target.execute('''UPDATE market_ops.crawl_job_queue SET status=%s,raw_observation_id=%s,
        observation_raw_run_id=%s,capture_at=%s,last_error_class='',updated_at=now()
        WHERE batch_id=%s AND source_job_id=%s''',
        ('detail_partial' if partial else 'detail_ok',raw_observation_id,raw_run_id,capture_at,scope['batch_id'],job_id))


def reconcile(target,source,scope):
    ids=[r['source_job_id'] for r in target.execute('''SELECT q.source_job_id
        FROM market_ops.crawl_job_queue q JOIN market_ops.crawl_job_scopes l USING(batch_id,source_job_id)
        WHERE l.batch_id=%s AND l.position_id=%s AND l.location_id=%s
        AND q.status IN ('detail_pending','detail_retry')''',(scope['batch_id'],scope['position_id'],scope['location_id']))]
    if not ids:return 0
    captures=source.execute('''SELECT DISTINCT ON(o.source_job_id) o.source_job_id,o.raw_job_id,
        o.crawl_run_id,o.source_captured_at,o.record_level FROM raw.job_observations o
        JOIN raw.crawl_runs r USING(crawl_run_id) WHERE o.source_job_id=ANY(%s)
        AND o.fetch_status='success' AND r.run_purpose='production' AND r.schedule_key LIKE %s
        AND o.capture_source='selenium_live' AND o.source_captured_at IS NOT NULL
        ORDER BY o.source_job_id,o.raw_job_id''',(ids,'topcv-batch/'+str(scope['batch_id'])+'/%')).fetchall()
    for row in captures:
        mark_success(target,scope,row['source_job_id'],row['crawl_run_id'],row['raw_job_id'],
                     row['source_captured_at'],partial=row['record_level']=='detail_partial')
    return len(captures)


def pending_for_scope(target,source,scope,now=None,limit=5):
    reconcile(target,source,scope)
    return list(target.execute('''SELECT q.*,l.job_url,l.listing_payload,
        l.original_discovered_at AS discovered_at,l.source_discovery_id AS discovery_id,
        l.source_discovery_run_id,l.source_page_number FROM market_ops.crawl_job_queue q
        JOIN market_ops.crawl_job_scopes l USING(batch_id,source_job_id)
        WHERE l.batch_id=%s AND l.position_id=%s AND l.location_id=%s
          AND q.status IN ('detail_pending','detail_retry') AND q.next_eligible_at<=%s
        ORDER BY q.last_attempt ASC NULLS FIRST,q.first_seen,q.source_job_id LIMIT %s''',
        (scope['batch_id'],scope['position_id'],scope['location_id'],now or datetime.now(timezone.utc),limit)))


def mark_attempt(target,scope,job_id,now=None):
    return target.execute('''UPDATE market_ops.crawl_job_queue SET attempt_count=attempt_count+1,
        last_attempt=%s,updated_at=now() WHERE batch_id=%s AND source_job_id=%s
        AND status IN ('detail_pending','detail_retry') RETURNING source_job_id''',
        (now or datetime.now(timezone.utc),scope['batch_id'],job_id)).fetchone() is not None


def mark_failure(target,scope,job_id,error_class,now=None,*,max_attempts=2,retry_delay_seconds=300,terminal=None):
    if terminal:
        if terminal not in ('not_found','quarantined','skipped_with_reason'):raise ValueError('Invalid job terminal state')
        target.execute('''UPDATE market_ops.crawl_job_queue SET status=%s,last_error_class=%s,
            updated_at=now() WHERE batch_id=%s AND source_job_id=%s''',(terminal,error_class,scope['batch_id'],job_id))
        return terminal
    row=target.execute('''UPDATE market_ops.crawl_job_queue SET recoverable_failures=recoverable_failures+1,
        last_error_class=%s,status=CASE WHEN recoverable_failures+1>=%s THEN 'skipped_with_reason' ELSE 'detail_retry' END,
        next_eligible_at=%s,updated_at=now() WHERE batch_id=%s AND source_job_id=%s RETURNING status''',
        (error_class,max_attempts,(now or datetime.now(timezone.utc))+timedelta(seconds=retry_delay_seconds),scope['batch_id'],job_id)).fetchone()
    return row['status']


def defer_host(target,scope,job_id,error_class):
    target.execute('''UPDATE market_ops.crawl_job_queue SET last_error_class=%s,updated_at=now()
        WHERE batch_id=%s AND source_job_id=%s AND status IN ('detail_pending','detail_retry')''',
        (error_class,scope['batch_id'],job_id))


def scope_counts(target,scope,now=None):
    return dict(target.execute('''SELECT count(*) AS total,
        count(*) FILTER(WHERE q.status IN ('detail_pending','detail_retry')) AS pending,
        count(*) FILTER(WHERE q.status IN ('detail_pending','detail_retry') AND q.next_eligible_at<=%s) AS due,
        count(*) FILTER(WHERE q.status IN ('detail_ok','detail_partial')) AS success,
        count(*) FILTER(WHERE q.status IN ('not_found','quarantined','skipped_with_reason')) AS skipped,
        MIN(q.next_eligible_at) FILTER(WHERE q.status IN ('detail_pending','detail_retry')) AS next_eligible_at
        FROM market_ops.crawl_job_queue q JOIN market_ops.crawl_job_scopes l USING(batch_id,source_job_id)
        WHERE l.batch_id=%s AND l.position_id=%s AND l.location_id=%s''',
        (now or datetime.now(timezone.utc),scope['batch_id'],scope['position_id'],scope['location_id'])).fetchone())


def queue_summary(target,batch_id):
    if batch_id is None:return {'jobs_total':0,'jobs_saved':0,'jobs_pending':0,'jobs_skipped':0}
    return dict(target.execute('''SELECT count(*) AS jobs_total,
        count(*) FILTER(WHERE status IN ('detail_ok','detail_partial')) AS jobs_saved,
        count(*) FILTER(WHERE status IN ('detail_pending','detail_retry')) AS jobs_pending,
        count(*) FILTER(WHERE status IN ('not_found','quarantined','skipped_with_reason')) AS jobs_skipped
        FROM market_ops.crawl_job_queue WHERE batch_id=%s''',(batch_id,)).fetchone())


def record_batch(target,result,day,started_at=None):
    finished=datetime.now(timezone.utc)
    started_at=started_at or finished-timedelta(seconds=float(result.get('elapsed_seconds',0)))
    return target.execute('''INSERT INTO market_ops.collection_batch_log
        (snapshot_date,started_at,finished_at,status,details_committed,listings_committed,jobs_skipped,report)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s) RETURNING batch_id''',
        (day,started_at,finished,result.get('status','unknown'),int(result.get('details_committed',0)),
         int(result.get('listings_committed',0)),int(result.get('jobs_skipped',0)),Jsonb(json.loads(json.dumps(result,default=str))))).fetchone()['batch_id']


def latest_batch(target,day=None):
    row=target.execute('SELECT * FROM market_ops.collection_batch_log ORDER BY batch_id DESC LIMIT 1').fetchone()
    return dict(row) if row else None
