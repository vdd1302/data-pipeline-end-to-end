"""Read-only operational health: tells the operator when unattended collection is stalled.

Nothing here opens a browser or touches the source site. A blocked host is reported,
never retried or reset: reopening a hard denial remains an explicit `review-host` action.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
import urllib.request

from . import coordination as C
from .safety import require_source_approval

CRITICAL, WARNING, INFO = 'critical', 'warning', 'info'
EXIT_UNHEALTHY = 4
DEFAULT_STALE_HOURS = 30
DASHBOARD_STALE_HOURS = 26


def _alert(code, severity, message, **evidence):
    return {'code': code, 'severity': severity, 'message': message, **evidence}


def evaluate_health(target, *, now=None, stale_hours=DEFAULT_STALE_HOURS, environ=None):
    now = C.utc_now(now)
    alerts = []
    env = os.environ if environ is None else environ
    # Crawling is an operator-run step unless Airflow crawling is explicitly enabled; a held
    # source then needs attention but is not a pipeline failure.
    source_severity = CRITICAL if env.get('TOPCV_AIRFLOW_CRAWL_ENABLED') == 'YES' else WARNING

    gate = C.host_gate(target, now)
    state = gate['host_state']
    if state in ('ACCESS_DENIED', 'MANUAL_HOLD') or gate.get('operator_review_required'):
        alerts.append(_alert(
            'HOST_BLOCKED', source_severity,
            'Source denied access; collection is halted until an operator reviews it '
            '(run `review-host` only after the source confirms access is allowed).',
            host_state=state, denial_reason=gate.get('denial_reason') or '',
            signal_count=gate.get('signal_count'),
            evidence=gate.get('safe_evidence') or {}))
    elif state == 'RATE_LIMITED':
        until = gate.get('next_allowed_at')
        alerts.append(_alert('HOST_RATE_LIMITED', WARNING,
                             'Source rate limited; waiting for the cooldown to expire.',
                             next_allowed_at=until.isoformat() if until else None))

    try:
        require_source_approval(environ)
    except PermissionError:
        alerts.append(_alert('AUTHORIZATION_MISSING', source_severity,
                             'Live collection gate is not configured; no source navigation will happen.'))

    last = target.execute('SELECT max(last_success_at) AS t FROM market_ops.crawl_scope_queue').fetchone()['t']
    if last is None or now - last > timedelta(hours=stale_hours):
        alerts.append(_alert('STALE_DATA', CRITICAL,
                             f'No successful capture in the last {stale_hours}h; run the manual crawl.',
                             last_success_at=last.isoformat() if last else None))

    # Superset reads market_dw views live (no query cache), so dashboards are only as fresh
    # as the last warehouse refresh.
    overview = target.execute('SELECT last_successful_refresh AS t, observed_jobs AS n '
                              'FROM market_dw.collection_overview').fetchone() or {}
    refreshed = overview.get('t')
    if refreshed is None or now - refreshed > timedelta(hours=DASHBOARD_STALE_HOURS):
        alerts.append(_alert('DASHBOARD_DATA_STALE', CRITICAL,
                             f'Warehouse not refreshed in the last {DASHBOARD_STALE_HOURS}h; dashboards show old data.',
                             last_successful_refresh=refreshed.isoformat() if refreshed else None))

    counts = {row['status']: row['n'] for row in target.execute(
        '''SELECT status, count(*) AS n FROM market_ops.crawl_scope_queue
           WHERE batch_id=(SELECT batch_id FROM market_ops.crawl_batches ORDER BY batch_date DESC LIMIT 1)
           GROUP BY status''')}
    review = counts.get('needs_review', 0) + counts.get('exhausted', 0)
    if review:
        alerts.append(_alert('SCOPE_NEEDS_REVIEW', WARNING,
                             f'{review} scope(s) in needs_review/exhausted; inspect crawl_scope_errors.',
                             scopes=review))

    backlog = target.execute('''SELECT count(*) AS n FROM market_ops.crawl_job_queue
        WHERE status IN ('detail_pending','detail_retry')''').fetchone()['n']
    if backlog:
        alerts.append(_alert('DETAIL_BACKLOG', INFO,
                             f'{backlog} job(s) still waiting for detail capture.', jobs=backlog))

    healthy = not any(a['severity'] == CRITICAL for a in alerts)
    return {'healthy': healthy, 'checked_at': now.isoformat(), 'host_state': state,
            'alerts': alerts, 'latest_batch_scope_status': counts,
            'business_status': 'SOURCE_BLOCKED' if any(a['code'] == 'HOST_BLOCKED' for a in alerts)
                               else 'NO_NEW_DATA'}


def notify(result, environ=None):
    """POST critical alerts to PIPELINE_ALERT_WEBHOOK_URL when configured. Never raises."""
    env = os.environ if environ is None else environ
    url = (env.get('PIPELINE_ALERT_WEBHOOK_URL') or '').strip()
    critical = [a for a in result['alerts'] if a['severity'] == CRITICAL]
    if not url or not url.lower().startswith('https://') or not critical:
        return False
    body = json.dumps({'text': 'TopCV pipeline: ' + '; '.join(a['code'] for a in critical),
                       'alerts': critical}, default=str).encode()
    try:
        req = urllib.request.Request(url, data=body, headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req, timeout=10).close()
        return True
    except Exception:
        return False
