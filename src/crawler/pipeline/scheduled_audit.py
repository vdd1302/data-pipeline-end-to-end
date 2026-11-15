"""Read back the exact scheduled scope from Raw before reporting success."""
from __future__ import annotations

from datetime import timedelta

from src.raw_storage.contracts import compute_content_hash
from src.raw_storage.ingestion import _config
from src.raw_storage.schedule import normalize_schedule_slot, scheduled_run_id


class ScheduledAuditError(ValueError):
    pass


def read_scheduled_scope(cfg, task, *, mode, schedule_key, scheduled_for,
                         connection_factory=None, expected_database='topcv_raw_storage'):
    """Return committed metrics, or None when this slot has never been written.

    No browser or writes. Incomplete prior runs are returned for operator review;
    they must never cause an automatic replay of already committed data.
    """
    key, due = normalize_schedule_slot(schedule_key, scheduled_for)
    if key is None or mode not in {'discovery', 'full'}:
        raise ScheduledAuditError('An exact schedule slot and mode are required.')
    if connection_factory is None:
        from src.raw_storage.db import get_connection
        connection_factory = get_connection
    with connection_factory(read_only=True) as conn:
        actual = conn.execute('SELECT current_database() AS name').fetchone()['name']
        if actual != expected_database:
            raise ScheduledAuditError('Unexpected Raw database; audit cancelled.')
        row = conn.execute('''
            SELECT r.crawl_run_id, r.run_status, r.run_purpose,
                   r.observation_method, r.config_sha256, r.stop_reason,
                   r.schedule_key, r.scheduled_for,
                   r.total_discovered, r.total_fetched, r.total_failed,
                   (SELECT count(*) FROM raw.search_tasks t
                     WHERE t.crawl_run_id=r.crawl_run_id) AS tasks,
                   (SELECT count(*) FROM raw.search_tasks t
                     WHERE t.crawl_run_id=r.crawl_run_id
                       AND t.position_id=%s AND t.location_id=%s) AS matching_tasks,
                   (SELECT count(*) FROM raw.search_tasks t
                     WHERE t.crawl_run_id=r.crawl_run_id AND t.status='success') AS completed_tasks,
                   (SELECT count(*) FROM raw.search_pages p
                     WHERE p.crawl_run_id=r.crawl_run_id AND p.fetch_status='success') AS pages,
                   (SELECT count(*) FROM raw.job_discoveries d
                     WHERE d.crawl_run_id=r.crawl_run_id) AS discovery_rows,
                   (SELECT count(DISTINCT d.source_job_id) FROM raw.job_discoveries d
                     WHERE d.crawl_run_id=r.crawl_run_id) AS jobs,
                   (SELECT min(d.discovered_at) FROM raw.job_discoveries d
                     WHERE d.crawl_run_id=r.crawl_run_id) AS first_capture,
                   (SELECT max(d.discovered_at) FROM raw.job_discoveries d
                     WHERE d.crawl_run_id=r.crawl_run_id) AS last_capture,
                   (SELECT count(*) FROM raw.job_observations o
                     WHERE o.crawl_run_id=r.crawl_run_id) AS observations,
                   (SELECT count(*) FROM raw.job_observations o
                     WHERE o.crawl_run_id=r.crawl_run_id AND o.fetch_status='success'
                       AND o.capture_source='selenium_live' AND o.is_temporal_eligible IS TRUE
                       AND o.source_captured_at >= %s AND o.source_captured_at < %s
                       AND EXISTS (SELECT 1 FROM raw.job_discoveries d
                         WHERE d.crawl_run_id=o.crawl_run_id AND d.source_job_id=o.source_job_id)
                   ) AS verified_details
            FROM raw.crawl_runs r WHERE r.source_code='topcv'
              AND r.schedule_key=%s AND r.scheduled_for=%s
        ''', (task.position_id, task.location_id, due, due + timedelta(days=1), key, due)).fetchone()
    if row is None:
        return None
    expected_id = scheduled_run_id(key, due)
    if (str(row['crawl_run_id']) != str(expected_id)
            or row['config_sha256'] != compute_content_hash(_config(cfg.snapshot))
            or row['run_purpose'] != 'production'
            or row['observation_method'] != 'selenium_live'):
        raise ScheduledAuditError('Scheduled Raw identity, configuration or provenance mismatch.')
    # A writer can fail before it has created a task/page; keep that run held.
    complete_shape = row['tasks'] == row['matching_tasks'] == row['completed_tasks'] == 1
    fresh = (row['first_capture'] is not None
             and due <= row['first_capture'] <= row['last_capture'] < due + timedelta(days=1))
    consistent = (row['total_discovered'] == row['jobs']
                  and row['discovery_rows'] >= row['jobs']
                  and row['total_fetched'] == row['verified_details']
                  and row['total_failed'] == 0)
    status = row['run_status']
    if mode == 'discovery':
        accepted = (status == 'partial' and complete_shape and fresh and consistent
                    and row['pages'] == 1 and row['jobs'] == row['discovery_rows'] > 0
                    and row['observations'] == 0
                    and row['stop_reason'] == 'verified_listing_checkpoint_import_no_detail')
        status = 'already_captured' if accepted else 'prior_run_requires_review'
    else:
        accepted = (status == 'success' and complete_shape and fresh and consistent
                    and row['pages'] >= 1
                    and row['jobs'] == row['verified_details'] == row['observations'] > 0
                    and row['stop_reason'] == 'observed_pagination_end_not_market_coverage')
        status = 'success' if accepted else 'prior_run_requires_review'
    return {
        'status': status, 'run_id': str(row['crawl_run_id']),
        'job_count': int(row['jobs']), 'db_discoveries': int(row['discovery_rows']),
        'details_ok': int(row['verified_details']), 'observation_rows': int(row['observations']),
        'pages_committed': int(row['pages']), 'fresh_capture': bool(fresh),
        'database_status': 'verified' if accepted and mode == 'full' else
                           'already_imported_identical' if accepted else 'requires_review',
        'reason': row['stop_reason'], 'schedule_key': key,
        'scheduled_for': due.isoformat(), 'reused_slot': True,
    }
