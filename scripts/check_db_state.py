"""Inspect current Raw, Coordination, and Warehouse operational state."""
from dotenv import dotenv_values
import os
from src.raw_storage.db import PROJECT_ROOT, get_connection
from src.market_pipeline.warehouse import target_connection


def main():
    for env_name in ('.env.market', '.env'):
        for key, value in dotenv_values(PROJECT_ROOT / env_name).items():
            if key not in os.environ and value is not None:
                os.environ[key] = value

    with get_connection(read_only=True, autocommit=True) as conn_raw, target_connection(read_only=True) as conn_dw:
        print('=== RAW CRAWL RUNS ===')
        for r in conn_raw.execute(
            'SELECT crawl_run_id, run_status, started_at, finished_at, stop_reason '
            'FROM raw.crawl_runs ORDER BY started_at DESC LIMIT 5'
        ).fetchall():
            print(r)

        print('\n=== RAW SEARCH TASKS ===')
        for r in conn_raw.execute(
            'SELECT task_id, position_id, location_id, status, last_completed_page '
            'FROM raw.search_tasks ORDER BY started_at DESC LIMIT 5'
        ).fetchall():
            print(r)

        print('\n=== RAW ERRORS ===')
        for r in conn_raw.execute(
            'SELECT error_id, error_type, error_kind, error_message '
            'FROM raw.crawl_errors ORDER BY occurred_at DESC LIMIT 5'
        ).fetchall():
            print(r)

        print('\n=== HOST ACCESS GATE ===')
        for r in conn_dw.execute('SELECT * FROM market_ops.host_access').fetchall():
            print(r)

        print('\n=== LATEST CRAWL BATCH ===')
        batch = conn_dw.execute(
            'SELECT batch_id, batch_date, expected_scopes, status, updated_at '
            'FROM market_ops.crawl_batches ORDER BY batch_date DESC LIMIT 1'
        ).fetchone()
        print(batch)
        if batch:
            print('\n=== SCOPE QUEUE (STATUS SUMMARY) ===')
            for r in conn_dw.execute(
                'SELECT status, count(*) AS n FROM market_ops.crawl_scope_queue '
                'WHERE batch_id=%s GROUP BY status ORDER BY status',
                (batch['batch_id'],),
            ).fetchall():
                print(r)

            print('\n=== SCOPE QUEUE (NON-PENDING OR HELD) ===')
            for r in conn_dw.execute(
                'SELECT position_id, location_id, status, failure_count, listing_cursor, '
                'details_committed, last_error_class, next_eligible_at '
                'FROM market_ops.crawl_scope_queue WHERE batch_id=%s AND status != %s '
                'ORDER BY scope_order',
                (batch['batch_id'], 'pending'),
            ).fetchall():
                print(r)

            print('\n=== JOB QUEUE (STATUS SUMMARY) ===')
            for r in conn_dw.execute(
                'SELECT status, count(*) AS n FROM market_ops.crawl_job_queue '
                'WHERE batch_id=%s GROUP BY status ORDER BY status',
                (batch['batch_id'],),
            ).fetchall():
                print(r)


if __name__ == '__main__':
    main()
