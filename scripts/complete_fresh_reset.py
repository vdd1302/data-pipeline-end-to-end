"""Reset coordination/raw/accountability state cleanly for a disposable test reset."""
from dotenv import dotenv_values
import os

from src.raw_storage.db import PROJECT_ROOT, get_connection as get_raw_conn
from src.market_pipeline.warehouse import target_connection as get_dw_conn


def reset_all():
    for env_name in ('.env.market', '.env'):
        for key, value in dotenv_values(PROJECT_ROOT / env_name).items():
            if key not in os.environ and value is not None:
                os.environ[key] = value

    print('1. Clearing raw runs, tasks, pages, discoveries, observations, errors from Raw storage...')
    with get_raw_conn() as raw_conn:
        with raw_conn.transaction():
            raw_conn.execute('DELETE FROM raw.job_observations')
            raw_conn.execute('DELETE FROM raw.job_discoveries')
            raw_conn.execute('DELETE FROM raw.search_pages')
            e = raw_conn.execute('DELETE FROM raw.crawl_errors').rowcount
            t = raw_conn.execute('DELETE FROM raw.search_tasks').rowcount
            r = raw_conn.execute('DELETE FROM raw.crawl_runs').rowcount
        print(f'   Deleted {e} errors, {t} tasks, {r} runs from raw storage.')

    print('2. Clearing coordination queues, host gate holds, and ingest accounting from DW...')
    with get_dw_conn() as dw_conn:
        with dw_conn.transaction():
            for table in (
                'market_ops.crawl_scope_errors',
                'market_ops.crawl_job_scopes',
                'market_ops.crawl_job_queue',
                'market_ops.crawl_listing_pages',
                'market_ops.crawl_scope_queue',
                'market_ops.crawl_batches',
                'market_ops.raw_ingest_accounting',
                'market_ops.raw_quarantine',
                'market_ops.warehouse_listing_observations',
                'market_ops.listing_ingest_rejections',
                'market_ops.publication_state',
                'market_ops.pipeline_runs',
                'market_ops.scope_queue',
                'market_ops.host_backoff',
            ):
                dw_conn.execute(f'DELETE FROM {table}')
            dw_conn.execute(
                "INSERT INTO market_ops.host_access(host_key, host_state, operator_review_required, denial_reason) "
                "VALUES ('www.topcv.vn', 'AVAILABLE', false, '') "
                "ON CONFLICT (host_key) DO UPDATE SET host_state='AVAILABLE', next_allowed_at=NULL, "
                "operator_review_required=false, denial_reason='', updated_at=now()"
            )
            dw_conn.execute('UPDATE market_ops.refresh_checkpoint SET last_raw_job_id=0 WHERE id=1')
        print('   Cleared coordination & accountability tables and reset host_access/checkpoint.')


if __name__ == '__main__':
    reset_all()
