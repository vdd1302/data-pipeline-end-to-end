"""Reset crawl queues cleanly and reseed a fresh daily batch for today."""
import sys
from pathlib import Path
from datetime import datetime, timezone
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.crawler.config import load_runtime_config
from src.market_pipeline.coordination import create_daily_batch, set_host_available

from psycopg.rows import dict_row

DW_SUPERUSER_URL = 'postgresql://postgres:1234@127.0.0.1:5432/topcv_data_warehouse'

def reset_crawl_queues():
    print("1. Connecting as superuser to clear stale crawl queues...")
    with psycopg.connect(DW_SUPERUSER_URL, row_factory=dict_row) as conn:
        with conn.transaction():
            conn.execute("DELETE FROM market_ops.crawl_scope_errors;")
            conn.execute("DELETE FROM market_ops.crawl_job_scopes;")
            conn.execute("DELETE FROM market_ops.crawl_job_queue;")
            conn.execute("DELETE FROM market_ops.crawl_listing_pages;")
            conn.execute("DELETE FROM market_ops.crawl_scope_queue;")
            conn.execute("DELETE FROM market_ops.crawl_batches;")
            conn.execute("DELETE FROM market_ops.raw_quarantine;")
            conn.execute("DELETE FROM market_ops.host_backoff;")
        print("   Cleared stale crawl queues and batches successfully.")

        set_host_available(conn, 'operator-fresh-queue-reset')
        print("   Host access set to AVAILABLE.")

        cfg = load_runtime_config(Path(__file__).resolve().parent.parent / 'config' / 'crawl.windows-pilot.yaml')
        batch = create_daily_batch(conn, cfg, now=datetime.now(timezone.utc))
        print(f"2. Reseeded fresh daily batch: {batch['batch_id']} with {len(batch.get('scopes', []))} scopes.")

if __name__ == '__main__':
    reset_crawl_queues()
