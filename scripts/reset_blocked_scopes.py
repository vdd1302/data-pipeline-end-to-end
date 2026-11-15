"""Record explicit operator host review and re-queue host-blocked/retry scopes."""
import argparse
from dotenv import dotenv_values
import os

from src.market_pipeline.coordination import set_host_available
from src.market_pipeline.safety import require_source_approval
from src.market_pipeline.warehouse import target_connection
from src.raw_storage.db import PROJECT_ROOT


def unblock(review_reference: str):
    for env_name in ('.env.market', '.env'):
        for key, value in dotenv_values(PROJECT_ROOT / env_name).items():
            if key not in os.environ and value is not None:
                os.environ[key] = value
    require_source_approval()
    with target_connection() as conn:
        gate = set_host_available(conn, review_reference)
        with conn.transaction():
            scopes = conn.execute(
                "UPDATE market_ops.crawl_scope_queue "
                "SET status='partial_ready', work_remaining=true, failure_count=0, "
                "    next_eligible_at=now(), lease_owner=NULL, lease_expires_at=NULL, updated_at=now() "
                "WHERE status IN ('blocked_by_host', 'retry_pending') AND listing_cursor > 0"
            ).rowcount
            pending = conn.execute(
                "UPDATE market_ops.crawl_scope_queue "
                "SET status='pending', work_remaining=true, failure_count=0, "
                "    next_eligible_at=now(), lease_owner=NULL, lease_expires_at=NULL, updated_at=now() "
                "WHERE status IN ('blocked_by_host', 'retry_pending') AND listing_cursor = 0"
            ).rowcount
            batches = conn.execute(
                "UPDATE market_ops.crawl_batches SET status='OPEN', updated_at=now() WHERE status='SOURCE_BLOCKED'"
            ).rowcount
        print(
            f"Host state={gate['host_state']}; re-queued {scopes} partial + {pending} pending scopes "
            f"across {batches} batches."
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--review-reference', required=True, help='Operator review reference for audit log')
    args = parser.parse_args()
    unblock(args.review_reference)
