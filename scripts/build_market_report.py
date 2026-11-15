"""Read-only operational report; partial coverage never certifies full-market data."""
from datetime import datetime, timezone
import json
from pathlib import Path
from src.market_pipeline.warehouse import target_connection, verify_nonnull
from src.market_pipeline.coordination import read_batch_summary, host_gate
from src.raw_storage.db import get_connection

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'artifacts/market'


def artifact(name):
    path = OUT / name
    return json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else {'not_run': True}


def verification(result):
    """Report independent observations; historical tests are not current evidence."""
    warehouse = result.get('warehouse', {})
    scope = result.get('batch_summary', {})
    publication = warehouse.get('publication_state') or {}
    published_status = publication.get('quality_status') == 'PASS'
    warehouse_verified = (warehouse.get('physical_columns_checked', 0) > 0
                          and warehouse.get('null_values') == 0 and published_status)
    superset = result.get('superset', {})
    charts = superset.get('charts', [])
    chart_api_verified = (superset.get('dashboard_policy_version') == 'readme-partial-v1'
                          and superset.get('validation_performed') is True
                          and superset.get('all_charts_passed') is True
                          and bool(charts) and all(chart.get('passed') is True for chart in charts))
    checks = {'warehouse_quality_released': warehouse_verified,
              'chart_api_verified': chart_api_verified,
              'scope_coverage_complete': bool(scope.get('scope_coverage_complete')),
              'scope_coverage_target_required': False,
              'runtime_acceptance_verified': False,
              'production_certified': False}
    latest = result.get('latest_pipeline_result') or {}
    status = latest.get('business_status') or ('SOURCE_BLOCKED' if not result.get('host', {}).get('allowed') else 'NO_NEW_DATA')
    return status, checks


def main():
    with target_connection(read_only=True) as conn, get_connection(read_only=True, autocommit=True) as raw:
        batch = read_batch_summary(conn)
        host = host_gate(conn)
        publication = conn.execute('SELECT * FROM market_ops.publication_state WHERE id=1').fetchone()
        latest = conn.execute("SELECT * FROM market_ops.pipeline_runs WHERE stage='evaluate' "
                              'ORDER BY recorded_at DESC,pipeline_run_id DESC LIMIT 1').fetchone()
        result = {
            'recorded_at_utc': datetime.now(timezone.utc).isoformat(),
            'batch_summary': batch, 'host': host,
            'latest_pipeline_result': latest,
            'recent_pipeline_stages': list(conn.execute('SELECT pipeline_run_id,dag_id,run_id,stage,business_status,batch_id,recorded_at '
                                                        'FROM market_ops.pipeline_runs ORDER BY pipeline_run_id DESC LIMIT 20')),
            'warehouse': {**verify_nonnull(conn),
                'publication_state': publication,
                'collection_overview': conn.execute('SELECT * FROM market_dw.collection_overview').fetchone(),
                'coverage_freshness': list(conn.execute('SELECT * FROM market_dw.coverage_freshness ORDER BY batch_date DESC LIMIT 10')),
                'fact_observations': conn.execute('SELECT count(*) AS n FROM market_dw.fct_job_snapshot').fetchone()['n'],
                'published_detail_observations': conn.execute('SELECT count(*) AS n FROM market_dw.published_jobs').fetchone()['n'],
                'observed_listing_records': conn.execute('SELECT count(*) AS n FROM market_dw.observed_listings').fetchone()['n'],
                'open_quarantine': conn.execute("SELECT count(*) AS n FROM market_ops.raw_quarantine WHERE status='OPEN'").fetchone()['n'],
                'ingest_accounting': list(conn.execute('SELECT disposition,count(*) AS n FROM market_ops.raw_ingest_accounting '
                                                       'GROUP BY disposition ORDER BY disposition'))},
            'raw': {'listing_pages': raw.execute("SELECT count(*) AS n FROM raw.search_pages WHERE fetch_status='success'").fetchone()['n'],
                    'detail_observations': raw.execute("SELECT count(*) AS n FROM raw.job_observations WHERE fetch_status='success'").fetchone()['n']},
            'airflow': {'url': 'http://localhost:8096', 'timezone': 'Asia/Ho_Chi_Minh',
                        'daily_schedule': '0 19 * * *', 'recovery_schedule': '10,40 * * * *',
                        'warehouse_schedule': '20,50 * * * *'},
            'superset': artifact('superset.json'),
            'historical_test_artifacts': {'operational': artifact('tests.json'), 'full_suite': artifact('full-suite.json')},
            'observation_scope_label': 'Observed sample; coverage may be partial',
            'market_totals_claimed': False,
            'lifecycle_inference_from_missing_jobs_allowed': False,
        }
    result['business_status'], result['verification'] = verification(result)
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / 'acceptance.json').write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + '\n', encoding='utf-8')
    print(json.dumps({'report': str(OUT / 'acceptance.json'), 'business_status': result['business_status'],
                      'verification': result['verification'],
                      'batch_id': batch.get('batch_id'), 'host_state': host.get('host_state')}, default=str))


if __name__ == '__main__':
    main()
