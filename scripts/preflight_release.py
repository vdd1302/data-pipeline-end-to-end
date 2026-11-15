"""Read-only release preflight. No live crawl, SQL writes or browser startup.

Usage: python -m scripts.preflight_release [--database] [--report PATH]
"""
from __future__ import annotations

import argparse
import ast
from datetime import datetime, timezone
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def inspect(*, database=False):
    from src.crawler.config import load_runtime_config
    from src.market_pipeline.crawl import plan
    from src.market_pipeline.warehouse import verify_schema_contract, target_connection
    import yaml

    cfg = load_runtime_config('config/crawl.airflow.yaml')
    scopes = plan(cfg)
    errors = []
    files = list(ROOT.joinpath('src').rglob('*.py')) + list(ROOT.joinpath('scripts').rglob('*.py'))
    for file in files:
        try:
            ast.parse(file.read_text(encoding='utf-8'), filename=str(file))
        except (SyntaxError, UnicodeError) as exc:
            errors.append({'module': str(file.relative_to(ROOT)), 'error': type(exc).__name__})
    docker = yaml.safe_load((ROOT/'airflow/docker-compose.market.yml').read_text(encoding='utf-8'))
    for name, service in docker['services'].items():
        if name not in ('market-db','market-init','market-permissions') and not service.get('restart'):
            errors.append({'module': name, 'error': 'missing restart policy'})
    contract = json.loads((ROOT/'config/market_dw_schema_contract.json').read_text(encoding='utf-8'))
    if len(contract['tables']) != 10:
        errors.append({'module': 'market_dw', 'error': 'expected 10 B3 tables'})
    pg = {'status': 'NOT_RUN', 'reason': 'Use --database against a known TEST database or read-only production credentials'}
    if database:
        try:
            with target_connection(read_only=True) as conn:
                meta = verify_schema_contract(conn)
                pg = {'status':'PASS', 'database': conn.info.dbname, **meta}
        except Exception as exc:
            pg = {'status':'FAIL', 'error_type': type(exc).__name__,
                  'reason': 'Check read-only connection, schema version and permitted DB'}
            errors.append({'module':'postgres_warehouse', 'error':type(exc).__name__})
    return {
        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
        'status': 'PASS_STATIC_ONLY' if not errors else 'FAIL',
        'static_python_files': len(files),
        'scope_count': scopes['scope_count'],
        'observed_route_count': sum(s['route_evidence']=='live_verified' for s in scopes['scopes']),
        'unverified_route_count': sum(s['route_evidence']!='live_verified' for s in scopes['scopes']),
        'warehouse_tables_in_contract': len(contract['tables']),
        'postgres_database': pg,
        'live_collection': 'NOT_RUN_BY_DESIGN',
        'airflow_runtime': 'NOT_RUN',
        'superset_runtime': 'NOT_RUN',
        'issues': errors,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', action='store_true', help='Optional read-only Warehouse metadata check')
    parser.add_argument('--report', type=Path)
    args = parser.parse_args(argv)
    report = inspect(database=args.database)
    rendered = json.dumps(report,indent=2,ensure_ascii=False)
    print(rendered)
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(rendered+'\n',encoding='utf-8')
    return 0 if not report['issues'] else 2

if __name__ == '__main__':
    raise SystemExit(main())
