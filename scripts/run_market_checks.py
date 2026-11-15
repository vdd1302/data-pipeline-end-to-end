"""Run portable core tests, plus isolated PostgreSQL checks on request."""
import argparse
import json
import os
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--postgres',action='store_true')
    args = parser.parse_args()
    if args.postgres:
        from psycopg.conninfo import conninfo_to_dict
        test_dsn = os.environ.get('MARKET_TEST_DSN', '')
        if not test_dsn or conninfo_to_dict(test_dsn).get('dbname') != 'market_integration_test':
            raise RuntimeError('Set MARKET_TEST_DSN to an existing isolated market_integration_test database')
    # PostgreSQL suites additionally guard the exact disposable database name.
    if not args.postgres and os.environ.get('MARKET_TEST_DSN'):
        raise RuntimeError('Use --postgres explicitly when MARKET_TEST_DSN is configured')
    sys.path.insert(0,str(ROOT/'tests'))
    suite = unittest.defaultTestLoader.discover(str(ROOT/'tests'))
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    report = {'tests':result.testsRun,'failures':len(result.failures),'errors':len(result.errors),
              'skipped':len(result.skipped),'passed':result.wasSuccessful(),'postgres_included':args.postgres}
    path = ROOT/'artifacts/market/tests.json'
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
