"""Configure the existing local Superset using private stdin, not shell text."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', default='topcv-a61d-superset-isolated-superset-1')
    parser.add_argument('--skip-validation', action='store_true',
                        help='Update BI configuration without executing chart validation requests')
    args = parser.parse_args()
    settings = dotenv_values(ROOT / '.env.market')
    uri = settings.get('MARKET_READER_URL')
    if not uri:
        raise RuntimeError('Run scripts.prepare_market_runtime first')
    payload = (ROOT / 'scripts/market_superset_payload.py').read_text(encoding='utf-8')
    payload += ('\ntry:\n    publish(' + repr(uri) + ', validate=' + repr(not args.skip_validation) + ')\nexcept Exception as exc:\n'
        '    import traceback, os\n'
        '    print("MARKET_ERROR=" + type(exc).__name__)\n'
        '    frames=[{"file":os.path.basename(f.filename),"line":f.lineno,"function":f.name} for f in traceback.extract_tb(exc.__traceback__)]\n'
        '    print("MARKET_ERROR_CONTEXT=" + json.dumps(frames[-8:]))\n'
        '    if isinstance(exc, FileNotFoundError):\n'
        '        print("MARKET_ERROR_FILE=" + str(exc.filename))\n'
        '    raise SystemExit(1)\n')
    result = subprocess.run(['docker','exec','-i',args.container,'python'],
        input=payload, text=True, encoding='utf-8', capture_output=True, timeout=240)
    lines = [line for line in result.stdout.splitlines() if line.startswith('MARKET_RESULT=')]
    if result.returncode or not lines:
        # Driver exceptions may echo a DSN; never print stderr or raw traceback.
        errors = [line for line in result.stdout.splitlines() if line.startswith('MARKET_ERROR')]
        print('\n'.join(errors) or 'Superset configuration failed; no credentials were logged.')
        return 1
    report = json.loads(lines[-1].split('=',1)[1])
    output = ROOT / 'artifacts/market/superset.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False,indent=2))
    if args.skip_validation:
        return 0 if report.get('configuration_published') else 2
    return 0 if report.get('all_charts_passed') else 2


if __name__ == '__main__':
    raise SystemExit(main())
