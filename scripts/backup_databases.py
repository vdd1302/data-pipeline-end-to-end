"""Back up Raw, the warehouse and the Airflow metadata; verify every dump; prune old sets.

Run from the host (Windows) with a Python that can write outside the project folder:
    python -m scripts.backup_databases [--keep 7] [--out .backups/scheduled]

* Raw and warehouse live in the host PostgreSQL; Airflow metadata lives in the compose
  `market-db` container (dumped with docker exec, so no password leaves the container).
* A dump only counts if `pg_restore --list` can read it back. The set is written to a
  temporary folder and renamed on success, so a failed run never looks like a good backup.
* pg_dump must be at least as new as the server (the server here is PostgreSQL 18).
"""
from __future__ import annotations
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from dotenv import dotenv_values
import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_DB_CONTAINER = 'topcv-market-e2e-market-db-1'


def load_env():
    for name in ('.env.market', '.env'):
        for key, value in dotenv_values(ROOT / name).items():
            if key not in os.environ and value is not None:
                os.environ[key] = value


def find_tool(name, server_major):
    candidates = sorted(Path('C:/Program Files/PostgreSQL').glob(f'*/bin/{name}.exe'),
                        key=lambda p: int(p.parents[1].name) if p.parents[1].name.isdigit() else 0, reverse=True)
    for path in candidates:
        major = int(path.parents[1].name) if path.parents[1].name.isdigit() else 0
        if major >= server_major:
            return str(path)
    found = shutil.which(name)
    if found:
        out = subprocess.run([found, '--version'], capture_output=True, text=True).stdout
        match = re.search(r'(\d+)\.', out)
        if match and int(match.group(1)) >= server_major:
            return found
    raise RuntimeError(f'{name} {server_major}+ not found; a pg_dump older than the server cannot dump it')


def server_major(dsn):
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        return int(conn.execute('SHOW server_version_num').fetchone()[0]) // 10000


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def dump_host_db(label, dsn, folder, schemas=()):
    info = conninfo_to_dict(dsn)
    major = server_major(dsn)
    pg_dump = find_tool('pg_dump', major)
    pg_restore = find_tool('pg_restore', major)
    target = folder / f'{label}.dump'
    env = dict(os.environ, PGPASSWORD=info.get('password') or '')
    command = [pg_dump, '-Fc', '--no-owner', '-h', info.get('host', '127.0.0.1'), '-p', str(info.get('port', 5432)),
               '-U', info['user'], '-d', info['dbname'], '-f', str(target)]
    for schema in schemas:  # the Raw role only owns/reads schema raw; legacy schemas need an admin dump
        command += ['--schema', schema]
    done = subprocess.run(command, env=env, capture_output=True, text=True)
    if done.returncode:
        raise RuntimeError(f'pg_dump failed for {label}: {done.stderr.strip().splitlines()[-1:]}')
    return verify(label, target, pg_restore)


def dump_airflow(folder):
    target = folder / 'airflow_metadata.dump'
    with open(target, 'wb') as out:
        done = subprocess.run(['docker', 'exec', AIRFLOW_DB_CONTAINER, 'pg_dump', '-Fc', '--no-owner',
                               '-U', 'market_owner', '-d', 'market'], stdout=out, stderr=subprocess.PIPE)
    if done.returncode:
        raise RuntimeError('airflow metadata dump failed: ' + done.stderr.decode(errors='replace').strip()[-200:])
    pg_restore = find_tool('pg_restore', 16)
    return verify('airflow_metadata', target, pg_restore)


def verify(label, path, pg_restore):
    listing = subprocess.run([pg_restore, '--list', str(path)], capture_output=True, text=True)
    entries = sum(1 for line in listing.stdout.splitlines() if line and not line.startswith(';'))
    if listing.returncode or entries == 0 or path.stat().st_size < 1024:
        raise RuntimeError(f'{label} dump is unreadable or empty')
    return {'name': path.name, 'bytes': path.stat().st_size, 'toc_entries': entries, 'sha256': sha256(path)}


def prune(base, keep):
    sets = sorted((p for p in base.iterdir() if p.is_dir() and re.fullmatch(r'\d{8}T\d{6}Z', p.name)), reverse=True)
    for old in sets[keep:]:
        shutil.rmtree(old)
    return len(sets[keep:])


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--keep', type=int, default=7, help='number of backup sets to keep')
    parser.add_argument('--out', type=Path, default=ROOT / '.backups' / 'scheduled')
    parser.add_argument('--skip-airflow', action='store_true')
    args = parser.parse_args()
    if args.keep < 1:
        parser.error('--keep must be at least 1')
    load_env()
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    args.out.mkdir(parents=True, exist_ok=True)
    work = args.out / (stamp + '.partial')
    work.mkdir()
    try:
        files = [dump_host_db('topcv_raw_storage', os.environ['TOPCV_RAW_DATABASE_URL'], work, schemas=('raw',)),
                 dump_host_db('topcv_data_warehouse', os.environ['TOPCV_DW_DATABASE_URL'], work)]
        if not args.skip_airflow:
            files.append(dump_airflow(work))
        (work / 'manifest.json').write_text(json.dumps(
            {'created_utc': stamp, 'files': files, 'verified_with': 'pg_restore --list'}, indent=2), encoding='utf-8')
        work.rename(args.out / stamp)
    except Exception:
        shutil.rmtree(work, ignore_errors=True)
        raise
    removed = prune(args.out, args.keep)
    print(json.dumps({'backup_set': stamp, 'files': files, 'pruned_sets': removed}, indent=2))


if __name__ == '__main__':
    raise SystemExit(main())
