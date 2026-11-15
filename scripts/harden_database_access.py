"""One-time DB access hardening: least-privilege Raw role and rotated pipeline passwords.

Before: the pipeline reads and writes Raw as the `postgres` superuser with a weak password.
After:  Raw uses `topcv_raw_app` (SELECT/INSERT/UPDATE on schema raw only, no DDL, no DELETE,
        not a superuser) and the warehouse role gets a new random password.

Run once, on the host, with a Python that can write outside the project folder:
    python -m scripts.harden_database_access --dry-run     # check only, changes nothing
    python -m scripts.harden_database_access --recreate    # apply, then restart Airflow services

The admin (superuser) password is asked interactively and never stored. New passwords are random
and are written only to .env and .env.market (originals are saved as *.pre-hardening-<time>).
Nothing is written until the new roles have been verified by actually logging in as them.
The `postgres` password itself is NOT changed: other databases on this server may use it.
"""
from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

ROOT = Path(__file__).resolve().parents[1]
RAW_ROLE = 'topcv_raw_app'
RAW_TABLES = ('crawl_runs', 'search_tasks', 'search_pages', 'job_discoveries', 'job_observations', 'crawl_errors')
UPDATED_TABLES = ('crawl_runs', 'search_tasks', 'search_pages')


def connect(host, port, dbname, user, password):
    return psycopg.connect(make_conninfo(host=host, port=port, dbname=dbname, user=user, password=password,
                                         connect_timeout=10), autocommit=True)


def apply_roles(args, admin_password, raw_password, dw_password):
    with connect(args.host, args.port, 'postgres', args.admin_user, admin_password) as conn:
        if not conn.execute('SELECT rolsuper FROM pg_roles WHERE rolname = current_user').fetchone()[0]:
            raise SystemExit('The admin account must be a superuser.')
        conn.execute("SET log_statement = 'none'")  # keep new passwords out of the server log
        if not conn.execute('SELECT 1 FROM pg_roles WHERE rolname = %s', (RAW_ROLE,)).fetchone():
            conn.execute(sql.SQL('CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION')
                         .format(sql.Identifier(RAW_ROLE)))
        conn.execute(sql.SQL('ALTER ROLE {} PASSWORD {}').format(sql.Identifier(RAW_ROLE), sql.Literal(raw_password)))
        if not conn.execute('SELECT 1 FROM pg_roles WHERE rolname = %s', (args.dw_role,)).fetchone():
            raise SystemExit(f'Warehouse role {args.dw_role} does not exist.')
        conn.execute(sql.SQL('ALTER ROLE {} PASSWORD {}').format(sql.Identifier(args.dw_role), sql.Literal(dw_password)))
        conn.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(sql.Identifier(args.raw_db), sql.Identifier(RAW_ROLE)))
    with connect(args.host, args.port, args.raw_db, args.admin_user, admin_password) as conn:
        role = sql.Identifier(RAW_ROLE)
        conn.execute(sql.SQL('GRANT USAGE ON SCHEMA raw TO {}').format(role))
        conn.execute(sql.SQL('GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA raw TO {}').format(role))
        conn.execute(sql.SQL('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw TO {}').format(role))
        # Anything the pipeline never needs stays denied: DELETE/TRUNCATE (reset tool uses admin), DDL.
        conn.execute(sql.SQL('REVOKE DELETE, TRUNCATE, REFERENCES, TRIGGER ON ALL TABLES IN SCHEMA raw FROM {}').format(role))


def verify_raw_role(args, password):
    with connect(args.host, args.port, args.raw_db, RAW_ROLE, password) as conn:
        problems = []
        if conn.execute('SELECT rolsuper OR rolcreatedb OR rolcreaterole FROM pg_roles WHERE rolname = current_user').fetchone()[0]:
            problems.append('role has elevated attributes')
        for table in RAW_TABLES:
            conn.execute(sql.SQL('SELECT count(*) FROM raw.{}').format(sql.Identifier(table))).fetchone()
            for privilege in ('INSERT',) + (('UPDATE',) if table in UPDATED_TABLES else ()):
                if not conn.execute('SELECT has_table_privilege(current_user, %s, %s)', (f'raw.{table}', privilege)).fetchone()[0]:
                    problems.append(f'missing {privilege} on raw.{table}')
            if conn.execute('SELECT has_table_privilege(current_user, %s, %s)', (f'raw.{table}', 'DELETE')).fetchone()[0]:
                problems.append(f'unexpected DELETE on raw.{table}')
        try:
            conn.execute('CREATE TABLE raw.hardening_probe(x int)')
            conn.execute('DROP TABLE raw.hardening_probe')
            problems.append('role can create tables in raw')
        except psycopg.errors.InsufficientPrivilege:
            pass
    if problems:
        raise SystemExit('Raw role verification failed: ' + '; '.join(problems))


def verify_dw_role(args, password):
    with connect(args.host, args.port, args.dw_db, args.dw_role, password) as conn:
        conn.execute('SELECT count(*) FROM market_ops.host_access').fetchone()
        conn.execute('SELECT count(*) FROM market_dw.collection_overview').fetchone()


def compose_project(explicit=None):
    """The running stack is not named like the compose file (it is `topcv-market-e2e`); without -p,
    compose would target a different, stale stack. Read the name from the running scheduler."""
    if explicit:
        return explicit
    out = subprocess.run(['docker', 'ps', '--filter', 'label=com.docker.compose.service=market-scheduler',
                          '--format', '{{.Label "com.docker.compose.project"}}'],
                         capture_output=True, text=True).stdout.split()
    projects = sorted(set(out))
    if len(projects) != 1:
        raise SystemExit('Cannot tell which compose project runs Airflow '
                         f'(found: {projects or "none running"}); pass --project NAME.')
    return projects[0]


def rewrite_value(value, user, password):
    """Swap credentials in either a postgresql:// URL or a key=value conninfo string."""
    if '://' in value:
        parts = urlsplit(value)
        host = parts.hostname or ''
        netloc = f'{user}:{password}@{host}' + (f':{parts.port}' if parts.port else '')
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    info = conninfo_to_dict(value)
    info.update(user=user, password=password)
    return make_conninfo(**info)


def rewrite_env_file(path, changes):
    """changes: {KEY: (user, password)}. Keeps quoting, comments and every other line as is."""
    out, touched = [], []
    for line in path.read_text(encoding='utf-8').splitlines():
        match = re.fullmatch(r'([A-Za-z_][A-Za-z0-9_]*)=(["\']?)(.*)\2', line)
        if match and match.group(1) in changes:
            key, quote, value = match.groups()
            out.append(f'{key}={quote}{rewrite_value(value, *changes[key])}{quote}')
            touched.append(key)
        else:
            out.append(line)
    return '\n'.join(out) + '\n', touched


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=5432)
    parser.add_argument('--admin-user', default='postgres')
    parser.add_argument('--raw-db', default='topcv_raw_storage')
    parser.add_argument('--dw-db', default='topcv_data_warehouse')
    parser.add_argument('--dw-role', default='topcv_local_pipeline')
    parser.add_argument('--env-dir', type=Path, default=ROOT)
    parser.add_argument('--dry-run', action='store_true', help='verify the plan and the env files; change nothing')
    parser.add_argument('--recreate', action='store_true', help='recreate the Airflow services after writing the env files')
    parser.add_argument('--project', help='compose project name of the running Airflow stack (auto-detected by default)')
    args = parser.parse_args()

    env_file, market_file = args.env_dir / '.env', args.env_dir / '.env.market'
    plan = {env_file: {'TOPCV_RAW_DATABASE_URL': RAW_ROLE},
            market_file: {'TOPCV_RAW_CONTAINER_URL': RAW_ROLE, 'TOPCV_DW_DATABASE_URL': args.dw_role,
                          'MARKET_CONTAINER_DW_URL': args.dw_role}}
    for path, keys in plan.items():  # fail early if an expected key is missing
        _, touched = rewrite_env_file(path, {k: ('x', 'y') for k in keys})
        if set(touched) != set(keys):
            raise SystemExit(f'{path.name} is missing: {sorted(set(keys) - set(touched))}')
    if args.dry_run:
        print('Dry run OK: env files have every key to rewrite. No database or file was changed.')
        return 0

    admin_password = os.environ.get('PGADMIN_PASSWORD') or getpass.getpass(f'Password of {args.admin_user} (not stored): ')
    raw_password, dw_password = secrets.token_urlsafe(24), secrets.token_urlsafe(24)
    apply_roles(args, admin_password, raw_password, dw_password)
    verify_raw_role(args, raw_password)
    verify_dw_role(args, dw_password)
    print('Roles created/updated and verified by logging in as them.')

    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    for path, keys in plan.items():
        passwords = {RAW_ROLE: raw_password, args.dw_role: dw_password}
        text, _ = rewrite_env_file(path, {k: (user, passwords[user]) for k, user in keys.items()})
        shutil.copy2(path, path.with_name(f'{path.name}.pre-hardening-{stamp}'))
        path.write_text(text, encoding='utf-8')
        print(f'Updated {path.name} (old copy: {path.name}.pre-hardening-{stamp}; delete it once everything works).')

    if args.recreate:
        compose = ['docker', 'compose', '-p', compose_project(args.project), '--env-file', str(market_file),
                   '-f', str(ROOT / 'airflow/docker-compose.market.yml')]
        subprocess.run(compose + ['up', '-d', '--force-recreate', 'market-api', 'market-scheduler', 'market-dag-processor'],
                       check=True)
        print('Airflow services recreated with the new credentials.')
    else:
        print('Next: recreate the Airflow services so they read the new .env.market (use --recreate).')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
