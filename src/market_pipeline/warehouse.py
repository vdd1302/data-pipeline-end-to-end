"""Incremental Raw -> existing Cleaning -> staging -> non-null warehouse.

Raw is read only here and may be on another PostgreSQL server. Missing measures
use -1 plus known flags; averages include known VND/month midpoints only.
The historical, frozen B3 acceptance schemas are not modified.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path

from dotenv import dotenv_values
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from src.cleaning.raw_batch_reader import _TOPCV_POSTING_ELIGIBILITY_SQL
from src.cleaning.staging_mapper import map_raw_observation
from src.cleaning.staging_repository import upsert_staging_batch
from src.cleaning.skill_cleaner import canonical_skill_names
from src.cleaning.position_cleaner import normalize_position
from src.cleaning.company_cleaner import normalize_company
from src.cleaning.location_cleaner import normalize_location
from src.crawler.parser import normalize_job_link
from src.raw_storage.db import get_connection, PROJECT_ROOT
from .catalog import POSITIONS, POSITION_IDS, UNKNOWN, UNDISCLOSED, VERSION

ETL_VERSION = VERSION + '-readiness-v1'


class DataQualityError(RuntimeError):
    business_status = 'FAILED_DQ'


def target_connection(*, read_only=False):
    settings = dotenv_values(PROJECT_ROOT / '.env.market')
    dsn = os.environ.get('TOPCV_DW_DATABASE_URL', settings.get('TOPCV_DW_DATABASE_URL'))
    if not dsn:
        raise RuntimeError('Thiếu TOPCV_DW_DATABASE_URL trong môi trường hoặc .env.market.')
    try:
        return psycopg.connect(dsn, row_factory=dict_row, autocommit=True, connect_timeout=10,
                               options='-c timezone=UTC -c statement_timeout=120000 -c lock_timeout=10000 '
                               '-c default_transaction_read_only=' + ('on' if read_only else 'off'))
    except psycopg.Error:
        raise RuntimeError('Không kết nối được warehouse; kiểm tra cấu hình và dịch vụ.') from None


ROLE_FAMILIES = {
    'software_engineer': 'Software Development',
    'backend_developer': 'Software Development',
    'frontend_developer': 'Software Development',
    'data_analyst': 'Data Analytics',
    'fullstack_developer': 'Software Development',
    'ai_engineer': 'AI & Data Science',
    'data_scientist': 'AI & Data Science',
    'data_engineer': 'Data Engineering',
    'devops_engineer': 'Cloud & DevOps',
    'cybersecurity_specialist': 'Cybersecurity',
    'business_analyst': 'Business & Systems Analysis',
}


def verify_schema_contract(conn):
    """Fail closed when the actual DW differs from the reviewed physical schema.

    CREATE TABLE IF NOT EXISTS is not a migration for already-existing tables.
    We check every required column instead of trusting a historical receipt.
    """
    expected = json.loads((PROJECT_ROOT / 'config/market_dw_schema_contract.json').read_text(encoding='utf-8'))['tables']
    rows = list(conn.execute("SELECT table_name,column_name FROM information_schema.columns "
        "WHERE table_schema='market_dw' AND table_name IN "
        "(SELECT table_name FROM information_schema.tables "
        " WHERE table_schema='market_dw' AND table_type='BASE TABLE')"))
    actual = {}
    for row in rows:
        actual.setdefault(row['table_name'], set()).add(row['column_name'])
    diff = {}
    for table in sorted(set(actual) | set(expected)):
        missing = sorted(set(expected.get(table, ())) - actual.get(table, set()))
        extra = sorted(actual.get(table, set()) - set(expected.get(table, ())))
        if missing or extra or table not in actual or table not in expected:
            diff[table] = {'missing': missing, 'unexpected': extra}
    if diff:
        raise RuntimeError('Warehouse schema drift detected; do not reprocess or migrate automatically: '
                           + json.dumps(diff,ensure_ascii=False,sort_keys=True))
    return {'schema_contract': 'market_dw-v3', 'tables_verified': len(expected),
            'physical_columns_verified': sum(map(len,actual.values()))}


def initialize_raw(raw_conn):
    """Additive, idempotent migration on the configured Raw storage DB."""
    allowed = {'topcv_raw_storage', 'market_integration_test', 'raw_storage_restore_test_v1'}
    if raw_conn.info.dbname not in allowed and not raw_conn.info.dbname.startswith('topcv_test_'):
        raise ValueError('Refusing Raw bootstrap in an unrecognized database.')
    with raw_conn.transaction():
        raw_conn.execute((PROJECT_ROOT / 'sql/raw_partial_observations.sql').read_text(encoding='utf-8'))


def initialize(conn):
    """Additive, idempotent schema installation on the configured DW only."""
    allowed = {'market', 'market_integration_test', 'topcv_data_warehouse'}
    if conn.info.dbname not in allowed and not conn.info.dbname.startswith('topcv_test_'):
        raise ValueError('Refusing DW bootstrap in an unrecognized database; expected market or dedicated test DW.')
    stage = (PROJECT_ROOT / 'sql/staging_schema.sql').read_text(encoding='utf-8')
    with conn.transaction():
        if conn.execute("SELECT to_regclass('market_dw.fct_job_snapshot') IS NULL AS fresh").fetchone()['fresh']:
            conn.execute(stage)
            conn.execute((PROJECT_ROOT / 'sql/market_pipeline.sql').read_text(encoding='utf-8'))
        else:
            verify_schema_contract(conn)
        conn.execute((PROJECT_ROOT / 'sql/market_partial_collection.sql').read_text(encoding='utf-8'))
        conn.execute((PROJECT_ROOT / 'sql/market_crawl_coordination.sql').read_text(encoding='utf-8'))
        conn.execute((PROJECT_ROOT / 'sql/market_data_readiness.sql').read_text(encoding='utf-8'))
        conn.execute('UPDATE market_dw.dim_position SET in_scope=false '
                     'WHERE in_scope AND NOT (position_id=ANY(%s))', (list(POSITIONS),))
        # Sentinel date for unknown/unparsed dates (B3 Role-Playing Date)
        conn.execute('''INSERT INTO market_dw.dim_date VALUES (-1, '1970-01-01', 1, 1, 1, 1, 1970, 197001, 197001)
                        ON CONFLICT (date_key) DO NOTHING''')
        for key, name in {**POSITIONS, 'unknown': UNKNOWN}.items():
            family = ROLE_FAMILIES.get(key, 'Other IT')
            conn.execute('''INSERT INTO market_dw.dim_position
                (position_id, position_name, in_scope, role_family, classification_version)
                VALUES (%s,%s,%s,%s,%s)
                ON CONFLICT (position_id) DO UPDATE SET position_name=EXCLUDED.position_name,
                in_scope=EXCLUDED.in_scope,
                role_family=EXCLUDED.role_family, classification_version=EXCLUDED.classification_version''',
                (key, name, key != 'unknown', family, 'topcv-cleaning-v1'))
        experience_seed = [
            ('Không yêu cầu', 'Không yêu cầu', Decimal('0.0'), Decimal('0.0'), 0),
            ('Dưới 1 năm', 'Dưới 1 năm', Decimal('0.0'), Decimal('1.0'), 1),
            ('1-<3 năm', '1-<3 năm', Decimal('1.0'), Decimal('3.0'), 2),
            ('3-<5 năm', '3-<5 năm', Decimal('3.0'), Decimal('5.0'), 3),
            ('>=5 năm', '>=5 năm', Decimal('5.0'), Decimal('-1.0'), 4),
            (UNKNOWN, UNKNOWN, Decimal('-1.0'), Decimal('-1.0'), 5),
        ]
        for exp in experience_seed:
            conn.execute('INSERT INTO market_dw.dim_experience VALUES (%s,%s,%s,%s,%s) '
                         'ON CONFLICT (experience_key) DO NOTHING', exp)
    return verify_schema_contract(conn)


def _text(value, default=UNDISCLOSED):
    return str(value).strip() if value is not None and str(value).strip() else default


def _key(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def fact_record(cleaned, *, salary_raw=None):
    """Pure, auditable missing-data policy, preserving partial observations."""
    if cleaned['cleaning_status'] == 'failed' or not cleaned.get('canonical_job_url'):
        raise ValueError('Missing posting identity, title or canonical job URL')
    name = cleaned.get('position_name')
    position_id = POSITION_IDS.get(name, 'unknown')
    position_status = ('unclassified' if position_id == 'unknown' else
                       'classified' if position_id in POSITIONS else 'outside_requested_positions')
    if name and position_id == 'unknown':
        position_id = 'other:' + _key(name)[:20]
        position_status = 'outside_requested_positions'
    salary_fields = ('salary_min_vnd', 'salary_max_vnd', 'salary_midpoint_vnd')
    salary_values = {key: cleaned.get(key) for key in salary_fields}
    if any(value is not None for value in salary_values.values()) and (
            cleaned.get('salary_currency') != 'VND' or cleaned.get('salary_period') != 'month'):
        raise ValueError('Only normalized VND/month measures may enter salary analytics')
    for value in salary_values.values():
        if value is not None and (not Decimal(str(value)).is_finite() or value < 0):
            raise ValueError('Invalid salary measure')
    if cleaned.get('salary_negotiable_flag'):
        salary_status = 'negotiable'
    elif cleaned.get('salary_midpoint_vnd') is not None:
        salary_status = 'disclosed_monthly_vnd'
    elif any(value is not None for value in salary_values.values()):
        salary_status = 'open_bound_monthly_vnd'
    elif cleaned.get('salary_currency') not in (None, 'VND'):
        salary_status = 'foreign_currency'
    elif cleaned.get('salary_period') not in (None, 'month'):
        salary_status = 'non_monthly'
    elif cleaned.get('salary_disclosed_flag') or any(flag in cleaned.get('quality_flags',[]) for flag in (
            'salary_negative_amount','salary_inverted_range','salary_text_unrecognized',
            'salary_amount_ambiguous','salary_format_ambiguous','salary_unit_unknown')):
        salary_status = 'unparsed'
    else:
        salary_status = 'undisclosed'
    d = cleaned['snapshot_date']
    date_key = d.year * 10000 + d.month * 100 + d.day
    exp_key = _text(cleaned.get('experience_bucket'), UNKNOWN)
    p_date = cleaned.get('posted_date')
    posted_date_key = (p_date.year * 10000 + p_date.month * 100 + p_date.day) if isinstance(p_date, (date, datetime)) else -1
    d_date = cleaned.get('deadline_date')
    deadline_date_key = (d_date.year * 10000 + d_date.month * 100 + d_date.day) if isinstance(d_date, (date, datetime)) else -1
    result = {
        'posting_natural_key': cleaned['posting_natural_key'],
        'crawl_run_id': cleaned['crawl_run_id'], 'raw_job_id': cleaned['raw_job_id'],
        'source_job_id': cleaned['source_job_id'],
        'canonical_job_url': cleaned['canonical_job_url'],
        'snapshot_date': cleaned['snapshot_date'], 'scraped_at': cleaned['scraped_at'],
        'posting_key': cleaned['posting_natural_key'],
        'snapshot_date_key': date_key,
        'posted_date_key': posted_date_key,
        'deadline_date_key': deadline_date_key,
        'position_id': position_id,
        'company_key': _text(cleaned.get('company_natural_key'), 'unknown'),
        'experience_key': exp_key,
        'position_status': position_status,
        'job_title': cleaned['job_title_clean'],
        'job_level': _text(cleaned.get('job_level_clean'), UNKNOWN),
        'job_description': _text(cleaned.get('job_description_clean')),
        'job_requirements': _text(cleaned.get('job_requirements_clean')),
        'benefits': _text(cleaned.get('benefits_clean')),
        'posting_count': 1,
        **{key: value if value is not None else -1 for key, value in salary_values.items()},
        'salary_min_known': salary_values['salary_min_vnd'] is not None,
        'salary_max_known': salary_values['salary_max_vnd'] is not None,
        'salary_midpoint_known': salary_values['salary_midpoint_vnd'] is not None,
        'salary_disclosed_flag': cleaned.get('salary_disclosed_flag', False) or salary_status in ('disclosed_monthly_vnd', 'open_bound_monthly_vnd'),
        'salary_negotiable_flag': bool(cleaned.get('salary_negotiable_flag') or salary_status == 'negotiable'),
        'salary_status': salary_status,
        'salary_currency': _text(cleaned.get('salary_currency'), UNKNOWN),
        'salary_period': _text(cleaned.get('salary_period'), UNKNOWN),
        'salary_label': _text(salary_raw),
        'experience_min_years': cleaned.get('experience_min_years') if cleaned.get('experience_min_years') is not None else -1,
        'experience_max_years': cleaned.get('experience_max_years') if cleaned.get('experience_max_years') is not None else -1,
        'experience_min_known': cleaned.get('experience_min_years') is not None,
        'experience_max_known': cleaned.get('experience_max_years') is not None,
        'experience_bucket': _text(cleaned.get('experience_bucket'), UNKNOWN),
        'posted_date_label': _text(cleaned.get('posted_date')),
        'deadline_date_label': _text(cleaned.get('deadline_date')),
        # A capture does not prove first market appearance or disappearance.
        'is_new': False,
        'is_continuing': False,
        'is_reappeared': False,
        'lifecycle_status': 'unknown',
        'content_changed_flag': bool(cleaned.get('content_changed_flag', False)),
        'observation_method': _text(cleaned.get('observation_method'), 'unknown'),
        'is_temporal_eligible': cleaned['is_temporal_eligible'],
        'cleaning_status': cleaned['cleaning_status'],
        'quality_flags': list(cleaned['quality_flags']),
        'content_hash': _text(cleaned.get('content_hash'), 'unknown'),
        'cleaning_version': cleaned.get('cleaning_version') or VERSION,
    }
    if any(value is None for value in result.values()):
        raise ValueError('NULL is forbidden in warehouse facts')
    return result


def load_record(conn, cleaned, *, salary_raw=None):
    record = fact_record(cleaned, salary_raw=salary_raw)
    pid = record['position_id']
    # 1. Dimension dim_job_posting
    conn.execute('''INSERT INTO market_dw.dim_job_posting
        (posting_key, posting_natural_key, source_job_id, canonical_job_url, job_title, job_level, job_description, job_requirements, benefits)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (posting_key) DO UPDATE SET canonical_job_url=EXCLUDED.canonical_job_url,
        posting_natural_key=EXCLUDED.posting_natural_key,
        job_title=EXCLUDED.job_title,job_level=EXCLUDED.job_level,
        job_description=EXCLUDED.job_description,job_requirements=EXCLUDED.job_requirements,
        benefits=EXCLUDED.benefits''',
        (record['posting_key'], record['posting_natural_key'], record['source_job_id'], record['canonical_job_url'],
         record['job_title'], record['job_level'], record['job_description'],
         record['job_requirements'], record['benefits']))
    # 2. Dimension dim_date
    dates_to_insert = [record['snapshot_date']]
    if isinstance(cleaned.get('posted_date'), (date, datetime)):
        dates_to_insert.append(cleaned['posted_date'])
    if isinstance(cleaned.get('deadline_date'), (date, datetime)):
        dates_to_insert.append(cleaned['deadline_date'])
    for dt in dates_to_insert:
        d_key = dt.year * 10000 + dt.month * 100 + dt.day
        isocal = dt.isocalendar()
        conn.execute('''INSERT INTO market_dw.dim_date VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
           ON CONFLICT (date_key) DO NOTHING''',
           (d_key, dt, dt.day, isocal.week, dt.month, (dt.month - 1) // 3 + 1, dt.year,
            dt.year * 100 + dt.month, isocal.year * 100 + isocal.week))
    # 3. Dimension dim_position
    family = ROLE_FAMILIES.get(pid, 'Other IT')
    conn.execute('''INSERT INTO market_dw.dim_position
        (position_id, position_name, in_scope, role_family, classification_version)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (position_id) DO UPDATE SET position_name=EXCLUDED.position_name,
        in_scope=EXCLUDED.in_scope,
        role_family=EXCLUDED.role_family, classification_version=EXCLUDED.classification_version''',
        (pid, _text(cleaned.get('position_name'), UNKNOWN), pid in POSITIONS, family, 'topcv-cleaning-v1'))
    # 4. Dimension dim_company
    company_nat = _text(cleaned.get('company_natural_key'), record['company_key'])
    conn.execute('''INSERT INTO market_dw.dim_company
        (company_key, company_natural_key, company_name, company_url, company_size, industry, observed_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (company_key) DO UPDATE SET company_name=EXCLUDED.company_name,
        company_natural_key=EXCLUDED.company_natural_key,
        company_url=EXCLUDED.company_url,company_size=EXCLUDED.company_size,
        industry=EXCLUDED.industry,observed_at=EXCLUDED.observed_at
        WHERE EXCLUDED.observed_at >= market_dw.dim_company.observed_at''',
        (record['company_key'], company_nat, _text(cleaned.get('company_name_clean'), UNKNOWN),
        _text(cleaned.get('company_url_clean')), _text(cleaned.get('company_size_clean')),
        _text(cleaned.get('company_industry_clean')), cleaned['scraped_at']))
    # 5. Dimension dim_experience
    exp_key = record['experience_key']
    conn.execute('''INSERT INTO market_dw.dim_experience VALUES (%s,%s,%s,%s,%s)
       ON CONFLICT (experience_key) DO NOTHING''',
       (exp_key, exp_key,
        record['experience_min_years'] if record['experience_min_known'] else Decimal('-1.0'),
        record['experience_max_years'] if record['experience_max_known'] else Decimal('-1.0'),
        5))
    columns = tuple(record)
    bound = {**record, 'quality_flags': Jsonb(record['quality_flags'])}
    conn.execute('INSERT INTO market_dw.fct_job_snapshot (' + ','.join(columns) + ') VALUES ('
                 + ','.join('%(' + key + ')s' for key in columns) + ') '
                 'ON CONFLICT (posting_natural_key,crawl_run_id) DO UPDATE SET '
                 + ','.join(key + '=EXCLUDED.' + key for key in columns
                            if key not in ('posting_natural_key', 'crawl_run_id'))
                 + ' WHERE EXCLUDED.scraped_at >= market_dw.fct_job_snapshot.scraped_at', bound)
    # If an older duplicate of the same posting/run was seen, retain its latest bridges.
    current = conn.execute('SELECT raw_job_id FROM market_dw.fct_job_snapshot '
                           'WHERE posting_natural_key=%s AND crawl_run_id=%s',
                           (record['posting_natural_key'], record['crawl_run_id'])).fetchone()
    if current['raw_job_id'] != record['raw_job_id']:
        return
    identity = (record['posting_natural_key'], record['crawl_run_id'])
    conn.execute('DELETE FROM market_dw.bridge_job_location WHERE posting_natural_key=%s AND crawl_run_id=%s', identity)
    places = cleaned.get('locations_clean') or [{'city': UNKNOWN, 'district': UNKNOWN, 'location_name': UNKNOWN}]
    places = {tuple(_text(p.get(k), UNKNOWN) for k in ('city','district','location_name')) for p in places}
    for place in sorted(places):
        key = _key(place)
        conn.execute('INSERT INTO market_dw.dim_location VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING', (key, *place))
        conn.execute('INSERT INTO market_dw.bridge_job_location VALUES (%s,%s,%s,%s)',
                     (*identity, key, Decimal(1)/len(places)))
    conn.execute('DELETE FROM market_dw.bridge_job_skill WHERE posting_natural_key=%s AND crawl_run_id=%s', identity)
    for skill in sorted(set(cleaned.get('skills_clean') or [UNDISCLOSED])):
        is_known = skill in canonical_skill_names()
        conn.execute('''INSERT INTO market_dw.dim_skill
           (skill_key, skill_name, skill_category, is_known, normalization_version)
           VALUES (%s,%s,%s,%s,%s)
           ON CONFLICT (skill_key) DO UPDATE SET is_known=EXCLUDED.is_known,skill_name=EXCLUDED.skill_name,
           skill_category=EXCLUDED.skill_category,normalization_version=EXCLUDED.normalization_version''',
           (skill.casefold(), skill, 'Technical' if is_known else 'General', is_known, 'topcv-skill-rules-v2'))
        conn.execute('INSERT INTO market_dw.bridge_job_skill VALUES (%s,%s,%s)', (*identity, skill.casefold()))


SOURCE_SQL = f'''SELECT o.*,r.source_code,r.crawler_version,r.run_status,
    r.snapshot_date AS run_snapshot_date,r.run_purpose,r.observation_method,
    o.is_temporal_eligible AS raw_temporal_eligible
    FROM raw.job_observations o JOIN raw.crawl_runs r USING (crawl_run_id)
    WHERE o.fetch_status='success' AND {_TOPCV_POSTING_ELIGIBILITY_SQL}
      AND r.run_purpose IS DISTINCT FROM 'test'
      AND o.raw_job_id > %s ORDER BY o.raw_job_id LIMIT %s'''


def verify_nonnull(conn):
    """Check schema constraints AND actual values in every physical DW column."""
    columns = list(conn.execute("SELECT table_name,column_name,is_nullable FROM information_schema.columns "
        "WHERE table_schema='market_dw' AND table_name IN "
        "(SELECT table_name FROM information_schema.tables WHERE table_schema='market_dw' AND table_type='BASE TABLE')"))
    from psycopg import sql
    if not columns or any(c['is_nullable'] != 'NO' for c in columns):
        raise RuntimeError('Warehouse has nullable columns')
    nulls = 0
    for c in columns:
        nulls += conn.execute(sql.SQL('SELECT count(*) AS n FROM market_dw.{} WHERE {} IS NULL').format(
            sql.Identifier(c['table_name']), sql.Identifier(c['column_name']))).fetchone()['n']
    if nulls:
        raise RuntimeError('Warehouse contains NULL values')
    return {'physical_columns_checked': len(columns), 'null_values': nulls}


_DETAIL_SELECT = f'''SELECT o.*,r.source_code,r.crawler_version,r.run_status,
    r.snapshot_date AS run_snapshot_date,r.run_purpose,r.observation_method,
    o.is_temporal_eligible AS raw_temporal_eligible,
    (o.fetch_status='success' AND {_TOPCV_POSTING_ELIGIBILITY_SQL}
      AND r.run_purpose IS DISTINCT FROM 'test') AS etl_eligible
    FROM raw.job_observations o JOIN raw.crawl_runs r USING(crawl_run_id)'''

_LISTING_SELECT = '''SELECT d.discovery_id,d.source_job_id,d.job_url,d.listing_payload,
    d.discovered_at,d.crawl_run_id,d.page_id,t.position_id,t.location_id,
    p.fetched_at AS listing_captured_at,COALESCE(p.source_page_number,p.page_number) AS source_page_number
    FROM raw.job_discoveries d JOIN raw.crawl_runs r USING(crawl_run_id)
    JOIN raw.search_pages p ON p.page_id=d.page_id AND p.crawl_run_id=d.crawl_run_id
    JOIN raw.search_tasks t ON t.task_id=d.task_id AND t.crawl_run_id=d.crawl_run_id
    WHERE p.fetch_status='success' AND r.source_code='topcv'
      AND r.run_purpose='production' AND r.observation_method='selenium_live'
      AND d.listing_payload IS NOT NULL'''


def _accounted_ids(target, ids):
    if not ids:
        return set()
    return {row['raw_job_id'] for row in target.execute(
        'SELECT raw_job_id FROM market_ops.raw_ingest_accounting '
        'WHERE cleaning_version=%s AND raw_job_id=ANY(%s)',(ETL_VERSION,list(ids)))}


def _detail_batches(source, upper, size=250):
    after = 0
    while after < upper:
        rows = list(source.execute(_DETAIL_SELECT +
            ' WHERE o.raw_job_id>%s AND o.raw_job_id<=%s ORDER BY o.raw_job_id LIMIT %s',
            (after,upper,size)))
        if not rows:
            break
        after = rows[-1]['raw_job_id']
        yield rows


def _listing_batches(source, size=250):
    after = 0
    while True:
        rows = list(source.execute(_LISTING_SELECT +
            ' AND d.discovery_id>%s ORDER BY d.discovery_id LIMIT %s',(after,size)))
        if not rows:
            break
        after = rows[-1]['discovery_id']
        yield rows


def _listing_accounted(target, ids):
    if not ids:
        return set()
    return {r['discovery_id'] for r in target.execute('''
        SELECT discovery_id FROM market_ops.warehouse_listing_observations
        WHERE cleaning_version=%s AND discovery_id=ANY(%s)
        UNION SELECT discovery_id FROM market_ops.listing_ingest_rejections
        WHERE cleaning_version=%s AND discovery_id=ANY(%s)''',
        (ETL_VERSION,list(ids),ETL_VERSION,list(ids)))}


def _host_status(target):
    row = target.execute('SELECT host_state,next_allowed_at,operator_review_required '
        'FROM market_ops.host_access WHERE host_key=%s',('www.topcv.vn',)).fetchone()
    now = datetime.now(timezone.utc)
    blocked = bool(row and (row['operator_review_required'] or
        row['host_state'] in ('ACCESS_DENIED','MANUAL_HOLD') or
        (row['host_state']=='RATE_LIMITED' and
         (row['next_allowed_at'] is None or row['next_allowed_at']>now))))
    return {'host_state':row['host_state'] if row else 'AVAILABLE','source_blocked':blocked}


def evaluate_data_readiness(*, source=None, target=None, batch_id=None):
    """Count committed, unaccounted evidence rather than completed scopes.

    An ID below the old watermark is still found if it committed late or was
    never accounted. A quarantined ID remains explicitly retryable by version.
    """
    from contextlib import ExitStack
    with ExitStack() as stack:
        source = source or stack.enter_context(get_connection(read_only=True,autocommit=True))
        target = target or stack.enter_context(target_connection(read_only=True))
        upper = source.execute('SELECT COALESCE(max(raw_job_id),0) AS n FROM raw.job_observations').fetchone()['n']
        eligible, unaccounted, oldest_unprocessed = 0, 0, None
        for rows in _detail_batches(source,upper):
            accounted = _accounted_ids(target,[r['raw_job_id'] for r in rows])
            unaccounted += sum(r['raw_job_id'] not in accounted for r in rows)
            eligible += sum(bool(r['etl_eligible']) and r['raw_job_id'] not in accounted for r in rows)
            for row in rows:
                if row['etl_eligible'] and row['raw_job_id'] not in accounted:
                    captured = row['scraped_at']
                    oldest_unprocessed = min(oldest_unprocessed,captured) if oldest_unprocessed else captured
        retry = target.execute('''SELECT count(*) AS n FROM market_ops.raw_quarantine
            WHERE cleaning_version=%s AND status='OPEN' AND attempts<5 AND next_retry_at<=now()''',
            (ETL_VERSION,)).fetchone()['n']
        listings = 0
        for rows in _listing_batches(source):
            accounted = _listing_accounted(target,[r['discovery_id'] for r in rows])
            listings += sum(r['discovery_id'] not in accounted for r in rows)
        unreleased = target.execute('''SELECT
            (SELECT count(*) FROM market_ops.raw_ingest_accounting
                WHERE cleaning_version=%s AND disposition='LOADED' AND NOT publication_ready)
            +(SELECT count(*) FROM market_ops.warehouse_listing_observations
                WHERE cleaning_version=%s AND NOT publication_ready) AS n''',
            (ETL_VERSION,ETL_VERSION)).fetchone()['n']
        return {'eligible_unprocessed_raw_record_count':eligible,
                'unaccounted_raw_record_count':unaccounted,
                'quarantined_retry_due_count':retry,'listing_ready_count':listings,
                'pending_publication_count':unreleased,
                'oldest_unprocessed_raw_age_seconds':max(0,(datetime.now(timezone.utc)-oldest_unprocessed).total_seconds())
                    if oldest_unprocessed else None,
                'ready':bool(eligible or retry or listings or unreleased),'raw_upper_bound':upper,
                'batch_id':batch_id,**_host_status(target)}


def _write_accounting(target, row, disposition, *, reason='', cleaned=None):
    target.execute('''INSERT INTO market_ops.raw_ingest_accounting
        (raw_job_id,cleaning_version,disposition,source_job_id,crawl_run_id,
         posting_natural_key,snapshot_date,record_level,content_hash,reason_code,publication_ready)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false)
        ON CONFLICT(raw_job_id,cleaning_version) DO UPDATE SET
          disposition=EXCLUDED.disposition,posting_natural_key=EXCLUDED.posting_natural_key,
          record_level=EXCLUDED.record_level,reason_code=EXCLUDED.reason_code,
          publication_ready=false,accounted_at=now()''',
        (row['raw_job_id'],ETL_VERSION,disposition,row['source_job_id'],row['crawl_run_id'],
         (cleaned or {}).get('posting_natural_key'),row['snapshot_date'],
         row.get('record_level') or 'detail_partial',row.get('content_hash'),reason))


def _process_detail(target, row, counts):
    counts['selected'] += 1
    if not row['etl_eligible']:
        _write_accounting(target,row,'EXCLUDED',reason='source_record_not_eligible')
        counts['excluded'] += 1
        return
    try:
        cleaned = map_raw_observation(row,cleaning_version=ETL_VERSION)
        if row.get('record_level')=='detail_partial':
            cleaned['quality_flags'] = list(dict.fromkeys([*cleaned['quality_flags'],'detail_partial']))
            if cleaned['cleaning_status']=='success':
                cleaned['cleaning_status']='partial'
        fact_record(cleaned,salary_raw=row['raw_payload'].get('salary_raw'))
    except (ValueError,TypeError,KeyError) as exc:
        _write_accounting(target,row,'QUARANTINED',reason='mapping_contract_error')
        target.execute('''INSERT INTO market_ops.raw_quarantine
            (raw_job_id,cleaning_version,error_type,reason_code,next_retry_at)
            VALUES(%s,%s,%s,'mapping_contract_error',now()+interval '15 minutes')
            ON CONFLICT(raw_job_id,cleaning_version) DO UPDATE SET
                error_type=EXCLUDED.error_type,status='OPEN',attempts=market_ops.raw_quarantine.attempts+1,
                next_retry_at=now()+interval '15 minutes',last_seen_at=now(),resolved_at=NULL''',
            (row['raw_job_id'],ETL_VERSION,type(exc).__name__))
        target.execute('''INSERT INTO market_ops.mapping_errors(raw_job_id,error_type,last_seen_at)
            VALUES(%s,%s,now()) ON CONFLICT(raw_job_id) DO UPDATE SET
            error_type=EXCLUDED.error_type,last_seen_at=now()''',(row['raw_job_id'],type(exc).__name__))
        counts['rejected'] += 1
        counts['quarantined'] += 1
        return
    # SQL failures are infrastructure/DQ failures, never hidden as mapping skips.
    upsert_staging_batch(target,[cleaned])
    load_record(target,cleaned,salary_raw=row['raw_payload'].get('salary_raw'))
    current = target.execute('SELECT raw_job_id FROM market_dw.fct_job_snapshot '
        'WHERE posting_natural_key=%s AND crawl_run_id=%s',
        (cleaned['posting_natural_key'],row['crawl_run_id'])).fetchone()
    disposition = 'LOADED' if current and current['raw_job_id']==row['raw_job_id'] else 'SUPERSEDED'
    _write_accounting(target,row,disposition,cleaned=cleaned)
    if disposition=='LOADED':
        target.execute('''UPDATE market_ops.raw_ingest_accounting SET disposition='SUPERSEDED',
            publication_ready=false,reason_code='newer_observation_same_grain',accounted_at=now()
            WHERE cleaning_version=%s AND crawl_run_id=%s AND posting_natural_key=%s
              AND raw_job_id<>%s AND disposition='LOADED' ''',
            (ETL_VERSION,row['crawl_run_id'],cleaned['posting_natural_key'],row['raw_job_id']))
    target.execute('DELETE FROM market_ops.mapping_errors WHERE raw_job_id=%s',(row['raw_job_id'],))
    target.execute('''UPDATE market_ops.raw_quarantine SET status='RESOLVED',resolved_at=now(),
        last_seen_at=now() WHERE raw_job_id=%s AND cleaning_version=%s AND status='OPEN' ''',
        (row['raw_job_id'],ETL_VERSION))
    counts['loaded'] += disposition=='LOADED'
    counts['superseded'] += disposition=='SUPERSEDED'


def _listing_record(row):
    payload = row['listing_payload']
    if not isinstance(payload,dict):
        raise ValueError('Invalid listing payload')
    identity = normalize_job_link(row['job_url'],'https://www.topcv.vn')
    if identity is None or identity!=(row['job_url'],row['source_job_id']):
        raise ValueError('Invalid listing identity')
    title = payload.get('job_title_raw')
    if not isinstance(title,str) or not title.strip():
        raise ValueError('Missing listing title')
    position = normalize_position(title)
    company = normalize_company(payload.get('company_name_raw'),payload.get('company_url_raw'),
                                payload.get('company_size_raw'),payload.get('company_industry_raw'))
    location = normalize_location(payload.get('location_raw'))
    captured = row['listing_captured_at']
    if captured.tzinfo is None:
        raise ValueError('Listing capture time is unknown')
    return {'discovery_id':row['discovery_id'],'raw_page_id':row['page_id'],
        'raw_run_id':row['crawl_run_id'],'source_job_id':row['source_job_id'],
        'canonical_job_url':row['job_url'],'position_id':row['position_id'],
        'location_id':row['location_id'],'snapshot_date':captured.astimezone(timezone(timedelta(hours=7))).date(),
        'listing_captured_at':captured,'original_discovered_at':row['discovered_at'],
        'source_page_number':row['source_page_number'],'job_title':position.job_title_clean or title.strip(),
        'normalized_position':position.position_name,'company_name':company.company_name_clean,
        'city':location.city_clean,'salary_label':payload.get('salary_raw'),
        'content_hash':_key(payload),'cleaning_version':ETL_VERSION,'publication_ready':False}


def _ingest_listings(source,target,counts,reprocess=False):
    from psycopg import sql
    for rows in _listing_batches(source):
        accounted = set() if reprocess else _listing_accounted(target,[r['discovery_id'] for r in rows])
        with target.transaction():
            for row in rows:
                if row['discovery_id'] in accounted:
                    continue
                try:
                    record = _listing_record(row)
                except (ValueError,TypeError,KeyError):
                    target.execute('''INSERT INTO market_ops.listing_ingest_rejections
                        (discovery_id,cleaning_version,reason_code) VALUES(%s,%s,'invalid_listing_evidence')
                        ON CONFLICT(discovery_id,cleaning_version) DO UPDATE SET
                        rejected_at=now(),reason_code=EXCLUDED.reason_code''',(row['discovery_id'],ETL_VERSION))
                    counts['listings_quarantined'] += 1
                    continue
                batch = target.execute('SELECT batch_id FROM market_ops.crawl_listing_pages '
                    'WHERE raw_page_id=%s ORDER BY batch_id LIMIT 1',(row['page_id'],)).fetchone()
                record['batch_id'] = batch['batch_id'] if batch else None
                columns = list(record)
                target.execute(sql.SQL('INSERT INTO market_ops.warehouse_listing_observations ({}) '
                    'VALUES ({}) ON CONFLICT(discovery_id) DO UPDATE SET {}').format(
                    sql.SQL(',').join(map(sql.Identifier,columns)),
                    sql.SQL(',').join(sql.Placeholder() for _ in columns),
                    sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(k),sql.Identifier(k))
                                      for k in columns if k!='discovery_id')),
                    tuple(record[k] for k in columns))
                target.execute('DELETE FROM market_ops.listing_ingest_rejections '
                    'WHERE discovery_id=%s AND cleaning_version=%s',(row['discovery_id'],ETL_VERSION))
                counts['listings_loaded'] += 1


def evaluate_quality(target, *, source=None):
    """Reconcile every accounted detail and verify published fact grain/bridges."""
    from contextlib import ExitStack
    with ExitStack() as stack:
        source = source or stack.enter_context(get_connection(read_only=True,autocommit=True))
        try:
            physical = {**verify_schema_contract(target),**verify_nonnull(target)}
        except RuntimeError as exc:
            raise DataQualityError('Physical warehouse quality failed') from exc
        checkpoint = target.execute('SELECT last_raw_job_id FROM market_ops.refresh_checkpoint WHERE id=1').fetchone()
        upper = int(checkpoint['last_raw_job_id']) if checkpoint else 0
        missing_raw = 0
        for rows in _detail_batches(source,upper):
            accounted = _accounted_ids(target,[r['raw_job_id'] for r in rows])
            missing_raw += sum(r['raw_job_id'] not in accounted for r in rows)
        gaps = target.execute('''SELECT
          count(*) FILTER(WHERE s.raw_job_id IS NULL) AS missing_staging,
          count(*) FILTER(WHERE f.raw_job_id IS NULL) AS missing_fact
          FROM market_ops.raw_ingest_accounting a
          LEFT JOIN staging.job_observations_clean s ON s.raw_job_id=a.raw_job_id
              AND s.cleaning_version=a.cleaning_version
          LEFT JOIN market_dw.fct_job_snapshot f ON f.raw_job_id=a.raw_job_id
              AND f.cleaning_version=a.cleaning_version
          WHERE a.cleaning_version=%s AND a.disposition='LOADED' ''',(ETL_VERSION,)).fetchone()
        bad_quarantine = target.execute('''SELECT count(*) AS n FROM market_ops.raw_ingest_accounting a
            LEFT JOIN market_ops.raw_quarantine q USING(raw_job_id,cleaning_version)
            WHERE a.cleaning_version=%s AND a.disposition='QUARANTINED'
              AND (q.raw_job_id IS NULL OR q.status<>'OPEN')''',(ETL_VERSION,)).fetchone()['n']
        bad_bridges = target.execute('''SELECT count(*) AS n FROM market_dw.fct_job_snapshot f
            LEFT JOIN LATERAL(SELECT count(*) AS n,sum(allocation_weight) AS weight
                FROM market_dw.bridge_job_location b WHERE b.posting_natural_key=f.posting_natural_key
                  AND b.crawl_run_id=f.crawl_run_id) loc ON TRUE
            LEFT JOIN LATERAL(SELECT count(*) AS n FROM market_dw.bridge_job_skill b
                WHERE b.posting_natural_key=f.posting_natural_key AND b.crawl_run_id=f.crawl_run_id) skill ON TRUE
            WHERE loc.n=0 OR skill.n=0 OR abs(COALESCE(loc.weight,0)-1)>0.000001''').fetchone()['n']
        duplicate_grains = target.execute('''SELECT count(*) AS n FROM(
            SELECT posting_natural_key,crawl_run_id FROM market_dw.fct_job_snapshot
            GROUP BY posting_natural_key,crawl_run_id HAVING count(*)>1) duplicated''').fetchone()['n']
        result = {**physical,'unaccounted_raw_records':missing_raw,
                  'missing_staging_records':gaps['missing_staging'],'missing_fact_records':gaps['missing_fact'],
                  'unexplained_quarantine_records':bad_quarantine,'invalid_bridge_grains':bad_bridges,
                  'duplicate_fact_grains':duplicate_grains,'quality_status':'PASS','quality_passed':True,
                  'reconciliation_gap':missing_raw+gaps['missing_staging']+gaps['missing_fact']+bad_quarantine}
        if result['reconciliation_gap'] or bad_bridges or duplicate_grains:
            raise DataQualityError('Raw, staging, fact, quarantine or bridge reconciliation failed')
        return result


def _public_report(value):
    if isinstance(value,dict):
        private={'password','passwd','secret','token','cookie','cookies','authorization','database_url'}
        return {str(k):('[REDACTED]' if str(k).casefold() in private else _public_report(v)) for k,v in value.items()}
    if isinstance(value,(list,tuple)):
        return [_public_report(v) for v in value]
    if isinstance(value,str) and ('postgresql://' in value or 'postgresql+psycopg' in value):
        return '[REDACTED_CONNECTION]'
    return value


def record_pipeline_run(target, dag_id, run_id, status, details, *, stage='summary', batch_id=None):
    allowed={'SUCCESS_FULL','SUCCESS_PARTIAL','NO_NEW_DATA','SOURCE_BLOCKED','FAILED_DQ','FAILED_INFRA'}
    if status not in allowed or not isinstance(details,dict):
        raise ValueError('Invalid pipeline business status or report')
    public=json.loads(json.dumps(_public_report(details),default=str,ensure_ascii=False))
    return target.execute('''INSERT INTO market_ops.pipeline_runs
        (dag_id,run_id,stage,batch_id,business_status,details) VALUES(%s,%s,%s,%s,%s,%s)
        ON CONFLICT(dag_id,run_id,stage) DO UPDATE SET batch_id=EXCLUDED.batch_id,
            business_status=EXCLUDED.business_status,details=EXCLUDED.details,recorded_at=now()
        RETURNING pipeline_run_id''',(dag_id,run_id,stage,batch_id,status,Jsonb(public))).fetchone()['pipeline_run_id']


def refresh(*, source=None, target=None, reprocess=False, batch_size=250, batch_id=None):
    if type(batch_size) is not int or not 1<=batch_size<=5000:
        raise ValueError('batch_size must be 1..5000')
    from contextlib import ExitStack
    with ExitStack() as stack:
        source=source or stack.enter_context(get_connection(read_only=True,autocommit=True))
        target=target or stack.enter_context(target_connection())
        if not target.execute('SELECT pg_try_advisory_lock(72042701) AS locked').fetchone()['locked']:
            raise RuntimeError('Warehouse refresh already running')
        try:
            verify_schema_contract(target)
            upper=source.execute('SELECT COALESCE(max(raw_job_id),0) AS n FROM raw.job_observations').fetchone()['n']
            checkpoint=target.execute('SELECT last_raw_job_id FROM market_ops.refresh_checkpoint WHERE id=1').fetchone()
            if checkpoint and checkpoint['last_raw_job_id']>upper:
                raise DataQualityError('Raw source identity or checkpoint changed; inspect restore')
            counts={'selected':0,'loaded':0,'rejected':0,'quarantined':0,'excluded':0,
                    'superseded':0,'unchanged':0,'listings_loaded':0,'listings_quarantined':0}
            # Quarantined IDs do not disappear below the watermark. Retry them
            # by cleaning version, with a bounded mapping-only retry budget.
            if not reprocess:
                retry_ids=[r['raw_job_id'] for r in target.execute('''SELECT raw_job_id
                    FROM market_ops.raw_quarantine WHERE cleaning_version=%s AND status='OPEN'
                    AND attempts<5 AND next_retry_at<=now() ORDER BY next_retry_at,raw_job_id LIMIT %s''',
                    (ETL_VERSION,batch_size))]
                if retry_ids:
                    retry_rows=list(source.execute(_DETAIL_SELECT+' WHERE o.raw_job_id=ANY(%s)',(retry_ids,)))
                    with target.transaction():
                        for row in retry_rows:
                            _process_detail(target,row,counts)
            # Sweep ID coverage, not just > watermark. This catches late commits
            # and legacy gaps while the accounting PK keeps replay idempotent.
            after=0
            for rows in _detail_batches(source,upper,batch_size):
                accounted=set() if reprocess else _accounted_ids(target,[r['raw_job_id'] for r in rows])
                with target.transaction():
                    for row in rows:
                        if row['raw_job_id'] in accounted:
                            counts['unchanged']+=1
                            continue
                        _process_detail(target,row,counts)
                    after=rows[-1]['raw_job_id']
                    target.execute('''INSERT INTO market_ops.refresh_checkpoint
                        (id,last_raw_job_id,cleaning_version,updated_at) VALUES(1,%s,%s,now())
                        ON CONFLICT(id) DO UPDATE SET last_raw_job_id=EXCLUDED.last_raw_job_id,
                        cleaning_version=EXCLUDED.cleaning_version,updated_at=now()''',(after,ETL_VERSION))
            _ingest_listings(source,target,counts,reprocess)
            quality=evaluate_quality(target,source=source)
            with target.transaction():
                counts['newly_released_records']=target.execute('''SELECT
                    (SELECT count(*) FROM market_ops.raw_ingest_accounting WHERE cleaning_version=%s
                        AND disposition='LOADED' AND NOT publication_ready)
                    +(SELECT count(*) FROM market_ops.warehouse_listing_observations
                        WHERE cleaning_version=%s AND NOT publication_ready) AS n''',
                    (ETL_VERSION,ETL_VERSION)).fetchone()['n']
                target.execute("UPDATE market_ops.raw_ingest_accounting SET publication_ready=true "
                    "WHERE cleaning_version=%s AND disposition='LOADED'",(ETL_VERSION,))
                target.execute('UPDATE market_ops.warehouse_listing_observations SET publication_ready=true '
                    'WHERE cleaning_version=%s',(ETL_VERSION,))
                if counts['loaded'] or counts['listings_loaded'] or counts['newly_released_records']:
                    target.execute('''INSERT INTO market_ops.publication_state
                        (id,cleaning_version,last_successful_refresh,released_raw_watermark,quality_status)
                        VALUES(1,%s,now(),%s,'PASS') ON CONFLICT(id) DO UPDATE SET
                        cleaning_version=EXCLUDED.cleaning_version,last_successful_refresh=now(),
                        released_raw_watermark=EXCLUDED.released_raw_watermark,quality_status='PASS' ''',
                        (ETL_VERSION,upper))
                target.execute('INSERT INTO market_ops.refresh_log(selected,loaded,rejected,nonnull_verified) '
                    'VALUES(%s,%s,%s,true)',(counts['selected'],counts['loaded'],counts['rejected']))
            coverage=target.execute('SELECT * FROM market_dw.coverage_freshness '
                +('WHERE batch_id=%s::uuid ' if batch_id else '')+'ORDER BY batch_date DESC LIMIT 1',
                (batch_id,) if batch_id else ()).fetchone()
            host=_host_status(target)
            changed=bool(counts['loaded'] or counts['listings_loaded'] or counts['newly_released_records'])
            business=('SUCCESS_FULL' if changed and coverage and coverage['coverage_complete'] else
                      'SUCCESS_PARTIAL' if changed else 'SOURCE_BLOCKED' if host['source_blocked'] else 'NO_NEW_DATA')
            return {**counts,**quality,'business_status':business,'status':business,
                    'checkpoint':upper,'upper_bound':upper,'cleaning_version':ETL_VERSION,
                    'publish_requires_dq_pass':True,'coverage':dict(coverage) if coverage else None,
                    'reconciliation':quality,'overview':target.execute('SELECT * FROM market_dw.collection_overview').fetchone(),
                    **host}
        finally:
            target.execute('SELECT pg_advisory_unlock(72042701)')
