"""M3: verified TopCV discovery -> detail files + resumable, read-only checkpoint.

No DB connections and no site-wide searching. Only job links in a validated M2
manifest are fetched. A completed record is reusable only when the corresponding
saved file passes its checksum and identity checks. Challenges stop the run.
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import urlsplit

from selenium.common.exceptions import WebDriverException

from src.crawler.parser import (
    AccessChallenge, LayoutChanged, normalize_job_link, to_raw_job_payload,
)
from src.crawler.topcv import NavigationError
from src.crawler.pipeline.discovery import verified_route_for, validate_page_url, scope_verified, _fold
from src.raw_storage.contracts import compute_content_hash


class DiscoveryValidationError(ValueError):
    """Refuse an untrusted or inconsistent discovery manifest."""


class CheckpointMismatchError(ValueError):
    """Do not overwrite output from another scope or different discovery."""


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(data) -> bytes:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':'),
                      allow_nan=False).encode('utf-8')


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp')
    temp.write_bytes(data)
    temp.replace(path)


def _write_json(path: Path, data) -> str:
    blob = (json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False) + '\n').encode('utf-8')
    _atomic_bytes(path, blob)
    return _sha(blob)


def load_discovery_manifest(path: Path, task) -> tuple[dict, list[dict], str]:
    """Validate a genuine M2 report before any browser is opened.

    The report is a local checkpoint, not a signed provenance document: this
    checks structural consistency and scope, not cryptographic authenticity.
    """
    if not path.is_file():
        raise DiscoveryValidationError(f'M2 discovery JSON not found: {path}')
    try:
        report = json.loads(path.read_text(encoding='utf-8-sig'))
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise DiscoveryValidationError('Discovery checkpoint is not valid UTF-8 JSON.') from exc
    if not isinstance(report, dict) or report.get('mode') != 'M2_verified_public_discovery_no_db_writes':
        raise DiscoveryValidationError('Input must be an M2 verified-public discovery report.')
    route = verified_route_for(task.position_id, task.location_id)
    if route is None or report.get('verified_entry_url') != route:
        raise DiscoveryValidationError('Discovery route is not verified for the selected task.')
    meta = report.get('task')
    if not isinstance(meta, dict) or (meta.get('position_id'), meta.get('location_id'),
                                      meta.get('keyword'), meta.get('city')) != (
                                          task.position_id, task.location_id,
                                          task.keyword, task.search_label):
        raise DiscoveryValidationError('Discovery keyword/city does not match configured task.')
    # M2 only persists pages after validating route, title and city. Reject
    # manifest states where a page failed validation, but allow a capped sample.
    if report.get('status') not in {
        'no_verified_next_link', 'max_pages_reached', 'repeated_page_no_new_jobs',
        'explicit_empty_page',
    }:
        raise DiscoveryValidationError('Discovery did not finish at an accepted verified-page boundary.')
    pages = report.get('pages')
    jobs = report.get('jobs')
    if not isinstance(pages, list) or not pages or not isinstance(jobs, list) or not jobs:
        raise DiscoveryValidationError('Discovery must include verified pages and jobs.')
    page_urls = {}
    for expected_page, page in enumerate(pages, 1):
        if (not isinstance(page, dict) or page.get('page') != expected_page or
                not isinstance(page.get('url'), str) or
                not validate_page_url(page['url'], route, page=expected_page)):
            raise DiscoveryValidationError('Discovery page number/URL is inconsistent.')
        page_urls[expected_page] = page['url']
    normalized = []
    seen = set()
    for index, entry in enumerate(jobs):
        if not isinstance(entry, dict):
            raise DiscoveryValidationError(f'Job entry {index} must be an object.')
        sid = entry.get('source_job_id')
        url = entry.get('job_url')
        identity = normalize_job_link(url, 'https://www.topcv.vn')
        if (not isinstance(sid, str) or not sid.isascii() or not sid.isdecimal()
                or identity is None or identity != (url, sid) or sid in seen):
            raise DiscoveryValidationError(f'Invalid, duplicate or mismatched TopCV job at index {index}.')
        first_page = entry.get('first_page')
        if type(first_page) is not int or page_urls.get(first_page) != entry.get('page_url'):
            raise DiscoveryValidationError(f'Job {sid} refers to an unverified discovery page.')
        listing = entry.get('listing')
        if listing is not None:
            if not isinstance(listing, dict):
                raise DiscoveryValidationError(f'Job {sid} listing metadata must be an object.')
            # M2 enriched listing metadata is optional; older M2 reports
            # have only identity and URL.
            if listing.get('source_job_id', sid) != sid:
                raise DiscoveryValidationError(f'Job {sid} listing ID mismatch.')
        seen.add(sid)
        normalized.append({'source_job_id': sid, 'job_url': url,
                           'first_page': first_page, 'page_url': entry['page_url'],
                           'listing': listing})
    if (type(report.get('unique_job_count')) is not int or
            report['unique_job_count'] != len(jobs)):
        raise DiscoveryValidationError('Discovery unique job count mismatch.')
    fingerprint = _sha(_canonical_json({
        'route': route, 'task': meta, 'pages': page_urls, 'jobs': normalized,
    }))
    return report, normalized, fingerprint


def _check_completed_record(record: dict, job: dict, results_dir: Path) -> bool:
    if record.get('status') != 'success':
        return False
    sid = job['source_job_id']
    if record.get('source_job_id') != sid or record.get('job_url') != job['job_url']:
        raise CheckpointMismatchError(f'Existing success record identity differs: {sid}')
    path = results_dir / (sid + '.json')
    if not path.is_file():
        raise CheckpointMismatchError(f'Previously completed detail file is missing: {sid}.json')
    data = path.read_bytes()
    if _sha(data) != record.get('file_sha256'):
        raise CheckpointMismatchError(f'Detail output has been modified: {sid}.json')
    try:
        saved = json.loads(data)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CheckpointMismatchError(f'Detail file is invalid JSON: {sid}.json') from exc
    if (saved.get('source_job_id') != sid or
            saved.get('requested_job_url') != job['job_url'] or
            saved.get('raw_payload_hash') != compute_content_hash(saved.get('raw_payload', {}))):
        raise CheckpointMismatchError(f'Detail output identity/payload changed: {sid}.json')
    return True


def run_detail(crawler, task, cfg, *, discovery_path: Path, report_path: Path,
               max_jobs: int = 5, retry_failed: bool = False,
               sleep_fn=time.sleep) -> dict:
    """Fetch up to max_jobs, in discovery order; safely extend on later runs.

    A failed parse is reported per job. Two consecutive layout failures stop
    further requests (likely page redesign). Access/transport errors stop
    immediately, never auto-retry a challenge or an unverified layout.
    """
    if type(max_jobs) is not int or not 1 <= max_jobs <= 100:
        raise ValueError('max_jobs must be an integer between 1 and 100.')
    discovery_path = Path(discovery_path).resolve()
    report_path = Path(report_path).resolve()
    _, jobs, fingerprint = load_discovery_manifest(discovery_path, task)
    # The data folder is determined by checkpoint name, never by untrusted IDs
    # other than validated numeric TopCV job IDs.
    results_dir = report_path.parent / f'topcv_detail_{task.position_id}_{task.location_id}'
    if report_path.exists():
        try:
            report = json.loads(report_path.read_text(encoding='utf-8'))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise CheckpointMismatchError('Existing M3 checkpoint is corrupt; refusing overwrite.') from exc
        if (report.get('mode') != 'M3_verified_detail_no_database_writes' or
                report.get('manifest_sha256') != fingerprint or
                report.get('position_id') != task.position_id or
                report.get('location_id') != task.location_id):
            raise CheckpointMismatchError('Existing M3 checkpoint belongs to another discovery or scope.')
        if not isinstance(report.get('records'), dict):
            raise CheckpointMismatchError('Existing M3 checkpoint records are invalid.')
        was_access_blocked = report.get('status') in {
            'stopped_on_access_challenge', 'stopped_on_previous_access_challenge'}
    else:
        report = {
            'version': 1, 'mode': 'M3_verified_detail_no_database_writes',
            'started_at_utc': _utc(), 'position_id': task.position_id,
            'location_id': task.location_id, 'manifest_sha256': fingerprint,
            'discovery_path': str(discovery_path),
            'discovery_unique_job_count': len(jobs), 'records': {},
            'listing_enrichment': {}, 'listing_enrichment_errors': [],
            'database_writes': 0, 'raw_storage_integrated': False,
            'coverage_confirmed': False,
        }

        was_access_blocked = False

    selected = jobs[:max_jobs]
    report.setdefault('listing_enrichment', {})
    report.setdefault('listing_enrichment_errors', [])
    report['requested_job_limit'] = max_jobs
    report['selected_jobs'] = len(selected)
    report['run_started_at_utc'] = _utc()
    report['status'] = 'running'
    report['last_error'] = None
    records = report['records']

    def persist():
        report['updated_at_utc'] = _utc()
        report['successful_jobs_in_selection'] = sum(
            _check_completed_record(records.get(j['source_job_id'], {}), j, results_dir)
            for j in selected)
        report['failed_jobs_in_selection'] = sum(
            records.get(j['source_job_id'], {}).get('status') == 'failed_parse'
            for j in selected)
        report['untouched_jobs_in_selection'] = sum(
            j['source_job_id'] not in records for j in selected)
        _write_json(report_path, report)

    # Validate every pre-existing success BEFORE any browsing or modification.
    for j in jobs:
        existing = records.get(j['source_job_id'], {})
        if existing.get('status') == 'success':
            _check_completed_record(existing, j, results_dir)
    persist()
    if was_access_blocked or any(records.get(j['source_job_id'], {}).get('status') == 'blocked'
                                 for j in selected):
        # Re-running this command is NOT authorization to retry a challenge.
        # A fresh operator-controlled checkpoint is required after resolving it.
        report['status'] = 'stopped_on_previous_access_challenge'
        report['last_error'] = 'Previous access challenge; M3 refuses automatic re-attempt.'
        persist()
        return report
    # Older M2 reports contain only identity/URL, not listing business fields.
    # A brand detail sometimes omits the company name; the parser correctly
    # refuses to guess it. Re-read ONLY the verified M2 page(s) to enrich the
    # selected unfinished jobs, when needed. If a job has disappeared, do not
    # fabricate missing fields. Cache the enrichment in this M3 checkpoint.
    cached_listing = report['listing_enrichment']
    # M8.5I: never refresh a listing solely for a job that this invocation
    # will NOT fetch. The old filter excluded successes but accidentally
    # re-fetched listing pages for failed_parse rows even with retry_failed=False.
    # Modern M2 manifests with embedded listing metadata already avoid this
    # extra navigation entirely; preserve the legacy fallback for genuinely
    # unfinished jobs lacking that data.
    def detail_will_be_requested(job):
        status = records.get(job['source_job_id'], {}).get('status')
        return status != 'success' and (status != 'failed_parse' or retry_failed)

    missing_listing = [j for j in selected
                       if j['listing'] is None and j['source_job_id'] not in cached_listing
                       and detail_will_be_requested(j)]
    report['listing_refresh_needed_for_jobs'] = len(missing_listing)
    report['listing_refresh_skipped_failed_parse_jobs'] = sum(
        records.get(j['source_job_id'], {}).get('status') == 'failed_parse'
        and not retry_failed for j in selected)
    if missing_listing:
        route = verified_route_for(task.position_id, task.location_id, config_path=cfg.config_path)
        page_numbers = sorted({j['first_page'] for j in missing_listing})
        pages_by_number = {j['first_page']: j['page_url'] for j in missing_listing}
        for page_no in page_numbers:
            try:
                url = pages_by_number[page_no]
                report['current_stage'] = 'listing_refresh_navigation'
                if page_no == 1:
                    crawler.open_public_listing(url)
                else:
                    crawler.open_observed_result_page(url, route, page=page_no)
                report['current_stage'] = 'listing_refresh_readiness'
                ready = crawler.wait_for_listing_ready(timeout_seconds=cfg.selenium.explicit_wait_seconds)
                if not ready.get('ready'):
                    raise NavigationError('Listing enrichment is not ready.')
                report['current_stage'] = 'listing_refresh_validation'
                inspected = crawler.inspect_navigation_controls(limit=20, stage='search_results', keyword=task.keyword)
                if not scope_verified(current_url=crawler._driver().current_url, route=route,
                                      page=page_no, title=inspected.get('page_title', ''),
                                      task=task, location_label=(inspected.get('observed_state') or {}).get('location_label', '')):
                    raise NavigationError('Listing enrichment scope cannot be verified.')
                report['current_stage'] = 'listing_refresh_capture'
                cap = crawler.fetch_page_capture()
                wanted = {j['source_job_id']: j for j in missing_listing if j['first_page'] == page_no}
                for item in cap.items:
                    sid = str(item.get('source_job_id', ''))
                    if sid not in wanted or item.get('job_url') != wanted[sid]['job_url']:
                        continue
                    if _fold(task.search_label) not in _fold(item.get('location_raw') or ''):
                        continue
                    fields = ('source_job_id', 'job_title_raw', 'company_name_raw',
                              'company_url_raw', 'location_raw', 'salary_raw',
                              'experience_raw', 'posted_time_raw', 'updated_at_raw',
                              'job_tags_raw')
                    cached_listing[sid] = {k:item[k] for k in fields if k in item}
                report['last_completed_stage'] = 'listing_refresh_capture'
                print(f'LISTING ENRICHED: page={page_no} matched={sum(s in cached_listing for s in wanted)}')
                persist()
            except AccessChallenge as exc:
                report['status'] = 'stopped_on_access_challenge'
                # M8.5G: a legacy M2 checkpoint may trigger a *listing refresh*
                # before any detail request. Preserve the stage in the journal;
                # the coarse status alone previously obscured the distinction.
                report['blocked_stage'] = 'listing_refresh'
                report['blocked_substage'] = report.get('current_stage', 'unknown')
                report['last_error'] = f'Listing refresh: {type(exc).__name__}: {str(exc)[:250]}'
                persist()
                return report
            except (NavigationError, LayoutChanged, WebDriverException, TimeoutError, OSError) as exc:
                # Still allow valid standard details; brand requiring unavailable
                # listing company will report failed_parse, not fictitious data.
                report['listing_enrichment_failed_stage'] = report.get('current_stage', 'unknown')
                report['listing_enrichment_errors'].append(
                    f'Page {page_no}: {type(exc).__name__}: {str(exc)[:250]}')
                persist()
                print(f'LISTING ENRICHMENT NOT VERIFIED: page={page_no}; no metadata accepted.')

    consecutive_layout_failures = 0
    requested_count = 0
    for job in selected:
        sid, url = job['source_job_id'], job['job_url']
        previous = records.get(sid, {})
        if _check_completed_record(previous, job, results_dir):
            consecutive_layout_failures = 0
            continue
        if previous.get('status') == 'failed_parse' and not retry_failed:
            continue
        if requested_count:
            sleep_fn(cfg.selenium.delay_between_jobs_seconds)
        requested_count += 1
        attempts = int(previous.get('attempts', 0)) + 1
        try:
            report['current_stage'] = 'detail_navigation_or_capture'
            capture = crawler.fetch_detail_capture(url)
            report['current_stage'] = 'detail_identity_and_payload_validation'
            actual = normalize_job_link(capture.job_url, 'https://www.topcv.vn')
            if (actual is None or actual[1] != sid or
                    capture.parsed.get('source_job_id') != sid):
                raise LayoutChanged('Detail identity does not match M2 discovery.')
            listing = job['listing'] if job['listing'] is not None else cached_listing.get(sid)
            payload = to_raw_job_payload(capture.parsed, listing)
            entry = {
                'source_job_id': sid, 'requested_job_url': url,
                'actual_job_url': capture.job_url,
                'first_discovered_page': job['first_page'],
                'discovery_page_url': job['page_url'],
                'captured_at_utc': capture.captured_at.isoformat(),
                'detail_layout': capture.parsed.get('detail_layout'),
                'detail_parser_version': capture.parsed.get('parser_version'),
                'raw_payload': payload.to_dict(),
                'raw_payload_hash': compute_content_hash(payload.to_dict()),
                'parsed_detail': capture.parsed,
                'listing_metadata': listing,
                'raw_html_saved': False,
                'note': 'Offline detail artifact only; no PostgreSQL write.',
            }
            report['current_stage'] = 'detail_checkpoint_write'
            file_hash = _write_json(results_dir / (sid + '.json'), entry)
            records[sid] = {
                'source_job_id': sid, 'job_url': url, 'status': 'success',
                'attempts': attempts, 'file': str((results_dir / (sid + '.json')).relative_to(report_path.parent)),
                'file_sha256': file_hash, 'detail_layout': entry['detail_layout'],
                'raw_payload_hash': entry['raw_payload_hash'],
                'captured_at_utc': entry['captured_at_utc'],
            }
            consecutive_layout_failures = 0
            report['last_completed_stage'] = 'detail_checkpoint_write'
            print(f'DETAIL OK: job_id={sid} layout={entry["detail_layout"]}')
        except AccessChallenge as exc:
            report['status'] = 'stopped_on_access_challenge'
            report['blocked_stage'] = 'detail_navigation_or_capture'
            report['blocked_substage'] = report.get('current_stage', 'unknown')
            report['last_error'] = f'{type(exc).__name__}: {str(exc)[:250]}'
            records[sid] = {'source_job_id': sid, 'job_url': url,
                            'status': 'blocked', 'attempts': attempts,
                            'error': report['last_error']}
            persist()
            print('DETAIL STOP: access challenge; no automatic retry.')
            break
        except LayoutChanged as exc:
            report['failed_stage'] = report.get('current_stage', 'unknown')
            records[sid] = {'source_job_id': sid, 'job_url': url,
                            'status': 'failed_parse', 'attempts': attempts,
                            'missing_fields': list(exc.missing_fields),
                            'error': f'{type(exc).__name__}: {str(exc)[:250]}'}
            consecutive_layout_failures += 1
            persist()
            print(f'DETAIL PARSE FAILED: job_id={sid} ({str(exc)[:100]})')
            if consecutive_layout_failures >= 2:
                report['status'] = 'stopped_on_repeated_layout_error'
                report['last_error'] = 'Two consecutive detail layouts failed validation.'
                persist()
                break
        except (NavigationError, WebDriverException, TimeoutError, OSError) as exc:
            report['failed_stage'] = report.get('current_stage', 'unknown')
            report['status'] = 'stopped_on_navigation_error'
            report['last_error'] = f'{type(exc).__name__}: {str(exc)[:250]}'
            records[sid] = {'source_job_id': sid, 'job_url': url,
                            'status': 'navigation_stopped', 'attempts': attempts,
                            'error': report['last_error']}
            persist()
            print(f'DETAIL STOP: navigation failed for job_id={sid}')
            break
        persist()
    else:
        report['status'] = ('sample_completed' if report['successful_jobs_in_selection'] == len(selected)
                            else 'sample_completed_with_failed_jobs')
    persist()
    return report
