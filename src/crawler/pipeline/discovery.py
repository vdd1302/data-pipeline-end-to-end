"""M2 read-only TopCV discovery of independently verified public result routes.

No guessed search URLs, form submission, detail requests, database connection or
unverified page-number construction. Only links extracted from the CURRENT
verified pagination DOM can be followed. A missing pagination control is an
uncertain coverage boundary, not proof that every job has been discovered.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time
from urllib.parse import parse_qs, parse_qsl, urlencode, urljoin, urlsplit

from bs4 import BeautifulSoup
from selenium.common.exceptions import WebDriverException

from src.crawler.parser import AccessChallenge, LayoutChanged, normalize_job_link
from src.crawler.topcv import NavigationError

# Historical M1h route for offline legacy fixtures ONLY. NEVER authorize live
# discovery from this constant; the canonical M5 registry is authoritative.
VERIFIED_ROUTES: dict[tuple[str, str], str] = {
    ('data_engineer', 'ha_noi'):
        'https://www.topcv.vn/tim-viec-lam-data-engineer-tai-ha-noi-l1cr257cb261cl285',
}

# These are candidate pagination *containers*, not verified TopCV selectors.
# A safe href, visible link text, explicit page=N, and same route are ALL needed.
PAGER_SELECTORS = (
    '.pagination', '[class*="pagination"]', '[class*="paging"]',
    'nav[aria-label*="page" i]', 'a[rel="next"]',
)


def _fold(s: str) -> str:
    import unicodedata
    s = unicodedata.normalize('NFD', str(s)).casefold()
    return ''.join(x for x in s if unicodedata.category(x) != 'Mn').replace('đ', 'd')


# TopCV's own search flags that the site itself adds when a keyword search is submitted in the
# UI (e.g. ...-kl1?type_keyword=1&sba=1&locations=l1&saturday_status=0). Nothing else is ever
# accepted in a route: no tracking, auth, redirect or free-text parameters.
ROUTE_QUERY_KEYS = frozenset({'type_keyword', 'disable_auto_detect_type_keyword', 'sba',
                              'locations', 'saturday_status'})
_ROUTE_QUERY_VALUE = re.compile(r'[A-Za-z0-9_-]{1,24}')
_KEYWORD_ROUTE_PATH = re.compile(r'/tim-viec-lam-[a-z0-9-]+-tai-(?:ha-noi|ho-chi-minh)-kl[0-9]+')


def route_query_allowed(query: str) -> bool:
    """True for a nonempty query made only of allow-listed TopCV search flags."""
    pairs = parse_qsl(query, keep_blank_values=True)
    keys = [key for key, _ in pairs]
    return (bool(pairs) and len(keys) == len(set(keys))
            and all(key in ROUTE_QUERY_KEYS and _ROUTE_QUERY_VALUE.fullmatch(value) for key, value in pairs))


def is_keyword_route(route: str) -> bool:
    """A keyword + city results route (`...-tai-<city>-kl<n>`), as opposed to the IT-category `cr257` form."""
    return _KEYWORD_ROUTE_PATH.fullmatch(urlsplit(route).path) is not None


def validate_page_url(url: str, route: str, *, page: int) -> bool:
    """Only allow same public result path and exact page query from actual DOM.

    Page 1 is the exact audited link. For the usual routes that link has no query and
    page>=2 must have a single positive numeric 'page' parameter. A keyword route may
    carry TopCV's allow-listed search flags (see ROUTE_QUERY_KEYS); its page>=2 link may
    repeat those flags plus exactly one numeric 'page'. Any other parameter is refused.
    """
    try:
        a, b = urlsplit(url), urlsplit(route)
        if (a.scheme, a.hostname, a.port, a.path) != ('https', 'www.topcv.vn', None, b.path):
            return False
        if a.username or a.password or a.fragment or a.netloc != b.netloc:
            return False
        if page == 1:
            if not b.query:
                return url == route and not a.query
            return url == route and route_query_allowed(b.query)
        params = parse_qs(a.query, keep_blank_values=True)
        page_ok = ('page' in params and len(params['page']) == 1 and
                   params['page'][0].isascii() and params['page'][0].isdigit() and
                   int(params['page'][0]) == page)
        if not b.query:
            return len(params) == 1 and page_ok
        flags = [(key, value) for key, value in parse_qsl(a.query, keep_blank_values=True) if key != 'page']
        return (page_ok and route_query_allowed(b.query)
                and (not flags or route_query_allowed(urlencode(flags))))
    except (TypeError, ValueError):
        return False


def verified_route_for(position_id: str, location_id: str, *, config_path=None) -> str | None:
    """Use the single canonical M5 JSON registry, never historical URL constants.

    Invalid/missing evidence is NOT promoted to verified. The legacy
    ``VERIFIED_ROUTES`` is retained only for historical fixture tests.
    """
    from .m8_registry import read_verified_registry, M8RegistryError

    root = (Path(config_path).resolve().parent if config_path is not None
            else Path(__file__).resolve().parents[3] / 'config')
    registry_path = root / 'verified_public_routes.json'
    if not registry_path.exists():
        registry_path = root / 'm5_verified_public_routes.json'
    try:
        return read_verified_registry(registry_path).get(
            (position_id, location_id))
    except (M8RegistryError, OSError, TypeError, ValueError):
        return None


class VerifiedPageSinkError(RuntimeError):
    """The optional persistence sink failed; do not advance a checkpoint."""


@dataclass(frozen=True)
class CalibratedFirstPage:
    """A listing capture from an already-audited CURRENT Selenium page.

    The M5b consumer must still independently validate URL, title, city and
    listing data before recording even the first discovery. This is *not*
    a persistent trust token or a skip-validation switch.
    """
    route: str
    title: str
    location_label: str
    capture: Any


@dataclass(frozen=True)
class PaginationEvidence:
    next_url: str | None
    status: str
    candidate_count: int
    pager_detected: bool


def pagination_from_html(html: str, current_url: str, route: str, page: int) -> PaginationEvidence:
    """Read only observed anchor hrefs; never invent ?page=2 URLs.

    If TopCV changes pagination DOM (JS-only next, other parameter patterns),
    stop and report it for live calibration. No speculative clicks.
    """
    soup = BeautifulSoup(html, 'html.parser')
    anchors = []
    pager_detected = False
    seen_nodes = set()
    for selector in PAGER_SELECTORS:
        for node in soup.select(selector):
            if id(node) in seen_nodes:
                continue
            seen_nodes.add(id(node))
            pager_detected = True
            if node.name == 'a':
                anchors.append(node)
            else:
                anchors.extend(node.select('a[href], a[data-href]'))
    links: set[str] = set()
    for a in anchors:
        if (a.has_attr('hidden') or a.get('aria-hidden') == 'true' or
                'display:none' in str(a.get('style', '')).replace(' ', '').lower()):
            continue
        href = a.get('href') or a.get('data-href')
        if not isinstance(href, str) or not href.strip():
            continue
        label = ' '.join(filter(None, (a.get_text(' ', strip=True), a.get('aria-label', ''), a.get('title', ''))))
        rel = a.get('rel', [])
        is_next = ('next' in rel or re.search(r'\b(next|tiếp|sau)\b', _fold(label)) is not None
                   or label.strip() == str(page+1) or label.strip() in ('>', '›', '»'))
        if not is_next:
            continue
        candidate = urljoin(current_url, href)
        if validate_page_url(candidate, route, page=page+1):
            links.add(candidate)
    if len(links) == 1:
        return PaginationEvidence(next(iter(links)), 'observed_next_link', len(anchors), True)
    if len(links) > 1:
        return PaginationEvidence(None, 'ambiguous_next_links', len(anchors), True)
    return PaginationEvidence(None, 'no_verified_next_link', len(anchors), pager_detected)


def verify_listing_identity(item: Any) -> tuple[bool, str]:
    """Use the same TopCV URL contract as the listing/detail parsers.

    Both /viec-lam/.../ID.html and
    /brand/.../tuyen-dung/...-jID.html are allowed, but only when the
    host/path/ID are verified, the URL is canonical and the ID matches the
    card's source_job_id. Never accept arbitrary URLs or infer a missing ID.
    """
    if not isinstance(item, dict):
        return False, 'malformed_card'
    source_job_id = item.get('source_job_id')
    if (isinstance(source_job_id, bool) or
            not isinstance(source_job_id, (str, int)) or
            not str(source_job_id).isascii() or
            not str(source_job_id).isdigit()):
        return False, 'invalid_source_job_id'
    job_url = item.get('job_url')
    if not isinstance(job_url, str):
        return False, 'missing_job_url'
    identity = normalize_job_link(job_url, 'https://www.topcv.vn')
    if identity is None:
        return False, 'invalid_topcv_job_url'
    canonical_url, url_id = identity
    if canonical_url != job_url or urlsplit(job_url).scheme != 'https':
        return False, 'noncanonical_or_insecure_url'
    if url_id != str(source_job_id):
        return False, 'url_job_id_mismatch'
    return True, 'verified'


def _keyword_in_heading(keyword: str, title: str) -> bool:
    """Whole keyword with optional hyphens/spaces (Back-End vs Backend).

    Does not accept unrelated synonyms; live URL, city, and card checks remain
    independently required. Needed by the fixed 10-position IT config.
    """
    expected, observed = _fold(keyword), _fold(title)
    # TopCV headings use both spellings for the same security role. Keep the
    # remaining keyword, city, route and per-card checks independent.
    expected = re.sub(r'\bcyber[ -]+security\b', 'cybersecurity', expected)
    observed = re.sub(r'\bcyber[ -]+security\b', 'cybersecurity', observed)
    if expected in observed:
        return True
    if '-' not in expected:
        return False
    compressed = lambda text: re.sub(r'[\s-]+', '', text)
    return compressed(expected) in compressed(observed)


def scope_verified(*, current_url: str, route: str, page: int, title: str, task, location_label: str) -> bool:
    return (validate_page_url(current_url, route, page=page)
            and _keyword_in_heading(task.keyword, title)
            and _fold(task.search_label) in _fold(title)
            and _fold(task.search_label) in _fold(location_label))


def total_from_heading(title: str, task) -> int | None:
    if not (_keyword_in_heading(task.keyword, title) and _fold(task.search_label) in _fold(title)):
        return None
    m = re.search(r'Tuyển dụng\s+([\d.,]+)\s+việc làm', title, re.I)
    return int(re.sub(r'[^\d]', '', m.group(1))) if m else None


def _atomic_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    tmp.replace(path)


def run_discovery(crawler, task, cfg, *, max_pages: int, report_path: Path,
                  sleep_fn=time.sleep, on_verified_page=None,
                  first_page_evidence: CalibratedFirstPage | None = None) -> dict:
    """Crawl a verified route until an observed pagination boundary.

    ``on_verified_page(page, capture, items)`` is an optional M4b sink. It
    runs after URL/title/city checks, before checkpoint advancement. A sink
    exception is fatal, never interpreted as an empty page or layout change.

    The checkpoint is a recovery/diagnostic journal, not an auto-resume token.
    Never silently skip pages in later runs based on old checkpoint contents.
    """
    if not isinstance(max_pages, int) or isinstance(max_pages, bool) or not 1 <= max_pages <= 10:
        raise ValueError('max_pages must be 1..10')
    registered_route = verified_route_for(task.position_id, task.location_id, config_path=cfg.config_path)
    route = registered_route
    if first_page_evidence is not None:
        if not isinstance(first_page_evidence, CalibratedFirstPage):
            raise ValueError('first_page_evidence must be a CalibratedFirstPage')
        route = first_page_evidence.route
        city_code = {'ha_noi': 'l1', 'ho_chi_minh': 'l2'}.get(task.location_id)
        # Check that the route is an actual scoped TopCV result URL. Never
        # override a differently registered route; no arbitrary URL injection.
        if (not isinstance(route, str) or not validate_page_url(route, route, page=1)
                or city_code is None or f'-{city_code}cr257' not in urlsplit(route).path
                or (registered_route is not None and registered_route != route)):
            raise ValueError('Calibrated first-page route conflicts with scoped registry or TopCV URL requirements.')
    report = {
        'version': 1, 'mode': 'M2_verified_public_discovery_no_db_writes',
        'started_at_utc': datetime.now(timezone.utc).isoformat(),
        'task': {'position_id': task.position_id, 'location_id': task.location_id,
                 'keyword': task.keyword, 'city': task.search_label},
        'verified_entry_url': route, 'status': 'started', 'pages': [],
        'unique_job_count': 0, 'duplicate_cards_within_run': 0,
        'jobs': [], 'errors': [], 'database_writes': 0, 'detail_requests': 0,
        'coverage_confirmed': False,
        'first_page_reused_from_calibration': first_page_evidence is not None,
    }
    def persist():
        report['updated_at_utc'] = datetime.now(timezone.utc).isoformat()
        _atomic_json(report_path, report)

    if route is None:
        report['status'] = 'unverified_position_city_route'
        persist()
        return report
    seen: dict[str, dict] = {}
    visited: set[str] = set()
    next_url = route
    stage = 'listing_navigation'
    try:
        for page in range(1, max_pages+1):
            if next_url in visited:
                report['status'] = 'repeated_page_url'
                break
            reuse_page = page == 1 and first_page_evidence is not None
            if reuse_page:
                stage = 'listing_reuse_validation'
                # Do not call open_public_listing(), readiness, or parser again.
                # The browser is still in the same session that calibrated the
                # capture. Verify it has not navigated away in the meantime.
                current_url = crawler._driver().current_url
                title = first_page_evidence.title
                location_label = first_page_evidence.location_label
                capture = first_page_evidence.capture
            else:
                stage = 'listing_navigation'
                if page > 1:
                    sleep_fn(cfg.selenium.delay_between_pages_seconds)
                    crawler.open_observed_result_page(next_url, route, page=page)
                else:
                    crawler.open_public_listing(route)
                stage = 'listing_readiness'
                ready = crawler.wait_for_listing_ready(timeout_seconds=cfg.selenium.explicit_wait_seconds)
                if not ready['ready']:
                    report['status'] = 'listing_not_ready'
                    report['errors'].append(ready.get('reason', 'unknown'))
                    break
                stage = 'listing_scope_inspection'
                current_url = crawler._driver().current_url
                state = crawler.inspect_navigation_controls(limit=30, stage='search_results', keyword=task.keyword)
                title = state.get('page_title', '')
                location_label = (state.get('observed_state') or {}).get('location_label') or ''
                stage = 'listing_capture_or_parse'
                capture = crawler.fetch_page_capture()
            stage = 'listing_scope_validation'
            visited.add(next_url)
            if (capture.page_url != current_url or
                    not scope_verified(current_url=current_url, route=route, page=page,
                                       title=title, task=task, location_label=location_label)):
                report['status'] = 'scope_unverified'
                report['errors'].append(f'Page {page}: current route/capture/title/city label not verified')
                break
            items = capture.items
            if not items:
                report['status'] = 'explicit_empty_page'
                break
            # A malformed card must never become an accepted discovery.
            # Prevent an unexpected KeyError from being reported as a clean
            # stop, and require the same public identity shape as the parser.
            stage = 'listing_card_validation'
            identity_checks = [verify_listing_identity(item) for item in items]
            if any(not valid for valid, _ in identity_checks):
                report['status'] = 'listing_identity_unverified'
                # Diagnostic counts only: no raw URLs, page HTML or private tokens.
                issue_counts: dict[str, int] = {}
                for valid, issue in identity_checks:
                    if not valid:
                        issue_counts[issue] = issue_counts.get(issue, 0) + 1
                report['identity_issue_counts'] = issue_counts
                report['errors'].append(
                    f'Page {page}: {sum(issue_counts.values())} cards failed canonical URL/ID verification'
                )
                break
            ids = [str(item['source_job_id']) for item in items]
            # Reject mixed/out-of-scope locations rather than silently creating
            # a wrong-city discovery, even if the result-page title is correct.
            if any(_fold(task.search_label) not in _fold(item.get('location_raw') or '')
                   for item in items):
                report['status'] = 'listing_city_scope_mismatch'
                report['errors'].append(f'Page {page}: one or more job locations lack the configured city')
                break
            new_ids = []
            for item in items:
                id_ = str(item['source_job_id'])
                if id_ not in seen:
                    # Keep structured listing fields for M3 fallback, not full
                    # HTML or duplicated card markup. Older M2 reports without
                    # this optional field remain valid inputs for M3.
                    listing_fields = (
                        'source_job_id', 'job_title_raw', 'company_name_raw',
                        'company_url_raw', 'salary_raw', 'location_raw',
                        'experience_raw', 'posted_time_raw', 'updated_at_raw',
                        'job_tags_raw',
                    )
                    listing = {key: item[key] for key in listing_fields if key in item}
                    seen[id_] = {'source_job_id': id_, 'job_url': item['job_url'],
                                 'first_page': page, 'first_position': item.get('result_position'),
                                 'page_url': current_url, 'listing': listing}
                    new_ids.append(id_)
                else:
                    report['duplicate_cards_within_run'] += 1
            evidence = pagination_from_html(capture.page_html, current_url, route, page)
            if on_verified_page is not None:
                stage = 'verified_listing_sink'
                try:
                    on_verified_page(page, capture, items)
                except Exception as exc:
                    # Do not treat a failed database commit as successful M2
                    # discovery, even if the browser has valid cards.
                    raise VerifiedPageSinkError('Verified listing could not be committed.') from exc
            stage = 'listing_checkpoint'
            report['pages'].append({
                'page': page, 'url': current_url, 'cards': len(items),
                'unique_ids_on_page': len(set(ids)), 'new_unique_jobs': len(new_ids),
                'sample_job_ids': ids[:10], 'pagination_status': evidence.status,
                'next_url_observed': evidence.next_url,
                'pagination_container_observed': evidence.pager_detected,
                'pagination_anchor_count': evidence.candidate_count,
                'heading_total_jobs': total_from_heading(title, task),
                'captured_at_utc': capture.captured_at.isoformat(),
            })
            report['jobs'] = list(seen.values())
            report['unique_job_count'] = len(seen)
            persist()
            if not new_ids:
                report['status'] = 'repeated_page_no_new_jobs'
                break
            if evidence.status == 'ambiguous_next_links':
                report['status'] = 'pagination_ambiguous'
                break
            if evidence.next_url is None:
                report['status'] = 'no_verified_next_link'
                # This only records potential coverage, NOT a guarantee.
                heading_total = total_from_heading(title, task)
                report['heading_total_matches_unique_count'] = (
                    heading_total == len(seen) if heading_total is not None else None)
                break
            next_url = evidence.next_url
        else:
            report['status'] = 'max_pages_reached'
    except AccessChallenge as exc:
        report['status'] = 'stopped_on_access_challenge'
        report['blocked_stage'] = stage
        report['errors'].append(type(exc).__name__)
    except (NavigationError, LayoutChanged, WebDriverException, TimeoutError, OSError, ValueError) as exc:
        report['status'] = 'stopped_on_navigation_or_parse_error'
        report['failed_stage'] = stage
        report['errors'].append(f'{type(exc).__name__}: {str(exc)[:250]}')
    finally:
        persist()
    return report
