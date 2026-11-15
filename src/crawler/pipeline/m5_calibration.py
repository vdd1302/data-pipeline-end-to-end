"""M5a safe route calibration & explicit operator promotion (no database)."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from urllib.parse import urlsplit

from src.crawler.pipeline.discovery import (
    CalibratedFirstPage, _fold, is_keyword_route, pagination_from_html, scope_verified, validate_page_url,
    verified_route_for,
)
from src.crawler.parser import AccessChallenge, LayoutChanged


class CalibrationError(ValueError):
    pass


def candidate_for(cfg, task, candidate_url=None):
    """Never create candidate URLs from slugs or concatenate unknown codes."""
    if candidate_url is not None:
        url = candidate_url
    else:
        candidates_path = cfg.config_path.parent / 'route_candidates.json'
        if not candidates_path.exists():
            candidates_path = cfg.config_path.parent / 'm5_route_candidates.json'
        data = json.loads(candidates_path.read_text(encoding='utf-8'))
        matches = [r for r in data.get('candidates', [])
                   if r.get('position_id') == task.position_id and r.get('location_id') == task.location_id]
        if len(matches) != 1:
            raise CalibrationError('No uniquely observed public URL candidate for this scope.')
        url = matches[0].get('url')
    if not isinstance(url, str) or not validate_page_url(url, url, page=1):
        raise CalibrationError('Candidate must be an exact public HTTPS TopCV results URL; only TopCV search flags '
                               'are allowed in its query, and no fragment.')
    city_code = {'ha_noi': 'l1', 'ho_chi_minh': 'l2'}.get(task.location_id)
    city_slug = {'ha_noi': 'ha-noi', 'ho_chi_minh': 'ho-chi-minh'}.get(task.location_id)
    path = urlsplit(url).path
    it_category_route = bool(city_code) and f'-{city_code}cr257' in path
    # Operator-supplied keyword + city route (what the site's own search produces).
    keyword_route = bool(city_code) and is_keyword_route(url) and path.endswith(f'-tai-{city_slug}-kl{city_code[1:]}')
    if not (it_category_route or keyword_route):
        raise CalibrationError('Candidate does not carry the expected old-administration city route code '
                               '(IT category -l<n>cr257, or keyword route -kl<n>).')
    return url


def evaluate_candidate(*, candidate_url, task, final_url, title, location_label, cards, page_html):
    """Independent browser proof; no hidden DOM search or URL normalization."""
    matches_route = validate_page_url(final_url, candidate_url, page=1)
    verified_scope = scope_verified(current_url=final_url, route=candidate_url, page=1,
                                    title=title, task=task, location_label=location_label)
    cities = [x.get('location_raw') or '' for x in cards]
    every_city = bool(cards) and all(_fold(task.search_label) in _fold(city) for city in cities)
    ids = [str(x.get('source_job_id') or '') for x in cards]
    stable_ids = bool(ids) and all(x.isdigit() for x in ids) and len(set(ids)) == len(ids)
    # Suppress URLs/full payloads in diagnostic; list only sanitized sample identifiers.
    pag = pagination_from_html(page_html, final_url, candidate_url, 1) if verified_scope else None
    passed = bool(matches_route and verified_scope and every_city and stable_ids)
    return {
        'status': 'live_verified' if passed else 'scope_or_listing_unverified',
        'expected_route': candidate_url, 'actual_route': final_url.split('?')[0],
        'route_exact': matches_route, 'scope_verified': verified_scope,
        'all_cards_city_verified': every_city, 'parsed_cards': len(cards),
        'unique_job_ids': len(set(ids)), 'sample_ids': ids[:6],
        'location_label': location_label, 'page_title': title,
        'pagination_status': pag.status if pag else None,
        'scope_city': task.search_label, 'scope_keyword': task.keyword,
        'database_connections': 0, 'database_writes': 0, 'detail_requests': 0,
    }


def _safe_readiness_observation(readiness: dict) -> dict:
    """Report bounded DOM-state evidence; never copy full HTML or form values.

    This is a diagnostic, not proof the page is empty or blocked. Values come
    from the read-only, fixed-selector Selenium listing probe.
    """
    last = readiness.get('last_observation') or {}
    if not isinstance(last, dict):
        return {}
    keys = (
        'document_ready_state', 'listing_container_present',
        'listing_cards_count', 'empty_marker_present', 'empty_marker_visible',
        'verified_structure_present',
    )
    result = {k: last.get(k) for k in keys if k in last}
    for key in ('relevant_visible_containers', 'main_top_sections'):
        entries = last.get(key) or []
        if not isinstance(entries, list):
            continue
        rows = []
        for entry in entries[:8]:
            if not isinstance(entry, dict):
                continue
            rows.append({k: str(entry[k])[:120] if isinstance(entry[k], str) else entry[k]
                         for k in ('tag', 'id', 'class', 'visible') if k in entry
                         and isinstance(entry[k], (str, bool))})
        result[key] = rows
    return result


def calibrate_browser(crawler, task, cfg, candidate_url, *, retain_capture=False):
    """One URL, one navigation, no retries on challenge; no DB transaction."""
    crawler.open_public_listing(candidate_url)
    readiness = crawler.wait_for_listing_ready(timeout_seconds=cfg.selenium.explicit_wait_seconds)
    if not readiness['ready']:
        result = {'status': 'results_not_ready', 'candidate_url': candidate_url,
                  'reason': readiness.get('reason'),
                  'readiness_observation': _safe_readiness_observation(readiness),
                  'database_writes': 0, 'detail_requests': 0}
        return (result, None) if retain_capture else result
    state = crawler.inspect_navigation_controls(limit=20, stage='search_results', keyword=task.keyword)
    title = state.get('page_title', '')
    location_label = (state.get('observed_state') or {}).get('location_label') or ''
    if not scope_verified(current_url=crawler._driver().current_url, route=candidate_url,
                          page=1, title=title, task=task, location_label=location_label):
        result = evaluate_candidate(candidate_url=candidate_url, task=task,
                final_url=crawler._driver().current_url, title=title, location_label=location_label,
                cards=[], page_html='')
        return (result, None) if retain_capture else result
    cap = crawler.fetch_page_capture()
    result = evaluate_candidate(candidate_url=candidate_url, task=task,
        final_url=cap.page_url, title=title, location_label=location_label,
        cards=cap.items, page_html=cap.page_html)
    if not retain_capture:
        return result
    evidence = (CalibratedFirstPage(candidate_url, title, location_label, cap)
                if result['status'] == 'live_verified' else None)
    return result, evidence


def promote_route(cfg, task, report):
    """Operator opt-in, atomic local registry write; refuses conflicting routes."""
    if report.get('status') != 'live_verified' or not all((
        report.get('route_exact') is True, report.get('scope_verified') is True,
        report.get('all_cards_city_verified') is True, report.get('parsed_cards', 0) > 0,
    )):
        raise CalibrationError('Only an accepted live browser calibration can be promoted.')
    candidate_url = candidate_for(cfg, task, report['expected_route'])
    existing_route = verified_route_for(task.position_id, task.location_id, config_path=cfg.config_path)
    if existing_route and existing_route != candidate_url:
        raise CalibrationError('An incompatible route is already verified. Do not overwrite it.')
    registry = cfg.config_path.parent / 'verified_public_routes.json'
    if not registry.exists():
        registry = cfg.config_path.parent / 'm5_verified_public_routes.json'
    if registry.exists():
        data = json.loads(registry.read_text(encoding='utf-8'))
        if data.get('version') != 1 or not isinstance(data.get('routes'), list):
            raise CalibrationError('Existing registry invalid; not overwriting.')
    else:
        data = {'version': 1, 'routes': []}
    key = (task.position_id, task.location_id)
    entries = [r for r in data['routes']
               if (r.get('position_id'), r.get('location_id')) != key]
    entries.append({
        'position_id': key[0], 'location_id': key[1], 'route': candidate_url,
        'status': 'live_verified',
        'evidence': {
            'scope_verified': True, 'all_cards_city_verified': True,
            'parsed_cards': report['parsed_cards'],
            'audited_at_utc': report['audited_at_utc'],
            'job_id_sample_sha256': hashlib.sha256(','.join(report['sample_ids']).encode()).hexdigest(),
        },
    })
    data['routes'] = entries
    tmp = registry.with_suffix('.json.tmp')
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding='utf-8')
    tmp.replace(registry)
    return registry


def with_timestamp(report):
    return {'version': 1, 'mode': 'm5a_public_route_browser_calibration',
            'audited_at_utc': datetime.now(timezone.utc).isoformat(), **report}
