"""M5b: one-session browser calibration -> verified discovery, no database.

Only use captured browser evidence while the SAME crawler/session is still
open. A blocked or redirected page cannot produce a successful checkpoint.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from selenium.common.exceptions import WebDriverException

from src.crawler.parser import AccessChallenge, LayoutChanged
from src.crawler.topcv import NavigationError
from src.crawler.pipeline.discovery import _atomic_json, run_discovery
from src.crawler.pipeline.m5_calibration import (
    calibrate_browser, promote_route, with_timestamp,
)


def run_m5b(crawler, task, cfg, candidate_url: str, *, max_pages: int,
            report_path: Path, discovery_path: Path, promote: bool = False,
            sleep_fn=None) -> dict:
    """Audit one public page, then reuse the exact validated capture in M2.

    Does not invoke detail fetching, RawIngestion or database connectors.
    The M2-format discovery checkpoint is written only after M5a calibration
    succeeds; a challenge or invalid route does not replace existing M2 data.
    """
    report = {
        'version': 1, 'mode': 'M5b_unified_calibration_discovery_no_db',
        'started_at_utc': datetime.now(timezone.utc).isoformat(),
        'task': {'position_id': task.position_id, 'location_id': task.location_id,
                 'keyword': task.keyword, 'city': task.search_label},
        'status': 'started', 'calibration': None,
        'discovery_status': None, 'discovery_path': None,
        'first_page_reused': False, 'promoted': False,
        'database_connections': 0, 'database_writes': 0, 'detail_requests': 0,
        'coverage_confirmed': False,
    }
    report_path = Path(report_path)
    discovery_path = Path(discovery_path)
    try:
        calibration, first_page = calibrate_browser(
            crawler, task, cfg, candidate_url, retain_capture=True,
        )
        report['calibration'] = with_timestamp(calibration)
        if calibration.get('status') != 'live_verified' or first_page is None:
            report['status'] = 'calibration_not_verified'
            return report

        # A local operator has explicitly asked to retain this calibrated
        # route. Promotion is never a prerequisite for immediate M5b discovery.
        if promote:
            registry = promote_route(cfg, task, report['calibration'])
            report['promoted'] = True
            report['registry_path'] = str(registry)

        # IMPORTANT: pass the live first-page evidence into M2, not a new URL.
        kwargs = {'sleep_fn': sleep_fn} if sleep_fn is not None else {}
        discovery = run_discovery(
            crawler, task, cfg, max_pages=max_pages,
            report_path=discovery_path, first_page_evidence=first_page,
            **kwargs,
        )
        report['discovery_status'] = discovery['status']
        report['discovery_path'] = str(discovery_path)
        report['first_page_reused'] = discovery['first_page_reused_from_calibration']
        report['pages_captured'] = len(discovery['pages'])
        report['unique_job_ids'] = discovery['unique_job_count']
        # No "complete" claim: absent pagination does not prove coverage.
        report['status'] = ('discovery_captured'
                            if discovery['pages'] and discovery['status'] in (
                                'no_verified_next_link', 'max_pages_reached')
                            else 'discovery_incomplete_or_blocked')
        return report
    except AccessChallenge:
        report['status'] = 'stopped_on_access_challenge'
        report['error_type'] = 'AccessChallenge'
        return report
    except (LayoutChanged, NavigationError, WebDriverException, TimeoutError, ValueError, OSError) as exc:
        report['status'] = 'stopped_on_navigation_or_parse_error'
        report['error_type'] = type(exc).__name__
        return report
    finally:
        report['updated_at_utc'] = datetime.now(timezone.utc).isoformat()
        _atomic_json(report_path, report)
