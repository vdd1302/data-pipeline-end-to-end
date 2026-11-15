"""T1 shared TopCV listing/detail -> Raw business payload boundary.

Pure functions: no Selenium, HTTP, PostgreSQL, state mutation or write calls.
The caller must separately prove route/scope, capture provenance, access
permission and any DB transaction.  A successful adaptation does not make an
old saved HTML document a new or temporally eligible market observation.
"""
from __future__ import annotations

from dataclasses import dataclass
import unicodedata
from urllib.parse import urlsplit
from collections.abc import Mapping

from src.crawler.parser import LayoutChanged, normalize_job_link, to_raw_job_payload
from src.raw_storage.contracts import RawJobPayload, compute_content_hash

_REQUIRED = ('job_title_raw', 'company_name_raw', 'location_raw',
             'job_description_raw', 'job_requirements_raw')


def _fold(value: str) -> str:
    norm = unicodedata.normalize('NFD', str(value)).casefold().replace('đ', 'd')
    return ''.join(ch for ch in norm if unicodedata.category(ch) != 'Mn')


@dataclass(frozen=True)
class T1RawDetail:
    source_job_id: str
    job_url: str
    payload: RawJobPayload
    content_hash: str
    parser_version: str
    record_level: str = 'detail_complete'
    missing_fields: tuple[str, ...] = ()


def adapt_topcv_detail(*, source_job_id: str, expected_job_url: str,
                       captured_job_url: str, parsed_detail: Mapping,
                       listing: Mapping, expected_city: str | None = None,
                       allow_partial: bool = False) -> T1RawDetail:
    """Reject mismatched identity, city or incomplete required fields BEFORE Raw.

    Identity URL must be the exact canonical URL from verified discovery; a
    redirect to another public job path (even same ID) requires review instead
    of silently attaching an unrelated HTML document.
    """
    if (not isinstance(source_job_id, str) or not source_job_id.isascii()
            or not source_job_id.isdecimal()):
        raise LayoutChanged('T1: invalid ASCII TopCV source job ID')
    if not isinstance(parsed_detail, Mapping) or not isinstance(listing, Mapping):
        raise LayoutChanged('T1: expected parsed listing and detail mappings')
    expected = normalize_job_link(expected_job_url, 'https://www.topcv.vn')
    captured = normalize_job_link(captured_job_url, 'https://www.topcv.vn')
    if (expected is None or captured is None or expected[1] != source_job_id
            or expected != captured or urlsplit(expected_job_url).scheme != 'https'
            or expected_job_url != expected[0] or captured_job_url != expected_job_url):
        raise LayoutChanged('T1: detail URL does not match verified discovery URL and ID')
    parsed_url = parsed_detail.get('job_url')
    if (parsed_detail.get('source_job_id') != source_job_id
            or (parsed_url is not None and parsed_url != expected_job_url)):
        raise LayoutChanged('T1: parsed detail identity differs from discovery')
    if str(listing.get('source_job_id')) != source_job_id:
        raise LayoutChanged('T1: listing identity differs from detail')
    if expected_city is not None:
        if (not isinstance(expected_city, str) or not expected_city.strip()
                or _fold(expected_city) not in _fold(listing.get('location_raw') or '')):
            raise LayoutChanged('T1: listing location disagrees with requested city')
    if type(allow_partial) is not bool:
        raise ValueError('allow_partial must be boolean')
    payload = (to_raw_job_payload(parsed_detail, listing, allow_partial=True) if allow_partial
               else to_raw_job_payload(parsed_detail, listing))
    if not isinstance(payload, RawJobPayload):
        raise LayoutChanged('T1: adapter did not create RawJobPayload')
    data = payload.to_dict()
    missing = tuple(key for key in _REQUIRED
                    if not isinstance(data.get(key), str) or not data[key].strip())
    if (not allow_partial and missing) or 'job_title_raw' in missing:
        raise LayoutChanged('T1: required Raw business field missing')
    parser_version = parsed_detail.get('parser_version')
    if not isinstance(parser_version, str) or not parser_version.strip() or len(parser_version) > 100:
        raise LayoutChanged('T1: missing TopCV detail parser provenance')
    return T1RawDetail(source_job_id, expected_job_url, payload,
                       compute_content_hash(data), parser_version,
                       'detail_partial' if missing else 'detail_complete', missing)
