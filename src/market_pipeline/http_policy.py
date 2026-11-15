"""HTTP status and Retry-After handling, independent of browser transport."""
from datetime import datetime, timedelta, timezone
from collections.abc import Mapping
import re
from email.utils import parsedate_to_datetime
from src.crawler.parser import AccessChallenge


def access_error(status, headers, *, now=None):
    headers = headers if isinstance(headers, Mapping) else {}
    normalized = {str(key).casefold(): str(value).strip() for key, value in headers.items()}
    if type(status) in (int, float) and 100 <= status <= 599 and status == int(status):
        status = int(status)
    is_challenge = normalized.get('cf-mitigated', '').casefold() == 'challenge'
    if status not in (403, 429) and not is_challenge:
        return None
    now = now or datetime.now(timezone.utc)
    retry_at = None
    value = normalized.get('retry-after', '')
    try:
        if value.isascii() and value.isdigit():
            retry_at = now + timedelta(seconds=min(int(value), 31536000))
        elif value:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = None
    except (ValueError, TypeError, OverflowError):
        retry_at = None
    # A Cloudflare server header identifies the server, not why access failed.
    # Explicit rate limiting always takes priority over a challenge marker.
    kind = 'rate_limited' if status == 429 else ('challenge' if is_challenge else 'access_denied')
    error = AccessChallenge(kind=kind, retry_after=retry_at)
    error.http_status = status
    error.reason_code = 'http_rate_limited' if status == 429 else ('http_challenge' if is_challenge else 'http_access_denied')
    evidence = {'response_class': 'HOST_RATE_LIMIT' if status == 429 else 'HOST_ACCESS_DENIED'}
    if type(status) is int:
        evidence['http_status'] = status
    if is_challenge:
        evidence['cf_mitigated'] = 'challenge'
    ray = normalized.get('cf-ray', '')
    if re.fullmatch(r'[A-Za-z0-9-]{1,80}', ray):
        evidence['cf_ray'] = ray
    error.safe_response = evidence
    return error
