"""M4.3.6 – TopCV Date / observation-time normalization V1.

Scope: deterministic absolute date parsing + observation provenance assessment.
No network, no database writes, no guesses about relative dates or HTML save time.

For this project, snapshot_date is the crawler's *Vietnam local calendar date*.
The local time comparison uses a fixed UTC+07:00 offset because the current
project scope is Vietnam. This avoids Windows zoneinfo/tzdata dependencies.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

VIETNAM_TZ = timezone(timedelta(hours=7))


@dataclass(frozen=True)
class DateResult:
    posted_date: date | None = None
    deadline_date: date | None = None
    observation_method: str = "unknown"
    is_temporal_eligible: bool = False
    quality_flags: list[str] = field(default_factory=list)


_PREFIX = re.compile(
    r"^(?:ngày\s+đăng|đăng\s+ngày|hạn\s+ứng\s+tuyển|hạn\s+nộp\s+hồ\s+sơ|"
    r"hạn\s+nộp|posted(?:\s+on)?|deadline|application\s+deadline|expires(?:\s+on)?)"
    r"\s*[:：]\s*",
    flags=re.IGNORECASE,
)
_DATE_SHAPE = re.compile(r"^(?:\d{4}-\d{1,2}-\d{1,2}|\d{1,2}[/.-]\d{1,2}[/.-]\d{4})$")
_RELATIVE_CUES = re.compile(
    r"\b(?:h[oô]m\s+nay|h[oô]m\s+qua|ng[aà]y\s+tr[uư][oớ]c|"
    r"c[oò]n\s+\d+\s+ng[aà]y|today|yesterday|tomorrow|"
    r"\d+\s+(?:days?|weeks?|months?)\s+ago|"
    r"\d+\s+ng[aà]y\s+tr[uư][oớ]c)\b",
    flags=re.IGNORECASE,
)


def _parse_absolute(raw: str | None, label: str) -> tuple[date | None, list[str]]:
    """Accept explicit calendar dates only, TopCV dd/mm/yyyy or ISO yyyy-mm-dd.

    Prefixes like 'Hạn ứng tuyển: ' are allowed. Do not infer dates from
    '3 ngày trước', 'Hôm nay', or 'còn 2 ngày': those depend on capture time.
    """
    if raw is None:
        return None, []
    if not isinstance(raw, str):
        return None, [f"{label}_invalid_type"]

    text = unicodedata.normalize("NFKC", raw).strip()
    if not text:
        return None, []
    text = _PREFIX.sub("", text).strip()
    if not _DATE_SHAPE.fullmatch(text):
        # Relative and other unsupported descriptions both remain NULL; keep
        # separate flags to support future rule development.
        if _RELATIVE_CUES.search(text):
            return None, [f"{label}_relative_unresolved"]
        return None, [f"{label}_unrecognized"]

    parts: list[int]
    if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", text):
        year, month, day = [int(v) for v in text.split("-")]
    else:
        day, month, year = [int(v) for v in re.split(r"[/.-]", text)]
    try:
        return date(year, month, day), []
    except ValueError:
        return None, [f"{label}_invalid_calendar_date"]


def _observation_method(crawler_version: str | None) -> str:
    if not isinstance(crawler_version, str):
        return "unknown"
    version = crawler_version.strip().casefold()
    if version.startswith("m3-manual-html-import") or version.startswith("topcv-saved-html"):
        return "manual_html"
    if (version.startswith("m3-single-ingestion") or
            version.startswith("m3-batch") or
            version.startswith("topcv-selenium")):
        return "selenium_live"
    return "unknown"


def normalize_dates(
    posted_date_raw: str | None,
    deadline_raw: str | None,
    *,
    snapshot_date: date | None,
    scraped_at: datetime | None,
    crawler_version: str | None,
    fetch_status: str | None,
) -> DateResult:
    """Normalize posting dates and classify individual observation provenance.

    is_temporal_eligible=True only means one *live* observed job has credible
    local observation time. It does NOT mean a crawl_run was complete enough
    for whole-market trends, absence detection, or is_continuing/reappeared.
    """
    posted, flags = _parse_absolute(posted_date_raw, "posted_date")
    deadline, deadline_flags = _parse_absolute(deadline_raw, "deadline_date")
    flags.extend(deadline_flags)

    valid_snapshot = type(snapshot_date) is date
    if not valid_snapshot:
        flags.append("snapshot_date_missing_or_invalid")
    if valid_snapshot and posted is not None and posted > snapshot_date:
        flags.append("posted_date_after_snapshot")
    if posted is not None and deadline is not None and deadline < posted:
        flags.append("deadline_before_posted_date")

    method = _observation_method(crawler_version)
    if method == "manual_html":
        flags.append("manual_html_observation_time_unverified")
    elif method == "unknown":
        flags.append("observation_method_unknown")

    valid_scrape_time = False
    if scraped_at is None:
        flags.append("scraped_at_missing")
    elif not isinstance(scraped_at, datetime):
        flags.append("scraped_at_invalid_type")
    elif scraped_at.tzinfo is None or scraped_at.utcoffset() is None:
        flags.append("scraped_at_timezone_missing")
    else:
        valid_scrape_time = True
        if (valid_snapshot and
                scraped_at.astimezone(VIETNAM_TZ).date() != snapshot_date):
            flags.append("snapshot_scraped_at_mismatch")
            valid_scrape_time = False

    if fetch_status != "success":
        flags.append("fetch_status_not_success")

    temporal_ok = (
        method == "selenium_live"
        and valid_snapshot
        and valid_scrape_time
        and fetch_status == "success"
    )
    return DateResult(
        posted_date=posted,
        deadline_date=deadline,
        observation_method=method,
        is_temporal_eligible=temporal_ok,
        quality_flags=list(dict.fromkeys(flags)),
    )
