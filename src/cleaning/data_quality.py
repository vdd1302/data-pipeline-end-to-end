"""M4.3.6 – Row-level staging data quality policy V1.

This module evaluates the *completeness of an already normalized row*, not
whether a whole crawl batch is complete or safe for market-trend metrics.
Time-series eligibility comes from date_cleaner and must be applied separately.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping, Any


@dataclass(frozen=True)
class DataQualityResult:
    cleaning_status: str = "failed"
    quality_flags: list[str] = field(default_factory=list)


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _locations_valid(value: Any) -> bool:
    return (
        isinstance(value, list)
        and bool(value)
        and all(
            isinstance(place, dict)
            and any(_has_text(place.get(key)) for key in ("city", "location_name"))
            for place in value
        )
    )


_DATA_WARNINGS = {
    "posted_date_invalid_calendar_date",
    "posted_date_unrecognized",
    "posted_date_after_snapshot",
    "deadline_date_invalid_calendar_date",
    "deadline_date_unrecognized",
    "deadline_before_posted_date",
}


def assess_cleaning_quality(
    cleaned: Mapping[str, Any],
    inherited_flags: Iterable[str] | None = None,
) -> DataQualityResult:
    """Return 'success', 'partial', or 'failed' for a staging candidate.

    `cleaned` carries schema field names, e.g. source_job_id,
    job_title_clean, company_name_clean, locations_clean,
    job_description_clean, job_requirements_clean.

    Critical missing identity/title -> failed; incomplete secondary fields
    or untrusted posting/deadline calendar data -> partial. Optional posted
    date / skills / negotiable salary missing are NOT considered a failure.
    Provenance flags like 'manual_html_observation_time_unverified' do NOT
    downgrade otherwise valid cleaning – use is_temporal_eligible separately.
    """
    if not isinstance(cleaned, Mapping):
        raise TypeError("cleaned must be a Mapping")
    if inherited_flags is None:
        flags: list[str] = []
    else:
        flags = []
        for value in inherited_flags:
            if not isinstance(value, str):
                raise TypeError("inherited_flags must contain strings only")
            if value not in flags:
                flags.append(value)

    critical_missing = False
    partial = False

    if not _has_text(cleaned.get("source_job_id")):
        flags.append("source_job_id_missing")
        critical_missing = True
    if not _has_text(cleaned.get("job_title_clean")):
        flags.append("job_title_missing")
        critical_missing = True

    if not _has_text(cleaned.get("company_name_clean")):
        flags.append("company_name_missing_for_dw")
        partial = True
    if not _locations_valid(cleaned.get("locations_clean")):
        flags.append("locations_missing_or_invalid_for_dw")
        partial = True
    if not _has_text(cleaned.get("job_description_clean")):
        flags.append("job_description_missing")
        partial = True
    if not _has_text(cleaned.get("job_requirements_clean")):
        flags.append("job_requirements_missing")
        partial = True
    if any(flag in flags for flag in _DATA_WARNINGS):
        partial = True

    status = "failed" if critical_missing else "partial" if partial else "success"
    return DataQualityResult(status, list(dict.fromkeys(flags)))
