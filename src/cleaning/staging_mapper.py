"""M4.3.7 – Map one joined TopCV raw observation into staging columns.

Pure transformation: no PostgreSQL connection, no I/O, no state.
The Repository must provide a JOIN of raw.job_observations and raw.crawl_runs.
It remains responsible for incremental selection, batched UPSERT, and cleaned_at.
"""
from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import replace
from datetime import date, datetime
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

from src.cleaning.salary_cleaner import normalize_salary
from src.cleaning.experience_cleaner import normalize_experience
from src.cleaning.position_cleaner import normalize_position
from src.cleaning.company_cleaner import normalize_company
from src.cleaning.location_cleaner import normalize_location
from src.cleaning.skill_cleaner import normalize_skills
from src.cleaning.date_cleaner import normalize_dates
from src.cleaning.data_quality import assess_cleaning_quality

CLEANING_VERSION = "topcv-cleaning-v1"

# EXACT staging.job_observations_clean field names; `cleaned_at` is DB managed.
MAPPER_COLUMNS = (
    "raw_job_id", "crawl_run_id", "source_job_id", "snapshot_date",
    "observation_method", "is_temporal_eligible", "job_title_clean",
    "position_name", "role_family", "company_name_clean", "company_size_clean",
    "company_industry_clean", "city_clean", "district_clean",
    "location_detail_clean", "salary_min_vnd", "salary_max_vnd",
    "salary_currency", "salary_period", "salary_negotiable_flag",
    "experience_min_years", "experience_max_years", "experience_bucket",
    "posted_date", "deadline_date", "skills_clean", "job_level_clean",
    "job_description_clean", "job_requirements_clean", "benefits_clean",
    "cleaning_status", "quality_flags", "posting_natural_key",
    "canonical_job_url", "content_hash", "scraped_at",
    "position_classification_version", "company_natural_key",
    "company_url_clean", "locations_clean", "salary_midpoint_vnd",
    "salary_disclosed_flag", "skill_normalization_version",
    "skill_evidence_clean", "cleaning_version",
)
DB_MANAGED_COLUMNS = ("cleaned_at",)
STAGING_COLUMNS = MAPPER_COLUMNS + DB_MANAGED_COLUMNS


class StagingMappingError(ValueError):
    """Bad record metadata or inconsistent JOIN; caller logs and handles row."""


def _safe_raw_str(payload: Mapping[str, Any], key: str, flags: list[str]) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        flags.append(f"{key}_invalid_type")
        return None
    return value


def _clean_text(value: str | None) -> str | None:
    """Trim/normalize without deleting interior paragraph boundaries."""
    if value is None:
        return None
    if re.search(r'</?(?:p|div|br|ul|ol|li|strong|b|em|span|script|style)\b', value, re.I):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(value, 'html.parser')
        for node in soup.select('script,style'):
            node.decompose()
        value = soup.get_text('\n', strip=True)
    text = unicodedata.normalize("NFKC", html.unescape(value)).replace("\r\n", "\n")
    text = re.sub(r"[ \t\u00a0]+", " ", text).strip()
    return text or None


def _canonical_job_url(value: Any) -> tuple[str | None, list[str]]:
    if value is None:
        return None, ["job_url_missing"]
    if not isinstance(value, str) or not value.strip():
        return None, ["job_url_invalid"]
    try:
        url = urlsplit(value.strip())
        if (url.scheme.lower() not in {"http", "https"}
                or (url.hostname or "").lower() not in {"topcv.vn", "www.topcv.vn"}
                or url.username or url.password
                or url.port not in {None, 80, 443}
                or not url.path.strip("/")):
            return None, ["job_url_invalid"]
        path = url.path.rstrip("/")
        return urlunsplit(("https", "www.topcv.vn", path, "", "")), []
    except ValueError:
        return None, ["job_url_invalid"]


def map_raw_observation(row: Mapping[str, Any], *, cleaning_version: str = CLEANING_VERSION) -> dict[str, Any]:
    """Map one joined raw observation into 45 DB-insertable staging fields.

    Mandatory JOIN columns: raw_job_id, crawl_run_id, source_job_id, job_url,
    snapshot_date, scraped_at, content_hash, fetch_status, raw_payload,
    source_code, crawler_version, run_status, run_snapshot_date.
    `run_status` is intentionally NOT used to infer temporal eligibility:
    partial crawl runs may contain valid individual observations.
    """
    if not isinstance(row, Mapping):
        raise StagingMappingError("joined raw observation must be a mapping")
    if not isinstance(cleaning_version, str) or not cleaning_version.strip():
        raise StagingMappingError("cleaning_version must be a nonempty string")
    required = (
        "raw_job_id", "crawl_run_id", "source_job_id", "job_url",
        "snapshot_date", "scraped_at", "content_hash", "fetch_status",
        "raw_payload", "source_code", "crawler_version", "run_status",
        "run_snapshot_date",
    )
    absent = [key for key in required if key not in row]
    if absent:
        raise StagingMappingError(f"missing joined raw columns: {', '.join(absent)}")

    raw_job_id = row["raw_job_id"]
    if type(raw_job_id) is not int or raw_job_id <= 0:
        raise StagingMappingError("raw_job_id must be a positive integer")
    crawl_run_id = row["crawl_run_id"]
    if not isinstance(crawl_run_id, UUID):
        raise StagingMappingError("crawl_run_id must be uuid.UUID (psycopg default)")
    source_job_id = row["source_job_id"]
    if not isinstance(source_job_id, str) or not source_job_id.strip():
        raise StagingMappingError("source_job_id missing/invalid")
    source_job_id = source_job_id.strip()
    source_code = row["source_code"]
    if not isinstance(source_code, str) or source_code.strip().casefold() != "topcv":
        raise StagingMappingError("source_code must be 'topcv' for this mapper")
    snapshot_date = row["snapshot_date"]
    if type(snapshot_date) is not date:
        raise StagingMappingError("snapshot_date must be datetime.date")
    if row["run_snapshot_date"] != snapshot_date:
        raise StagingMappingError("JOIN observation/run snapshot dates disagree")
    if not isinstance(row["raw_payload"], Mapping):
        raise StagingMappingError("raw_payload must be a JSON object (Python mapping)")

    payload = row["raw_payload"]
    flags: list[str] = []
    salary = normalize_salary(_safe_raw_str(payload, "salary_raw", flags))
    experience = normalize_experience(_safe_raw_str(payload, "experience_raw", flags))
    position = normalize_position(_safe_raw_str(payload, "job_title_raw", flags))
    company = normalize_company(
        _safe_raw_str(payload, "company_name_raw", flags),
        _safe_raw_str(payload, "company_url_raw", flags),
        _safe_raw_str(payload, "company_size_raw", flags),
        _safe_raw_str(payload, "company_industry_raw", flags),
    )
    loc_raw = payload.get("location_raw")
    if not (loc_raw is None or isinstance(loc_raw, str)
            or (isinstance(loc_raw, list) and all(isinstance(s, str) for s in loc_raw))):
        flags.append("location_raw_invalid_type")
        loc_raw = None
    location = normalize_location(loc_raw)

    requirements_raw = _safe_raw_str(payload, "job_requirements_raw", flags)
    tags = payload.get("skill_tags_raw")
    if tags is not None and not (isinstance(tags, list) and all(isinstance(t, str) for t in tags)):
        flags.append("skill_tags_raw_invalid_type")
        tags = None
    skill = normalize_skills(tags, _clean_text(requirements_raw))

    explicit_live = (
        row.get("run_purpose") == "production"
        and row.get("observation_method") == "selenium_live"
        and row.get("capture_source") == "selenium_live"
        and row.get("raw_temporal_eligible") is True
        and row.get("source_captured_at") == row["scraped_at"]
    )
    date_info = normalize_dates(
        _safe_raw_str(payload, "posted_date_raw", flags),
        _safe_raw_str(payload, "deadline_raw", flags),
        snapshot_date=snapshot_date,
        scraped_at=row["scraped_at"],
        crawler_version="topcv-selenium-explicit-provenance" if explicit_live else row["crawler_version"],
        fetch_status=row["fetch_status"],
    )
    # Explicit database provenance takes precedence over a version-name heuristic.
    if "capture_source" in row:
        live = explicit_live
        method = ("manual_html" if row.get("capture_source") == "historical_saved_html"
                  else row.get("observation_method") or "unknown")
        date_info = replace(date_info, observation_method=method,
                            is_temporal_eligible=date_info.is_temporal_eligible and live)
        if not live:
            flags.append("explicit_provenance_not_live_production")
    canonical_url, url_flags = _canonical_job_url(row["job_url"])
    flags.extend(url_flags)

    cleaned = {
        "raw_job_id": raw_job_id,
        "crawl_run_id": crawl_run_id,
        "source_job_id": source_job_id,
        "snapshot_date": snapshot_date,
        "observation_method": date_info.observation_method,
        "is_temporal_eligible": date_info.is_temporal_eligible,
        "job_title_clean": position.job_title_clean,
        "position_name": position.position_name,
        "role_family": position.role_family,
        "company_name_clean": company.company_name_clean,
        "company_size_clean": company.company_size_clean,
        "company_industry_clean": company.company_industry_clean,
        "city_clean": location.city_clean,
        "district_clean": location.district_clean,
        "location_detail_clean": location.location_detail_clean,
        "salary_min_vnd": salary.salary_min_vnd,
        "salary_max_vnd": salary.salary_max_vnd,
        "salary_currency": salary.salary_currency,
        "salary_period": salary.salary_period,
        "salary_negotiable_flag": salary.salary_negotiable_flag,
        "experience_min_years": experience.experience_min_years,
        "experience_max_years": experience.experience_max_years,
        "experience_bucket": experience.experience_bucket,
        "posted_date": date_info.posted_date,
        "deadline_date": date_info.deadline_date,
        "skills_clean": skill.skills_clean,
        "job_level_clean": _clean_text(_safe_raw_str(payload, "job_level_raw", flags)),
        "job_description_clean": _clean_text(_safe_raw_str(payload, "job_description_raw", flags)),
        "job_requirements_clean": _clean_text(requirements_raw),
        "benefits_clean": _clean_text(_safe_raw_str(payload, "benefits_raw", flags)),
        "posting_natural_key": f"topcv:{source_job_id}",
        "canonical_job_url": canonical_url,
        "content_hash": row["content_hash"],
        "scraped_at": row["scraped_at"],
        "position_classification_version": position.position_classification_version,
        "company_natural_key": company.company_natural_key,
        "company_url_clean": company.company_url_clean,
        "locations_clean": location.locations_clean,
        "salary_midpoint_vnd": salary.salary_midpoint_vnd,
        "salary_disclosed_flag": salary.salary_disclosed_flag,
        "skill_normalization_version": skill.skill_normalization_version,
        "skill_evidence_clean": skill.skill_evidence_clean,
        "cleaning_version": cleaning_version.strip(),
    }
    inherited_flags = flags + [
        flag for result in (
            salary, experience, position, company, location, skill, date_info
        ) for flag in result.quality_flags
    ]
    quality = assess_cleaning_quality(cleaned, inherited_flags)
    cleaned["cleaning_status"] = quality.cleaning_status
    cleaned["quality_flags"] = quality.quality_flags
    if set(cleaned) != set(MAPPER_COLUMNS):
        raise AssertionError("mapper and staging contract no longer agree")
    return cleaned
