"""TopCV company normalization V1 (no network access).

Prefer the stable TopCV company id in a valid company URL when available.
Name-based identity is only a fallback and carries a quality flag.
Do not strip legal name terms (TNHH, CP, ...), or conflate companies
with matching names but different TopCV ids.
"""
from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit


_COMPANY_PATH = re.compile(r"^/cong-ty/[^/]+/(\d+)\.html/?$", re.IGNORECASE)


@dataclass(frozen=True)
class CompanyResult:
    company_name_clean: str | None = None
    company_natural_key: str | None = None
    company_url_clean: str | None = None
    company_size_clean: str | None = None
    company_industry_clean: str | None = None
    quality_flags: list[str] = field(default_factory=list)


def _tidy_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", value)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _company_url(raw_url: str | None) -> tuple[str | None, str | None, bool]:
    """Return (canonical_url, topcv_company_id, invalid_url)."""
    candidate = _tidy_text(raw_url)
    if candidate is None:
        return None, None, False
    try:
        parts = urlsplit(candidate)
        hostname = (parts.hostname or "").lower()
        if parts.scheme.lower() != "https" or hostname not in {
            "topcv.vn", "www.topcv.vn"
        } or parts.port not in (None, 443) or parts.username or parts.password:
            return None, None, True
        match = _COMPANY_PATH.fullmatch(parts.path)
        if not match:
            return None, None, True
        company_id = match.group(1)
        path = parts.path.rstrip("/")
        return urlunsplit(("https", "www.topcv.vn", path, "", "")), company_id, False
    except ValueError:
        return None, None, True


def normalize_company(
    company_name_raw: str | None,
    company_url_raw: str | None = None,
    company_size_raw: str | None = None,
    company_industry_raw: str | None = None,
) -> CompanyResult:
    """Normalize display attributes and provide a stable, traceable natural key."""
    name = _tidy_text(company_name_raw)
    size = _tidy_text(company_size_raw)
    industry = _tidy_text(company_industry_raw)
    url, company_id, bad_url = _company_url(company_url_raw)
    flags: list[str] = []
    if name is None:
        flags.append("company_name_missing")
    if bad_url:
        flags.append("company_url_invalid")

    if company_id:
        natural_key = f"topcv_company:{company_id}"
    elif name:
        # Hash for a compact deterministic key; source is still the raw name.
        key_text = unicodedata.normalize("NFKC", name).casefold()
        digest = hashlib.sha256(key_text.encode("utf-8")).hexdigest()
        natural_key = f"topcv_name_sha256:{digest}"
        flags.append("company_identity_name_fallback")
    else:
        natural_key = None
        flags.append("company_identity_unavailable")

    return CompanyResult(
        company_name_clean=name,
        company_natural_key=natural_key,
        company_url_clean=url,
        company_size_clean=size,
        company_industry_clean=industry,
        quality_flags=flags,
    )
