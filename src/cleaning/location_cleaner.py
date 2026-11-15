"""Conservative TopCV location normalization V1 (offline, deterministic).

Raw field: location_raw (job WORK location, never company registered address).
Staging mapping:
  city_clean, district_clean, location_detail_clean, locations_clean (JSONB),
  quality_flags (merged by the orchestration layer).

Each element of locations_clean maps to dim_location(city, district, location_name).
Multiple elements feed bridge_snapshot_location, not extra fact rows.
Do not interpret legacy district labels marked 'cũ' as current districts.
"""
from __future__ import annotations

import html
import re
import unicodedata
from dataclasses import dataclass, field


@dataclass(frozen=True)
class LocationResult:
    city_clean: str | None = None
    district_clean: str | None = None
    location_detail_clean: str | None = None
    locations_clean: list[dict[str, str | None]] = field(default_factory=list)
    quality_flags: list[str] = field(default_factory=list)


def _clean(value: str) -> str:
    value = unicodedata.normalize("NFKC", html.unescape(value))
    return re.sub(r"\s+", " ", value).strip()


def _fold(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return value.casefold().replace("đ", "d")


# Deliberately limited to the project's two target cities.
_HANOI = re.compile(r"\b(?:ha\s*noi)\b", re.I)
_HCM = re.compile(r"\b(?:ho\s*chi\s*minh|hcm|sai\s*gon)\b", re.I)

# Use the raw string for stripping the city prefix, keeping original accents.
_PREFIX = re.compile(
    r"^\s*(?:(?:thành\s+phố|tp)\.?\s*)?"
    r"(?:hà\s+nội|ha\s+noi|hồ\s+chí\s+minh|ho\s+chi\s+minh|hcm|sài\s+gòn)"
    r"\s*[:\-–—]\s*",
    re.I,
)

_CITY_ONLY = re.compile(
    r"^(?:(?:thành\s+phố|tp)\.?\s*)?"
    r"(?:hà\s+nội|ha\s+noi|hồ\s+chí\s+minh|ho\s+chi\s+minh|hcm|sài\s+gòn)$",
    re.I,
)

_DISTRICT = re.compile(
    r"(?:^|[,;\s(])(?:quận|huyện|thị\s+xã)\s+"
    r"(?P<name>[^,;()]+)",
    re.I,
)

_NON_GEOGRAPHIC = {
    "remote", "work from home", "wfh", "toan quoc", "lam viec tu xa",
    "khong co dinh", "linh hoat", "online",
}


def _is_explicit_city(text: str) -> bool:
    # Full city-only entry or a city-prefixed specific address.
    return bool(_CITY_ONLY.fullmatch(text.strip()) or _PREFIX.match(text))


def _split_location_entries(raw: str) -> list[str]:
    """Split only when each part independently identifies a city.

    Do NOT split normal address commas (street / ward / district), or newlines
    that merely continue the same address.
    """
    # Semicolon/newline/pipe; split only when every part is clearly city-scoped.
    candidates = re.split(r"\s*[;|\n]+\s*", raw)
    if len(candidates) > 1 and all(_is_explicit_city(_clean(p)) for p in candidates):
        return [_clean(p) for p in candidates]

    # Simple city lists, e.g. "Hà Nội, TP.HCM" / "Hà Nội và TP.HCM".
    for delimiter in (r"\s*,\s*", r"\s+và\s+", r"\s*/\s*"):
        candidates = re.split(delimiter, raw, flags=re.I)
        if len(candidates) > 1 and all(
            _CITY_ONLY.fullmatch(_clean(p)) for p in candidates
        ):
            return [_clean(p) for p in candidates]

    return [_clean(raw)]


def _get_city(text: str) -> tuple[str | None, bool]:
    folded = _fold(text)
    has_hanoi = bool(_HANOI.search(folded))
    has_hcm = bool(_HCM.search(folded))
    if has_hanoi and has_hcm:
        return None, True
    if has_hanoi:
        return "Hà Nội", False
    if has_hcm:
        return "TP. Hồ Chí Minh", False
    return None, False


def _get_district(text: str) -> tuple[str | None, bool]:
    folded = _fold(text)
    # Ignore historical district hints (quận/huyện ... cũ) when generating a
    # current-looking dim_location.district. Preserve text in location_name.
    legacy = bool(re.search(
        r"\b(?:quan|huyen|thi xa)\b[^,;()]*\bcu\b", folded,
        flags=re.I,
    ))
    if legacy:
        return None, True
    match = _DISTRICT.search(text)
    if not match:
        return None, False
    value = _clean(match.group("name"))
    # Avoid an accidentally captured vague or overly long district.
    if len(value) > 40 or not value or _fold(value).endswith(" cu"):
        return None, True
    return value, False


def normalize_location(location_raw: str | list[str] | None) -> LocationResult:
    """Normalize job location while retaining ambiguity and multi-location grain.

    Returned locations_clean is JSONB-compatible list[dict], NOT a bridge table.
    Caller must merge quality_flags with other cleaning module flags.
    """
    if location_raw is None or (isinstance(location_raw, str) and not location_raw.strip()):
        return LocationResult(quality_flags=["location_missing"])

    if isinstance(location_raw, str):
        source_items = _split_location_entries(location_raw)
        raw_display = _clean(location_raw)
    elif isinstance(location_raw, list) and all(isinstance(x, str) for x in location_raw):
        if not location_raw:
            return LocationResult(quality_flags=["location_missing"])
        source_items = []
        for item in location_raw:
            source_items.extend(_split_location_entries(item))
        raw_display = "; ".join(_clean(x) for x in location_raw if _clean(x))
    else:
        return LocationResult(quality_flags=["location_invalid_type"])

    flags: list[str] = []
    locations: list[dict[str, str | None]] = []
    seen: set[tuple[str, str, str]] = set()

    for item in source_items:
        if not item:
            continue
        if _fold(item).strip() in _NON_GEOGRAPHIC:
            flags.append("location_not_geographic")
            continue

        city, ambiguous = _get_city(item)
        district, legacy = _get_district(item)
        if ambiguous:
            flags.append("location_multiple_cities_ambiguous")
        if city is None:
            flags.append("location_city_unknown")
        if legacy:
            flags.append("location_legacy_district_reference")

        name = _PREFIX.sub("", item).strip() or item
        key = (_fold(city or ""), _fold(district or ""), _fold(name))
        if key in seen:
            flags.append("location_duplicate_removed")
            continue
        seen.add(key)
        locations.append({
            "city": city,
            "district": district,
            "location_name": name,
        })

    if not locations and "location_not_geographic" not in flags:
        flags.append("location_missing")
    if len(locations) > 1:
        flags.append("location_multiple_values")

    # Scalar convenience fields are filled only when unambiguous.
    cities = {loc["city"] for loc in locations}
    city_clean = next(iter(cities)) if len(cities) == 1 else None
    district_clean = locations[0]["district"] if len(locations) == 1 else None

    return LocationResult(
        city_clean=city_clean,
        district_clean=district_clean,
        location_detail_clean=raw_display or None,
        locations_clean=locations,
        quality_flags=list(dict.fromkeys(flags)),
    )
