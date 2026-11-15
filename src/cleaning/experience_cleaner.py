"""TopCV experience normalization V1.

Values are years. Do not invent an upper bound for a single-year requirement.
Buckets classify the *minimum required* years, not a full range.
Bucket thresholds are a V1 proposal; reconcile with the final Dimension Catalog
before loading dim_experience.

The staging schema NUMERIC(4,1) stores tenths of a year: unsupported smaller
fractions are not rounded silently.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class ExperienceResult:
    experience_min_years: Decimal | None = None
    experience_max_years: Decimal | None = None
    experience_bucket: str | None = None
    quality_flags: list[str] = field(default_factory=list)


def _normalize(text: str) -> str:
    text = text.casefold().replace("đ", "d")
    text = "".join(
        char for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    text = re.sub(r"[–—−]", "-", text)
    return " ".join(text.split())


_NUMBER_RE = re.compile(
    r"(?<![\w])(?P<amount>\d+(?:[.,]\d+)?)"
    r"\s*(?P<plus>\+)?\s*"
    r"(?P<unit>years?|yrs?|yr|nam|months?|mos?|mo|thang)?(?![\w])",
    re.IGNORECASE,
)
_YEAR_UNITS = {"nam", "year", "years", "yr", "yrs"}
_MONTH_UNITS = {"thang", "month", "months", "mo", "mos"}
_STEP = Decimal("0.1")  # staging numeric(4,1)


def _bucket(min_years: Decimal, no_requirement: bool = False) -> str:
    if no_requirement:
        return "Không yêu cầu"
    if min_years < Decimal("1"):
        return "Dưới 1 năm"
    if min_years < Decimal("3"):
        return "1-<3 năm"
    if min_years < Decimal("5"):
        return "3-<5 năm"
    return ">=5 năm"


def _years(number: str, unit: str) -> Decimal:
    value = Decimal(number.replace(",", "."))
    if value < 0:
        raise ValueError("negative years")
    if unit in _MONTH_UNITS:
        value /= Decimal(12)
    elif unit not in _YEAR_UNITS:
        raise ValueError("unknown unit")
    if value != value.quantize(_STEP):
        raise ArithmeticError("unsupported decimal precision for staging numeric(4,1)")
    if value > Decimal(100):
        raise ValueError("unrealistic experience")
    return value


def normalize_experience(experience_raw: str | None) -> ExperienceResult:
    """Normalize a TopCV experience requirement without guessing missing limits."""
    if experience_raw is None or not experience_raw.strip():
        return ExperienceResult(quality_flags=["experience_missing"])

    text = _normalize(experience_raw)

    if re.search(
        r"\b(khong (yeu cau|can)( kinh nghiem)?|chua co kinh nghiem|"
        r"no experience( required)?|without experience)\b",
        text,
    ):
        return ExperienceResult(
            experience_min_years=Decimal("0"),
            experience_bucket="Không yêu cầu",
        )

    numbers = list(_NUMBER_RE.finditer(text))
    if not numbers:
        return ExperienceResult(quality_flags=["experience_unrecognized"])
    if len(numbers) > 2:
        return ExperienceResult(quality_flags=["experience_format_ambiguous"])

    units = [m.group("unit") for m in numbers if m.group("unit")]
    if not units:
        return ExperienceResult(quality_flags=["experience_unit_unknown"])

    # Inherit the explicit unit in e.g. "1 - 3 năm" but do not guess from a
    # mixture of explicit year and month units.
    only_unit = units[0] if len(set(units)) == 1 else None
    values: list[Decimal] = []
    try:
        for match in numbers:
            unit = match.group("unit") or only_unit
            if unit is None:
                raise ValueError("ambiguous units")
            values.append(_years(match.group("amount"), unit))
    except ArithmeticError:
        return ExperienceResult(quality_flags=["experience_precision_exceeds_staging"])
    except (ValueError, InvalidOperation):
        return ExperienceResult(quality_flags=["experience_value_invalid_or_ambiguous"])

    flags: list[str] = []
    if len(values) == 2:
        separator = text[numbers[0].end():numbers[1].start()]
        if not re.search(r"-|\bden\b|\bto\b|~", separator):
            return ExperienceResult(quality_flags=["experience_format_ambiguous"])
        minimum, maximum = values
        if minimum > maximum:
            return ExperienceResult(quality_flags=["experience_inverted_range"])
    else:
        amount = values[0]
        prefix = text[:numbers[0].start()].strip()
        suffix = text[numbers[0].end():].strip()
        if re.search(r"\bduoi\b|\bit hon\b|(?<![<])<(?![=])", prefix):
            minimum, maximum = Decimal("0"), amount
            flags.append("experience_upper_bound_exclusive")
        elif re.search(r"\btoi da\b|\bkhong qua\b|<=", prefix):
            minimum, maximum = Decimal("0"), amount
        elif re.search(r"\btren\b|\bhon\b|(?<![>])>(?![=])", prefix):
            minimum, maximum = amount, None
            flags.append("experience_lower_bound_exclusive")
        elif (
            numbers[0].group("plus")
            or re.search(r"\b(tu|toi thieu|it nhat)\b|>=", prefix)
            or re.search(r"\b(tro len|or more|minimum)\b", suffix)
        ):
            minimum, maximum = amount, None
        else:
            minimum, maximum = amount, None
            flags.append("experience_single_value_assumed_minimum")

    return ExperienceResult(
        experience_min_years=minimum,
        experience_max_years=maximum,
        experience_bucket=_bucket(minimum),
        quality_flags=flags,
    )
