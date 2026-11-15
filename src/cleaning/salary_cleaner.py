"""TopCV Salary Normalization V1.

Policy: VND/month only for DW salary numeric measures. No foreign FX guessing.
The raw salary string is never overwritten.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True)
class SalaryResult:
    salary_min_vnd: int | None = None
    salary_max_vnd: int | None = None
    salary_midpoint_vnd: int | None = None
    salary_currency: str | None = None
    salary_period: str | None = None
    salary_disclosed_flag: bool | None = None
    salary_negotiable_flag: bool | None = None
    quality_flags: list[str] = field(default_factory=list)


def _ascii(value: str) -> str:
    value = value.casefold().replace("đ", "d").replace("₫", "d")
    return "".join(
        char for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    )


_AMOUNT_RE = re.compile(
    r"(?<![a-z0-9])(?P<number>\d+(?:[.,]\d+)*)"
    r"\s*(?P<unit>trieu|tr|ty|ti|nghin|ngan|k|vnd|dong|d)?",
    re.IGNORECASE,
)
_MULTIPLIERS = {
    "trieu": 1_000_000,
    "tr": 1_000_000,
    "ty": 1_000_000_000,
    "ti": 1_000_000_000,
    "nghin": 1_000,
    "ngan": 1_000,
    "k": 1_000,
    "vnd": 1,
    "dong": 1,
    "d": 1,
}


def _number(value: str) -> Decimal:
    # 10.000.000 / 1,000,000 -> grouping separator
    if re.fullmatch(r"\d{1,3}([.,]\d{3})+", value):
        return Decimal(value.replace(".", "").replace(",", ""))
    if "." in value and "," in value:
        decimal_mark = "." if value.rfind(".") > value.rfind(",") else ","
        grouping_mark = "," if decimal_mark == "." else "."
        value = value.replace(grouping_mark, "").replace(decimal_mark, ".")
    else:
        value = value.replace(",", ".")
    return Decimal(value)


def _period(text: str) -> str | None:
    match = re.search(r"(?:/|\bper\b|\bmoi\b|\btheo\b)\s*(thang|nam|ngay|gio|tuan)\b", text)
    if match:
        return {"thang": "month", "nam": "year", "ngay": "day", "gio": "hour", "tuan": "week"}[match.group(1)]
    return None


def normalize_salary(salary_raw: str | None) -> SalaryResult:
    """Normalize one TopCV salary. Numeric VND values mean VND per month.

    Unknown, USD, and other-period amounts are not converted to VND/month.
    Open ranges store only the known bound and never fabricate midpoint.
    """
    if salary_raw is None or not salary_raw.strip():
        return SalaryResult(quality_flags=["salary_missing"])

    text = _ascii(" ".join(salary_raw.strip().split()))
    if re.search(r'^\s*[-−]\s*\d|[-−]\s*[-−]\s*\d', text):
        return SalaryResult(quality_flags=['salary_negative_amount'])
    negotiated = bool(re.search(r"thoa thuan|thuong luong|negotiable", text))
    unit_usd = bool(re.search(r"\busd\b|\bus\$\b|\$|\bdo la\b", text))
    explicit_period = _period(text)
    matches = list(_AMOUNT_RE.finditer(text))
    flags: list[str] = []

    if not matches:
        if negotiated:
            return SalaryResult(salary_disclosed_flag=False, salary_negotiable_flag=True)
        return SalaryResult(
            salary_disclosed_flag=False,
            salary_negotiable_flag=None,
            quality_flags=["salary_text_unrecognized"],
        )

    currency = "USD" if unit_usd else None
    units = [m.group("unit") for m in matches if m.group("unit")]
    if not unit_usd and (
        units
        or re.search(r"\bvnd\b|\bdong\b|\btrieu\b|\bnghin\b|\bngan\b|[₫đ]", salary_raw.casefold())
    ):
        currency = "VND"

    if unit_usd:
        return SalaryResult(
            salary_currency="USD",
            salary_period=explicit_period,
            salary_disclosed_flag=True,
            salary_negotiable_flag=negotiated,
            quality_flags=["foreign_currency_requires_fx_policy"],
        )

    if currency != "VND":
        return SalaryResult(
            salary_period=explicit_period,
            salary_disclosed_flag=True,
            salary_negotiable_flag=negotiated,
            quality_flags=["salary_unit_unknown"],
        )

    period = explicit_period or "month"
    if explicit_period is None:
        flags.append("salary_period_assumed_monthly_topcv")
    if period != "month":
        return SalaryResult(
            salary_currency="VND", salary_period=period,
            salary_disclosed_flag=True, salary_negotiable_flag=negotiated,
            quality_flags=["non_monthly_salary_not_converted"],
        )

    unit_multipliers = {_MULTIPLIERS[u] for u in units}
    fallback_multiplier = next(iter(unit_multipliers)) if len(unit_multipliers) == 1 else None
    values: list[int] = []

    try:
        for match in matches:
            unit = match.group("unit")
            multiplier = _MULTIPLIERS[unit] if unit else fallback_multiplier
            if multiplier is None:
                raise ValueError("ambiguous unit")
            number = _number(match.group("number")) * multiplier
            if number <= 0 or number != number.to_integral_value():
                raise ValueError("invalid amount")
            values.append(int(number))
    except (InvalidOperation, ValueError):
        return SalaryResult(
            salary_currency="VND", salary_period="month",
            salary_disclosed_flag=True, salary_negotiable_flag=negotiated,
            quality_flags=flags + ["salary_amount_ambiguous"],
        )

    # Don't interpret unrelated numbers as salary values.
    if len(values) > 2:
        return SalaryResult(
            salary_currency="VND", salary_period="month",
            salary_disclosed_flag=True, salary_negotiable_flag=negotiated,
            quality_flags=flags + ["salary_format_ambiguous"],
        )

    if len(values) == 2:
        between = text[matches[0].end():matches[1].start()]
        if not re.search(r"-|–|—|\bden\b|\bto\b", between):
            return SalaryResult(
                salary_currency="VND", salary_period="month",
                salary_disclosed_flag=True, salary_negotiable_flag=negotiated,
                quality_flags=flags + ["salary_format_ambiguous"],
            )
        minimum, maximum = values
        if minimum > maximum:
            return SalaryResult(
                salary_currency="VND", salary_period="month",
                salary_disclosed_flag=True, salary_negotiable_flag=negotiated,
                quality_flags=flags + ["salary_inverted_range"],
            )
    else:
        value = values[0]
        if re.search(r"\b(tren|hon|tu|from|over|above)\b", text):
            minimum, maximum = value, None
            flags.append("salary_open_upper_bound")
        elif re.search(r"\b(duoi|toi da|toi|den|upto|up to|under|below|maximum|len toi|len den)\b", text):
            minimum, maximum = None, value
            flags.append("salary_open_lower_bound")
        else:
            minimum = maximum = value

    midpoint = (minimum + maximum) // 2 if minimum is not None and maximum is not None else None
    return SalaryResult(
        salary_min_vnd=minimum,
        salary_max_vnd=maximum,
        salary_midpoint_vnd=midpoint,
        salary_currency="VND",
        salary_period="month",
        salary_disclosed_flag=True,
        salary_negotiable_flag=negotiated,
        quality_flags=flags,
    )
