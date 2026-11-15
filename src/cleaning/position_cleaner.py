"""TopCV job-position normalization V1 (offline, deterministic, conservative).

Staging mapping:
  job_title_clean, position_name, role_family,
  position_classification_version.

The small role taxonomy below is PROVISIONAL: confirm categories against the
Dimension Catalog before loading dim_position. Unknown/ambiguous titles are
preserved, never silently forced into a role family.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field


CLASSIFICATION_VERSION = "topcv-position-rules-v2.1"


@dataclass(frozen=True)
class PositionResult:
    job_title_clean: str | None = None
    position_name: str | None = None
    role_family: str | None = None
    position_classification_version: str | None = None
    quality_flags: list[str] = field(default_factory=list)


def _tidy_text(value: str | None) -> str | None:
    if value is None:
        return None
    # Normalize unicode and whitespace without erasing source terminology.
    text = unicodedata.normalize("NFKC", value)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _ascii(value: str) -> str:
    value = value.casefold().replace("đ", "d").replace("₫", "d")
    return "".join(
        char for char in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(char)
    ).translate(str.maketrans({'–': '-', '—': '-', '‐': '-', '‑': '-'}))


# Specific IT roles (matched first; conflicts between distinct specific roles are flagged)
_SPECIFIC_ROLE_RULES = (
    (re.compile(r"\bfull[\s-]?stack\b", re.I),
     "Fullstack Developer", "Software Engineering"),
    (re.compile(r"\b(?:back[\s-]?end|backend|node[\s.]?js|golang)\b", re.I),
     "Backend Developer", "Software Engineering"),
    (re.compile(r"\b(?:front[\s-]*end|frontend|web\s+ui|vue[\s.]?js|angular|lap\s+trinh\s+giao\s+dien|phat\s+trien\s+giao\s+dien)\b", re.I),
     "Frontend Developer", "Software Engineering"),
    (re.compile(r"\b(?:mobile|android|ios|flutter|react[\s-]?native)\b", re.I),
     "Mobile Developer", "Software Engineering"),
    (re.compile(r"\b(?:devops|devsecops|cloud\s+engineer|sre|site\s+reliability)\b", re.I),
     "DevOps Engineer", "Infrastructure & Operations"),
    (re.compile(r"\b(?:database\s+(?:engineer|administrator)|dba)\b", re.I),
     "Database Engineer (DBA)", "Database Administration"),
    (re.compile(r"\b(?:data\s+engineer|big\s*data|ky\s+su\s+du\s+lieu|data\s+engineering|data\s+lead)\b", re.I),
     "Data Engineer", "Data Engineering"),
    (re.compile(r"\b(?:data\s+analyst|phan\s+tich\s+du\s+lieu|bi\s+analyst|business\s+intelligence)\b", re.I),
     "Data Analyst", "Data Analytics"),
    (re.compile(r"\b(?:data\s+scientist|khoa\s+hoc\s+du\s+lieu)\b", re.I),
     "Data Scientist", "Data Science"),
    (re.compile(r"\b(?:machine\s+learning|ml\s+engineer|a[.\s]?i[.]?\s+(?:engineer|developer|researcher|scientist)|ky\s+su\s+ai|(?:ky\s+su|lap\s+trinh\s+vien|nha\s+nghien\s+cuu)\s+tri\s+tue\s+nhan\s+tao|artificial\s+intelligence|generative\s+ai|future\s+ai)\b", re.I),
     "Machine Learning Engineer", "Machine Learning"),
    (re.compile(r"\b(?:cyber\s*security|an\s+toan\s+thong\s+tin|an\s+ninh\s+mang|information\s+security|infosec|bao\s+mat|soc\s+analyst|security\s+engineer)\b", re.I),
     "Cybersecurity Specialist", "Security"),
    (re.compile(r"\b(?:business\s+analyst|phan\s+tich\s+nghiep\s+vu)\b", re.I),
     "Business Analyst", "Product & Analysis"),
    (re.compile(r"\b(?:qa|qc|tester|test\s+engineer|kiem\s+thu)\b", re.I),
     "QA/QC Engineer", "Quality Assurance"),
    (re.compile(r"\b(?:solution\s+architect|technical\s+lead|tech\s+lead|head\s+of\s+technology|kien\s+truc\s+su|engineering\s+manager)\b", re.I),
     "Solution Architect / Tech Lead", "Architecture & Leadership"),
)

# Generic base software engineering role fallback
_GENERIC_ROLE_PATTERN = re.compile(
    r"\b(?:software\s+(?:engineer|developer)|ky\s+su\s+phan\s+mem|ky\s+thuat\s+phan\s+mem|lap\s+trinh\s+vien|lap\s+trinh|developer|phat\s+trien\s+phan\s+mem|embedded|phat\s+trien\s+app)\b",
    re.I,
)


def normalize_position(job_title_raw: str | None) -> PositionResult:
    """Keep a cleaned title; classify recognized job roles deterministically."""
    title = _tidy_text(job_title_raw)
    if title is None:
        return PositionResult(quality_flags=["position_title_missing"])

    ascii_title = _ascii(title)
    matched = [(name, family) for pattern, name, family in _SPECIFIC_ROLE_RULES
               if pattern.search(ascii_title)]
    # Parenthesized technology lists do not override an explicit main role.
    # Generic titles such as "Developer (Backend)" still use the full title.
    main_title = re.sub(r'\([^)]*\)', ' ', ascii_title)
    main_matches = [(name,family) for pattern,name,family in _SPECIFIC_ROLE_RULES
                    if pattern.search(main_title)]
    if main_matches:
        matched = main_matches
    # Accent folding must not turn Bà Rịa/Ba Đình into the BA job acronym.
    if re.search(r'\bBA\b(?!\s+(?:RIA|DINH)\b)',title) and not any(name=='Business Analyst' for name,_ in matched):
        matched.append(('Business Analyst','Product & Analysis'))

    # If Fullstack is present, it takes precedence over secondary front/backend mentions
    if any(name == "Fullstack Developer" for name, _ in matched):
        matched = [m for m in matched if m[0] == "Fullstack Developer"]

    # If both Frontend and Backend are matched, classify as Fullstack Developer
    has_front = any(name == "Frontend Developer" for name, _ in matched)
    has_back = any(name == "Backend Developer" for name, _ in matched)
    if has_front and has_back:
        matched = [("Fullstack Developer", "Software Engineering")]

    # Retain the domain in titles such as "Backend Tech Lead"; seniority is
    # distinct from the position taxonomy. Standalone architects remain separate.
    if len(matched)>1:
        matched = [match for match in matched if match[0] != 'Solution Architect / Tech Lead']

    if len(matched) == 1:
        name, family = matched[0]
        return PositionResult(
            job_title_clean=title,
            position_name=name,
            role_family=family,
            position_classification_version=CLASSIFICATION_VERSION,
        )

    if len(matched) > 1:
        return PositionResult(
            job_title_clean=title,
            quality_flags=["position_multiple_role_matches_review"],
        )

    # Fallback to generic software engineer if title matches generic development terms
    if _GENERIC_ROLE_PATTERN.search(ascii_title):
        return PositionResult(
            job_title_clean=title,
            position_name="Software Engineer",
            role_family="Software Engineering",
            position_classification_version=CLASSIFICATION_VERSION,
        )

    return PositionResult(
        job_title_clean=title,
        quality_flags=["position_unclassified_review"],
    )
