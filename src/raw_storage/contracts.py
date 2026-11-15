from dataclasses import asdict, dataclass
from typing import Any

import hashlib
import json


@dataclass
class RawJobPayload:
    # Core business fields
    job_title_raw: str
    company_name_raw: str
    location_raw: str

    salary_raw: str | None = None
    experience_raw: str | None = None

    job_description_raw: str | None = None
    job_requirements_raw: str | None = None

    posted_date_raw: str | None = None
    deadline_raw: str | None = None

    # Extended fields
    company_url_raw: str | None = None
    company_size_raw: str | None = None
    company_industry_raw: str | None = None

    skill_tags_raw: list[str] | None = None
    benefits_raw: str | None = None
    job_level_raw: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def compute_content_hash(payload: dict[str, Any]) -> str:
    """
    Hash chỉ business payload.
    Không đưa crawl_run_id/scraped_at vào vì chúng thay đổi theo lần crawl.
    """

    canonical_json = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":")
    )

    return hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()
