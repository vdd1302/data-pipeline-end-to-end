"""Parse HTML TopCV offline theo docs/DOM_PARSER_SPEC.md.

SỬA: parser chỉ đọc HTML, không mở Selenium/kết nối DB/đọc .env. Kết quả
listing giữ thứ tự và card trùng cho RawIngestion.save_search_page(). Detail
giữ cả text/HTML/diagnostics; dùng to_raw_job_payload() trước save_job_detail()
để identity và metadata không lọt vào business payload/content_hash.
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
import json
import re
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup, Comment, NavigableString, Tag

from src.raw_storage.contracts import RawJobPayload
from .constants import RAW_PAYLOAD_FIELDS, REQUIRED_DETAIL_FIELDS, TOPCV_HOSTS
from . import selectors as S


LISTING_PARSER_VERSION = "topcv-listing-v2"
DETAIL_PARSER_VERSION = "topcv-detail-parser-v2"
_STANDARD_JOB_PATH = re.compile(r"/viec-lam/[^/?#]+/([0-9]+)\.html")
_BRAND_JOB_PATH = re.compile(r"/brand/[^/?#]+/tuyen-dung/[^/?#]+-j([0-9]+)\.html")
_BLOCK_TAGS = frozenset({
    "address", "article", "blockquote", "div", "dl", "dt", "dd", "h1", "h2",
    "h3", "h4", "h5", "h6", "li", "ol", "p", "pre", "section", "table", "tr", "ul",
})
_IGNORED_TAGS = frozenset({"script", "style", "template", "noscript", "svg"})


class LayoutChanged(ValueError):
    """HTML chưa đủ/không đúng bố cục; không lưu như kết quả thành công."""

    def __init__(self, message: str, *, missing_fields=()):
        super().__init__(message)
        self.missing_fields = tuple(missing_fields)


class AccessChallenge(ValueError):
    """Trang xác minh/chặn truy cập; việc chờ xác minh thuộc Selenium crawler."""

    def __init__(self, message="TopCV yêu cầu xác minh hoặc chặn truy cập.", *, kind="challenge", retry_after=None):
        super().__init__(message)
        self.kind = kind
        self.retry_after = retry_after


def _public_url(href, base_url: str) -> str | None:
    if not isinstance(href, str) or not href.strip() or not isinstance(base_url, str):
        return None
    href = href.strip()
    if re.search(r"[\x00-\x20\\]", href):
        return None
    try:
        parsed = urlsplit(urljoin(base_url, href))
        if (parsed.scheme not in {"http", "https"} or parsed.hostname not in TOPCV_HOSTS
                or parsed.username is not None or parsed.password is not None
                or parsed.port not in {None, 80, 443}):
            return None
    except ValueError:
        return None
    return urlunsplit((parsed.scheme, parsed.netloc.lower(), parsed.path, "", ""))


def normalize_job_link(href: str, base_url: str) -> tuple[str, str] | None:
    """URL công khai TopCV -> (URL bỏ tracking, ID); URL không hợp lệ -> None.

    SỬA: giữ đường dẫn Brand thật, nhận cả alias /viec-lam/{brand}/{id}.html.
    Không lấy identifier trong JSON-LD làm Job ID: mẫu thật chứa ID công ty.
    """
    canonical = _public_url(href, base_url)
    if canonical is None:
        return None
    path = urlsplit(canonical).path
    match = _STANDARD_JOB_PATH.fullmatch(path) or _BRAND_JOB_PATH.fullmatch(path)
    return (canonical, match.group(1)) if match else None


def _hidden(node: Tag) -> bool:
    style = re.sub(r"\s+", "", str(node.get("style", ""))).casefold()
    return (node.name in _IGNORED_TAGS or node.has_attr("hidden")
            or node.get("aria-hidden") == "true" or "display:none" in style
            or "visibility:hidden" in style)


def _text(node: Tag | None) -> str | None:
    """SỬA: giữ paragraph/list/br và direct text; không chèn space giữa inline.

    Chỉ chuẩn hóa khoảng trắng trình bày. Không chuẩn hóa lương/ngày/địa danh,
    không suy kỹ năng từ mô tả; các phép biến đổi đó thuộc lớp cleaning.
    """
    if node is None:
        return None
    chunks: list[str] = []

    def walk(current):
        if isinstance(current, Comment):
            return
        if isinstance(current, NavigableString):
            chunks.append(str(current))
            return
        if not isinstance(current, Tag) or _hidden(current):
            return
        if current.name in {"br", "hr"}:
            chunks.append("\n")
            return
        block = current.name in _BLOCK_TAGS
        if block:
            chunks.append("\n")
        for child in current.children:
            walk(child)
        if block:
            chunks.append("\n")

    walk(node)
    lines = [re.sub(r"\s+", " ", line).strip() for line in "".join(chunks).splitlines()]
    return "\n".join(line for line in lines if line) or None


def _flat(node: Tag | None) -> str | None:
    value = _text(node)
    return value.replace("\n", " ") if value else None


def _label(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip().rstrip(":：").strip().casefold()


def _one(root, selector: str, *, required=False) -> Tag | None:
    nodes = root.select(selector)
    if len(nodes) > 1:
        raise LayoutChanged(f"Selector cần một phần tử nhưng tìm thấy nhiều: {selector}.")
    if not nodes:
        if required:
            raise LayoutChanged(f"Thiếu phần tử chính: {selector}.")
        return None
    return nodes[0]


def _document(html: str, anti_bot: Mapping | None = None) -> BeautifulSoup:
    if not isinstance(html, str) or not html.strip():
        raise LayoutChanged("HTML phải là chuỗi không rỗng.")
    soup = BeautifulSoup(html, "html.parser")
    settings = {} if anti_bot is None else anti_bot
    if not isinstance(settings, Mapping):
        raise ValueError("anti_bot phải là mapping hoặc None.")
    if settings.get("detection_enabled", True):
        title = (_flat(soup.title) or "").casefold()
        # Empty marker/container rỗng không phải bằng chứng nội dung hợp lệ.
        # Chỉ bỏ kiểm tra body rộng khi đã có title công việc thật, tránh bắt
        # nhầm từ "access denied" được mô tả trong một tin Cybersecurity.
        primary = soup.select_one(",".join((S.CARD_JOB_LINK, S.STANDARD_TITLE, S.QUICK_TITLE, S.BRAND_TITLE)))
        body = (_flat(soup.body or soup) or "").casefold() if primary is None else ""
        title_markers = settings.get("challenge_title_markers", ("just a moment", "checking your browser", "attention required"))
        body_markers = settings.get("challenge_body_markers", (
            "verify you are human", "verifying you are human", "checking your browser",
            "xác minh bạn là con người",
        ))
        denied_markers = settings.get("access_denied_markers", (
            "sorry, you have been blocked", "you are unable to access topcv.vn", "access denied", "error 1020",
        ))
        # Check hard denial before title markers: Cloudflare block pages use
        # "Attention Required! | Cloudflare" together with "Sorry, you have been blocked".
        if any(str(marker).strip().casefold() in body for marker in denied_markers if str(marker).strip()):
            error = AccessChallenge(kind="access_denied")
            error.reason_code = 'access_denied_body_marker'
            raise error
        if any(str(marker).strip().casefold() in title for marker in title_markers if str(marker).strip()):
            error = AccessChallenge()
            error.reason_code = 'challenge_title_marker'
            raise error
        if any(str(marker).strip().casefold() in body for marker in body_markers if str(marker).strip()):
            error = AccessChallenge()
            error.reason_code = 'challenge_body_marker'
            raise error
    return soup



def check_challenge(html: str, anti_bot: Mapping | None = None) -> None:
    """Raise AccessChallenge for challenge/denied HTML; otherwise return None.

    Uses exactly the same detection logic as the listing/detail parsers, so
    navigation cannot disagree with parser challenge classification.
    """
    _document(html, anti_bot)


def _page_url(value: str) -> None:
    if _public_url(value, "https://www.topcv.vn") is None:
        raise LayoutChanged("URL trang phải là URL HTTP(S) công khai của TopCV.")


def _attribute_text(node: Tag | None, attribute: str) -> str | None:
    value = node.get(attribute) if node else None
    return value.strip() or None if isinstance(value, str) else None


def _card(card: Tag, page_url: str, rank: int) -> dict:
    link = _one(card, S.CARD_JOB_LINK, required=True)
    identity = normalize_job_link(link.get("href"), page_url)
    if identity is None:
        raise LayoutChanged("Card có URL việc làm không hợp lệ.")
    url, identifier = identity
    advertised_id = card.get(S.CARD_JOB_ID_ATTRIBUTE)
    if advertised_id is not None and str(advertised_id) != identifier:
        raise LayoutChanged("data-job-id của card không khớp ID trong URL.")
    title = _flat(link)
    if not title:
        raise LayoutChanged("Card thiếu tiêu đề công việc chính.", missing_fields=("job_title_raw",))
    company_node = card.select_one(S.CARD_COMPANY_NAME)
    company_link = card.select_one(S.CARD_COMPANY_LINK)
    company = _flat(company_node)
    updated = card.select_one(S.CARD_UPDATED_AT)
    # Chỉ lấy .tag, tránh nhân đôi cùng dữ liệu trong .tag-quickview.
    tags = [_flat(node) for node in card.select(S.CARD_TAGS)]
    tooltips = [_attribute_text(node, "data-original-title") for node in card.select(S.CARD_TAG_TOOLTIPS)]
    return {
        "job_id": identifier, "source_job_id": identifier,
        "job_title": title, "job_title_raw": title, "job_url": url,
        "job_url_raw": link.get("href"), "company_name": company, "company_name_raw": company,
        "company_url_raw": _public_url(company_link.get("href"), page_url) if company_link else None,
        "salary_raw": _flat(card.select_one(S.CARD_SALARY)),
        "location_raw": _flat(card.select_one(S.CARD_LOCATION)),
        "experience_raw": _flat(card.select_one(S.CARD_EXPERIENCE)),
        "posted_time_raw": _flat(card.select_one(S.CARD_POSTED_TIME)),
        "updated_at_raw": _attribute_text(updated, "data-original-title"),
        "job_tags_raw": [value for value in tags if value],
        "job_tags_tooltips_raw": [value for value in tooltips if value],
        "listing_badges_raw": [value for node in card.select(S.CARD_BADGES) if (value := _flat(node))],
        "listing_html_raw": str(card), "listing_text_raw": _text(card),
        "listing_selector": S.CARD_ROOT, "result_position": rank,
        "parser_version": LISTING_PARSER_VERSION,
    }


def parse_listing_page(html: str, page_url: str, *, anti_bot: Mapping | None = None,
                       allow_partial=False, rejected_cards=None) -> list[dict]:
    """Giữ mọi card đúng thứ tự; chỉ empty marker thật mới cho phép trả [].

    SỬA: không bỏ qua card lỗi hoặc dedup tại parser: Raw đánh rank theo list
    đầu vào và tự giữ một discovery/job/page. Chấp nhận một card HTML độc lập
    từ bộ khảo sát, nhưng không quét card liên quan trên một trang detail.
    """
    _page_url(page_url)
    soup = _document(html, anti_bot)
    root = _one(soup, S.LISTING_ROOT)
    if root is not None:
        cards = root.select(S.LISTING_CARD)
        if not cards:
            if soup.select_one(S.LISTING_EMPTY):
                return []
            raise LayoutChanged("Vùng listing chưa có card và chưa có marker không kết quả.")
    else:
        # Các fixture card là fragment top-level, không phải detail có related cards.
        top = soup.body or soup
        elements = [node for node in top.children if isinstance(node, Tag) and node.name not in _IGNORED_TAGS]
        if len(elements) == 1 and elements[0] in soup.select(S.CARD_ROOT):
            cards = elements
        elif soup.select_one(S.LISTING_EMPTY) and not soup.select_one(
                ",".join((S.STANDARD_ROOT, S.QUICK_ROOT, S.BRAND_ROOT, S.CARD_ROOT))):
            return []
        else:
            raise LayoutChanged("Không nhận diện được trang listing TopCV.")
    parsed, rejected = [], []
    for rank, card in enumerate(cards, 1):
        try:
            parsed.append(_card(card, page_url, rank))
        except LayoutChanged:
            if not allow_partial:
                raise
            rejected.append({'rank': rank, 'reason_code': 'invalid_listing_card'})
    if rejected_cards is not None:
        rejected_cards.extend(rejected)
    if rejected and not parsed:
        error = LayoutChanged('All visible cards failed listing identity validation')
        error.reason_code = 'all_listing_cards_rejected'
        raise error
    return parsed


def _section_key(heading: Tag) -> str | None:
    # Địa điểm có span chú thích địa giới; chỉ nhãn chính dùng để phân loại.
    plain = BeautifulSoup(str(heading), "html.parser")
    for note in plain.select("span"):
        note.decompose()
    label = _label(_flat(plain))
    for field, aliases in S.SECTION_LABEL_ALIASES.items():
        if any(label == _label(alias) or label.startswith(_label(alias) + " (") for alias in aliases):
            return field
    return None


def _labeled(root: Tag, item_selector: str, label_selector: str, value_selector: str, *, company=False) -> dict:
    result = {}
    for item in root.select(item_selector):
        label = _label(_flat(item.select_one(label_selector)))
        value = _flat(item.select_one(value_selector))
        if not label or not value:
            continue
        for field, aliases in S.LABEL_ALIASES.items():
            # SỬA: cùng nhãn Địa điểm ở header là nơi làm việc; trong sidebar
            # công ty là địa chỉ doanh nghiệp. Không gộp hai ngữ cảnh này.
            if field.startswith("company_") != company:
                continue
            if label in {_label(alias) for alias in aliases}:
                if field in result and result[field] != value:
                    raise LayoutChanged(f"Có nhiều giá trị khác nhau cho nhãn {field}.")
                result[field] = value
    return result


def _capture(result: dict, field: str, body: Tag | None) -> None:
    if body is None:
        return
    value = _text(body)
    if value:
        if result.get(field) is not None and result[field] != value:
            raise LayoutChanged(f"Có nhiều section khác nhau cho {field}.")
        result[field] = value
        result[field.removesuffix("_raw") + "_html_raw"] = str(body)


def _skills(node: Tag) -> list[str]:
    # SỬA: từng chip/link/list item là một giá trị; text comma là dạng đã gặp
    # trên LG CNS. Không tách kỹ năng bằng khoảng trắng hay quét description.
    entries = node.select("li, a")
    if entries:
        leaves = [entry for entry in entries if not entry.select("li, a")]
        return [value for entry in leaves if (value := _flat(entry))]
    text = _text(node) or ""
    return [value.strip() for value in re.split(r"[,\n]", text) if value.strip()]


def _groups(section: Tag | None, result: dict, *, benefits=False) -> None:
    if section is None:
        return
    group_key = "benefits_groups_raw" if benefits else "requirement_groups_raw"
    skills = result.get("skill_tags_raw") or []
    for group in section.select(S.STANDARD_REQUIREMENT_GROUPS):
        heading = _flat(group.select_one(S.STANDARD_REQUIREMENT_GROUP_LABEL))
        value_node = group.select_one(S.STANDARD_REQUIREMENT_GROUP_VALUE)
        value = _text(value_node)
        if not heading or not value:
            continue
        result[group_key][heading] = value
        if not benefits and _label(heading) in {_label(label) for label in S.SKILL_GROUP_LABELS}:
            skills.extend(_skills(value_node))
    if skills:
        # Chỉ loại chip trùng hệt nhau, không chuẩn hóa tên kỹ năng trong Raw.
        result["skill_tags_raw"] = list(dict.fromkeys(skills))


def _overview(root: Tag, result: dict, group_selector: str, label_selector: str, value_selector: str) -> None:
    """Giữ tag tổng quan đúng nhóm; không coi học vấn/chuyên môn là skill."""
    for group in root.select(group_selector):
        heading = _flat(group.select_one(label_selector))
        values = [value for node in group.select(value_selector) if (value := _flat(node))]
        if heading and values:
            result["overview_groups_raw"].setdefault(heading, []).extend(values)
            result["job_tags_raw"].extend(values)


def _standard(root: Tag, result: dict, job_url: str) -> None:
    result["job_title_raw"] = _flat(_one(root, S.STANDARD_TITLE, required=True))
    values = _labeled(root, S.STANDARD_HEADER_ITEMS, S.STANDARD_HEADER_LABEL, S.STANDARD_HEADER_VALUE)
    values.update(_labeled(root, S.STANDARD_GENERAL_ITEMS, S.STANDARD_GENERAL_LABEL, S.STANDARD_GENERAL_VALUE))
    values.update(_labeled(root, S.STANDARD_COMPANY_INFO, S.STANDARD_COMPANY_LABEL, S.STANDARD_COMPANY_VALUE, company=True))
    result.update(values)
    result["salary_raw"] = _flat(root.select_one(S.STANDARD_SALARY)) or result.get("salary_raw")
    if not result.get("deadline_raw"):
        fallback = _flat(root.select_one(S.STANDARD_DEADLINE)) or ""
        # .box-applied-cv có cả nhãn/đếm ngược; giữ nguyên chuỗi ngày được in.
        date = re.search(r"\b[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\b", fallback)
        result["deadline_raw"] = date.group() if date else None
    # Cùng employer xuất hiện ở header và sidebar. Chỉ chấp nhận bản lặp khi
    # tên và URL thống nhất; không lấy một tên tùy ý nếu DOM có xung đột.
    names = {_flat(node) for node in root.select(S.STANDARD_COMPANY_NAME)} - {None}
    links = {_public_url(node.get("href"), job_url)
             for node in root.select(S.STANDARD_COMPANY_LINK)} - {None}
    if len(names) > 1 or len(links) > 1:
        raise LayoutChanged("Employer ở header/sidebar không thống nhất.")
    result["company_name_raw"] = next(iter(names), None)
    result["company_url_raw"] = next(iter(links), None)
    _overview(root, result, S.STANDARD_OVERVIEW_GROUPS, S.STANDARD_OVERVIEW_LABEL, S.STANDARD_OVERVIEW_VALUES)
    for section in root.select(S.STANDARD_SECTIONS):
        heading = section.select_one(S.STANDARD_SECTION_HEADING)
        field = _section_key(heading) if heading else None
        if field:
            _capture(result, field, section.select_one(S.STANDARD_SECTION_BODY))
            if field in {"requirements_raw", "benefits_raw"}:
                _groups(section, result, benefits=field == "benefits_raw")
                if field == "benefits_raw" and result["benefits_groups_raw"]:
                    extra = "\n".join(f"{label}\n{value}"
                                      for label, value in result["benefits_groups_raw"].items())
                    result["benefits_raw"] = "\n".join(value for value in (result["benefits_raw"], extra) if value)
                    result["benefits_html_raw"] = str(section)
    for item in root.select(S.STANDARD_ADDRESS_ITEMS):
        heading = item.select_one(S.STANDARD_ADDRESS_LABEL)
        field = _section_key(heading) if heading else None
        if field in {"workplace_address_raw", "working_time_raw"}:
            _capture(result, field, item.select_one(S.STANDARD_ADDRESS_VALUE))


def _quick(root: Tag, result: dict, job_url: str) -> None:
    result["job_title_raw"] = _flat(_one(root, S.QUICK_TITLE, required=True))
    values = root.select(S.QUICK_HEADER_VALUES)
    # SỬA: mẫu Quick View không có label; chỉ nhận đúng cấu trúc 3 giá trị đã
    # khảo sát, tránh âm thầm đổi vị trí salary/location/experience khi DOM đổi.
    if len(values) != len(S.QUICK_HEADER_FIELDS):
        raise LayoutChanged("Header Quick View không còn đúng cấu trúc đã khảo sát.")
    for field, node in zip(S.QUICK_HEADER_FIELDS, values):
        result[field] = _flat(node)
    company = _one(root, S.QUICK_COMPANY_NAME)
    link = root.select_one(S.QUICK_COMPANY_LINK)
    result["company_name_raw"] = _flat(company)
    result["company_url_raw"] = _public_url(link.get("href"), job_url) if link else None
    result["company_size_raw"] = _flat(root.select_one(S.QUICK_COMPANY_SIZE))
    result["company_address_raw"] = _flat(root.select_one(S.QUICK_COMPANY_ADDRESS))
    for heading in root.select(S.QUICK_SECTIONS):
        field = _section_key(heading)
        if field:
            body = heading.find_next_sibling()
            if body and body in root.select(S.QUICK_SECTION_BODY):
                _capture(result, field, body)
    _capture(result, "workplace_address_raw", root.select_one(S.QUICK_ADDRESS))


def _brand(root: Tag, result: dict, job_url: str) -> None:
    primary = _one(root, S.BRAND_PRIMARY_INFO, required=True)
    result["job_title_raw"] = _flat(_one(primary, S.BRAND_TITLE, required=True))
    result.update(_labeled(primary, S.BRAND_BASIC_ITEMS, S.BRAND_BASIC_LABEL, S.BRAND_BASIC_VALUE))
    result.update(_labeled(root, S.BRAND_GENERAL_ITEMS, S.BRAND_GENERAL_LABEL, S.BRAND_GENERAL_VALUE))
    result["deadline_raw"] = _flat(primary.select_one(S.BRAND_DEADLINE))
    _overview(root, result, S.BRAND_OVERVIEW_GROUPS, S.BRAND_OVERVIEW_LABEL, S.BRAND_OVERVIEW_VALUES)
    for section in primary.select(S.BRAND_SECTIONS):
        heading = section.select_one(S.BRAND_SECTION_HEADING)
        field = _section_key(heading) if heading else None
        if field:
            _capture(result, field, section.select_one(S.BRAND_SECTION_BODY))
    # SỬA: không lấy employer từ job liên quan/footer/brand slug. Bộ mẫu chưa có
    # field employer đã xác minh trong primary header; adapter nhận từ listing.
    result["company_name_raw"] = None


def _check_detail_identity(soup: BeautifulSoup, root: Tag, result: dict, identifier: str, job_url: str) -> None:
    evidence = 0
    for canonical in soup.select('link[rel="canonical"]'):
        identity = normalize_job_link(canonical.get("href"), job_url)
        if identity is None or identity[1] != identifier:
            raise LayoutChanged("Canonical không khớp Job ID được yêu cầu.")
        evidence += 1
    selector = {"standard": S.STANDARD_PRIMARY_JOB_ID,
                "quick_view": S.QUICK_PRIMARY_JOB_ID, "brand": S.BRAND_PRIMARY_JOB_ID}[result["detail_layout"]]
    for node in root.select(selector):
        observed = node.get("data-job-id")
        if observed is not None and str(observed) != identifier:
            raise LayoutChanged("Job ID trong vùng detail chính không khớp URL.")
        if observed is not None:
            evidence += 1
    if result["detail_layout"] == "quick_view":
        for node in root.select(S.QUICK_DETAIL_LINK):
            identity = normalize_job_link(node.get("href"), job_url)
            if identity is None or identity[1] != identifier:
                raise LayoutChanged("Link chi tiết Quick View không khớp Job ID.")
            evidence += 1
    if not evidence:
        raise LayoutChanged("Detail chưa có bằng chứng Job ID/canonical để xác nhận đúng trang.")


def _json_jobs(soup: BeautifulSoup) -> list[dict]:
    jobs = []

    def visit(value):
        if isinstance(value, list):
            for child in value:
                visit(child)
        elif isinstance(value, dict):
            kind = value.get("@type")
            if kind == "JobPosting" or isinstance(kind, list) and "JobPosting" in kind:
                jobs.append(value)
            if "@graph" in value:
                visit(value["@graph"])

    for script in soup.select(S.JSON_LD_SELECTOR):
        try:
            visit(json.loads(script.string or script.get_text()))
        except (TypeError, ValueError):
            continue
    return jobs


def _date_key(value: str) -> str:
    for pattern in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value[:10], pattern).date().isoformat()
        except ValueError:
            continue
    return value


def _optional_json_dates(jobs: list[dict], result: dict) -> None:
    # Chỉ dùng JobPosting có title tương ứng với primary job. Không dùng JSON-LD
    # industry làm company industry, description tổng hợp làm section, hay ID NTD.
    candidates = []
    for job in jobs:
        if _label(job.get("title") if isinstance(job.get("title"), str) else None) != _label(result["job_title_raw"]):
            continue
        organization = job.get("hiringOrganization")
        name = organization.get("name") if isinstance(organization, dict) else None
        if result.get("company_name_raw") and isinstance(name, str) and _label(name) != _label(result["company_name_raw"]):
            continue
        if isinstance(job.get("url"), str):
            identity = normalize_job_link(job["url"], result["job_url"])
            if identity is None or identity[1] != result["source_job_id"]:
                continue
        candidates.append(job)
    if not candidates:
        return
    distinct = {json.dumps(job, sort_keys=True, ensure_ascii=False) for job in candidates}
    if len(distinct) != 1:
        result["warnings"].append({"code": "ambiguous_json_ld"})
        return
    for field, key in (("posted_date_raw", "datePosted"), ("deadline_raw", "validThrough")):
        value = candidates[0].get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        if not result.get(field):
            result[field] = value
            result["field_sources"][field] = "json_ld"
        elif _date_key(result[field]) != _date_key(value):
            result["warnings"].append({"code": "source_conflict", "field": field,
                                       "dom": result[field], "json_ld": value})


def parse_detail_page(html: str, job_url: str, *, anti_bot: Mapping | None = None,
                      allow_partial=False) -> dict:
    """Parse Standard/Quick View/Brand, giữ identity ngoài business payload.

    SỬA: dictionary khảo sát có nhiều metadata nên không truyền nguyên dict vào
    save_job_detail(). to_raw_job_payload(detail, listing) là adapter kiểm tra
    đủ 5 trường bắt buộc và bổ sung company Brand từ cùng discovery khi cần.
    """
    identity = normalize_job_link(job_url, "https://www.topcv.vn")
    if identity is None:
        raise LayoutChanged("URL detail TopCV không hợp lệ.")
    url, identifier = identity
    soup = _document(html, anti_bot)
    jobs = _json_jobs(soup)
    layouts = [(name, soup.select(selector)) for name, selector in (
        ("standard", S.STANDARD_ROOT), ("quick_view", S.QUICK_ROOT), ("brand", S.BRAND_ROOT)
    )]
    candidates = [(name, nodes[0]) for name, nodes in layouts if len(nodes) == 1]
    if len(candidates) != 1 or any(len(nodes) > 1 for _, nodes in layouts):
        raise LayoutChanged("Không nhận diện duy nhất một bố cục detail TopCV.")
    layout, root = candidates[0]
    # SỬA: loại related jobs trước mọi phép đọc nội dung hoặc ID.
    for related in list(root.select(S.RELATED_CONTAINERS)):
        related.decompose()
    result = {field: None for field in RAW_PAYLOAD_FIELDS}
    result.update({
        "job_id": identifier, "source_job_id": identifier, "job_url": url,
        "page_url": job_url, "detail_layout": layout, "job_title": None, "company_name": None,
        "description_raw": None, "requirements_raw": None, "workplace_address_raw": None,
        "working_time_raw": None, "education_raw": None, "vacancies_raw": None,
        "work_mode_raw": None, "employment_type_raw": None, "company_address_raw": None,
        "job_tags_raw": [], "overview_groups_raw": {}, "requirement_groups_raw": {}, "benefits_groups_raw": {},
        "warnings": [], "field_sources": {}, "parser_version": DETAIL_PARSER_VERSION,
    })
    _check_detail_identity(soup, root, result, identifier, url)
    {"standard": _standard, "quick_view": _quick, "brand": _brand}[layout](root, result, url)
    if result["workplace_address_raw"]:
        result["location_raw"] = result["workplace_address_raw"]
    result["job_title"] = result["job_title_raw"]
    result["company_name"] = result["company_name_raw"]
    result["job_description_raw"] = result["description_raw"]
    result["job_requirements_raw"] = result["requirements_raw"]
    for field, value in result.items():
        if field.endswith("_raw") and value is not None:
            result["field_sources"][field] = "dom"
    _optional_json_dates(jobs, result)
    required = REQUIRED_DETAIL_FIELDS - ({"company_name_raw"} if layout == "brand" else set())
    missing = sorted(field for field in required if not isinstance(result.get(field), str) or not result[field].strip())
    if missing and (not allow_partial or 'job_title_raw' in missing):
        raise LayoutChanged("Detail thiếu trường bắt buộc: " + ", ".join(missing), missing_fields=missing)
    result['missing_fields'] = missing
    result['record_level'] = 'detail_partial' if missing else 'detail_complete'
    result["page_text_raw"] = "\n".join(value for field in (
        "job_title_raw", "company_name_raw", "salary_raw", "location_raw", "experience_raw",
        "description_raw", "requirements_raw", "benefits_raw", "working_time_raw",
    ) if (value := result.get(field)))
    return result


def _payload_id(payload: Mapping) -> str:
    if not isinstance(payload, Mapping):
        raise LayoutChanged("Listing/detail phải là mapping.")
    identifiers = []
    for field in ("source_job_id", "job_id"):
        if field in payload and payload[field] is not None:
            value = payload[field]
            if type(value) is int:
                value = str(value)
            if not isinstance(value, str) or not re.fullmatch(r"[0-9]+", value):
                raise LayoutChanged(f"{field} phải là ID số TopCV.")
            identifiers.append(value)
    for field in ("job_url", "page_url"):
        if payload.get(field) is not None:
            identity = normalize_job_link(payload[field], "https://www.topcv.vn")
            if identity is None:
                raise LayoutChanged(f"{field} không phải URL việc làm TopCV.")
            identifiers.append(identity[1])
    if not identifiers or len(set(identifiers)) != 1:
        raise LayoutChanged("Identity trong payload không nhất quán hoặc bị thiếu.")
    return identifiers[0]


def merge_listing_and_detail(listing: Mapping, detail: Mapping) -> dict:
    """Giữ nguyên hai observation và báo khác biệt; không ghi đè dữ liệu Raw."""
    identifier = _payload_id(listing)
    if _payload_id(detail) != identifier:
        raise LayoutChanged("Không được merge listing/detail khác Job ID.")
    conflicts = {}
    for field in sorted(RAW_PAYLOAD_FIELDS & listing.keys() & detail.keys()):
        first, second = listing[field], detail[field]
        if first is not None and second is not None and first != second:
            conflicts[field] = {"listing": deepcopy(first), "detail": deepcopy(second)}
    return {"job_id": identifier, "source_job_id": identifier,
            "job_url": detail.get("job_url") or listing.get("job_url"),
            "listing": deepcopy(dict(listing)), "detail": deepcopy(dict(detail)), "conflicts": conflicts}


def to_raw_job_payload(detail: Mapping, listing: Mapping | None = None, *, allow_partial=False) -> RawJobPayload:
    """SỬA: bridge về 15 trường RawJobPayload, không đổi schema/SQL hiện tại.

    Metadata/HTML/diagnostics nằm ngoài payload; lưu HTML gốc bằng detail_html
    và parser_version bằng tham số riêng của RawIngestion.save_job_detail().
    Listing cùng ID chỉ bổ sung thông tin chưa có, không thay nội dung detail.
    """
    identifier = _payload_id(detail)
    if listing is not None and _payload_id(listing) != identifier:
        raise LayoutChanged("Không được bổ sung detail từ listing khác Job ID.")
    values = {field: deepcopy(detail.get(field)) for field in RAW_PAYLOAD_FIELDS}
    values["job_description_raw"] = values["job_description_raw"] or detail.get("description_raw")
    values["job_requirements_raw"] = values["job_requirements_raw"] or detail.get("requirements_raw")
    if listing is not None:
        for field in ("job_title_raw", "company_name_raw", "company_url_raw", "location_raw", "salary_raw", "experience_raw"):
            if values[field] is None or isinstance(values[field], str) and not values[field].strip():
                values[field] = deepcopy(listing.get(field))
    missing = sorted(field for field in REQUIRED_DETAIL_FIELDS
                     if not isinstance(values[field], str) or not values[field].strip())
    if missing and (not allow_partial or 'job_title_raw' in missing):
        raise LayoutChanged("Payload Raw thiếu trường bắt buộc: " + ", ".join(missing), missing_fields=missing)
    for field, value in values.items():
        if field == "skill_tags_raw":
            if value is not None and (not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value)):
                raise LayoutChanged("skill_tags_raw phải là list chuỗi hoặc None.")
        elif value is not None and not isinstance(value, str):
            raise LayoutChanged(f"{field} phải là chuỗi hoặc None.")
    return RawJobPayload(**values)
