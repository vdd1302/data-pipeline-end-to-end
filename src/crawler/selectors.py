"""Bộ chọn DOM TopCV đã đối chiếu với ``tests/topcv_real/*.html``.

SỬA: tách riêng Listing, Standard, Quick View và Brand; không dùng bộ chọn
toàn trang để lấy nội dung công việc chính. Các CSS dưới đây là bằng chứng
từ chín fixture, không phải cam kết DOM TopCV bất biến hay locator Selenium UI.

Parser phải chọn root đúng layout, loại RELATED_CONTAINERS rồi mới dùng các
bộ chọn con. Hai bản detail cũ trong ``data/debug/`` còn cung cấp bằng chứng
JSON-LD và header công ty tương thích ngược. ``None`` nghĩa là fixture không
có trường/cấu trúc đã xác minh;
không truyền None vào BeautifulSoup.select(). Label aliases dùng so khớp sau
khi chuẩn hoá khoảng trắng, bỏ dấu hai chấm/chú thích con và casefold; giá trị
raw không được đổi theo label. Nhãn tiếng Anh của các section đã xuất hiện
trong detail_standard_lgcns.html; các bản dịch metadata tiếng Anh là aliases
tương thích về ngữ nghĩa, chưa phải bằng chứng có layout metadata tiếng Anh.
"""


# SỬA: chỉ lấy card con trực tiếp của listing để tránh danh sách gợi ý lồng nhau.
LISTING_ROOT = ".job-list-search-result"
LISTING_CARD = ":scope > .job-item-search-result"
LISTING_EMPTY = ".none-suitable-job__content"
CARD_ROOT = ".job-item-search-result"
CARD_JOB_ID_ATTRIBUTE = "data-job-id"
CARD_POSITION_ATTRIBUTE = "data-job-position"
CARD_JOB_LINK = "h3.title > a[href]"
# Badge GẤP/Nổi bật nằm cạnh anchor; không lấy toàn bộ h3 làm tên công việc.
CARD_TITLE = "h3.title > a > span:first-child"
CARD_COMPANY_NAME = "a.company .company-name"
CARD_COMPANY_LINK = "a.company[href]"
CARD_SALARY = ".label-content > .salary"
# .address còn dùng cho ngày đăng; giới hạn anchor của cụm thông tin công việc.
CARD_LOCATION = ".label-content > a.address"
CARD_EXPERIENCE = ".label-content > .exp"
CARD_POSTED_TIME = ".label-update"
CARD_UPDATED_AT = ".label-update"
CARD_TAGS = ".tag .item-tag"
CARD_TAG_TOOLTIPS = ".tag [data-original-title], .tag-quickview .item-tag[data-original-title]"
CARD_BADGES = ".box-label-top .label, .tag-diamond-employer, .tag-job-flash"
TOOLTIP_ATTRIBUTE = "data-original-title"


# SỬA: các vùng này có ID, lương và mô tả của công việc KHÁC.
RELATED_CONTAINERS = ".box-job-similar, .box-job-information-job-similar, .premium-job-company"


# Standard: hai fixture job_detail.html và detail_standard_lgcns.html.
STANDARD_ROOT = ".job-detail__body"
STANDARD_PRIMARY_JOB_ID = ".top-tet-lucky-money-container[data-job-id]"
STANDARD_TITLE = "h1.box-header-job__title"
# Giữ cả anchor và phần chữ ngoài anchor, ví dụ tên vị trí kèm suffix.
STANDARD_SALARY = ".box-header-job__salary--title"
STANDARD_HEADER_ITEMS = ".box-header-job-list-info__item"
STANDARD_HEADER_LABEL = ".list-info__content__title"
STANDARD_HEADER_VALUE = ".list-info__content__desc"
# Fallback có tiền tố nhãn; ưu tiên deadline lấy bằng HEADER_LABEL/HEADER_VALUE.
STANDARD_DEADLINE = ".box-applied-cv"
STANDARD_SECTIONS = ".box-job-information-detail-item"
STANDARD_SECTION_HEADING = ".box-job-information-detail-item__title--title"
STANDARD_SECTION_BODY = ".box-job-information-detail-item__text"
STANDARD_ADDRESS_ITEMS = ".box-job-information-address-and-time-list__item"
STANDARD_ADDRESS_LABEL = ".box-job-information-address-and-time-list__item--title"
STANDARD_ADDRESS_VALUE = ".box-job-information-address-and-time-list__item--content"
STANDARD_GENERAL_ITEMS = ".box-job-information-general-info-list__item"
STANDARD_GENERAL_LABEL = ".box-job-information-general-info-list__item--content-title"
STANDARD_GENERAL_VALUE = ".box-job-information-general-info-list__item--content-desc"
STANDARD_COMPANY_INFO = ".box-company-info-detail__list--item"
STANDARD_COMPANY_LABEL = ".box-company-info-detail__list--item-label"
STANDARD_COMPANY_VALUE = ".box-company-info-detail__list--item-value"
STANDARD_COMPANY_NAME = ".box-company-info__detail a.name, .box-company-info-detail__header a.name"
STANDARD_COMPANY_LINK = ".box-company-info__detail a.name[href], .box-company-info-detail__header a.name[href]"

# Overview chứa chuyên môn/kinh nghiệm/học vấn/phúc lợi, không phải skills thuần.
STANDARD_OVERVIEW_GROUPS = ".job-tags__group"
STANDARD_OVERVIEW_LABEL = ".job-tags__group-name"
STANDARD_OVERVIEW_VALUES = ".job-tags__group-list-tag .item"
STANDARD_OVERVIEW_TAGS = STANDARD_OVERVIEW_VALUES
# SỬA: required-tag còn nằm trong benefits. Parser phải scope theo section trước.
STANDARD_REQUIREMENT_GROUPS = ".required-tag"
STANDARD_REQUIREMENT_GROUP_LABEL = ".required-tag__content--title"
STANDARD_REQUIREMENT_GROUP_VALUE = ".required-tag__content--desc"
# Dùng trong group đã khớp SKILL_GROUP_LABELS; không select toàn root để suy kỹ năng.
STANDARD_SKILL_TAGS = STANDARD_REQUIREMENT_GROUP_VALUE


# Quick View: detail_job_1.html, khác với Standard dù cùng job 2296084.
QUICK_ROOT = ".job-list-detail"
QUICK_INFO = ".box-job-info"
QUICK_HEADER = ".box-header"
QUICK_TITLE = ".box-title > h2.title"
QUICK_PRIMARY_JOB_ID = ".box-info-header [data-job-id]"
QUICK_DETAIL_LINK = ".box-link-redirect a[href]"
QUICK_COMPANY_NAME = ".job-detail__company .company-name-label a.name"
QUICK_COMPANY_LINK = ".job-detail__company .company-name-label a.name[href]"
QUICK_COMPANY_SIZE = ".job-detail__company .company-scale .company-value"
QUICK_COMPANY_ADDRESS = ".job-detail__company .company-address .company-value"
QUICK_HEADER_VALUES = ".box-info-header .box-item-value"
# Không có label trong ba ô; parser phải kiểm tra len(values)==len(fields).
QUICK_HEADER_FIELDS = ("salary_raw", "location_raw", "experience_raw")
# Các h3 và content-tab là sibling; địa điểm nằm trong box-address riêng.
QUICK_SECTIONS = ".box-job-info > h3"
QUICK_SECTION_HEADING = "h3"
QUICK_SECTION_BODY = ".content-tab"
QUICK_ADDRESS = ".box-address > div"
QUICK_ADDRESS_HEADING = ".box-address > h3"
# Fixture Quick View không có general-info, structured skill groups hay deadline.
QUICK_GENERAL_ITEMS = None
QUICK_GENERAL_LABEL = None
QUICK_GENERAL_VALUE = None
QUICK_REQUIREMENT_GROUPS = None
QUICK_SKILL_TAGS = None


# Brand: job_brand.html. Root phải giữ cả sidebar general-info và primary ID.
BRAND_ROOT = ".premium-job-detail"
BRAND_PRIMARY_INFO = ".premium-job-information"
BRAND_PRIMARY_JOB_ID = ".top-tet-lucky-money-container[data-job-id]"
BRAND_TITLE = ".premium-job-basic-information__content--title"
BRAND_BASIC_ITEMS = ".premium-job-basic-information .basic-information-item"
BRAND_BASIC_LABEL = ".basic-information-item__data--label"
BRAND_BASIC_VALUE = ".basic-information-item__data--value"
BRAND_DEADLINE = ".job-detail__info--deadline-date"
BRAND_SECTIONS = ".premium-job-information .premium-job-description__box"
BRAND_SECTION_HEADING = ".premium-job-description__box--title"
# Địa chỉ dùng div không class; không lấy CTA/custom form làm nội dung section.
BRAND_SECTION_BODY = ":scope > .premium-job-description__box--content, :scope > div:not([class])"
BRAND_GENERAL_ITEMS = ".premium-job-general-information__content--row"
BRAND_GENERAL_LABEL = ".general-information-data__label"
BRAND_GENERAL_VALUE = ".general-information-data__value"
BRAND_OVERVIEW_GROUPS = ".premium-job-information .job-tags__group"
BRAND_OVERVIEW_LABEL = STANDARD_OVERVIEW_LABEL
BRAND_OVERVIEW_VALUES = STANDARD_OVERVIEW_VALUES
# SỬA: không lấy tên công ty từ các card .premium-job-company hay suy từ slug.
BRAND_COMPANY_NAME = None
BRAND_COMPANY_LINK = None


# Nhãn section đã quan sát, gồm cả layout tiếng Anh của LG CNS.
SECTION_LABEL_ALIASES = {
    "description_raw": ("mô tả công việc", "job description"),
    "requirements_raw": ("yêu cầu ứng viên", "candidate requirements"),
    "benefits_raw": ("quyền lợi", "quyền lợi ứng viên", "quyền lợi được hưởng", "benefits"),
    "workplace_address_raw": ("địa điểm làm việc", "location"),
    "working_time_raw": ("thời gian làm việc", "work schedule"),
}

# SỬA: label ngắn Địa điểm trong company-info nghĩa là địa chỉ CÔNG TY.
# Parser chọn company_address_raw bằng ngữ cảnh company, không ghi đè workplace.
LABEL_ALIASES = {
    "salary_raw": ("mức lương", "salary"),
    "location_raw": ("địa điểm", "location"),
    "experience_raw": ("kinh nghiệm", "experience"),
    "deadline_raw": ("hạn ứng tuyển", "hạn nộp hồ sơ", "application deadline", "deadline"),
    "job_level_raw": ("cấp bậc", "job level", "level"),
    "education_raw": ("học vấn", "education"),
    "vacancies_raw": ("số lượng tuyển", "vacancies", "number of vacancies"),
    "work_mode_raw": ("hình thức làm việc", "work mode"),
    "employment_type_raw": ("loại hình làm việc", "employment type"),
    "company_size_raw": ("quy mô", "company size"),
    "company_industry_raw": ("ngành nghề", "lĩnh vực", "industry"),
    "company_address_raw": ("địa điểm", "địa chỉ", "company address", "address"),
}

# VI đã quan sát ở required-tag; EN skills là alias ngữ nghĩa, chưa có fixture EN.
SKILL_GROUP_LABELS = ("kỹ năng cần có", "kỹ năng nên có", "required skills", "preferred skills")
BENEFIT_GROUP_LABELS = ("quyền lợi", "phúc lợi", "phụ cấp", "benefits", "allowances")

# SỬA: chín fixture mới không có JSON-LD; hai detail data/debug cũ có JobPosting.
# Parser phải kiểm tra @type/ID và giữ xung đột, không thay thế DOM một cách mù quáng.
JSON_LD_SELECTOR = 'script[type="application/ld+json"]'
PAGE_PRIMARY_SELECTORS = ", ".join((
    LISTING_ROOT, CARD_ROOT, LISTING_EMPTY, STANDARD_ROOT, QUICK_ROOT, BRAND_ROOT,
))


# Navigation controls observed in the user's 2026-10-29 UI audit.
# Click targets grounded in the real TopCV UI audit; always verify applied state.
SEARCH_FORM = "form#frm-search-job"
KEYWORD_INPUT = "#frm-search-job input#keyword"
CATEGORY_TRIGGER = "#category-filter-desktop-container-box-search-page-job .select-category"
LOCATION_TRIGGER = "#location-selection-desktop-job-listing-box-search-job .select-multi-location"
SEARCH_BUTTON = "#frm-search-job button.btn-search-job[type='submit']"

# M1d: TopCV location-administration introduction modal, observed OPEN in
# location_picker_open, location_selected, and before_submit in user M1c audit.
LOCATION_NOTICE_OPEN = "#location-selection__notice-new-location.open"
LOCATION_NOTICE_CONFIRM = (
    "#location-selection__notice-new-location.open "
    ".notice-new-location__modal-footer-btn"
)

# M1e: observed in the 2026-10-29 M1d audit (location_picker_open).
# Unlike job cards, these selectors are strictly scoped to the search-form dropdown.
LOCATION_PICKER = "#location-selection-desktop-job-listing-box-search-job .location-dropdown"
LOCATION_MODE_OPTIONS = LOCATION_PICKER + " .location-dropdown-type__option"
LOCATION_CITY_OPTIONS = LOCATION_PICKER + " .location-dropdown-options__city"
LOCATION_CITY_TEXT = ".city-text__main"
LOCATION_CITY_CHECKBOX = ".location-dropdown-options__city-wrapper .city-checkbox"
LOCATION_APPLIED_LABEL = LOCATION_TRIGGER
