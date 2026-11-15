"""Đọc, kiểm tra và chuyển crawl.yaml thành cấu hình runtime cho crawler.

SỬA: load_config() giữ dict JSON-safe cho RawIngestion.start_run().
load_runtime_config() chuyển cấu hình đã kiểm tra thành CrawlerConfig: thông số
Selenium có kiểu rõ ràng, đường dẫn tuyệt đối và các task keyword/thành phố.
Module không khởi chạy Chrome, tạo thư mục hoặc kết nối database khi load.

Ví dụ cho crawler/entrypoint mới:
    config = load_runtime_config()
    options = config.selenium.build_chrome_options()
    # webdriver.Chrome(options=options) do topcv.py khởi tạo khi bắt đầu crawl.
    # driver.set_page_load_timeout(config.selenium.page_load_timeout_seconds)
    # WebDriverWait(driver, config.selenium.explicit_wait_seconds)
    # ingestion.start_run(config.snapshot)
    # for task in config.tasks:
    #     ingestion.start_task(run_id, task.position, task.location)
"""
from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import timedelta, timezone as datetime_timezone
from itertools import product
import math
import os
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from src.raw_storage.ingestion import load_crawl_config as _load_raw_config
from .constants import (
    BROWSER_NAME, CHALLENGE_ACTIONS, COLLECTION_STAGES, DEFAULT_CONFIG_PATH,
    GENERATED_FIELDS, IDENTITY_FIELDS, LOG_LEVELS, PROJECT_ROOT, QUERY_STRATEGY,
    RAW_PAYLOAD_FIELDS, REQUIRED_DETAIL_FIELDS, RETRY_REASONS, SOURCE_METHOD,
    STOP_ACTIONS, TOPCV_HOSTS,
)

if TYPE_CHECKING:
    from selenium.webdriver.chrome.options import Options

# Giữ tên ROOT để các caller của file config.py cũ tiếp tục dùng được.
ROOT = PROJECT_ROOT


@dataclass(frozen=True, slots=True)
class SeleniumConfig:
    """Thông số runtime; tên trường giữ cùng đơn vị giây như trong YAML."""

    browser: str
    headless: bool
    browser_instances: int
    browser_profile_dir: Path
    page_load_timeout_seconds: float
    explicit_wait_seconds: float
    delay_between_pages_seconds: float
    delay_between_jobs_seconds: float
    max_retries: int
    retry_backoff_seconds: tuple[float, ...]
    retry_on: tuple[str, ...]
    on_challenge: str
    require_target_page_after_verification: bool
    on_verification_timeout: str
    on_access_denied: str
    retry_on_challenge: bool
    on_unexpected_dialog: str
    manual_verification_timeout_seconds: float | None = None
    verification_poll_interval_seconds: float | None = None
    # Runtime-only (CLI/env, not YAML): 'selenium' is the unchanged default transport;
    # 'undetected' selects src/crawler/undetected.py. See scripts/run_market_pipeline.py.
    driver: str = "selenium"
    detached_navigation_seconds: float = 0.0
    challenge_grace_seconds: float = 0.0

    def build_chrome_options(self) -> Options:
        """Tạo Options mới cho webdriver.Chrome(options=...), chưa mở browser.

        SỬA: chuyển headless/profile sang Chrome Options ngay tại một chỗ.
        Timeout/explicit wait/delay/retry do crawler áp dụng sau khi tạo driver;
        chúng không phải tham số của ChromeOptions hoặc webdriver.Chrome().
        """
        from selenium.webdriver.chrome.options import Options

        options = Options()
        options.page_load_strategy = "eager"  # Giữ cách tải trang của demo cũ.

        # Remote debugging support: connects to genuine user Chrome instance (navigator.webdriver = false)
        debugger_address = os.environ.get('TOPCV_CHROME_DEBUGGER_ADDRESS')
        if debugger_address:
            if debugger_address.strip() in ('1', 'true', 'True'):
                debugger_address = '127.0.0.1:9222'
            options.add_experimental_option("debuggerAddress", debugger_address.strip())
            return options
        in_container = os.environ.get('TOPCV_CONTAINER_BROWSER') == '1'
        display = os.environ.get('DISPLAY', '')
        x_socket = Path(f"/tmp/.X11-unix/X{display[1:].split('.')[0]}") if display.startswith(':') else None
        use_xvfb = in_container and os.environ.get('TOPCV_CONTAINER_XVFB') == '1' and bool(x_socket and x_socket.exists())
        if (self.headless and not use_xvfb) or (in_container and not use_xvfb):
            options.add_argument("--headless=new")
        # Một argument duy nhất, kể cả khi đường dẫn có dấu hoặc khoảng trắng.
        options.add_argument(f"--user-data-dir={self.browser_profile_dir}")
        options.add_argument("--start-maximized")
        options.add_argument("--disable-notifications")
        # Do not conceal browser automation or bypass source challenges.
        # AccessChallenge is handled by stop-and-log at the crawler layer.
        # Opt-in, container-only transport. No changes to local Windows Chrome.
        # Headless plus stop-on-challenge is enforced by crawl.airflow.yaml.
        if in_container:
            options.binary_location = os.environ.get('CHROME_BINARY') or os.environ.get('CHROME_BIN', '/usr/bin/chromium')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1440,900')
            # Airflow container runs as a non-root user; retain Chromium sandbox.
        return options


@dataclass(frozen=True, slots=True)
class SearchTaskConfig:
    """Một truy vấn UI; không tự suy đoán URL/bộ lọc số của TopCV."""

    position_id: str
    keyword: str
    location_id: str
    search_label: str

    @property
    def position(self) -> dict:
        """Metadata mới cho RawIngestion.start_task(), không lộ state runtime."""
        return {"id": self.position_id, "keyword": self.keyword}

    @property
    def location(self) -> dict:
        return {"id": self.location_id, "search_label": self.search_label}


@dataclass(frozen=True, slots=True)
class CrawlerConfig:
    """Kết quả chuyển đổi YAML dành cho crawler và pipeline mới.

    SỬA: section runtime chỉ đọc; list được đổi thành tuple. Chỉ snapshot trả
    dict/list JSON-safe đúng YAML gốc để ghi Raw, không chứa Path/timezone.
    Lọc task phục vụ lần chạy hiện tại không làm thay đổi snapshot phạm vi gốc.
    """

    config_path: Path
    version: int
    project: Mapping[str, Any]
    source: Mapping[str, Any]
    scope: Mapping[str, Any]
    crawl: Mapping[str, Any]
    selenium: SeleniumConfig
    anti_bot: Mapping[str, Any]
    extraction: Mapping[str, Any]
    logging: Mapping[str, Any]
    tasks: tuple[SearchTaskConfig, ...]
    timezone: datetime_timezone
    _snapshot: dict = field(repr=False, compare=False)

    @property
    def snapshot(self) -> dict:
        """Bản sao cấu hình công khai cho RawIngestion.start_run(config=...)."""
        return deepcopy(self._snapshot)


def _mapping(value, name: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} phải là YAML mapping/object.")
    return value


def _text(value, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} phải là chuỗi không rỗng.")
    return value


def _enum(value, name: str, choices) -> None:
    _text(value, name)
    if value not in choices:
        raise ValueError(f"{name} phải thuộc: {', '.join(sorted(choices))}.")


def _boolean(value, name: str) -> None:
    if type(value) is not bool:
        raise ValueError(f"{name} phải là bool true/false.")


def _integer(value, name: str, minimum=1) -> None:
    # SỬA: bool là subclass của int trong Python; true không phải giới hạn trang.
    if type(value) is not int or value < minimum:
        raise ValueError(f"{name} phải là số nguyên >= {minimum}.")


def _number(value, name: str, *, positive=False) -> None:
    valid = type(value) in (int, float)
    try:
        valid = valid and math.isfinite(value) and (value > 0 if positive else value >= 0)
    except OverflowError:
        valid = False
    if not valid:
        bound = "> 0" if positive else ">= 0"
        raise ValueError(f"{name} phải là số hữu hạn {bound}.")


def _strings(value, name: str, *, nonempty=True) -> list[str]:
    if not isinstance(value, list) or (nonempty and not value):
        raise ValueError(f"{name} phải là list{' không rỗng' if nonempty else ''}.")
    for item in value:
        _text(item, name)
    if len(set(value)) != len(value):
        raise ValueError(f"{name} không được có giá trị trùng.")
    return value


def _scope(cfg: Mapping) -> tuple[list, list]:
    scope = _mapping(cfg.get("scope"), "scope")
    result = []
    for group, value_key in (("positions", "keyword"), ("locations", "search_label")):
        entries = scope.get(group)
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"scope.{group} phải là list không rỗng.")
        ids = []
        for index, entry in enumerate(entries):
            name = f"scope.{group}[{index}]"
            entry = _mapping(entry, name)
            identifier = _text(entry.get("id"), name + ".id")
            if identifier != identifier.strip():
                raise ValueError(f"{name}.id không được có khoảng trắng ở đầu/cuối.")
            _text(entry.get(value_key), name + "." + value_key)
            ids.append(identifier)
        # Kiểm tra string trước set(ids), tránh TypeError khi id là list/dict.
        if len(set(ids)) != len(ids):
            raise ValueError(f"scope.{group} có ID trùng.")
        result.append(entries)
    return result[0], result[1]


def _topcv_url(value, name: str, *, base=False) -> None:
    try:
        parsed = urlsplit(_text(value, name))
        valid = (parsed.scheme == "https" and parsed.hostname in TOPCV_HOSTS
                 and parsed.username is None and parsed.password is None
                 and parsed.port in (None, 443))
        if base:
            valid = valid and parsed.path in ("", "/") and not parsed.query and not parsed.fragment
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(f"{name} phải là URL HTTPS công khai của TopCV.")


def resolve_project_path(path: str | Path) -> Path:
    """Resolve profile/checkpoint/log paths without changing the config snapshot."""
    if not isinstance(path, (str, Path)) or not str(path).strip():
        raise ValueError("Đường dẫn phải là str/Path không rỗng.")
    target = Path(path).expanduser()
    return (target if target.is_absolute() else PROJECT_ROOT / target).resolve()


def load_config(path: str | Path | None = None) -> dict:
    """Đọc/kiểm tra YAML và trả snapshot; crawler mới dùng load_runtime_config()."""
    # SỬA: Raw dùng TOPCV_RAW_DATABASE_URL; YAML không cần raw_storage.database.
    # Loader chung đã kiểm tra source, timezone, bảng Raw, JSON và public snapshot.
    # Cả đường dẫn mặc định/relative đều tính từ project root, kể cả scheduler cwd khác.
    target = resolve_project_path(DEFAULT_CONFIG_PATH if path is None else path)
    cfg = _load_raw_config(target)

    project = _mapping(cfg.get("project"), "project")
    _text(project.get("name"), "project.name")
    source = _mapping(cfg.get("source"), "source")
    _enum(source.get("method"), "source.method", {SOURCE_METHOD})
    _text(source.get("language"), "source.language")
    _topcv_url(source.get("base_url"), "source.base_url", base=True)
    _topcv_url(source.get("entry_url"), "source.entry_url")

    scope = _mapping(cfg.get("scope"), "scope")
    _scope(cfg)
    _enum(scope.get("sector"), "scope.sector", {"IT"})
    _enum(scope.get("sector_filter_mode"), "scope.sector_filter_mode", {"ui_category"})
    _text(scope.get("sector_search_label"), "scope.sector_search_label")
    _text(scope.get("location_ui_mode"), "scope.location_ui_mode")
    _enum(scope.get("on_missing_filter"), "scope.on_missing_filter", STOP_ACTIONS)
    for key in ("require_sector_filter", "require_location_filter", "verify_filters_after_navigation"):
        _boolean(scope.get(key), "scope." + key)

    crawl = _mapping(cfg.get("crawl"), "crawl")
    _enum(crawl.get("query_strategy"), "crawl.query_strategy", {QUERY_STRATEGY})
    collect = _strings(crawl.get("collect"), "crawl.collect")
    if not set(collect) <= COLLECTION_STAGES or "listing" not in collect:
        raise ValueError("crawl.collect phải có listing và chỉ chứa listing/detail.")
    # Listing-only có thể dùng để inspect; pipeline không được báo run success
    # khi các job đã đăng ký vẫn chưa có detail, theo hợp đồng RawIngestion.
    _integer(crawl.get("max_pages_per_query"), "crawl.max_pages_per_query")
    if crawl.get("max_jobs_per_query") is not None:
        _integer(crawl["max_jobs_per_query"], "crawl.max_jobs_per_query")
    # Optional worker controls retain compatibility with old frozen snapshots.
    for key in ('max_details_per_batch', 'scope_detail_quantum', 'worker_time_budget_seconds',
                'listing_pages_per_slice', 'scope_retry_limit', 'job_retry_limit',
                'lease_seconds', 'scope_retry_delay_seconds', 'rate_limit_backoff_seconds'):
        if key in crawl:
            _integer(crawl[key], 'crawl.' + key)
    if crawl.get('max_navigations_per_session') is not None:
        _integer(crawl['max_navigations_per_session'], 'crawl.max_navigations_per_session')
    if 'scope_priority' in crawl:
        _enum(crawl['scope_priority'], 'crawl.scope_priority', {'default', 'detail_first', 'detail_only'})
    for key in ('min_navigation_interval_seconds', 'micro_batch_pause_seconds',
                'min_session_gap_seconds'):
        if key in crawl:
            _number(crawl[key], 'crawl.' + key)
    if crawl.get('min_navigation_interval_seconds', 15) < 15:
        raise ValueError('crawl.min_navigation_interval_seconds must be at least 15')
    if crawl.get('scope_retry_limit', 3) > 5 or crawl.get('job_retry_limit', 2) > 2:
        raise ValueError('Recovery policy permits at most five scope and two job attempts')
    if crawl.get('listing_pages_per_slice', 1) != 1:
        raise ValueError('Fair collection requires one listing page per scope slice')
    if crawl.get('lease_seconds', 180) < 90:
        raise ValueError('A scope lease must cover a bounded browser navigation')
    for key in ('seed_batches_only_from_daily_dag', 'preserve_pending_across_midnight',
                'allow_partial_detail', 'allow_verified_listing_only', 'hard_denial_requires_review'):
        if key in crawl:
            _boolean(crawl[key], 'crawl.' + key)
            if not crawl[key]:
                raise ValueError('The approved recovery design requires crawl.' + key)
    if crawl.get('auto_resume_after_access_block'):
        raise ValueError('Source access denial requires operator review, not timer recovery')
    for key in ("enabled", "stop_when_no_next_page", "stop_when_repeated_page",
                "checkpoint_enabled", "resume_after_failure", "auto_resume_after_access_block",
                "deduplicate_within_run", "retain_search_matches", "preserve_observations_across_runs"):
        _boolean(crawl.get(key), "crawl." + key)
    # SỬA: các hành vi này là hợp đồng cố định của Raw hiện tại, không phải
    # công tắc có thể tắt bằng YAML (unique observation/run và append history).
    for key in ("deduplicate_within_run", "retain_search_matches", "preserve_observations_across_runs"):
        if not crawl[key]:
            raise ValueError(f"crawl.{key} phải là true để tương thích Raw storage.")
    _text(crawl.get("checkpoint_directory"), "crawl.checkpoint_directory")
    dedup = _strings(crawl.get("deduplication_key"), "crawl.deduplication_key")
    if set(dedup) != {"source_code", "source_job_id"}:
        raise ValueError("crawl.deduplication_key phải khớp source_code/source_job_id của Raw.")

    selenium = _mapping(cfg.get("selenium"), "selenium")
    _enum(selenium.get("browser"), "selenium.browser", {BROWSER_NAME})
    _boolean(selenium.get("headless"), "selenium.headless")
    _integer(selenium.get("browser_instances"), "selenium.browser_instances")
    # SỬA: chỉ có một profile; chưa hỗ trợ nhiều Chrome dùng profile riêng.
    if selenium["browser_instances"] != 1:
        raise ValueError("selenium.browser_instances hiện chỉ hỗ trợ 1 với profile riêng.")
    for key in ("page_load_timeout_seconds", "explicit_wait_seconds"):
        _number(selenium.get(key), "selenium." + key, positive=True)
    for key in ("delay_between_pages_seconds", "delay_between_jobs_seconds"):
        _number(selenium.get(key), "selenium." + key)
    _text(selenium.get("browser_profile_dir"), "selenium.browser_profile_dir")
    _integer(selenium.get("max_retries"), "selenium.max_retries", minimum=0)
    backoff = selenium.get("retry_backoff_seconds")
    if not isinstance(backoff, list) or len(backoff) != selenium["max_retries"]:
        raise ValueError("selenium.retry_backoff_seconds cần một giá trị cho mỗi retry.")
    for delay in backoff:
        _number(delay, "selenium.retry_backoff_seconds")
    retry_on = _strings(selenium.get("retry_on"), "selenium.retry_on", nonempty=False)
    if not set(retry_on) <= RETRY_REASONS:
        raise ValueError("selenium.retry_on chỉ hỗ trợ lỗi điều hướng tạm thời.")
    _enum(selenium.get("on_challenge"), "selenium.on_challenge", CHALLENGE_ACTIONS)
    for key in ("require_target_page_after_verification", "retry_on_challenge"):
        _boolean(selenium.get(key), "selenium." + key)
    for key in ("on_verification_timeout", "on_access_denied", "on_unexpected_dialog"):
        _enum(selenium.get(key), "selenium." + key, STOP_ACTIONS)

    # SỬA: giữ manual pause đã có trong YAML; không yêu cầu duy nhất stop_and_log.
    # Trình duyệt phải hiện và thời gian chờ phải hữu hạn khi chọn xác minh thủ công.
    manual = selenium["on_challenge"] == "pause_for_manual_verification"
    # Giá trị manual nếu có vẫn phải hợp lệ, kể cả policy hiện tại là stop.
    for key in ("manual_verification_timeout_seconds", "verification_poll_interval_seconds"):
        if manual or key in selenium:
            _number(selenium.get(key), "selenium." + key, positive=True)
    if manual:
        if selenium["headless"]:
            raise ValueError("Xác minh thủ công yêu cầu selenium.headless=false.")
        if selenium["verification_poll_interval_seconds"] > selenium["manual_verification_timeout_seconds"]:
            raise ValueError("Khoảng poll không được lớn hơn thời gian chờ xác minh.")
        if not selenium["require_target_page_after_verification"]:
            raise ValueError("Sau xác minh phải kiểm tra trang listing/detail đã sẵn sàng.")
    if selenium["retry_on_challenge"]:
        raise ValueError("Không hỗ trợ tự retry challenge; dùng manual pause hoặc stop_and_log.")

    anti_bot = _mapping(cfg.get("anti_bot"), "anti_bot")
    for key in ("detection_enabled", "honor_retry_after"):
        _boolean(anti_bot.get(key), "anti_bot." + key)
    for key in ("challenge_title_markers", "challenge_body_markers", "access_denied_markers"):
        _strings(anti_bot.get(key), "anti_bot." + key, nonempty=anti_bot["detection_enabled"])
    for key in ("on_http_403", "on_http_429"):
        _enum(anti_bot.get(key), "anti_bot." + key, STOP_ACTIONS)

    extraction = _mapping(cfg.get("extraction"), "extraction")
    identity = _strings(extraction.get("identity_fields"), "extraction.identity_fields")
    required = set(_strings(extraction.get("required_fields"), "extraction.required_fields"))
    optional = set(_strings(extraction.get("optional_fields"), "extraction.optional_fields", nonempty=False))
    if set(identity) != set(IDENTITY_FIELDS):
        raise ValueError("extraction.identity_fields phải là source_job_id/job_url.")
    if (not REQUIRED_DETAIL_FIELDS <= required or not (required | optional) <= RAW_PAYLOAD_FIELDS
            or required & optional):
        raise ValueError("extraction required/optional fields không khớp RawJobPayload/RawIngestion.")
    generated = _strings(extraction.get("generated_fields"), "extraction.generated_fields")
    if not set(generated) <= GENERATED_FIELDS:
        raise ValueError("extraction.generated_fields chứa metadata Raw chưa hỗ trợ.")
    _boolean(extraction.get("allow_null_optional_fields"), "extraction.allow_null_optional_fields")

    logging = _mapping(cfg.get("logging"), "logging")
    _enum(logging.get("level"), "logging.level", LOG_LEVELS)
    _text(logging.get("directory"), "logging.directory")
    # Không chèn Path hoặc đổi relative path trong cfg: Raw cần snapshot JSON-safe.
    # load_runtime_config() thực hiện chuyển đổi trên bản riêng bên dưới.
    return cfg


def tasks(cfg: dict, position_id: str | None = None, location_id: str | None = None):
    """Plan position-outer/location-inner tasks; does not start a crawl.

    SỬA: không hard-code 10 vị trí/2 thành phố và không đổi kế hoạch theo enabled.
    Pipeline phải kiểm tra crawl.enabled trước khi thực thi. Mỗi task nhận bản
    sao metadata để thay đổi runtime không làm đổi config_snapshot hoặc task khác.
    """
    positions, locations = _scope(_mapping(cfg, "config"))
    if position_id is not None:
        _text(position_id, "position_id")
        positions = [position for position in positions if position["id"] == position_id]
        if not positions:
            raise ValueError("position_id không có trong scope.positions.")
    if location_id is not None:
        _text(location_id, "location_id")
        locations = [location for location in locations if location["id"] == location_id]
        if not locations:
            raise ValueError("location_id không có trong scope.locations.")
    return [(deepcopy(position), deepcopy(location)) for position, location in product(positions, locations)]


def _freeze(value):
    """SỬA: không cho thay đổi gián tiếp list/dict bên trong runtime config."""
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(child) for key, child in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(child) for child in value)
    return value


def load_runtime_config(
    path: str | Path | None = None, *,
    position_id: str | None = None, location_id: str | None = None,
) -> CrawlerConfig:
    """Đọc -> kiểm tra -> chuyển đổi cấu hình; chưa thực thi crawl.

    SỬA: đây là đầu vào cho topcv.py/crawl_pipeline.py mới. Đường dẫn luôn
    tính từ project root, không phụ thuộc cwd của scheduler hoặc vị trí YAML.
    Không truyền toàn bộ SeleniumConfig bằng **kwargs vào crawler demo cũ:
    constructor đó chưa nhận profile/explicit wait/retry/manual verification.
    """
    if path is None:
        environment_path = os.environ.get('M8_CRAWL_CONFIG')
        path = environment_path if isinstance(environment_path, str) and environment_path.strip() else DEFAULT_CONFIG_PATH
    config_path = resolve_project_path(path)
    snapshot = load_config(config_path)
    runtime = deepcopy(snapshot)

    runtime["crawl"]["checkpoint_directory"] = resolve_project_path(
        snapshot["crawl"]["checkpoint_directory"]
    )
    runtime["logging"]["directory"] = resolve_project_path(snapshot["logging"]["directory"])
    # Marker chuẩn hóa để so khớp text; không chuẩn hóa keyword/địa danh Raw.
    for key in ("challenge_title_markers", "challenge_body_markers", "access_denied_markers"):
        runtime["anti_bot"][key] = [marker.strip().casefold() for marker in snapshot["anti_bot"][key]]

    values = snapshot["selenium"]
    selenium = SeleniumConfig(
        browser=values["browser"], headless=values["headless"],
        browser_instances=values["browser_instances"],
        browser_profile_dir=resolve_project_path(values["browser_profile_dir"]),
        page_load_timeout_seconds=float(values["page_load_timeout_seconds"]),
        explicit_wait_seconds=float(values["explicit_wait_seconds"]),
        delay_between_pages_seconds=float(values["delay_between_pages_seconds"]),
        delay_between_jobs_seconds=float(values["delay_between_jobs_seconds"]),
        max_retries=values["max_retries"],
        retry_backoff_seconds=tuple(float(delay) for delay in values["retry_backoff_seconds"]),
        retry_on=tuple(values["retry_on"]), on_challenge=values["on_challenge"],
        require_target_page_after_verification=values["require_target_page_after_verification"],
        on_verification_timeout=values["on_verification_timeout"],
        on_access_denied=values["on_access_denied"], retry_on_challenge=values["retry_on_challenge"],
        on_unexpected_dialog=values["on_unexpected_dialog"],
        # SỬA: policy stop có thể không khai báo thời gian chờ xác minh thủ công.
        manual_verification_timeout_seconds=(
            float(values["manual_verification_timeout_seconds"])
            if "manual_verification_timeout_seconds" in values else None
        ),
        verification_poll_interval_seconds=(
            float(values["verification_poll_interval_seconds"])
            if "verification_poll_interval_seconds" in values else None
        ),
    )
    planned = tuple(
        SearchTaskConfig(position["id"], position["keyword"], location["id"], location["search_label"])
        for position, location in tasks(snapshot, position_id, location_id)
    )
    return CrawlerConfig(
        config_path=config_path, version=snapshot["version"],
        project=_freeze(runtime["project"]), source=_freeze(runtime["source"]),
        scope=_freeze(runtime["scope"]), crawl=_freeze(runtime["crawl"]),
        selenium=selenium, anti_bot=_freeze(runtime["anti_bot"]),
        extraction=_freeze(runtime["extraction"]), logging=_freeze(runtime["logging"]),
        tasks=planned,
        # Raw loader chỉ nhận Asia/Ho_Chi_Minh hoặc Asia/Bangkok (cùng UTC+7).
        # Dùng fixed offset để Windows không phụ thuộc gói tzdata bên ngoài.
        timezone=datetime_timezone(timedelta(hours=7), name=snapshot["project"]["timezone"]),
        _snapshot=snapshot,
    )
