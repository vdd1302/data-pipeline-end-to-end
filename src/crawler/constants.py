"""Hằng số của hợp đồng crawler; import module không mở browser hoặc database.

SỬA: timeout, delay, retry, vị trí, thành phố và đường dẫn profile vẫn lấy từ
crawl.yaml. File này chỉ chứa giá trị cố định để các module dùng thống nhất.
"""
from dataclasses import fields
from pathlib import Path
from src.raw_storage.contracts import RawJobPayload


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "crawl.yaml"

SOURCE_ID = "topcv"
SOURCE_METHOD = "selenium_ui"
BROWSER_NAME = "chrome"
QUERY_STRATEGY = "cross_product"
TOPCV_HOSTS = frozenset({"topcv.vn", "www.topcv.vn"})
COLLECTION_STAGES = frozenset({"listing", "detail"})

# SỬA: chấp nhận cả hai chính sách đã thống nhất; không ép manual pause thành stop.
CHALLENGE_ACTIONS = frozenset({"stop_and_log", "pause_for_manual_verification"})
STOP_ACTIONS = frozenset({"stop_and_log", "checkpoint_and_stop"})
RETRY_REASONS = frozenset({"navigation_timeout", "transient_navigation_error"})
LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

# Khớp RawIngestion và CHECK trong SQL. Không dùng 'completed' cho run/task.
RUN_TERMINAL_STATUSES = frozenset({"success", "partial", "failed", "blocked", "interrupted"})
TASK_TERMINAL_STATUSES = RUN_TERMINAL_STATUSES | {"skipped"}
PAGE_FETCH_STATUSES = frozenset({"success", "failed", "blocked"})
ERROR_STAGES = frozenset({
    "discovery", "fetch", "parse", "load", "navigate_search", "parse_listing",
    "open_detail", "parse_detail", "persist", "checkpoint", "scheduler",
})

# Hợp đồng dữ liệu hiện tại; không tạo thêm một bản sao của RawJobPayload.
IDENTITY_FIELDS = ("source_job_id", "job_url")
RAW_PAYLOAD_FIELDS = frozenset(field.name for field in fields(RawJobPayload))
REQUIRED_DETAIL_FIELDS = frozenset({
    "job_title_raw", "company_name_raw", "location_raw",
    "job_description_raw", "job_requirements_raw",
})

# SỬA: chỉ nhận metadata mà RawIngestion thực sự sinh/lưu ngoài raw_payload.
GENERATED_FIELDS = frozenset({
    "source_code", "crawl_run_id", "snapshot_date", "scraped_at",
    "content_hash", "fetch_status", "parser_version",
})
