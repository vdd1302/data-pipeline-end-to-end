"""PostgreSQL connections for Raw storage.

Read TOPCV_RAW_DATABASE_URL from the process environment or project-root .env.
Importing this module does not open a connection or change the environment.
Use ``with get_connection() as conn:`` to commit on success, roll back on an
exception, and close the connection. Keep Selenium work outside transactions.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import psycopg
from dotenv import dotenv_values
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = PROJECT_ROOT / ".env"
_DATABASE_URL_ENV = "TOPCV_RAW_DATABASE_URL"
_MAX_TIMEOUT = 2**31 - 1


def _validate_timeout(name: str, value: int, minimum: int) -> None:
    if type(value) is not int or not minimum <= value <= _MAX_TIMEOUT:
        raise ValueError(f"{name} must be an integer in {minimum}..{_MAX_TIMEOUT}")


def get_connection(
    *,
    autocommit: bool = False,
    read_only: bool = False,
    connect_timeout: int = 10,
    statement_timeout_ms: int = 60_000,
    lock_timeout_ms: int = 5_000,
) -> psycopg.Connection[dict[str, Any]]:
    """Return a new caller-owned connection with dictionary rows.

    Environment variables take precedence over the project's .env, including an
    explicitly blank value. Only the Raw URL is used; DW has its own connection.
    ``autocommit=False`` preserves the existing RawRepository transaction API.
    Set ``read_only=True`` for inspection, or ``autocommit=True`` when a writer
    manages explicit per-batch ``conn.transaction()`` boundaries.

    Timeouts use seconds for connect and milliseconds for statements/locks.
    A zero statement/lock timeout explicitly disables that timeout. Session
    timestamps use UTC; compute snapshot_date using the project's Vietnam time.
    No schema creation, retries, or migration execution happen here.
    """
    if type(autocommit) is not bool or type(read_only) is not bool:
        raise ValueError("autocommit and read_only must be bool")
    _validate_timeout("connect_timeout", connect_timeout, 1)
    _validate_timeout("statement_timeout_ms", statement_timeout_ms, 0)
    _validate_timeout("lock_timeout_ms", lock_timeout_ms, 0)

    # SỬA: dùng đường dẫn tuyệt đối để scheduler không phụ thuộc working directory.
    # Không load_dotenv() vào os.environ, tránh ảnh hưởng kết nối DW trong process.
    file_settings = dotenv_values(ENV_FILE)
    dsn = os.environ.get(_DATABASE_URL_ENV, file_settings.get(_DATABASE_URL_ENV))
    if not isinstance(dsn, str) or not dsn.strip():
        raise RuntimeError(
            "Thiếu TOPCV_RAW_DATABASE_URL trong môi trường hoặc file .env của dự án."
        )
    dsn = dsn.strip()
    try:
        connection_settings = conninfo_to_dict(dsn)
    except psycopg.Error:
        # Không nối lỗi parser gốc: thông báo đó có thể chứa URI hoặc mật khẩu.
        raise RuntimeError("TOPCV_RAW_DATABASE_URL không phải chuỗi kết nối PostgreSQL hợp lệ.") from None
    if not connection_settings.get("dbname", "").strip():
        raise RuntimeError("TOPCV_RAW_DATABASE_URL phải chỉ rõ tên database Raw.")

    # Giữ options của URL; các thiết lập được quản lý bên dưới có độ ưu tiên cuối.
    # Tham số timeout đã được kiểm tra số nguyên, không chèn SQL từ chuỗi tùy ý.
    options = " ".join(
        part
        for part in (
            connection_settings.get("options", "").strip(),
            "-c timezone=UTC",
            f"-c statement_timeout={statement_timeout_ms}",
            f"-c lock_timeout={lock_timeout_ms}",
            "-c default_transaction_read_only=" + ("on" if read_only else "off"),
        )
        if part
    )
    try:
        return psycopg.connect(
            dsn,
            row_factory=dict_row,
            autocommit=autocommit,
            connect_timeout=connect_timeout,
            application_name="topcv_raw_storage",
            options=options,
        )
    except psycopg.OperationalError:
        # Giữ loại lỗi để runner phân loại; không đưa thông tin đăng nhập vào log.
        raise psycopg.OperationalError(
            "Không thể kết nối Raw PostgreSQL. Kiểm tra TOPCV_RAW_DATABASE_URL, "
            "dịch vụ PostgreSQL, quyền truy cập và thời gian chờ kết nối."
        ) from None
