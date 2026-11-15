# Vận hành hằng ngày: crawl thủ công, Airflow xử lý dữ liệu

Cập nhật 12/11/2026. Tài liệu này thay thế các mô tả cũ trong `06_Live_E2E_Runbook_VI.md` ở những điểm sau: Raw và Warehouse nằm trong **PostgreSQL trên Windows (cổng 5432)**, còn `market-db` trong Docker chỉ giữ metadata Airflow; crawl do người chạy, Airflow không crawl.

## 1. Kiến trúc thực tế

| Thành phần | Nơi chạy | Ghi chú |
| --- | --- | --- |
| Raw (`topcv_raw_storage`, schema `raw`) | Postgres Windows `:5432` | Role runtime: `topcv_raw_app` sau khi tăng cường bảo mật (mục 3) |
| Warehouse (`topcv_data_warehouse`: `staging`, `market_ops`, `market_dw`) | Postgres Windows `:5432` | Role: `topcv_local_pipeline` |
| Airflow metadata | Docker `market-db` (`127.0.0.1:55440`) | Sao lưu bằng `docker exec` |
| Airflow UI | `http://localhost:8096` | `SimpleAuthManager`, chỉ bind localhost |
| Superset | `http://localhost:8090` | Đọc trực tiếp view `market_dw`, không cache |
| Crawler | Windows, Chrome hiện cửa sổ | Venv ngoài thư mục dự án: `C:\Users\Admin\venvs\topcv312` |

## 2. Quy trình mỗi ngày

1. **Trước 19:00, người crawl** (ngồi trước máy, giữ nhịp ≥ 15 phút giữa các phiên):
   ```powershell
   .\scripts\run_visible_crawl.ps1 -MaxScopes 1 -MaxNavigations 6
   ```
   Host bị khóa (`ACCESS_DENIED`)? Chỉ mở lại khi bạn quyết định thử lại, một lần:
   ```powershell
   C:\Users\Admin\venvs\topcv312\Scripts\python.exe -m scripts.run_market_pipeline review-host --config config/crawl.windows-pilot.yaml --review-reference "operator-manual-YYYY-MM-DD"
   ```
   Challenge: terminal đổ chuông, bạn có tối đa 180 giây để tự xác minh trong Chrome. Chặn cứng ("Sorry, you have been blocked") thì dừng, không có gì để bấm.
2. **19:00 Airflow tự chạy** `topcv_market_pipeline`: tạo batch → bỏ qua crawl → readiness → refresh → quality → evaluate → `health_check`. Dashboard Superset tự cập nhật theo warehouse.
3. **Xem kết quả:**
   ```powershell
   C:\Users\Admin\venvs\topcv312\Scripts\python.exe -m scripts.run_market_pipeline health
   ```
   Đỏ nghĩa là: 30 giờ không có dữ liệu crawl mới (`STALE_DATA`), hoặc warehouse 26 giờ chưa làm mới (`DASHBOARD_DATA_STALE`). Host bị khóa chỉ là cảnh báo vàng.

## 3. Tăng cường bảo mật (làm một lần, trước khi crawl)

Hiện pipeline dùng superuser `postgres` với mật khẩu yếu cho Raw. Script dưới đây tạo role `topcv_raw_app` (chỉ SELECT/INSERT/UPDATE trên schema `raw`, không DDL, không DELETE), đổi mật khẩu role warehouse, kiểm tra bằng cách đăng nhập thật bằng role mới, rồi mới ghi `.env` và `.env.market` (bản gốc được lưu thành `*.pre-hardening-<thời gian>`).

```powershell
# 1. Sao lưu trước
C:\Users\Admin\venvs\topcv312\Scripts\python.exe -m scripts.backup_databases
# 2. Kiểm tra kế hoạch, không thay đổi gì
C:\Users\Admin\venvs\topcv312\Scripts\python.exe -m scripts.harden_database_access --dry-run
# 3. Áp dụng và khởi động lại các service Airflow (hỏi mật khẩu postgres, không lưu)
C:\Users\Admin\venvs\topcv312\Scripts\python.exe -m scripts.harden_database_access --recreate
# 4. Kiểm tra lại
C:\Users\Admin\venvs\topcv312\Scripts\python.exe -m scripts.run_market_pipeline health
```

Sau khi mọi thứ chạy ổn, xóa hai file `*.pre-hardening-*` (chúng chứa mật khẩu cũ). Mật khẩu của chính `postgres` **không** bị đổi vì các DB khác trên máy chủ có thể đang dùng; hãy đổi riêng sau khi cập nhật các ứng dụng đó.

Lưu ý: stack đang chạy có tên project `topcv-market-e2e`, khác tên mặc định trong file compose (`topcv-market`). Lệnh `docker compose` thủ công phải thêm `-p topcv-market-e2e`, nếu không sẽ tác động nhầm vào stack cũ.

Lưu ý sau khi chuyển role: `initialize` và `scripts/complete_fresh_reset.py` cần quyền chủ sở hữu (DDL/DELETE) nên phải chạy bằng URL admin, không phải role runtime.

## 4. Sao lưu và khôi phục

* Sao lưu thủ công: `python -m scripts.backup_databases` (Raw chỉ schema `raw`, Warehouse đầy đủ, metadata Airflow). Mỗi bản được kiểm tra bằng `pg_restore --list`; giữ 7 bản mới nhất trong `.backups\scheduled\`.
* Đăng ký tự động hằng ngày lúc 18:30: `.\scripts\register_backup_task.ps1` (xóa bằng `-Remove`).
* `pg_dump` phải **không cũ hơn** máy chủ (máy chủ là PostgreSQL 18): script tự chọn bản trong `C:\Program Files\PostgreSQL\18\bin`.
* Khôi phục thử (đã kiểm chứng trên Postgres tạm: số dòng khớp 24/451/5 ở Raw, 6/6 ở Warehouse):
  ```powershell
  createdb -h 127.0.0.1 -U postgres topcv_restore_test
  pg_restore -h 127.0.0.1 -U postgres -d topcv_restore_test --no-owner <thư mục sao lưu>\topcv_raw_storage.dump
  ```
  Role là đối tượng toàn cục nên không nằm trong dump: nếu `pg_restore` báo `role "..." does not exist` ở các lệnh `GRANT`, tạo role đó trước hoặc bỏ qua các lỗi GRANT.

## 5. Danh sách kiểm thử end-to-end (ngày thử nghiệm)

- [ ] `health` trước khi bắt đầu; ghi lại `host_state`.
- [ ] (Tùy chọn, nên làm trước) Mục 3: tăng cường bảo mật, rồi `health` lại.
- [ ] `review-host` nếu host đang khóa, rồi `run_visible_crawl.ps1` vài phiên ngắn cách nhau ≥ 15 phút.
- [ ] Ghi lại số `details_committed` và `listings_committed` của từng phiên.
- [ ] 19:00: xem Airflow `topcv_market_pipeline` có run `scheduled` mới; 7 task `success` (trừ `crawl_scopes` = `skipped`).
- [ ] `health` sau 19:00: `healthy: true`, không có `STALE_DATA` hay `DASHBOARD_DATA_STALE`.
- [ ] Mở dashboard Superset, đối chiếu số liệu với `market_dw.collection_overview`.
- [ ] Chạy `python -m scripts.backup_databases` sau khi xong.

## 6. Xử lý sự cố nhanh

| Hiện tượng | Nguyên nhân thường gặp và cách xử lý |
| --- | --- |
| Chrome báo `SessionNotCreatedException`, "Access is denied" | Dùng nhầm `.venv312` trong thư mục dự án (mang nhãn Low Mandatory Level). Dùng venv `C:\Users\Admin\venvs\topcv312`. |
| `crawl` trả `status: cooldown` | Chưa đủ khoảng cách giữa các phiên (`min_session_gap_seconds`). Chờ theo `retry_in_seconds`. |
| `crawl` trả `manual_crawl_only` | Lệnh được gọi từ Airflow; crawl chỉ chạy tay. Chạy từ terminal của bạn. |
| `health` có `HOST_BLOCKED` | TopCV đã chặn. Dừng, chờ rồi mới cân nhắc `review-host`; không thử dồn dập. |
| DAG đỏ `DASHBOARD_DATA_STALE` | Warehouse không được làm mới 26 giờ: kiểm tra Docker, Postgres và `topcv_warehouse_recovery`. |
